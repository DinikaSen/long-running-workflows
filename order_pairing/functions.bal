import ballerina/data.xmldata;
import ballerina/email;
import ballerina/file;
import ballerina/io;
import ballerina/log;
import ballerina/workflow;

// Named type aliases required wherever a tuple/union type is passed as an explicit `T =`
// argument to callActivity/await/awaitHumanTask (an inline tuple type literal in that
// position is ambiguous with a list-constructor expression).
type OneFileArrival [string?];
type TwoFileArrival [string?, string?];

// =====================================================================================
// STEP 3: deliverLine CHILD WORKFLOW (defined before orderPairing, which starts it as a
// child workflow)
// =====================================================================================

@workflow:Workflow
function deliverLine(workflow:Context ctx, DeliverLineInput input) returns DeliverLineResult|error {
    string workflowId = check ctx.getWorkflowId();
    string idempotencyKey = input.orderId + "-" + input.line.lineNo.toString();

    LineValidation validation = check ctx->callActivity(validateLine, {"line": input.line}, T = LineValidation);
    if !validation.valid {
        string reason = validation.reason ?: "unknown validation failure";
        () _ = check ctx->callActivity(writeInvalidLine, {
            "orderId": input.orderId,
            "line": input.line,
            "reason": reason
        });
        log:printInfo("line validated", workflowId = workflowId, lineNo = input.line.lineNo, result = "INVALID");
        return {status: "INVALID", lineNo: input.line.lineNo, reason: reason};
    }
    log:printInfo("line validated", workflowId = workflowId, lineNo = input.line.lineNo, result = "VALID");

    CanonicalLine currentLine = input.line;
    while true {
        SubmitLineResult|error submitResult = ctx->callActivity(submitLine, {
            "parentWorkflowId": input.parentWorkflowId,
            "orderId": input.orderId,
            "costCentre": input.costCentre,
            "line": currentLine,
            "idempotencyKey": idempotencyKey
        }, T = SubmitLineResult, retryPolicy = <workflow:AutoRetry>{
            maxRetries: submitLineMaxRetries,
            retryDelay: submitLineInitialDelaySeconds,
            retryBackoff: submitLineBackoffCoefficient
        });

        if submitResult is SubmitLineResult && submitResult.outcome == "ACCEPTED" {
            string confirmationId = submitResult.confirmationId ?: "";
            () _ = check ctx->callActivity(writeConfirmation, {
                "orderId": input.orderId,
                "lineNo": currentLine.lineNo,
                "confirmationId": confirmationId
            });
            log:printInfo("delivered", workflowId = workflowId, lineNo = currentLine.lineNo,
                    confirmationId = confirmationId);
            return {status: "DELIVERED", lineNo: currentLine.lineNo, confirmationId: confirmationId};
        }

        string rejectReason = submitResult is SubmitLineResult ? (submitResult.reason ?: "rejected by partner")
            : submitResult.message();
        () _ = check ctx->callActivity(notifyOpsRejected, {
            "orderId": input.orderId,
            "lineNo": currentLine.lineNo,
            "idempotencyKey": idempotencyKey,
            "reason": rejectReason
        });
        log:printInfo("parked", workflowId = workflowId, lineNo = currentLine.lineNo, reason = rejectReason);

        ParkedDeliveryDecision|workflow:HumanTaskError decision = ctx->awaitHumanTask("parkedDelivery", {
            "idempotencyKey": idempotencyKey,
            "reason": rejectReason,
            "line": currentLine.toJson()
        }, T = ParkedDeliveryDecision, userRoles = "Ops", title = "Parked delivery: " + idempotencyKey);

        if decision is workflow:HumanTaskError {
            return {status: "ABANDONED", lineNo: currentLine.lineNo, reason: "human task failed or timed out"};
        }

        if decision.decision == "ABANDON" {
            return {status: "ABANDONED", lineNo: currentLine.lineNo, reason: rejectReason};
        }

        if decision.decision == "RETRY_WITH_FIX" {
            CanonicalLine? correctedLine = decision.correctedLine;
            if correctedLine is CanonicalLine {
                LineValidation revalidation = check ctx->callActivity(validateLine, {"line": correctedLine},
                        T = LineValidation);
                if !revalidation.valid {
                    string revalidationReason = revalidation.reason ?: "unknown validation failure";
                    () _ = check ctx->callActivity(writeInvalidLine, {
                        "orderId": input.orderId,
                        "line": correctedLine,
                        "reason": revalidationReason
                    });
                    return {status: "INVALID", lineNo: correctedLine.lineNo, reason: revalidationReason};
                }
                currentLine = correctedLine;
            }
        }
        // RETRY (and RETRY_WITH_FIX after successful revalidation) loop back to submitLine.
    }
}

// =====================================================================================
// STEP 2: orderPairing WORKFLOW
// =====================================================================================

@workflow:Workflow
function orderPairing(
        workflow:Context ctx,
        OrderPairingInput input,
        record {|
            future<string> orderFile;
            future<string> linesFile;
        |} events
) returns OrderPairingResult|error {
    string workflowId = check ctx.getWorkflowId();

    // Wait for at least one of the two files within the full timeout. If neither
    // arrives, both are missing and we go straight to TIMED_OUT.
    TwoFileArrival firstArrival = check ctx->await([events.orderFile, events.linesFile], 1,
            timeout = {seconds: pairingTimeoutSeconds}, T = TwoFileArrival);
    string? orderFilePath = firstArrival[0];
    string? linesFilePath = firstArrival[1];

    if orderFilePath is () || linesFilePath is () {
        // Exactly one file arrived (or the await above would already have errored).
        // Wait for the remaining one using the time left in the budget.
        decimal remainingSeconds = pairingTimeoutSeconds / 2;
        if orderFilePath is () {
            OneFileArrival secondArrival = check ctx->await([events.orderFile], 1,
                    timeout = {seconds: remainingSeconds}, T = OneFileArrival);
            orderFilePath = secondArrival[0];
        } else {
            OneFileArrival secondArrival = check ctx->await([events.linesFile], 1,
                    timeout = {seconds: remainingSeconds}, T = OneFileArrival);
            linesFilePath = secondArrival[0];
        }
    }

    if orderFilePath is () || linesFilePath is () {
        () _ = check ctx->callActivity(handleTimeout, {
            "workflowId": workflowId,
            "orderFilePath": orderFilePath,
            "linesFilePath": linesFilePath
        });
        return {status: "TIMED_OUT", orderId: input.orderId, delivered: 0, invalid: 0, parked: 0, abandoned: 0};
    }

    log:printInfo("pair complete", workflowId = workflowId, orderId = input.orderId);

    MergeResult mergeResult = check ctx->callActivity(mergeAndEnrich, {
        "workflowId": workflowId,
        "orderFilePath": orderFilePath,
        "linesFilePath": linesFilePath
    }, T = MergeResult);

    if mergeResult is InvalidOrderResult {
        return {
            status: "INVALID",
            orderId: input.orderId,
            delivered: 0,
            invalid: 0,
            parked: 0,
            abandoned: 0,
            reason: mergeResult.reason
        };
    }

    CanonicalOrder canonicalOrder = mergeResult.canonicalOrder;
    CanonicalLine[] deliverableLines = filterDeliverableLines(canonicalOrder.lines);

    string[] childWorkflowIds = [];
    foreach CanonicalLine deliverableLine in deliverableLines {
        string childId = check ctx->runChildWorkflow(deliverLine, input = {
            parentWorkflowId: workflowId,
            orderId: canonicalOrder.orderId,
            customer: canonicalOrder.customer,
            deptCode: canonicalOrder.deptCode,
            costCentre: canonicalOrder.costCentre,
            line: deliverableLine
        });
        childWorkflowIds.push(childId);
    }

    int delivered = 0;
    int invalid = 0;
    int abandoned = 0;
    foreach string childId in childWorkflowIds {
        DeliverLineResult childResult = check ctx->waitForChildWorkflow(childId, T = DeliverLineResult);
        if childResult.status == "DELIVERED" {
            delivered += 1;
        } else if childResult.status == "INVALID" {
            invalid += 1;
        } else {
            abandoned += 1;
        }
    }

    return {
        status: "DELIVERED_SUMMARY",
        orderId: canonicalOrder.orderId,
        delivered: delivered,
        invalid: invalid,
        parked: 0,
        abandoned: abandoned
    };
}

// =====================================================================================
// Activities: correlation / timeout handling
// =====================================================================================

type MergeResult MergedOrderResult|InvalidOrderResult;

type MergedOrderResult record {|
    CanonicalOrder canonicalOrder;
|};

type InvalidOrderResult record {|
    string reason;
|};

@workflow:Activity
function handleTimeout(string workflowId, string? orderFilePath, string? linesFilePath) returns error? {
    check file:createDir(parkedDir, file:RECURSIVE);
    if orderFilePath is string {
        check moveToDir(orderFilePath, parkedDir);
    }
    if linesFilePath is string {
        check moveToDir(linesFilePath, parkedDir);
    }
    check sendOpsEmail("Order pairing timed out",
            string `Workflow ${workflowId} timed out waiting for both files. ` +
            string `orderFile=${orderFilePath ?: "missing"}, linesFile=${linesFilePath ?: "missing"}`);
    log:printInfo("timed out", workflowId = workflowId);
}

@workflow:Activity
function mergeAndEnrich(string workflowId, string orderFilePath, string linesFilePath) returns MergeResult|error {
    string orderXmlContent = check io:fileReadString(orderFilePath);
    xml orderXml = check xml:fromString(orderXmlContent);
    OrderFile orderFile = check xmldata:parseAsType(orderXml);

    string linesXmlContent = check io:fileReadString(linesFilePath);
    xml linesXml = check xml:fromString(linesXmlContent);
    LinesFile linesFile = check xmldata:parseAsType(linesXml);

    string? costCentre = deptCodeToCostCentre[orderFile.deptCode];
    if costCentre is () {
        string reason = "unknown deptCode: " + orderFile.deptCode;
        check writeOrderError(orderFile.orderId, reason);
        log:printInfo("order invalid", workflowId = workflowId, orderId = orderFile.orderId, reason = reason);
        return {reason: reason};
    }

    CanonicalOrder canonicalOrder = mapToCanonicalOrder(orderFile, linesFile, costCentre);
    return {canonicalOrder: canonicalOrder};
}

// =====================================================================================
// Activities: line validation and error handling
// =====================================================================================

@workflow:Activity
isolated function validateLine(CanonicalLine line) returns LineValidation {
    if line.sku.trim().length() == 0 {
        return {valid: false, reason: "sku must not be empty"};
    }
    if line.qty <= 0 {
        return {valid: false, reason: "qty must be greater than 0"};
    }
    if line.amount <= 0d {
        return {valid: false, reason: "amount must be greater than 0"};
    }
    return {valid: true};
}

@workflow:Activity
function writeInvalidLine(string orderId, CanonicalLine line, string reason) returns error? {
    check file:createDir(errorDir, file:RECURSIVE);
    xml lineErrorXml = buildLineErrorXml(orderId, line, reason);
    string targetPath = check file:joinPath(errorDir, string `line-${orderId}-${line.lineNo}.xml`);
    check writeXmlAtomically(lineErrorXml, targetPath, errorDir);
}

function writeOrderError(string orderId, string reason) returns error? {
    check file:createDir(errorDir, file:RECURSIVE);
    xml orderErrorXml = xml `<orderError><orderId>${orderId}</orderId><reason>${reason}</reason></orderError>`;
    string targetPath = check file:joinPath(errorDir, string `order-${orderId}.xml`);
    check writeXmlAtomically(orderErrorXml, targetPath, errorDir);
}

// =====================================================================================
// Activity: submitLine (partner call) - see connections.bal / partner_call.bal
// =====================================================================================

@workflow:Activity
function submitLine(string parentWorkflowId, string orderId, string costCentre, CanonicalLine line,
        string idempotencyKey) returns SubmitLineResult|error {
    return callPartnerSubmitLine(parentWorkflowId, orderId, costCentre, line, idempotencyKey);
}

@workflow:Activity
function writeConfirmation(string orderId, int lineNo, string confirmationId) returns error? {
    check file:createDir(outDir, file:RECURSIVE);
    xml confirmationXml =
        xml `<confirmation><orderId>${orderId}</orderId><lineNo>${lineNo}</lineNo><confirmationId>${confirmationId}</confirmationId></confirmation>`;
    string targetPath = check file:joinPath(outDir, string `line-${orderId}-${lineNo}.xml`);
    check writeXmlAtomically(confirmationXml, targetPath, outDir);
}

// =====================================================================================
// Activities: email notifications
// =====================================================================================

@workflow:Activity
function notifyOpsRejected(string orderId, int lineNo, string idempotencyKey, string reason) returns error? {
    check sendOpsEmail("Line delivery parked: " + idempotencyKey,
            string `Order ${orderId}, line ${lineNo} was rejected or exhausted retries.` +
            string ` Idempotency key: ${idempotencyKey}. Reason: ${reason}`);
}

// Not a @workflow:Activity: this runs from the plain file-listener code in main.bal,
// outside any workflow context, so it is just a normal function.
function notifyLateFile(string filePath, string businessKey) returns error? {
    check sendOpsEmail("Late file parked: " + businessKey,
            string `File ${filePath} arrived after order ${businessKey} had already closed and was parked.`);
}

// =====================================================================================
// Helper (non-activity) functions shared by activities
// =====================================================================================

function moveToDir(string sourcePath, string targetDir) returns error? {
    check file:createDir(targetDir, file:RECURSIVE);
    string fileNameSegment = check file:basename(sourcePath);
    string targetPath = check file:joinPath(targetDir, fileNameSegment);
    check replaceWithRename(sourcePath, targetPath);
}

function writeXmlAtomically(xml content, string targetPath, string targetDir) returns error? {
    string tempPath = check file:createTemp(prefix = "tmp-", dir = targetDir);
    check io:fileWriteString(tempPath, content.toString());
    check replaceWithRename(tempPath, targetPath);
}

// file:rename on this runtime fails if targetPath already exists (unlike POSIX rename(2),
// it does not replace it), so remove any previous file at targetPath first. This is what
// lets writeConfirmation overwrite an existing confirmation on a submitLine retry, and
// lets moveToDir re-park/re-stage a same-named file, without either ever creating a
// duplicate or silently failing.
function replaceWithRename(string sourcePath, string targetPath) returns error? {
    boolean targetExists = check file:test(targetPath, file:EXISTS);
    if targetExists {
        check file:remove(targetPath);
    }
    check file:rename(sourcePath, targetPath);
}

function buildLineErrorXml(string orderId, CanonicalLine line, string reason) returns xml {
    string flagValue = line.flag ?: "";
    xml lineErrorXml =
        xml `<lineError><orderId>${orderId}</orderId><lineNo>${line.lineNo}</lineNo><sku>${line.sku}</sku><qty>${line.qty}</qty><amount>${line.amount}</amount><flag>${flagValue}</flag><reason>${reason}</reason></lineError>`;
    return lineErrorXml;
}

function sendOpsEmail(string subject, string body) returns error? {
    email:Error? sendResult = smtpClient->sendMessage({
        to: [opsEmailAddress],
        subject: subject,
        'from: emailFromAddress,
        body: body
    });
    if sendResult is email:Error {
        return sendResult;
    }
}
