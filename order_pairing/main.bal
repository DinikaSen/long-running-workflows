import ballerina/file;
import ballerina/io;
import ballerina/lang.regexp;
import ballerina/lang.runtime;
import ballerina/log;
import ballerina/workflow;
import ballerina/workflow.management;
import ballerina/workflow.management.rest as _;

// Importing workflow.management.rest activates the Workflow Management REST API when
// enableManagementApi = true is set under [ballerina.workflow.management.rest] in
// Config.toml (basic auth via [[ballerina.auth.users]]). Used for operational scripts such
// as bulk replay of parked deliveries.

listener file:Listener inputDirListener = new ({path: inputDir, recursive: false});

// Statuses considered "closed" (no longer accepting join/sendData) as reported by
// workflow.management:getWorkflowInfo. Anything else (RUNNING, SUSPENDED) is still open.
final string[] & readonly closedWorkflowStatuses = ["COMPLETED", "FAILED", "CANCELED", "TERMINATED"];

// order-<id>.xml and lines-<id>.xml for the same order can arrive at (almost) the same
// instant, and file:Listener gives no documented guarantee that onCreate calls for two
// such events are serialized. Without protection both calls would see the business key as
// "not yet registered" and each start its own orderPairing workflow instance. This
// in-memory set (pure data, guarded only by `lock`, no I/O inside the lock) makes "claim
// this business key for correlation" an atomic decision within this process; a concurrent
// arrival for the same key polls until the first one has finished registering it.
isolated map<boolean> businessKeysBeingClaimed = {};

// Guards the registry file's whole read-modify-write sequence so two DIFFERENT business
// keys racing to append a new entry at the same time can't clobber each other's write
// (last-writer-wins on a shared file). Held only for the duration of the file I/O.
isolated boolean registryWriteInProgress = false;

service "orderFileHandler" on inputDirListener {

    remote function onCreate(file:FileEvent fileEvent) returns error? {
        string filePath = fileEvent.name;
        string fileName = check file:basename(filePath);

        FileKind|() fileKind = classifyFileName(fileName);
        if fileKind is () {
            log:printInfo("ignoring unrecognised file", fileName = fileName);
            return;
        }

        check file:createDir(workDir, file:RECURSIVE);
        string workPath = check file:joinPath(workDir, fileName);
        check file:rename(filePath, workPath);

        string businessKey = "order-" + fileKind.orderId;
        log:printInfo("file received", businessKey = businessKey, fileName = fileName, dataName = fileKind.dataName);

        check correlateFile(businessKey, fileKind.orderId, fileKind.dataName, workPath);
    }
}

type FileKind record {|
    string orderId;
    string dataName;
|};

// Recognises order-<id>.xml and lines-<id>.xml, returning the id and which named data
// event the file corresponds to.
isolated function classifyFileName(string fileName) returns FileKind|() {
    string:RegExp orderPattern = re `^order-(.+)\.xml$`;
    string:RegExp linesPattern = re `^lines-(.+)\.xml$`;

    regexp:Groups? orderGroups = orderPattern.findGroups(fileName);
    if orderGroups is regexp:Groups {
        regexp:Span? idSpan = orderGroups[1];
        if idSpan is regexp:Span {
            return {orderId: idSpan.substring(), dataName: "orderFile"};
        }
    }

    regexp:Groups? linesGroups = linesPattern.findGroups(fileName);
    if linesGroups is regexp:Groups {
        regexp:Span? idSpan = linesGroups[1];
        if idSpan is regexp:Span {
            return {orderId: idSpan.substring(), dataName: "linesFile"};
        }
    }

    return ();
}

// Starts or joins the orderPairing workflow instance for this business key, then delivers
// the file path as the named data event. Refuses (parks + emails) if the prior instance
// with this business key has already closed.
//
// The whole "look up, maybe start, remember" sequence is wrapped by a per-business-key
// claim (see businessKeysBeingClaimed above) so that order-<id>.xml and lines-<id>.xml
// arriving at the same instant for the same order can never both see "not yet registered"
// and each start their own workflow instance.
function correlateFile(string businessKey, string orderId, string dataName, string filePath) returns error? {
    check claimBusinessKey(businessKey);
    string workflowId;
    do {
        workflowId = check startOrJoinWorkflow(businessKey, orderId, filePath);
    } on fail error e {
        releaseBusinessKey(businessKey);
        return e;
    }
    releaseBusinessKey(businessKey);

    if workflowId.length() == 0 {
        // Already closed - the file was parked and ops notified inside startOrJoinWorkflow.
        return;
    }

    check workflow:sendData(orderPairing, workflowId, dataName, filePath);
}

// Runs only while this process holds the claim on businessKey (see correlateFile). Returns
// "" if the order had already closed (file has been parked; caller must not sendData).
function startOrJoinWorkflow(string businessKey, string orderId, string filePath) returns string|error {
    string? existingWorkflowId = check lookupRegistry(businessKey);

    if existingWorkflowId is string {
        management:WorkflowExecutionInfo info = check management:getWorkflowInfo(existingWorkflowId);
        if closedWorkflowStatuses.indexOf(info.status) is int {
            log:printInfo("late file, order already closed", businessKey = businessKey, filePath = filePath);
            check moveToDir(filePath, parkedDir);
            check notifyLateFile(filePath, businessKey);
            return "";
        }
        return existingWorkflowId;
    }

    string workflowId = check workflow:run(orderPairing, {businessKey: businessKey, orderId: orderId});
    check saveRegistryEntry({businessKey: businessKey, workflowId: workflowId});
    log:printInfo("started orderPairing workflow", businessKey = businessKey, workflowId = workflowId);
    return workflowId;
}

// =====================================================================================
// Per-business-key claim: makes "check the registry, maybe start a workflow, remember it"
// atomic per key, without holding any lock during the I/O itself.
// =====================================================================================

function claimBusinessKey(string businessKey) returns error? {
    while true {
        boolean acquired = false;
        lock {
            if !businessKeysBeingClaimed.hasKey(businessKey) {
                businessKeysBeingClaimed[businessKey] = true;
                acquired = true;
            }
        }
        if acquired {
            return;
        }
        runtime:sleep(0.1);
    }
}

function releaseBusinessKey(string businessKey) {
    lock {
        _ = businessKeysBeingClaimed.removeIfHasKey(businessKey);
    }
}

// =====================================================================================
// Business-key registry: a small local JSON file (not a database) mapping
// businessKey -> workflowId, used only to route sendData calls to the right instance.
// Reads never need locking (a torn read just means "wait and try again" is not needed
// since JSON files are written atomically below); writes are serialized across ALL
// business keys with acquireRegistryWriteLock/releaseRegistryWriteLock so two different
// orders appending an entry at the same time can't clobber each other's write.
// =====================================================================================

function lookupRegistry(string businessKey) returns string?|error {
    RegistryEntry[] entries = check readRegistry();
    foreach RegistryEntry entry in entries {
        if entry.businessKey == businessKey {
            return entry.workflowId;
        }
    }
    return ();
}

function saveRegistryEntry(RegistryEntry newEntry) returns error? {
    check acquireRegistryWriteLock();
    RegistryEntry[]|error entries = readRegistry();
    if entries is error {
        releaseRegistryWriteLock();
        return entries;
    }
    entries.push(newEntry);
    json entriesJson = entries.toJson();
    string tempPath = check file:createTemp(prefix = "registry-", dir = check file:parentPath(registryPath));
    error? writeResult = io:fileWriteString(tempPath, entriesJson.toJsonString());
    if writeResult is error {
        releaseRegistryWriteLock();
        return writeResult;
    }
    // file:rename on this runtime fails if the destination already exists (it does not
    // replace it the way POSIX rename(2) would), so remove any previous registry file
    // first. The write lock already held by this function keeps this remove-then-rename
    // pair safe from concurrent writers.
    boolean destinationExists = check file:test(registryPath, file:EXISTS);
    if destinationExists {
        error? removeResult = file:remove(registryPath);
        if removeResult is error {
            releaseRegistryWriteLock();
            return removeResult;
        }
    }
    error? renameResult = file:rename(tempPath, registryPath);
    releaseRegistryWriteLock();
    return renameResult;
}

function acquireRegistryWriteLock() returns error? {
    while true {
        boolean acquired = false;
        lock {
            if !registryWriteInProgress {
                registryWriteInProgress = true;
                acquired = true;
            }
        }
        if acquired {
            return;
        }
        runtime:sleep(0.1);
    }
}

function releaseRegistryWriteLock() {
    lock {
        registryWriteInProgress = false;
    }
}

function readRegistry() returns RegistryEntry[]|error {
    boolean exists = check file:test(registryPath, file:EXISTS);
    if !exists {
        return [];
    }
    string content = check io:fileReadString(registryPath);
    if content.trim().length() == 0 {
        return [];
    }
    json registryJson = check content.fromJsonString();
    RegistryEntry[] entries = check registryJson.cloneWithType();
    return entries;
}
