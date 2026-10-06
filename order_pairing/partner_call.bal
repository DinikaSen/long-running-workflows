import ballerina/data.xmldata;
import ballerina/http;
import ballerina/log;

// SOAP fault parsed out of a SOAP 1.1 Fault body.
type SoapFault record {
    string faultcode;
    string faultstring;
};

// SubmitOrderLineResponse parsed out of a successful SOAP 1.1 response body.
type SubmitOrderLineResponse record {
    string confirmationId;
};

// Builds and sends the SOAP 1.1 request for a single line submission, then classifies the
// response. Uses a plain http:Client (never soap11:Client) so header values stay unquoted
// and the HTTP status is directly observable. The http:Client's own retry is disabled
// (connections.bal) - all retrying is done by the workflow engine's retry policy.
function callPartnerSubmitLine(string parentWorkflowId, string orderId, string costCentre, CanonicalLine line,
        string idempotencyKey) returns SubmitLineResult|error {
    string envelope = buildSoapEnvelope(orderId, line, costCentre);
    map<string|string[]> headers = {
        "Content-Type": "text/xml; charset=utf-8",
        "SOAPAction": "\"SubmitOrderLine\"",
        "X-Idempotency-Key": idempotencyKey,
        "X-Correlation-ID": parentWorkflowId
    };

    http:Response|http:ClientError response = partnerClient->post("", envelope, headers);

    if response is http:ClientError {
        log:printInfo("submit attempt", key = idempotencyKey, status = "transport-error",
                classification = "RETRY", detail = response.message());
        return response;
    }

    int statusCode = response.statusCode;

    if statusCode == http:STATUS_OK {
        string responseBody = check response.getTextPayload();
        xml responseXml = check xml:fromString(responseBody);
        xml responseElements = responseXml/**/<SubmitOrderLineResponse>;
        SubmitOrderLineResponse submitResponse = check xmldata:parseAsType(responseElements);
        string confirmationId = submitResponse.confirmationId;

        string? duplicateHeader = getHeaderOrNil(response, "X-Duplicate");
        if duplicateHeader is string && duplicateHeader == "true" {
            log:printInfo("partner deduplicated key", key = idempotencyKey);
        }

        log:printInfo("submit attempt", key = idempotencyKey, status = statusCode, classification = "ACCEPTED");
        return {outcome: "ACCEPTED", confirmationId: confirmationId};
    }

    if statusCode == http:STATUS_INTERNAL_SERVER_ERROR {
        string responseBody = check response.getTextPayload();
        xml responseXml = check xml:fromString(responseBody);
        xml faultElements = responseXml/**/<Fault>;
        SoapFault soapFault = check xmldata:parseAsType(faultElements);

        if soapFault.faultcode == "Client" {
            log:printInfo("submit attempt", key = idempotencyKey, status = statusCode, classification = "REJECTED",
                    reason = soapFault.faultstring);
            return {outcome: "REJECTED", reason: soapFault.faultstring};
        }

        log:printInfo("submit attempt", key = idempotencyKey, status = statusCode, classification = "RETRY",
                reason = soapFault.faultstring);
        return error(string `partner server fault: ${soapFault.faultstring}`);
    }

    log:printInfo("submit attempt", key = idempotencyKey, status = statusCode, classification = "RETRY");
    return error(string `unexpected HTTP status from partner: ${statusCode}`);
}

function buildSoapEnvelope(string orderId, CanonicalLine line, string costCentre) returns string {
    return string `<?xml version="1.0" encoding="utf-8"?>` +
        string `<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">` +
        string `<soap:Body><SubmitOrderLineRequest>` +
        string `<orderId>${orderId}</orderId>` +
        string `<lineNo>${line.lineNo}</lineNo>` +
        string `<sku>${line.sku}</sku>` +
        string `<qty>${line.qty}</qty>` +
        string `<amount>${line.amount}</amount>` +
        string `<costCentre>${costCentre}</costCentre>` +
        string `</SubmitOrderLineRequest></soap:Body></soap:Envelope>`;
}

isolated function getHeaderOrNil(http:Response response, string headerName) returns string? {
    string|http:HeaderNotFoundError headerValue = response.getHeader(headerName);
    if headerValue is string {
        return headerValue;
    }
    return ();
}
