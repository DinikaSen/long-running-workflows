import ballerina/email;
import ballerina/http;
import ballerina/io;

// Plain http:Client (never soap11:Client) used for the SOAP 1.1 partner call. mTLS is
// configured explicitly, the client's own retry is left disabled (engine handles retries),
// and hostname verification is enabled. Reused across every submitLine attempt.
final http:Client partnerClient = check new (partnerServiceUrl,
    timeout = partnerHttpTimeoutSeconds,
    secureSocket = {
        cert: caCertPath,
        key: {
            certFile: clientCertPath,
            keyFile: clientKeyPath
        },
        verifyHostName: true
    }
);

final email:SmtpClient smtpClient = check new (
    host = smtpHost,
    username = smtpUsername,
    password = smtpPassword,
    port = smtpPort,
    security = email:START_TLS_AUTO
);

// Department code -> cost centre lookup, loaded once at startup from a local JSON file.
final map<string> deptCodeToCostCentre = check loadDeptMap();

isolated function loadDeptMap() returns map<string>|error {
    string content = check io:fileReadString(deptMapPath);
    json deptMapJson = check content.fromJsonString();
    map<string> deptMap = check deptMapJson.cloneWithType();
    return deptMap;
}
