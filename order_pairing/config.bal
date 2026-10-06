// ===================== File system paths =====================

// Directory watched for incoming order-<id>.xml / lines-<id>.xml files.
configurable string inputDir = ?;

// Files are moved here immediately on arrival, before correlation.
configurable string workDir = ?;

// Late files (arriving after the order has already closed) and timed-out / rejected
// deliveries are moved here.
configurable string parkedDir = ?;

// Orders/lines that fail validation (unknown deptCode, invalid line) are written here.
configurable string errorDir = ?;

// Successful line delivery confirmations are written here.
configurable string outDir = ?;

// Local JSON file mapping deptCode -> costCentre, loaded once at startup.
configurable string deptMapPath = ?;

// Local JSON file used as the business-key -> workflow instance registry.
// This is plain local file bookkeeping (not a database) required because the published
// ballerina/workflow API (workflow:run) does not yet support starting a workflow under a
// caller supplied instance id (see README for details).
configurable string registryPath = ?;

// ===================== Correlation / timeout =====================

// How long the orderPairing workflow waits for both the order and lines files.
configurable decimal pairingTimeoutSeconds = 120;

// ===================== submitLine retry policy =====================

configurable int submitLineMaxRetries = 3;
configurable decimal submitLineInitialDelaySeconds = 20;
configurable decimal submitLineBackoffCoefficient = 2.0;

// ===================== Partner SOAP endpoint =====================

configurable string partnerServiceUrl = "https://localhost:9443/OrderLineService";
configurable decimal partnerHttpTimeoutSeconds = 60;

// mTLS: client certificate/key presented to the partner, and the CA used to verify the
// partner's server certificate. Same CA signs both certificates in this demo.
configurable string clientCertPath = "../certs/client/client.crt";
configurable string clientKeyPath = "../certs/client/client.key";
configurable string caCertPath = "../certs/ca/ca.crt";

// ===================== Email (Mailtrap SMTP) =====================

configurable string smtpHost = ?;
configurable int smtpPort = ?;
configurable string smtpUsername = ?;
configurable string smtpPassword = ?;
configurable string emailFromAddress = ?;
configurable string opsEmailAddress = ?;

