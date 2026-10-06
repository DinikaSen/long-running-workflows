// ===================== Input file records (order-<id>.xml, lines-<id>.xml) =====================

// Bound from order-<id>.xml. Open record: external file, exact shape not owned by us.
public type OrderFile record {
    string orderId;
    string customer;
    string deptCode;
};

// A single <line> element inside lines-<id>.xml. Open record: external file.
public type LineItem record {
    int lineNo;
    string sku;
    int qty;
    decimal amount;
    string flag?;
};

// Bound from lines-<id>.xml (root element wrapping repeated <line> elements).
public type LinesFile record {
    LineItem[] line;
};

// ===================== Canonical domain model =====================

// Canonical line after dept-code resolution, used inside CanonicalOrder.
public type CanonicalLine record {|
    int lineNo;
    string sku;
    int qty;
    decimal amount;
    string flag?;
|};

// The enriched, merged representation of an order + its lines, produced by mergeAndEnrich.
public type CanonicalOrder record {|
    string orderId;
    string customer;
    string deptCode;
    string costCentre;
    CanonicalLine[] lines;
|};

// ===================== Workflow inputs =====================

// Input to the top level "orderPairing" workflow.
public type OrderPairingInput record {|
    string businessKey;
    string orderId;
|};

// Input to the "deliverLine" child workflow.
public type DeliverLineInput record {|
    string parentWorkflowId;
    string orderId;
    string customer;
    string deptCode;
    string costCentre;
    CanonicalLine line;
|};

// ===================== Workflow results =====================

public type OrderPairingStatus "DELIVERED_SUMMARY"|"INVALID"|"TIMED_OUT";

public type OrderPairingResult record {|
    OrderPairingStatus status;
    string orderId;
    int delivered;
    int invalid;
    int parked;
    int abandoned;
    string reason?;
|};

public type DeliverLineStatus "DELIVERED"|"INVALID"|"ABANDONED";

public type DeliverLineResult record {|
    DeliverLineStatus status;
    int lineNo;
    string reason?;
    string confirmationId?;
|};

// ===================== Validation / partner call result values =====================

public type LineValidation record {|
    boolean valid;
    string reason?;
|};

public type SubmitOutcome "ACCEPTED"|"REJECTED";

public type SubmitLineResult record {|
    SubmitOutcome outcome;
    string reason?;
    string confirmationId?;
|};

// ===================== Human task (parkedDelivery) =====================

public type ParkedDeliveryDecisionType "RETRY"|"RETRY_WITH_FIX"|"ABANDON";

public type ParkedDeliveryDecision record {|
    ParkedDeliveryDecisionType decision;
    CanonicalLine correctedLine?;
|};

// ===================== Business-key registry (local JSON file, not a database) =====================

// Maps a business key (e.g. "order-1001") to the workflow instance id returned by
// workflow:run. Whether that instance is still open is checked live via
// workflow.management:getWorkflowInfo - this file only needs to remember the routing.
public type RegistryEntry record {|
    string businessKey;
    string workflowId;
|};

