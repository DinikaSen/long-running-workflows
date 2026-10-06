// Maps a single input LineItem to a CanonicalLine (same shape, direct field mapping).
function mapToCanonicalLine(LineItem lineItem) returns CanonicalLine => {
    lineNo: lineItem.lineNo,
    sku: lineItem.sku,
    qty: lineItem.qty,
    amount: lineItem.amount,
    flag: lineItem.flag
};

// Merges the parsed order and lines files into a CanonicalOrder, resolving costCentre
// from deptCode using the dept map loaded at startup.
function mapToCanonicalOrder(OrderFile orderFile, LinesFile linesFile, string costCentre) returns CanonicalOrder => {
    orderId: orderFile.orderId,
    customer: orderFile.customer,
    deptCode: orderFile.deptCode,
    costCentre: costCentre,
    lines: from LineItem lineItem in linesFile.line
        select mapToCanonicalLine(lineItem)
};

// Filters out lines where qty is 0 or flag is "SKIP".
isolated function filterDeliverableLines(CanonicalLine[] lines) returns CanonicalLine[] {
    return from CanonicalLine line in lines
        where line.qty != 0
        where line.flag != "SKIP"
        select line;
}
