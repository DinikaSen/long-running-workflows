# Order Pairing (Durable Workflows demo)

A local product demo that correlates an `order-<id>.xml` file with its matching
`lines-<id>.xml` file, enriches the order with a cost centre, and delivers each line to a
partner service via a durable Temporal-backed Ballerina workflow (`ballerina/workflow`).
No database is used anywhere; all state is either inside the durable workflow engine or in
plain local files.

## One important deviation from the literal spec (please read)

The spec asks to "start the durable workflow using the business key `order-<id>` as the
workflow instance ID: join if running, refuse if already closed." The published
`ballerina/workflow` module (version `0.10.0`, the version compatible with distribution
`2201.13.6`) does **not** yet support starting a workflow under a caller-chosen instance ID
— `workflow:run` always returns an engine-generated ID. A newer function,
`workflow:runWithId(fn, instanceId, input, ifRunning, ifClosed)`, exists on the module's
GitHub `main` branch with exactly the join/refuse-if-closed semantics you described, but it
has not been published to Ballerina Central yet.

To meet the requirement today, `main.bal` keeps a tiny local JSON file
(`registryPath`, e.g. `data/registry.json` — plain file bookkeeping, not a database) mapping
`businessKey -> workflowId`. On each file arrival it looks up the business key; if unknown,
it calls `workflow:run` and remembers the new ID; if known, it asks
`workflow.management:getWorkflowInfo(workflowId)` for the *live* status and either joins
(status still open) or refuses and parks the file (status closed). This gives the same
externally-observed behaviour as `runWithId(ifRunning=USE_EXISTING, ifClosed=REJECT_DUPLICATE)`
would.

**When `workflow:runWithId` is published**, this registry file and `correlateFile`'s
lookup/join logic in `main.bal` can be deleted and replaced with a single call:
```ballerina
string|workflow:InstanceAlreadyExistsError workflowId = workflow:runWithId(
    orderPairing, businessKey, {businessKey, orderId},
    ifRunning = workflow:USE_EXISTING, ifClosed = workflow:REJECT_DUPLICATE);
```
I'd recommend switching to it as soon as it ships — it removes a moving part.

## Other things worth knowing (call these out if you'd like changes)

- **The partner service is external, not mocked here.** `partnerServiceUrl` points at the
  `order_line_soap_mock` service already implemented at
  `/Users/dinika/WSO2/PoC/Region-Dalarna/integrations/supporting/order_line_soap_mock`. Run
  that as its own process (see its own README for how to switch its scenario/behaviour, e.g.
  the 503 and SLOW cases) and point `partnerServiceUrl`/`caCertPath` at it. `mock_partner.bal`
  in this package is deliberately empty.
- **XSD and test-data files**: my file-writing tool only accepts a fixed set of extensions
  and does not allow `.xsd` or `.xml`. Every XSD and test-data file is written as a real file
  at its exact intended path with a `.txt` suffix appended (e.g.
  `resources/xsd/order.xsd.txt`, `testdata/scenario1-happy-path/order-1001.xml.txt`) — strip
  the trailing `.txt` (`mv order.xsd.txt order.xsd`) to get the real file. `dept-map.json`
  needed no workaround since `.json` is a plain data file. Happy to adjust the naming
  convention if you'd prefer something else.
- **Cost centre lookup** uses `deptMapPath` (JSON, loaded once into an in-memory `map<string>`
  at module-init time), exactly as asked. A ready `data/dept-map.json` is included.
- The two-file wait in `orderPairing` is implemented as "wait for the first of the two
  (bounded by the full timeout), then wait for the second (bounded by the remaining time)"
  rather than a single call, because `ctx->await`'s documented behaviour returns a plain
  `error` (with no indication of which future, if any, completed) when the full set isn't
  met by the deadline — and the spec needs to know *which* file (if either) arrived so it can
  be moved to `parkedDir`. Both waits use the module's documented `Context.await` primitive;
  no manual polling or sleeping is involved.
- **Concurrent file arrivals are handled explicitly.** `ballerina/file`'s directory listener
  gives no documented guarantee that `onCreate` calls for two files are serialized, so
  `order-1001.xml` and `lines-1001.xml` (or two different orders) landing at the same instant
  are a real race, not a hypothetical one. `main.bal` closes two gaps: (1) a per-business-key
  in-memory claim (`businessKeysBeingClaimed`, guarded by a pure-data `lock` with no I/O
  inside it) makes "check the registry, maybe start the workflow, remember it" atomic per
  order, so the same order's two files can never both start their own workflow instance; (2)
  the registry file's read-modify-write is serialized across all keys
  (`registryWriteInProgress`) and written via temp-file-then-`file:rename`, so two different
  orders appending an entry at the same time can't lose one write, and a crash mid-write
  never leaves a half-written registry file on disk.

## Setup

### 0. Install and start Temporal

`ballerina/workflow` is a client to a real Temporal server, not an embedded engine, so
Temporal must be installed and running before this package starts:

```bash
brew install temporal        # or see https://docs.temporal.io/cli#install for other platforms
temporal server start-dev --db-filename temporal.db
```
`--db-filename temporal.db` is the local database file the spec asks for: without it,
`start-dev` keeps everything in memory and wipes all workflow history on every restart,
which would defeat both restart tests. With it, workflow/activity state is persisted to
`temporal.db` on disk, so killing and restarting either the Temporal dev server or the
Ballerina worker preserves in-flight workflows.

### 1. Start the partner service

The `order_line_soap_mock` service (already implemented separately at
`/Users/dinika/WSO2/PoC/Region-Dalarna/integrations/supporting/order_line_soap_mock`) must be
running before you trigger any `submitLine` activity. Start it per its own instructions and
note the port/certificates it uses so `partnerServiceUrl`/`caCertPath` below can point at it.

### 2. Certificates

The client certificate/key this package presents for mutual TLS
(`clientCertPath`/`clientKeyPath`), and the CA certificate used to verify the partner's
server certificate (`caCertPath`), must be issued by whatever CA the `order_line_soap_mock`
service trusts — see that service's own setup instructions for how its certificates are
generated and where its CA lives. Point this package's Config.toml paths at the matching
client cert/key and CA cert.

### 3. Config.toml

Everything environment-specific is a configurable — nothing is hard-coded. Two groups of
keys exist:

**a) This package's own configurables** — set these via the assistant's configuration
collection step before running (it will prompt you), or hand-write `Config.toml`:

```toml
[dinika.order_pairing]
inputDir = "./data/input"
workDir = "./data/work"
parkedDir = "./data/parked"
errorDir = "./data/error"
outDir = "./data/out"
deptMapPath = "./data/dept-map.json"
registryPath = "./data/registry.json"

pairingTimeoutSeconds = 120

submitLineMaxRetries = 3
submitLineInitialDelaySeconds = 20
submitLineBackoffCoefficient = 2.0

partnerServiceUrl = "https://localhost:9443/OrderLineService"
partnerHttpTimeoutSeconds = 60
clientCertPath = "../certs/client/client.crt"
clientKeyPath = "../certs/client/client.key"
caCertPath = "../certs/ca/ca.crt"

smtpHost = "sandbox.smtp.mailtrap.io"
smtpPort = 2525
smtpUsername = "<mailtrap-username>"
smtpPassword = "<mailtrap-password>"
emailFromAddress = "order-pairing@example.com"
opsEmailAddress = "ops@example.com"
```

**b) The workflow engine's own configuration** — these belong to `ballerina/workflow` and
`ballerina/workflow.management.rest`, not to this package, so they use their own Config.toml
tables:

```toml
[ballerina.workflow]
mode = "LOCAL"
url = "localhost:7233"
namespace = "default"
taskQueue = "ORDER_PAIRING_TASK_QUEUE"

[ballerina.workflow.management.rest]
enableManagementApi = true
port = 8234
enableBasicAuth = true

[[ballerina.auth.users]]
username = "admin"
password = "<choose-a-password>"
scopes = ["workflow:manage", "humantask:manage"]
```

`Config.toml` is git-ignored; commit only a `Config.toml.sample` with placeholders if you
want to share the shape without the secrets.

### 4. Registering with the local Integration Control Plane

Point your local WSO2 Integration Control Plane agent at this project directory and the
running Ballerina process the same way you register any other local integration; no
additional code is needed for this — it observes the standard Ballerina
observability/metrics output (see below), it does not require workflow-specific wiring.

### 5. Observability

Build/run with observability enabled so each run traces end to end:
```bash
bal run --observability-included
```
with, in `Config.toml`:
```toml
[ballerina.observe]
metricsEnabled = true
tracingEnabled = true
```
`ballerina/workflow.observe` (imported transitively) then emits workflow-specific spans
(start, sendData, human task decisions) alongside the standard Ballerina traces/metrics.

## Running

```bash
bal run
```
This starts, in one process, the `inputDir` file listener and (because
`enableManagementApi = true`) the Workflow Management REST API on the port configured under
`[ballerina.workflow.management.rest]`. Make sure Temporal and the `order_line_soap_mock`
partner service (see Setup steps 0-1) are already running first. Drop files into `inputDir`
and watch the logs — every key event is logged with the workflow ID as correlation ID (file
received, pair complete, line validated, submit attempt with classification, partner
deduplicated, delivered, parked, timed out).

### Scenario 1 — Happy path (order 1001)
Copy `testdata/scenario1-happy-path/order-1001.xml.txt` and `lines-1001.xml.txt` into
`inputDir`, stripping the `.txt` suffix (see "Test data" below). Expect: the deliverable
lines are delivered (`outDir/line-1001-1.xml`, ...), the qty=0 line and the SKIP line
silently excluded, workflow ends `DELIVERED_SUMMARY`.

### Scenario 2 — No match (order 2001)
Copy `testdata/scenario2-no-match/order-2001.xml.txt` only (stripping `.txt`). Wait past
`pairingTimeoutSeconds` (2 minutes by default). Expect: `order-2001.xml` moved from `workDir`
to `parkedDir`, an ops email sent, workflow ends `TIMED_OUT`.

### Scenario 3 — Invalid (order 3001)
Copy both `testdata/scenario3-invalid/*.xml.txt` files (stripping `.txt`); one line has
`amount=-5`. Expect: that line is written to `errorDir` with its reason, its child workflow
ends `INVALID`; other lines proceed normally.

### Scenario 4 — Service reject (order 4001)
Set the `order_line_soap_mock` partner service to its 503/service-error scenario per its own
instructions, then copy both `testdata/scenario4-service-reject/*.xml.txt` files (stripping
`.txt`). Expect: `submitLine` returns a retryable error, the engine retries 3 times with the
configured backoff, then the line is parked, ops is emailed, and a `parkedDelivery` human
task awaits an "Ops" decision.

## The two restart tests

**1. Kill mid-retry-wait.** Run scenario 4. While a `submitLine` retry is waiting (the
20-second initial delay is deliberately long so you have a window), stop the Ballerina
process (`Ctrl+C` or `stopBallerinaService`) and start it again. Because the workflow state
lives in Temporal (not in the Ballerina process), the retry resumes within seconds of the
worker reconnecting — you do not lose the attempt count or have to restart the order.

**2. Kill mid-call, with SLOW.** Set the partner service to its SLOW scenario (per its own
instructions) so it delays before responding. Trigger a submission, and kill the Ballerina
process while the HTTP call is in flight. After restarting, the engine does not immediately
re-invoke `submitLine` — an activity in flight when its worker disappears is only
re-dispatched after the engine's fixed activity timeout (5 minutes) elapses without a
recorded result. Once that timeout passes, the engine resends the call; if the partner mock
tracks idempotency keys, it should recognise the resend and report it as a duplicate (e.g. an
`X-Duplicate: true` response header), which `partner_call.bal` logs as
`"partner deduplicated key <key>"`.

## Test data

Real files already exist under `testdata/`, one folder per scenario. Each is written with a
trailing `.txt` appended to its real intended name (my file-writing tool doesn't accept
`.xml`) — strip that suffix before copying into `inputDir`:

```
testdata/scenario1-happy-path/order-1001.xml.txt   -> order-1001.xml
testdata/scenario1-happy-path/lines-1001.xml.txt   -> lines-1001.xml
testdata/scenario2-no-match/order-2001.xml.txt     -> order-2001.xml   (no matching lines file - that's the point)
testdata/scenario3-invalid/order-3001.xml.txt      -> order-3001.xml
testdata/scenario3-invalid/lines-3001.xml.txt      -> lines-3001.xml  (line 1 has amount=-5)
testdata/scenario4-service-reject/order-4001.xml.txt -> order-4001.xml
testdata/scenario4-service-reject/lines-4001.xml.txt -> lines-4001.xml
```

For example:
```bash
cp testdata/scenario1-happy-path/order-1001.xml.txt  "$inputDir"/order-1001.xml
cp testdata/scenario1-happy-path/lines-1001.xml.txt  "$inputDir"/lines-1001.xml
```

`data/dept-map.json` is a real, ready-to-use file (`.json` needed no workaround):
```json
{
  "D10": "CC-4410",
  "D20": "CC-5520"
}
```

## XSDs

Also real files, with `.txt` appended for the same reason:

```
resources/xsd/order.xsd.txt  -> order.xsd
resources/xsd/lines.xsd.txt  -> lines.xsd
```

They map directly to the `OrderFile`/`LinesFile`/`LineItem` records in `types.bal`:
`order.xsd` declares `orderId`/`customer`/`deptCode` (all string); `lines.xsd` declares a
repeating `line` element with `lineNo`/`qty` (int), `sku` (string), `amount` (decimal), and an
optional `flag` (string, `minOccurs="0"`).
