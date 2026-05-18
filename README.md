# temporal-ledger

A Java sample demonstrating a **durable ledger / payments** application built on [Temporal](https://temporal.io/).

Ledger operations — deposits, withdrawals, transfers — must be **exactly-once**, survive process restarts, and produce a complete, immutable audit trail. Temporal's durable execution model provides all of this without distributed transactions or complex compensating logic.

> **Status:** Initial scaffold — full implementation coming. Share the design doc to drive the next phase.

---

## What it demonstrates

| Concern | How it's addressed |
|---|---|
| Exactly-once money movement | WorkflowId `TXN-<transactionId>` deduplicates; idempotency keys on every activity |
| Funds reservation (hold) | `reserveFunds` activity places a hold before posting; released automatically on failure |
| Double-entry ledger post | `postLedgerEntry` activity writes both sides atomically |
| Fraud / AML screening | Heartbeating `screenTransaction` activity with configurable timeout |
| Long-lived accounts | `AccountWorkflow` runs indefinitely via continue-as-new; balance is durable state |
| Audit trail | `logAuditEvent` activity called on every state transition |
| Operator visibility | Search attributes (`LedgerAccountId`, `LedgerTxnStatus`, `LedgerTxnType`) |
| Balance query | `getAccountState()` query on `AccountWorkflow` — no DB round-trip needed |

---

## Project structure

```
temporal-ledger/
├── build.gradle / settings.gradle / gradlew
├── core/
│   └── src/main/java/io/temporal/samples/ledger/
│       ├── activities/
│       │   ├── LedgerActivities.java          (interface)
│       │   └── LedgerActivitiesImpl.java       (stub — replace with real integrations)
│       ├── model/
│       │   ├── Money.java
│       │   ├── TransactionType.java
│       │   ├── TransactionStatus.java
│       │   ├── CreateAccountRequest.java
│       │   ├── TransactionRequest.java
│       │   ├── TransactionResult.java
│       │   └── AccountState.java
│       ├── workflow/
│       │   ├── TransactionWorkflow.java        (interface)
│       │   ├── TransactionWorkflowImpl.java
│       │   ├── AccountWorkflow.java            (interface)
│       │   └── AccountWorkflowImpl.java
│       ├── LedgerConstants.java
│       ├── LedgerWorker.java
│       └── LedgerStarter.java
└── scripts/
    └── setup.sh                               (register search attributes)
```

---

## Workflow lifecycle

### TransactionWorkflow

```
PENDING
   │
   ▼
VALIDATING ──── invalid ──► FAILED
   │
   ▼
 screen ──────── flagged ──► FAILED
   │
   ▼
PROCESSING
   │
   ├── reserveFunds (if debit side exists)
   │
   ├── postLedgerEntry ──── error ──► releaseFunds ──► FAILED
   │
   ├── notifyParties
   │
   └──► COMPLETED
```

### AccountWorkflow

```
openAccount ──► ACTIVE (long-running)
     │
     │◄── submitTransaction (Signal) ──► child TransactionWorkflow
     │◄── submitTransaction (Signal) ──► child TransactionWorkflow
     │                  ...
     │◄── closeAccount (Signal)
     │
     └──► CLOSED
```

---

## Getting started

### Prerequisites

- Java 17
- [Temporal CLI](https://docs.temporal.io/cli) installed and on your PATH

### Run the dev server

```bash
temporal server start-dev
```

### Register search attributes

```bash
bash scripts/setup.sh
```

### Start the worker

```bash
./gradlew -q execute
```

### Run the starter (opens an account + submits a deposit)

```bash
./gradlew -q execute -PmainClass=io.temporal.samples.ledger.LedgerStarter
```

---

## Configuration

| Env var | Default | Description |
|---|---|---|
| `TEMPORAL_ADDRESS` | `localhost:7233` | Temporal gRPC endpoint |
| `TEMPORAL_NAMESPACE` | `default` | Temporal namespace |

---

## Activity integration points

Each activity in `LedgerActivitiesImpl` is stubbed with a log statement and a short sleep. In production, replace them with:

| Activity | Integration |
|---|---|
| `createAccount` | Account database (Postgres, etc.) |
| `validateTransaction` | Rules engine / limit service |
| `screenTransaction` | Fraud / AML vendor API (heartbeating) |
| `reserveFunds` | Account store hold API |
| `postLedgerEntry` | Double-entry ledger DB |
| `releaseFunds` | Account store release API |
| `reverseLedgerEntry` | Ledger reversal / correction API |
| `notifyParties` | Email / push notification service |
| `logAuditEvent` | Immutable audit log / SIEM |

---

## Running tests

```bash
./gradlew test
```
