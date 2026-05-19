# temporal-ledger

A Python sample demonstrating a **durable payments** application built on [Temporal](https://temporal.io/).

This sample contains two complementary workflow designs — the per-payment workflow is the core example:

| Workflow | File | Pattern |
|---|---|---|
| `PaymentWorkflow` | `ledger_workflow.py` | **One workflow per payment** — starts, runs through its lifecycle, completes |
| `PaymentLedgerWorkflow` | `temporal_as_ledger.py` | One entity workflow per account — runs indefinitely, manages many payments |

---

## Core sample: PaymentWorkflow (one workflow per payment)

Each payment is its own workflow. Temporal's durable execution means the payment progresses reliably through fraud screening and approval even if the worker crashes and restarts mid-flight — no extra state management or databases required.

```
PaymentWorkflow (workflow ID: payment:{entry_id})

submit (start workflow with PaymentRequest)
        │
        ▼
    PENDING ──── reserve_funds activity (DEBIT only) ──────────────────┐
        │                                                               │
fraud_check_passed (Update, called by fraud service)                   │
        ├── passed  → notify approver → AWAITING_APPROVAL             │
        └── failed  → release_funds → FRAUD_REJECTED  (workflow ends) │
                                                                        │
        │ (AWAITING_APPROVAL)                                          │
        ▼                                                               │
approve_payment / reject_payment (Update, called by approver)          │
    ├── approved → APPROVED → post_to_ledger_db → POSTED  (ends)      │
    └── rejected → release_funds → REJECTED              (ends)       │
                                                                        │
[24 h approval timeout] → release_funds → auto-REJECTED   (ends) ◄────┘
```

### What it demonstrates

| Concern | How it's addressed |
|---|---|
| Exactly-once payment posting | Workflow ID `payment:{entry_id}` deduplicates at the workflow level; `post_to_ledger_db` uses `INSERT OR IGNORE` with a composite idempotency key so retries never double-post |
| Funds reservation (hold) | `reserve_funds` activity fires on DEBIT submission; `release_funds` fires on fraud failure or rejection — safe to retry, idempotent |
| Durable state machine | Each stage waits via `wait_condition`; survives worker restarts |
| External service callbacks | Fraud service and approver drive state via Updates |
| Auto-reject SLA | 24 h `wait_condition` timeout auto-rejects if approver is unresponsive |
| Ledger post | `post_to_ledger_db` activity writes approved entry to SQLite |
| Payment confirmation | `send_payment_confirmation` activity fires after posting |
| Workflow completes | Unlike an entity workflow, `PaymentWorkflow` ends when done |

---

## Additional example: PaymentLedgerWorkflow (entity workflow per account)

`temporal_as_ledger.py` shows Temporal used as a ledger itself — one long-running entity workflow per account (`ledger:{account_id}`) that holds balance as in-memory state, accepts many payments as Updates, and never completes on its own. It demonstrates update-with-start (lazy account creation) and continue-as-new for unbounded history.

See the lifecycle diagram and deeper explanation in `entity_design.md`.

---

## Project structure

```
temporal-ledger/
├── requirements.txt
├── payments_ledger/
│   ├── models.py                       (Pydantic models shared by both workflows)
│   ├── worker.py                       (Temporal worker — registers both workflows)
│   ├── client.py                       (example client: per-payment workflow demo)
│   ├── activities/
│   │   └── ledger_activities.py        (post_to_ledger_db, notify_approver, …)
│   ├── workflows/
│   │   ├── ledger_workflow.py          (PaymentWorkflow — one per payment, core sample)
│   │   └── temporal_as_ledger.py       (PaymentLedgerWorkflow — entity workflow per account)
│   └── data/                           (SQLite database written by activities)
└── scripts/
    └── setup.sh                        (register search attributes)
```

---

## Getting started

### Prerequisites

- Python 3.11+
- [Temporal CLI](https://docs.temporal.io/cli) installed and on your PATH

### Install dependencies

```bash
pip install -r requirements.txt
```

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
python -m payments_ledger.worker
```

### Run the example client (submits a payment through fraud check and approval)

```bash
python -m payments_ledger.client
```

---

## Configuration

| Env var | Default | Description |
|---|---|---|
| `TEMPORAL_ADDRESS` | `localhost:7233` | Temporal gRPC endpoint |
| `TEMPORAL_NAMESPACE` | `default` | Temporal namespace |

---

## Activity integration points

Each activity in `payments_ledger/activities/ledger_activities.py` writes to a local SQLite database. In production, replace them with:

| Activity | Integration |
|---|---|
| `reserve_funds` | Account balance service / ledger hold API |
| `release_funds` | Account balance service / ledger hold release |
| `post_to_ledger_db` | Double-entry ledger DB (Postgres, etc.) |
| `notify_approver` | Email / push / ticketing system |
| `send_payment_confirmation` | Email / push notification service |
| `log_fraud_clearance` | Immutable audit log / SIEM |
| `reconcile_external` | External settlement / reconciliation API |

---

## Related Financial Examples

| Sample | Language | What it shows |
|---|---|---|
| [temporal-kyc-sample](https://github.com/joshmsmith/temporal-kyc-sample) | Java | Customer onboarding / KYC flow — long-running human-in-the-loop workflow with compliance review, 30-day SLA timer, and audit trail |
| [temporal-latency-optimization-scenarios](https://github.com/temporal-sa/temporal-latency-optimization-scenarios) | Java / Go | Real-time payment latency patterns — update-with-start with local activities for minimum round-trip latency, eager workflow start |

---

## Running tests

```bash
pytest
```
