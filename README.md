# temporal-ledger

A Python sample demonstrating a **durable ledger / payments** application built on [Temporal](https://temporal.io/).

Ledger operations — deposits, withdrawals, transfers — must be **exactly-once**, survive process restarts, and produce a complete, immutable audit trail. Temporal's durable execution model provides all of this without distributed transactions or complex compensating logic.

---

## What it demonstrates

| Concern | How it's addressed |
|---|---|
| Exactly-once money movement | WorkflowId `ledger:<accountId>` deduplicates; `entry_id` on every update |
| Funds reservation (hold) | Balance decremented and reserved atomically on `submit_payment`; released on rejection |
| Ledger post | `post_to_ledger_db` activity writes the approved entry to SQLite (swap in Postgres, etc.) |
| Fraud / AML screening | `fraud_check_passed` update carries risk score + flags from an external screener |
| Approval workflow | `request_approval` notifies an approver; auto-rejects after a 24-hour SLA |
| Long-lived accounts | `PaymentLedgerWorkflow` runs indefinitely via continue-as-new; balance is durable state |
| Payment confirmation | `send_payment_confirmation` activity fires after every successful post |
| External reconciliation | `reconcile_external` activity for periodic settlement |
| Balance query | `get_balance` query on `PaymentLedgerWorkflow` — no DB round-trip needed |

---

## Project structure

```
temporal-ledger/
├── requirements.txt
├── payments_ledger/
│   ├── models.py                  (dataclasses: LedgerEntry, PaymentRequest, EntryState, …)
│   ├── worker.py                  (Temporal worker entry point)
│   ├── client.py                  (example client: update-with-start demo)
│   ├── activities/
│   │   └── ledger_activities.py   (post_to_ledger_db, notify_approver, send_payment_confirmation, …)
│   ├── workflows/
│   │   └── ledger.py              (PaymentLedgerWorkflow)
│   └── data/                      (SQLite database written by activities)
└── scripts/
    └── setup.sh                   (register search attributes)
```

---

## Workflow lifecycle

### PaymentLedgerWorkflow (single long-running workflow per account)

```
submit_payment (Update)
        │
        ▼
    PENDING ──── insufficient funds ──► validator rejects
        │
        ▼
fraud_check_passed (Update)
        ├── passed ──► FRAUD_CLEARED
        └── failed ──► FRAUD_REJECTED  (reserved balance released)

        │ (FRAUD_CLEARED)
        ▼
request_approval (Update) ──► AWAITING_APPROVAL
        │                          │
        │                   [24 h timeout]
        │                          │
        │                          ▼
        │                    REJECTED  (reserved balance released)
        │
approve_payment (Update) ──► APPROVED
        │   post_to_ledger_db activity
        │   send_payment_confirmation activity
        └──► POSTED

reject_payment (Update)  ──► REJECTED  (reserved balance released)
void_payment   (Update)  ──► VOIDED

[history large] ──► continue-as-new (open entries carried forward)
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

### Run the example client (opens a ledger + submits a payment through the full approval flow)

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
| `post_to_ledger_db` | Double-entry ledger DB (Postgres, etc.) |
| `notify_approver` | Email / push / ticketing system |
| `send_payment_confirmation` | Email / push notification service |
| `log_fraud_clearance` | Immutable audit log / SIEM |
| `reconcile_external` | External settlement / reconciliation API |

---

## Running tests

```bash
pytest
```
