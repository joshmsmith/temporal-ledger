# Payments Ledger — Temporal Implementation Spec

## Overview

Implement a durable payments ledger using the Temporal Python SDK. Each ledger is an
**entity workflow** — one long-running workflow per account, addressed by a stable
workflow ID (`ledger:{account_id}`). It accepts payment operations as **Updates**
(synchronous request/response), maintains in-memory state, and persists side effects
via Activities.

The workflow is started lazily via **update-with-start**: if the ledger doesn't exist
yet, the first update starts it. No separate "create ledger" call is needed.

---

## Project structure

```
payments_ledger/
├── workflows/
│   └── ledger.py          # PaymentLedgerWorkflow
├── activities/
│   └── ledger_activities.py
├── models.py              # Dataclasses / Pydantic models
├── worker.py
└── client.py              # Example usage / starter
```

---

## Models (`models.py`)

```python
from __future__ import annotations
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Optional
import uuid


class EntryState(str, Enum):
    PENDING = "PENDING"
    FRAUD_CLEARED = "FRAUD_CLEARED"
    FRAUD_REJECTED = "FRAUD_REJECTED"
    AWAITING_APPROVAL = "AWAITING_APPROVAL"
    APPROVED = "APPROVED"
    POSTED = "POSTED"
    REJECTED = "REJECTED"
    VOIDED = "VOIDED"


class EntryType(str, Enum):
    DEBIT = "DEBIT"
    CREDIT = "CREDIT"


@dataclass
class LedgerInit:
    account_id: str
    currency: str = "USD"
    opening_balance: str = "0.00"   # Decimal as string for serialisation


@dataclass
class PaymentRequest:
    amount: str                      # Decimal as string
    entry_type: EntryType = EntryType.DEBIT
    reference: str = ""
    entry_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    metadata: dict = field(default_factory=dict)


@dataclass
class PaymentResult:
    entry_id: str
    state: EntryState
    new_balance: str                 # Decimal as string


@dataclass
class ApprovalRequest:
    entry_id: str
    approver_id: str


@dataclass
class ApprovalRequestResult:
    entry_id: str
    state: EntryState               # AWAITING_APPROVAL


@dataclass
class ApprovePaymentRequest:
    entry_id: str
    approved_by: str
    notes: str = ""


@dataclass
class ApprovePaymentResult:
    entry_id: str
    state: EntryState               # APPROVED or POSTED
    new_balance: str


@dataclass
class RejectPaymentRequest:
    entry_id: str
    rejected_by: str
    reason: str


@dataclass
class RejectPaymentResult:
    entry_id: str
    state: EntryState               # REJECTED


@dataclass
class VoidPaymentRequest:
    entry_id: str
    voided_by: str
    reason: str


@dataclass
class VoidPaymentResult:
    entry_id: str
    state: EntryState               # VOIDED
    new_balance: str


@dataclass
class FraudCheckResult:
    entry_id: str
    passed: bool
    risk_score: float
    flags: list = field(default_factory=list)   # e.g. ["velocity_breach"]
    checked_by: str = "fraud-service"


@dataclass
class FraudClearanceResult:
    entry_id: str
    new_state: EntryState           # FRAUD_CLEARED or FRAUD_REJECTED
    new_balance: str


@dataclass
class LedgerEntry:
    entry_id: str
    amount: str                     # Decimal as string
    entry_type: EntryType
    reference: str
    state: EntryState
    metadata: dict = field(default_factory=dict)
    fraud_metadata: Optional[FraudCheckResult] = None
    approver_id: Optional[str] = None
    approved_by: Optional[str] = None
    rejection_reason: Optional[str] = None


@dataclass
class LedgerState:
    """Carried forward across Continue-as-New boundaries."""
    account_id: str
    currency: str
    balance: str                    # Decimal as string
    entries: list = field(default_factory=list)     # list[LedgerEntry]
    reserved: str = "0.00"         # balance held pending approval


# --- Query response types ---

@dataclass
class BalanceResult:
    balance: str
    reserved: str
    currency: str


@dataclass
class EntriesResult:
    entries: list                   # list[LedgerEntry]


@dataclass
class PendingApprovalsResult:
    entries: list                   # list[LedgerEntry] with AWAITING_APPROVAL state
```

---

## Activities (`activities/ledger_activities.py`)

All activities are **sync** (use `ThreadPoolExecutor`). Pass idempotency keys
(`workflow_id + entry_id`) to every external call.

```python
from temporalio import activity
from models import LedgerEntry, FraudCheckResult


@activity.defn
def post_to_ledger_db(
    workflow_id: str,
    entry: LedgerEntry,
    new_balance: str,
) -> bool:
    """
    Write the approved entry and new balance to the ledger database.
    Must be idempotent: use (workflow_id, entry_id) as the upsert key.
    Returns True on success.
    """
    idempotency_key = f"{workflow_id}:{entry.entry_id}"
    # TODO: implement DB upsert
    raise NotImplementedError


@activity.defn
def notify_approver(
    workflow_id: str,
    entry_id: str,
    approver_id: str,
    amount: str,
    reference: str,
) -> bool:
    """
    Send approval request notification (email, Slack, push, etc.).
    Idempotency key: (workflow_id, entry_id, approver_id).
    Returns True if notification was sent or already sent.
    """
    idempotency_key = f"{workflow_id}:{entry_id}:{approver_id}"
    # TODO: implement notification
    raise NotImplementedError


@activity.defn
def send_payment_confirmation(
    workflow_id: str,
    entry_id: str,
    account_id: str,
    amount: str,
    new_balance: str,
) -> bool:
    """
    Send payment posted confirmation to the account holder.
    Idempotency key: (workflow_id, entry_id).
    """
    idempotency_key = f"{workflow_id}:{entry_id}"
    # TODO: implement confirmation
    raise NotImplementedError


@activity.defn
def log_fraud_clearance(
    workflow_id: str,
    entry_id: str,
    risk_score: float,
    flags: list,
    passed: bool,
) -> bool:
    """
    Write fraud check result to audit log.
    Idempotency key: (workflow_id, entry_id).
    """
    idempotency_key = f"{workflow_id}:{entry_id}"
    # TODO: implement audit log write
    raise NotImplementedError


@activity.defn
def reconcile_external(
    workflow_id: str,
    account_id: str,
    balance: str,
) -> dict:
    """
    Reconcile ledger balance against external system.
    Returns {"matched": bool, "external_balance": str, "discrepancies": list}.
    """
    # TODO: implement reconciliation
    raise NotImplementedError
```

---

## Workflow (`workflows/ledger.py`)

```python
from __future__ import annotations

import asyncio
from datetime import timedelta
from decimal import Decimal

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from models import (
        ApprovalRequest, ApprovalRequestResult,
        ApprovePaymentRequest, ApprovePaymentResult,
        BalanceResult, EntriesResult,
        EntryState, EntryType,
        FraudCheckResult, FraudClearanceResult,
        LedgerEntry, LedgerInit, LedgerState,
        PaymentRequest, PaymentResult,
        PendingApprovalsResult,
        RejectPaymentRequest, RejectPaymentResult,
        VoidPaymentRequest, VoidPaymentResult,
    )
    from activities.ledger_activities import (
        log_fraud_clearance,
        notify_approver,
        post_to_ledger_db,
        send_payment_confirmation,
        reconcile_external,
    )

# Default approval timeout: 24 hours. Override via LedgerInit if needed.
APPROVAL_TIMEOUT = timedelta(hours=24)


@workflow.defn
class PaymentLedgerWorkflow:

    @workflow.init
    def __init__(self, init: LedgerInit) -> None:
        self._account_id = init.account_id
        self._currency = init.currency
        self._balance = Decimal(init.opening_balance)
        self._reserved = Decimal("0.00")
        self._entries: dict[str, LedgerEntry] = {}

    # -------------------------------------------------------------------------
    # Main run loop
    # -------------------------------------------------------------------------

    @workflow.run
    async def run(self, init: LedgerInit) -> LedgerState:
        while True:
            # Yield until an update arrives or Continue-as-New is suggested.
            await workflow.wait_condition(
                lambda: workflow.info().is_continue_as_new_suggested()
            )

            # Drain pending handlers before continuing.
            await workflow.wait_condition(workflow.all_handlers_finished)

            # Carry essential state forward into a fresh history.
            state = self._snapshot()
            workflow.continue_as_new(
                LedgerInit(
                    account_id=self._account_id,
                    currency=self._currency,
                    opening_balance=str(self._balance),
                ),
                # NOTE: open entries (PENDING, AWAITING_APPROVAL) are serialised
                # into the new run's init if your LedgerInit supports them.
                # Extend LedgerInit with `open_entries: list[LedgerEntry] = []`
                # and restore them in __init__ to carry in-flight state across.
            )

    # -------------------------------------------------------------------------
    # Updates
    # -------------------------------------------------------------------------

    @workflow.update
    async def submit_payment(self, req: PaymentRequest) -> PaymentResult:
        amount = Decimal(req.amount)

        if req.entry_type == EntryType.DEBIT:
            self._balance -= amount
            self._reserved += amount

        entry = LedgerEntry(
            entry_id=req.entry_id,
            amount=req.amount,
            entry_type=req.entry_type,
            reference=req.reference,
            state=EntryState.PENDING,
            metadata=req.metadata,
        )
        self._entries[entry.entry_id] = entry

        return PaymentResult(
            entry_id=entry.entry_id,
            state=entry.state,
            new_balance=str(self._balance),
        )

    @submit_payment.validator
    def _validate_submit_payment(self, req: PaymentRequest) -> None:
        if Decimal(req.amount) <= 0:
            raise ValueError("Payment amount must be positive")
        if req.entry_type == EntryType.DEBIT:
            available = self._balance - self._reserved
            if Decimal(req.amount) > available:
                raise ValueError(
                    f"Insufficient funds: available {available}, requested {req.amount}"
                )

    # -------------------------------------------------------------------------

    @workflow.update
    async def fraud_check_passed(self, result: FraudCheckResult) -> FraudClearanceResult:
        entry = self._entries[result.entry_id]

        await workflow.execute_activity(
            log_fraud_clearance,
            args=[
                workflow.info().workflow_id,
                result.entry_id,
                result.risk_score,
                result.flags,
                result.passed,
            ],
            start_to_close_timeout=timedelta(seconds=15),
        )

        if result.passed:
            entry.state = EntryState.FRAUD_CLEARED
            entry.fraud_metadata = result
        else:
            entry.state = EntryState.FRAUD_REJECTED
            # Release the reserved balance — payment won't proceed.
            if entry.entry_type == EntryType.DEBIT:
                self._balance += Decimal(entry.amount)
                self._reserved -= Decimal(entry.amount)

        return FraudClearanceResult(
            entry_id=result.entry_id,
            new_state=entry.state,
            new_balance=str(self._balance),
        )

    @fraud_check_passed.validator
    def _validate_fraud_check(self, result: FraudCheckResult) -> None:
        entry = self._entries.get(result.entry_id)
        if entry is None:
            raise ValueError(f"Entry {result.entry_id} not found")
        if entry.state != EntryState.PENDING:
            raise ValueError(
                f"Fraud check only valid on PENDING entries; got {entry.state}"
            )

    # -------------------------------------------------------------------------

    @workflow.update
    async def request_approval(self, req: ApprovalRequest) -> ApprovalRequestResult:
        entry = self._entries[req.entry_id]
        entry.state = EntryState.AWAITING_APPROVAL
        entry.approver_id = req.approver_id

        await workflow.execute_activity(
            notify_approver,
            args=[
                workflow.info().workflow_id,
                req.entry_id,
                req.approver_id,
                entry.amount,
                entry.reference,
            ],
            start_to_close_timeout=timedelta(seconds=30),
        )

        return ApprovalRequestResult(
            entry_id=req.entry_id,
            state=entry.state,
        )

    @request_approval.validator
    def _validate_request_approval(self, req: ApprovalRequest) -> None:
        entry = self._entries.get(req.entry_id)
        if entry is None:
            raise ValueError(f"Entry {req.entry_id} not found")
        if entry.state != EntryState.FRAUD_CLEARED:
            raise ValueError(
                f"Approval only valid after fraud clearance; got {entry.state}"
            )
        if not req.approver_id:
            raise ValueError("approver_id is required")

    # -------------------------------------------------------------------------

    @workflow.update
    async def approve_payment(self, req: ApprovePaymentRequest) -> ApprovePaymentResult:
        entry = self._entries[req.entry_id]
        entry.state = EntryState.APPROVED
        entry.approved_by = req.approved_by

        # Release reserved funds and post.
        if entry.entry_type == EntryType.DEBIT:
            self._reserved -= Decimal(entry.amount)

        await workflow.execute_activity(
            post_to_ledger_db,
            args=[workflow.info().workflow_id, entry, str(self._balance)],
            start_to_close_timeout=timedelta(seconds=30),
        )

        entry.state = EntryState.POSTED

        await workflow.execute_activity(
            send_payment_confirmation,
            args=[
                workflow.info().workflow_id,
                entry.entry_id,
                self._account_id,
                entry.amount,
                str(self._balance),
            ],
            start_to_close_timeout=timedelta(seconds=15),
        )

        return ApprovePaymentResult(
            entry_id=req.entry_id,
            state=entry.state,
            new_balance=str(self._balance),
        )

    @approve_payment.validator
    def _validate_approve_payment(self, req: ApprovePaymentRequest) -> None:
        entry = self._entries.get(req.entry_id)
        if entry is None:
            raise ValueError(f"Entry {req.entry_id} not found")
        if entry.state != EntryState.AWAITING_APPROVAL:
            raise ValueError(
                f"Can only approve AWAITING_APPROVAL entries; got {entry.state}"
            )

    # -------------------------------------------------------------------------

    @workflow.update
    async def reject_payment(self, req: RejectPaymentRequest) -> RejectPaymentResult:
        entry = self._entries[req.entry_id]
        entry.state = EntryState.REJECTED
        entry.rejection_reason = req.reason

        # Reverse the debit reservation.
        if entry.entry_type == EntryType.DEBIT:
            self._balance += Decimal(entry.amount)
            self._reserved -= Decimal(entry.amount)

        return RejectPaymentResult(entry_id=req.entry_id, state=entry.state)

    @reject_payment.validator
    def _validate_reject_payment(self, req: RejectPaymentRequest) -> None:
        entry = self._entries.get(req.entry_id)
        if entry is None:
            raise ValueError(f"Entry {req.entry_id} not found")
        if entry.state not in (EntryState.AWAITING_APPROVAL, EntryState.FRAUD_CLEARED):
            raise ValueError(
                f"Can only reject AWAITING_APPROVAL or FRAUD_CLEARED entries; got {entry.state}"
            )

    # -------------------------------------------------------------------------

    @workflow.update
    async def void_payment(self, req: VoidPaymentRequest) -> VoidPaymentResult:
        entry = self._entries[req.entry_id]

        # Reverse the posted transaction.
        if entry.entry_type == EntryType.DEBIT:
            self._balance += Decimal(entry.amount)
        else:
            self._balance -= Decimal(entry.amount)

        entry.state = EntryState.VOIDED
        entry.rejection_reason = req.reason

        return VoidPaymentResult(
            entry_id=req.entry_id,
            state=entry.state,
            new_balance=str(self._balance),
        )

    @void_payment.validator
    def _validate_void_payment(self, req: VoidPaymentRequest) -> None:
        entry = self._entries.get(req.entry_id)
        if entry is None:
            raise ValueError(f"Entry {req.entry_id} not found")
        if entry.state != EntryState.POSTED:
            raise ValueError(
                f"Can only void POSTED entries; got {entry.state}"
            )

    # -------------------------------------------------------------------------
    # Queries (read-only, no state mutation)
    # -------------------------------------------------------------------------

    @workflow.query
    def get_balance(self) -> BalanceResult:
        return BalanceResult(
            balance=str(self._balance),
            reserved=str(self._reserved),
            currency=self._currency,
        )

    @workflow.query
    def get_entries(self) -> EntriesResult:
        return EntriesResult(entries=list(self._entries.values()))

    @workflow.query
    def get_pending_approvals(self) -> PendingApprovalsResult:
        pending = [
            e for e in self._entries.values()
            if e.state == EntryState.AWAITING_APPROVAL
        ]
        return PendingApprovalsResult(entries=pending)

    # -------------------------------------------------------------------------
    # Internal helpers
    # -------------------------------------------------------------------------

    def _snapshot(self) -> LedgerState:
        return LedgerState(
            account_id=self._account_id,
            currency=self._currency,
            balance=str(self._balance),
            entries=list(self._entries.values()),
            reserved=str(self._reserved),
        )
```

---

## Worker (`worker.py`)

```python
import asyncio
import concurrent.futures

from temporalio.client import Client
from temporalio.worker import Worker

from workflows.ledger import PaymentLedgerWorkflow
from activities.ledger_activities import (
    log_fraud_clearance,
    notify_approver,
    post_to_ledger_db,
    send_payment_confirmation,
    reconcile_external,
)

TASK_QUEUE = "payments-task-queue"


async def main():
    client = await Client.connect("localhost:7233")

    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
        worker = Worker(
            client,
            task_queue=TASK_QUEUE,
            workflows=[PaymentLedgerWorkflow],
            activities=[
                log_fraud_clearance,
                notify_approver,
                post_to_ledger_db,
                send_payment_confirmation,
                reconcile_external,
            ],
            activity_executor=executor,
        )
        await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
```

---

## Client — example usage (`client.py`)

```python
import asyncio
from decimal import Decimal

from temporalio.client import Client, WithStartWorkflowOperation
from temporalio.common import WorkflowIDConflictPolicy

from workflows.ledger import PaymentLedgerWorkflow
from models import (
    ApprovalRequest, ApprovePaymentRequest,
    FraudCheckResult,
    LedgerInit,
    PaymentRequest, EntryType,
)

TASK_QUEUE = "payments-task-queue"


async def main():
    client = await Client.connect("localhost:7233")

    account_id = "acct-001"
    ledger_id = f"ledger:{account_id}"

    # -------------------------------------------------------------------
    # 1. submit_payment via update-with-start
    #    Starts the ledger if it doesn't exist, attaches if it does.
    # -------------------------------------------------------------------
    start_op = WithStartWorkflowOperation(
        PaymentLedgerWorkflow.run,
        args=[LedgerInit(account_id=account_id, currency="USD", opening_balance="10000.00")],
        id=ledger_id,
        id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
        task_queue=TASK_QUEUE,
    )

    payment_result = await client.execute_update_with_start_workflow(
        PaymentLedgerWorkflow.submit_payment,
        PaymentRequest(
            amount="500.00",
            entry_type=EntryType.DEBIT,
            reference="INV-2024-001",
        ),
        start_workflow_operation=start_op,
    )
    print(f"Submitted: {payment_result}")

    handle = await start_op.workflow_handle()

    # -------------------------------------------------------------------
    # 2. fraud_check_passed (from your fraud service callback)
    # -------------------------------------------------------------------
    fraud_result = await handle.execute_update(
        PaymentLedgerWorkflow.fraud_check_passed,
        FraudCheckResult(
            entry_id=payment_result.entry_id,
            passed=True,
            risk_score=0.12,
            flags=[],
            checked_by="fraud-service-v2",
        ),
    )
    print(f"Fraud check: {fraud_result}")

    # -------------------------------------------------------------------
    # 3. request_approval
    # -------------------------------------------------------------------
    approval_req_result = await handle.execute_update(
        PaymentLedgerWorkflow.request_approval,
        ApprovalRequest(
            entry_id=payment_result.entry_id,
            approver_id="approver-jane",
        ),
    )
    print(f"Approval requested: {approval_req_result}")

    # -------------------------------------------------------------------
    # 4. approve_payment (called by the approver's system)
    # -------------------------------------------------------------------
    approve_result = await handle.execute_update(
        PaymentLedgerWorkflow.approve_payment,
        ApprovePaymentRequest(
            entry_id=payment_result.entry_id,
            approved_by="approver-jane",
            notes="Verified with vendor",
        ),
    )
    print(f"Approved: {approve_result}")

    # -------------------------------------------------------------------
    # 5. Query balance
    # -------------------------------------------------------------------
    balance = await handle.query(PaymentLedgerWorkflow.get_balance)
    print(f"Balance: {balance}")


if __name__ == "__main__":
    asyncio.run(main())
```

---

## State machine reference

```
submit_payment
    └─→ PENDING
            ├─→ fraud_check_passed(passed=True)  ──→ FRAUD_CLEARED
            │       └─→ request_approval          ──→ AWAITING_APPROVAL
            │               ├─→ approve_payment   ──→ APPROVED ──→ POSTED
            │               │       └─→ void_payment           ──→ VOIDED
            │               └─→ reject_payment    ──→ REJECTED
            └─→ fraud_check_passed(passed=False)  ──→ FRAUD_REJECTED
```

---

## Key design notes for the implementer

### Update validators
Validators run **before** the update is written to history. They are read-only —
no activities, no sleeps, no state mutations. Raise `ValueError` (or any exception)
to reject the update cleanly. This is your first line of defence for state machine
integrity and invalid inputs.

### Idempotency
Every activity receives `workflow_id + entry_id` as a compound idempotency key.
Implement upsert semantics in all database and notification activities so retries
are safe.

### Decimal serialisation
Use `str` on the wire (dataclass fields typed as `str`) and convert to `Decimal`
inside the workflow. This avoids floating-point precision loss across serialisation
boundaries.

### Continue-as-New
The workflow's `run` loop calls `continue_as_new` when
`workflow.info().is_continue_as_new_suggested()` is True (Temporal raises this flag
as history grows). Before continuing, await `all_handlers_finished()` to ensure
no in-flight updates are dropped. Extend `LedgerInit` with an `open_entries` field
to carry PENDING / AWAITING_APPROVAL entries across the boundary.

### update-with-start is not fully atomic
If no worker is available when the update is sent, the workflow may start but the
update may not be delivered. The SDK retries, but design the `submit_payment`
handler to be safe to receive twice for the same `entry_id` (idempotent by
`entry_id`).

### Approval timeout (optional enhancement)
Inside `request_approval`, instead of just flipping state, you can start an async
task that uses `workflow.wait_condition` with a timeout to auto-reject if no
`approve_payment` or `reject_payment` update arrives within the SLA window:

```python
# inside request_approval handler, after setting state
asyncio.ensure_future(self._approval_timeout_guard(req.entry_id))

async def _approval_timeout_guard(self, entry_id: str) -> None:
    timed_out = not await workflow.wait_condition(
        lambda: self._entries[entry_id].state not in (
            EntryState.AWAITING_APPROVAL, EntryState.FRAUD_CLEARED
        ),
        timeout=APPROVAL_TIMEOUT,
    )
    if timed_out:
        entry = self._entries.get(entry_id)
        if entry and entry.state == EntryState.AWAITING_APPROVAL:
            entry.state = EntryState.REJECTED
            entry.rejection_reason = "Auto-rejected: approval timeout"
            if entry.entry_type == EntryType.DEBIT:
                self._balance += Decimal(entry.amount)
                self._reserved -= Decimal(entry.amount)
```