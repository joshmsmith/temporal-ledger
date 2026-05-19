from __future__ import annotations

import asyncio
from datetime import timedelta
from decimal import Decimal

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from payments_ledger.models import (
        ApprovalRequest,
        ApprovalRequestResult,
        ApprovePaymentRequest,
        ApprovePaymentResult,
        BalanceResult,
        EntriesResult,
        EntryState,
        EntryType,
        FraudCheckResult,
        FraudClearanceResult,
        LedgerEntry,
        LedgerInit,
        LedgerState,
        PaymentRequest,
        PaymentResult,
        PendingApprovalsResult,
        RejectPaymentRequest,
        RejectPaymentResult,
        VoidPaymentRequest,
        VoidPaymentResult,
    )
    from payments_ledger.activities.ledger_activities import (
        log_fraud_clearance,
        notify_approver,
        post_to_ledger_db,
        send_payment_confirmation,
        reconcile_external,
    )

# Default approval SLA.  Auto-reject fires after this window.
APPROVAL_TIMEOUT = timedelta(hours=24)

# States that are still in flight and must be carried across Continue-as-New.
_OPEN_STATES = frozenset({
    EntryState.PENDING,
    EntryState.FRAUD_CLEARED,
    EntryState.AWAITING_APPROVAL,
})


@workflow.defn
class PaymentLedgerWorkflow:

    @workflow.init
    def __init__(self, init: LedgerInit) -> None:
        self._account_id: str = init.account_id
        self._currency: str = init.currency
        self._balance: Decimal = Decimal(init.opening_balance)
        self._reserved: Decimal = Decimal(init.reserved_balance)
        # pydantic_data_converter deserialises open_entries as List[LedgerEntry]
        # already, so no manual reconstruction is needed.
        self._entries: dict[str, LedgerEntry] = {
            e.entry_id: e for e in init.open_entries
        }

    # -------------------------------------------------------------------------
    # Main run loop
    # -------------------------------------------------------------------------

    @workflow.run
    async def run(self, init: LedgerInit) -> LedgerState:
        while True:
            # Yield until Temporal tells us history has grown large enough.
            await workflow.wait_condition(
                lambda: workflow.info().is_continue_as_new_suggested()
            )

            # Drain all pending update handlers before truncating history.
            await workflow.wait_condition(workflow.all_handlers_finished)

            # Only carry entries that are still in flight.
            open_entries = [e for e in self._entries.values() if e.state in _OPEN_STATES]

            workflow.continue_as_new(
                LedgerInit(
                    account_id=self._account_id,
                    currency=self._currency,
                    opening_balance=str(self._balance),
                    open_entries=open_entries,
                    reserved_balance=str(self._reserved),
                )
            )

    # -------------------------------------------------------------------------
    # Updates
    # -------------------------------------------------------------------------

    @workflow.update
    async def submit_payment(self, req: PaymentRequest) -> PaymentResult:
        # Idempotent re-submit: return existing result without mutating state.
        if req.entry_id in self._entries:
            existing = self._entries[req.entry_id]
            return PaymentResult(
                entry_id=existing.entry_id,
                state=existing.state,
                new_balance=str(self._balance),
            )

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
        # Already submitted with the same entry_id — idempotent, allow through.
        if req.entry_id in self._entries:
            return
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
            # Release the reserved balance — payment will not proceed.
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

        # Start the approval-timeout watchdog as a background coroutine.
        asyncio.ensure_future(self._approval_timeout_guard(req.entry_id))

        return ApprovalRequestResult(entry_id=req.entry_id, state=entry.state)

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

    async def _approval_timeout_guard(self, entry_id: str) -> None:
        """Auto-reject the entry if no approval/rejection arrives within the SLA."""
        resolved = await workflow.wait_condition(
            lambda: self._entries[entry_id].state
            not in (EntryState.AWAITING_APPROVAL, EntryState.FRAUD_CLEARED),
            timeout=APPROVAL_TIMEOUT,
        )
        if not resolved:
            entry = self._entries.get(entry_id)
            if entry and entry.state == EntryState.AWAITING_APPROVAL:
                entry.state = EntryState.REJECTED
                entry.rejection_reason = "Auto-rejected: approval timeout"
                if entry.entry_type == EntryType.DEBIT:
                    self._balance += Decimal(entry.amount)
                    self._reserved -= Decimal(entry.amount)

    # -------------------------------------------------------------------------

    @workflow.update
    async def approve_payment(self, req: ApprovePaymentRequest) -> ApprovePaymentResult:
        entry = self._entries[req.entry_id]
        entry.state = EntryState.APPROVED
        entry.approved_by = req.approved_by

        if entry.entry_type == EntryType.DEBIT:
            self._reserved -= Decimal(entry.amount)

        await workflow.execute_activity(
            post_to_ledger_db,
            args=[
                workflow.info().workflow_id,
                entry.entry_id,
                entry.amount,
                entry.entry_type.value,
                entry.state.value,
                str(self._balance),
            ],
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
