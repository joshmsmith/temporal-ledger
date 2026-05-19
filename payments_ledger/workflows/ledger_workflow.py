from __future__ import annotations

from datetime import timedelta

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from payments_ledger.models import (
        ApprovePaymentRequest,
        ApprovePaymentResult,
        EntryState,
        EntryType,
        FraudCheckResult,
        FraudClearanceResult,
        LedgerEntry,
        PaymentRequest,
        RejectPaymentRequest,
        RejectPaymentResult,
        SinglePaymentResult,
    )
    from payments_ledger.activities.ledger_activities import (
        log_fraud_clearance,
        notify_approver,
        post_to_ledger_db,
        release_funds,
        reserve_funds,
        send_payment_confirmation,
    )

# Auto-reject fires after this window if no approval is received.
APPROVAL_TIMEOUT = timedelta(hours=24)


@workflow.defn
class PaymentWorkflow:
    """
    One workflow per payment.  Workflow ID: ``payment:{entry_id}``

    Tracks a single payment from submission through posting or rejection, then
    **completes**.  This is in contrast to ``PaymentLedgerWorkflow`` in
    ``temporal_as_ledger.py``, which is an *entity workflow* — one long-running
    workflow per account that manages many payments and never completes on its own.

    Lifecycle
    ---------
    PENDING
        → ``fraud_check_passed`` update (sent by external fraud service)
            • passed  → notify approver, AWAITING_APPROVAL
            • failed  → FRAUD_REJECTED  (workflow ends)
        → ``approve_payment`` / ``reject_payment`` update (sent by approver)
            • approved → APPROVED → post to ledger → POSTED  (workflow ends)
            • rejected → REJECTED                            (workflow ends)
        → [24 h timeout] → auto-reject, REJECTED             (workflow ends)
    """

    def __init__(self) -> None:
        self._entry: LedgerEntry | None = None

    # -------------------------------------------------------------------------
    # Main run loop
    # -------------------------------------------------------------------------

    @workflow.run
    async def run(self, req: PaymentRequest) -> SinglePaymentResult:
        self._entry = LedgerEntry(
            entry_id=req.entry_id,
            amount=req.amount,
            entry_type=req.entry_type,
            reference=req.reference,
            state=EntryState.PENDING,
            metadata=req.metadata,
        )

        # Reserve funds immediately for debit payments.
        if req.entry_type == EntryType.DEBIT:
            await workflow.execute_activity(
                reserve_funds,
                args=[
                    workflow.info().workflow_id,
                    self._entry.entry_id,
                    self._entry.amount,
                    self._entry.entry_type.value,
                ],
                start_to_close_timeout=timedelta(seconds=30),
            )

        # ── Stage 1: wait for fraud check (external service sends an update) ──
        await workflow.wait_condition(
            lambda: self._entry.state != EntryState.PENDING
        )

        if self._entry.state == EntryState.FRAUD_REJECTED:
            if self._entry.entry_type == EntryType.DEBIT:
                await workflow.execute_activity(
                    release_funds,
                    args=[
                        workflow.info().workflow_id,
                        self._entry.entry_id,
                        self._entry.amount,
                        self._entry.entry_type.value,
                        "Fraud check failed",
                    ],
                    start_to_close_timeout=timedelta(seconds=30),
                )
            return SinglePaymentResult(
                entry_id=self._entry.entry_id,
                state=self._entry.state,
                rejection_reason="Fraud check failed",
            )

        # ── Stage 2: wait for approval decision (with 24 h auto-reject) ───────
        # State is AWAITING_APPROVAL — set inside fraud_check_passed handler
        # after the approver notification activity completes.
        resolved = await workflow.wait_condition(
            lambda: self._entry.state != EntryState.AWAITING_APPROVAL,
            timeout=APPROVAL_TIMEOUT,
        )

        if not resolved and self._entry.state == EntryState.AWAITING_APPROVAL:
            # SLA expired without a decision — auto-reject.
            self._entry.state = EntryState.REJECTED
            self._entry.rejection_reason = "Auto-rejected: approval SLA expired"

        if self._entry.state == EntryState.REJECTED:
            if self._entry.entry_type == EntryType.DEBIT:
                await workflow.execute_activity(
                    release_funds,
                    args=[
                        workflow.info().workflow_id,
                        self._entry.entry_id,
                        self._entry.amount,
                        self._entry.entry_type.value,
                        self._entry.rejection_reason or "Rejected",
                    ],
                    start_to_close_timeout=timedelta(seconds=30),
                )
            return SinglePaymentResult(
                entry_id=self._entry.entry_id,
                state=self._entry.state,
                rejection_reason=self._entry.rejection_reason,
            )

        # ── Stage 3: post approved payment to ledger ──────────────────────────
        await workflow.execute_activity(
            post_to_ledger_db,
            args=[
                workflow.info().workflow_id,
                self._entry.entry_id,
                self._entry.amount,
                self._entry.entry_type.value,
                EntryState.APPROVED.value,
                "",  # account balance not tracked per-payment
            ],
            start_to_close_timeout=timedelta(seconds=30),
        )

        self._entry.state = EntryState.POSTED

        await workflow.execute_activity(
            send_payment_confirmation,
            args=[
                workflow.info().workflow_id,
                self._entry.entry_id,
                self._entry.reference,  # use reference as recipient identifier
                self._entry.amount,
                "",  # account balance not tracked per-payment
            ],
            start_to_close_timeout=timedelta(seconds=15),
        )

        return SinglePaymentResult(
            entry_id=self._entry.entry_id,
            state=self._entry.state,
            approved_by=self._entry.approved_by,
        )

    # -------------------------------------------------------------------------
    # Updates
    # -------------------------------------------------------------------------

    @workflow.update
    async def fraud_check_passed(self, result: FraudCheckResult) -> FraudClearanceResult:
        """Called by the external fraud service with its screening decision."""
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
            self._entry.fraud_metadata = result
            # Notify approver and advance to AWAITING_APPROVAL in one step.
            await workflow.execute_activity(
                notify_approver,
                args=[
                    workflow.info().workflow_id,
                    self._entry.entry_id,
                    "unassigned",  # approver pool — assign via your routing logic
                    self._entry.amount,
                    self._entry.reference,
                ],
                start_to_close_timeout=timedelta(seconds=30),
            )
            self._entry.state = EntryState.AWAITING_APPROVAL
        else:
            self._entry.state = EntryState.FRAUD_REJECTED

        return FraudClearanceResult(
            entry_id=result.entry_id,
            new_state=self._entry.state,
            new_balance="",  # not tracked per-payment
        )

    @fraud_check_passed.validator
    def _validate_fraud_check(self, result: FraudCheckResult) -> None:
        if self._entry is None:
            raise ValueError("Workflow not yet initialised")
        if result.entry_id != self._entry.entry_id:
            raise ValueError(
                f"Entry ID mismatch: expected {self._entry.entry_id}, got {result.entry_id}"
            )
        if self._entry.state != EntryState.PENDING:
            raise ValueError(
                f"Fraud check only valid on PENDING entries; got {self._entry.state}"
            )

    # -------------------------------------------------------------------------

    @workflow.update
    async def approve_payment(self, req: ApprovePaymentRequest) -> ApprovePaymentResult:
        """Called by the approver to approve the payment."""
        self._entry.state = EntryState.APPROVED
        self._entry.approved_by = req.approved_by
        return ApprovePaymentResult(
            entry_id=req.entry_id,
            state=self._entry.state,
            new_balance="",  # not tracked per-payment
        )

    @approve_payment.validator
    def _validate_approve_payment(self, req: ApprovePaymentRequest) -> None:
        if self._entry is None:
            raise ValueError("Workflow not yet initialised")
        if self._entry.state != EntryState.AWAITING_APPROVAL:
            raise ValueError(
                f"Can only approve AWAITING_APPROVAL entries; got {self._entry.state}"
            )

    # -------------------------------------------------------------------------

    @workflow.update
    async def reject_payment(self, req: RejectPaymentRequest) -> RejectPaymentResult:
        """Called by the approver to reject the payment."""
        self._entry.state = EntryState.REJECTED
        self._entry.rejection_reason = req.reason
        return RejectPaymentResult(entry_id=req.entry_id, state=self._entry.state)

    @reject_payment.validator
    def _validate_reject_payment(self, req: RejectPaymentRequest) -> None:
        if self._entry is None:
            raise ValueError("Workflow not yet initialised")
        if self._entry.state != EntryState.AWAITING_APPROVAL:
            raise ValueError(
                f"Can only reject AWAITING_APPROVAL entries; got {self._entry.state}"
            )

    # -------------------------------------------------------------------------
    # Queries
    # -------------------------------------------------------------------------

    @workflow.query
    def get_state(self) -> EntryState:
        return self._entry.state if self._entry else EntryState.PENDING

    @workflow.query
    def get_entry(self) -> LedgerEntry | None:
        return self._entry
