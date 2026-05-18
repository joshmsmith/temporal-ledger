import asyncio

from temporalio.client import Client, WithStartWorkflowOperation
from temporalio.common import WorkflowIDConflictPolicy

from payments_ledger.workflows.ledger import PaymentLedgerWorkflow
from payments_ledger.models import (
    ApprovalRequest,
    ApprovePaymentRequest,
    FraudCheckResult,
    LedgerInit,
    PaymentRequest,
    EntryType,
)

TASK_QUEUE = "payments-task-queue"


async def main() -> None:
    client = await Client.connect("localhost:7233")

    account_id = "acct-001"
    ledger_id = f"ledger:{account_id}"

    # ------------------------------------------------------------------
    # 1. submit_payment via update-with-start
    #    Starts the ledger workflow if it doesn't exist; attaches if it does.
    # ------------------------------------------------------------------
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
    print(f"Submitted:          {payment_result}")

    handle = await start_op.workflow_handle()

    # ------------------------------------------------------------------
    # 2. fraud_check_passed  (simulating callback from fraud service)
    # ------------------------------------------------------------------
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
    print(f"Fraud check:        {fraud_result}")

    # ------------------------------------------------------------------
    # 3. request_approval
    # ------------------------------------------------------------------
    approval_req = await handle.execute_update(
        PaymentLedgerWorkflow.request_approval,
        ApprovalRequest(
            entry_id=payment_result.entry_id,
            approver_id="approver-jane",
        ),
    )
    print(f"Approval requested: {approval_req}")

    # ------------------------------------------------------------------
    # 4. approve_payment  (simulating approver clicking "Approve")
    # ------------------------------------------------------------------
    approve_result = await handle.execute_update(
        PaymentLedgerWorkflow.approve_payment,
        ApprovePaymentRequest(
            entry_id=payment_result.entry_id,
            approved_by="approver-jane",
            notes="Verified with vendor",
        ),
    )
    print(f"Approved:           {approve_result}")

    # ------------------------------------------------------------------
    # 5. Query balance
    # ------------------------------------------------------------------
    balance = await handle.query(PaymentLedgerWorkflow.get_balance)
    print(f"Balance:            {balance}")


if __name__ == "__main__":
    asyncio.run(main())
