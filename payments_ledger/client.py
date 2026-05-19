"""
Example client — per-payment workflow (primary sample)

Demonstrates ``PaymentWorkflow``: one workflow per payment, started for a
specific payment_id, progresses through fraud check and approval, then
completes when the payment is posted.

To see the entity-workflow (one workflow per account, runs indefinitely),
look at ``temporal_as_ledger.py`` and the "Entity workflow" section of the
README.
"""

import asyncio
import uuid

from temporalio.client import Client
from temporalio.contrib.pydantic import pydantic_data_converter

from payments_ledger.workflows.ledger_workflow import PaymentWorkflow
from payments_ledger.models import (
    ApprovePaymentRequest,
    EntryType,
    FraudCheckResult,
    PaymentRequest,
)

TASK_QUEUE = "payments-task-queue"


async def main() -> None:
    client = await Client.connect("localhost:7233", data_converter=pydantic_data_converter)

    payment_id = str(uuid.uuid4())
    req = PaymentRequest(
        entry_id=payment_id,
        amount="500.00",
        entry_type=EntryType.DEBIT,
        reference="INV-2024-001",
    )

    # ------------------------------------------------------------------
    # 1. Start a per-payment workflow.
    #    Workflow ID = payment:{payment_id} — one workflow per payment.
    #    The workflow starts PENDING and waits for external inputs.
    # ------------------------------------------------------------------
    handle = await client.start_workflow(
        PaymentWorkflow.run,
        req,
        id=f"payment:{payment_id}",
        task_queue=TASK_QUEUE,
    )
    print(f"Started:        payment:{payment_id}")

    # ------------------------------------------------------------------
    # 2. Fraud check callback (simulates external fraud service).
    #    On success the workflow notifies the approver and moves to
    #    AWAITING_APPROVAL automatically.
    # ------------------------------------------------------------------
    fraud_result = await handle.execute_update(
        PaymentWorkflow.fraud_check_passed,
        FraudCheckResult(
            entry_id=payment_id,
            passed=True,
            risk_score=0.12,
            flags=[],
            checked_by="fraud-service-v2",
        ),
    )
    print(f"Fraud check:    {fraud_result}")

    # ------------------------------------------------------------------
    # 3. Approver approves the payment.
    # ------------------------------------------------------------------
    approve_result = await handle.execute_update(
        PaymentWorkflow.approve_payment,
        ApprovePaymentRequest(
            entry_id=payment_id,
            approved_by="approver-jane",
            notes="Verified with vendor",
        ),
    )
    print(f"Approved:       {approve_result}")

    # ------------------------------------------------------------------
    # 4. Workflow posts to ledger and completes — await the final result.
    # ------------------------------------------------------------------
    result = await handle.result()
    print(f"Final state:    {result}")


if __name__ == "__main__":
    asyncio.run(main())
