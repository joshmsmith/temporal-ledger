import asyncio
import concurrent.futures

from temporalio.client import Client
from temporalio.worker import Worker

from payments_ledger.workflows.ledger import PaymentLedgerWorkflow
from payments_ledger.activities.ledger_activities import (
    log_fraud_clearance,
    notify_approver,
    post_to_ledger_db,
    send_payment_confirmation,
    reconcile_external,
)

TASK_QUEUE = "payments-task-queue"


async def main() -> None:
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
        print(f"Worker started, polling task queue: {TASK_QUEUE}")
        await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
