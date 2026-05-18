package io.temporal.samples.ledger;

import io.temporal.client.WorkflowClient;
import io.temporal.samples.ledger.activities.LedgerActivitiesImpl;
import io.temporal.samples.ledger.workflow.AccountWorkflowImpl;
import io.temporal.samples.ledger.workflow.TransactionWorkflowImpl;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.serviceclient.WorkflowServiceStubsOptions;
import io.temporal.worker.Worker;
import io.temporal.worker.WorkerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Long-running process that polls Temporal for workflow and activity tasks on the {@code ledger}
 * task queue.
 *
 * <p>Run with: {@code ./gradlew -q execute -PmainClass=io.temporal.samples.ledger.LedgerWorker}
 *
 * <p>Configure via environment variables:
 *
 * <ul>
 *   <li>{@code TEMPORAL_ADDRESS} — gRPC endpoint (default: {@code localhost:7233})
 *   <li>{@code TEMPORAL_NAMESPACE} — namespace (default: {@code default})
 * </ul>
 */
public class LedgerWorker {

  private static final Logger log = LoggerFactory.getLogger(LedgerWorker.class);

  public static void main(String[] args) {
    String address =
        System.getenv().getOrDefault("TEMPORAL_ADDRESS", "localhost:7233");
    String namespace =
        System.getenv().getOrDefault("TEMPORAL_NAMESPACE", "default");

    log.info("Connecting to Temporal at {} namespace={}", address, namespace);

    WorkflowServiceStubs service =
        WorkflowServiceStubs.newServiceStubs(
            WorkflowServiceStubsOptions.newBuilder().setTarget(address).build());

    WorkflowClient client = WorkflowClient.newInstance(service,
        io.temporal.client.WorkflowClientOptions.newBuilder()
            .setNamespace(namespace)
            .build());

    WorkerFactory factory = WorkerFactory.newInstance(client);
    Worker worker = factory.newWorker(LedgerConstants.TASK_QUEUE);

    worker.registerWorkflowImplementationTypes(
        TransactionWorkflowImpl.class, AccountWorkflowImpl.class);
    worker.registerActivitiesImplementations(new LedgerActivitiesImpl());

    factory.start();
    log.info("Ledger worker started on task queue '{}'", LedgerConstants.TASK_QUEUE);
  }
}
