package io.temporal.samples.ledger;

import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.samples.ledger.model.CreateAccountRequest;
import io.temporal.samples.ledger.model.Money;
import io.temporal.samples.ledger.model.TransactionRequest;
import io.temporal.samples.ledger.model.TransactionResult;
import io.temporal.samples.ledger.model.TransactionType;
import io.temporal.samples.ledger.workflow.AccountWorkflow;
import io.temporal.samples.ledger.workflow.TransactionWorkflow;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.serviceclient.WorkflowServiceStubsOptions;
import java.math.BigDecimal;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Quick-start client that opens an account and submits a sample deposit transaction.
 *
 * <p>Run with: {@code ./gradlew -q execute -PmainClass=io.temporal.samples.ledger.LedgerStarter}
 */
public class LedgerStarter {

  private static final Logger log = LoggerFactory.getLogger(LedgerStarter.class);

  public static void main(String[] args) {
    String address = System.getenv().getOrDefault("TEMPORAL_ADDRESS", "localhost:7233");
    String namespace = System.getenv().getOrDefault("TEMPORAL_NAMESPACE", "default");

    WorkflowServiceStubs service =
        WorkflowServiceStubs.newServiceStubs(
            WorkflowServiceStubsOptions.newBuilder().setTarget(address).build());

    WorkflowClient client =
        WorkflowClient.newInstance(
            service,
            io.temporal.client.WorkflowClientOptions.newBuilder()
                .setNamespace(namespace)
                .build());

    // --- Open an account ---
    String accountId = "acct-" + UUID.randomUUID().toString().substring(0, 8);
    CreateAccountRequest openReq =
        new CreateAccountRequest(
            accountId, "owner-001", "CHECKING", new Money(BigDecimal.ZERO, "USD"));

    AccountWorkflow accountWf =
        client.newWorkflowStub(
            AccountWorkflow.class,
            WorkflowOptions.newBuilder()
                .setWorkflowId(LedgerConstants.ACCOUNT_WORKFLOW_ID_PREFIX + accountId)
                .setTaskQueue(LedgerConstants.TASK_QUEUE)
                .build());

    WorkflowClient.start(accountWf::openAccount, openReq);
    log.info("Account workflow started for accountId={}", accountId);

    // --- Submit a deposit ---
    String txnId = "txn-" + UUID.randomUUID().toString().substring(0, 8);
    TransactionRequest deposit =
        new TransactionRequest(
            txnId,
            TransactionType.DEPOSIT,
            null,
            accountId,
            new Money(new BigDecimal("500.00"), "USD"),
            "Initial deposit");

    TransactionWorkflow txnWf =
        client.newWorkflowStub(
            TransactionWorkflow.class,
            WorkflowOptions.newBuilder()
                .setWorkflowId(LedgerConstants.TRANSACTION_WORKFLOW_ID_PREFIX + txnId)
                .setTaskQueue(LedgerConstants.TASK_QUEUE)
                .build());

    TransactionResult result = txnWf.processTransaction(deposit);
    log.info(
        "Transaction result: id={} status={} entry={}",
        result.getTransactionId(),
        result.getStatus(),
        result.getLedgerEntryId());
  }
}
