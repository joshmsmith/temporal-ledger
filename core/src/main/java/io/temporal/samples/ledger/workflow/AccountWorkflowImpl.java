package io.temporal.samples.ledger.workflow;

import io.temporal.activity.ActivityOptions;
import io.temporal.common.RetryOptions;
import io.temporal.samples.ledger.activities.LedgerActivities;
import io.temporal.samples.ledger.model.AccountState;
import io.temporal.samples.ledger.model.CreateAccountRequest;
import io.temporal.samples.ledger.model.TransactionRequest;
import io.temporal.workflow.Workflow;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Queue;
import org.slf4j.Logger;

/**
 * Long-running account workflow. Maintains current balance in memory (durable via event history /
 * replay) and processes inbound transaction signals sequentially.
 *
 * <p>Uses continue-as-new when history grows large to keep replay fast.
 */
public class AccountWorkflowImpl implements AccountWorkflow {

  private static final Logger log = Workflow.getLogger(AccountWorkflowImpl.class);
  private static final int CONTINUE_AS_NEW_THRESHOLD = 500;

  private final LedgerActivities activities =
      Workflow.newActivityStub(
          LedgerActivities.class,
          ActivityOptions.newBuilder()
              .setStartToCloseTimeout(Duration.ofMinutes(1))
              .setRetryOptions(
                  RetryOptions.newBuilder()
                      .setInitialInterval(Duration.ofSeconds(1))
                      .setMaximumInterval(Duration.ofSeconds(30))
                      .build())
              .build());

  // Mutable workflow state — persisted via event history
  private String accountId;
  private String ownerId;
  private String accountType;
  private BigDecimal balance = BigDecimal.ZERO;
  private String currency = "USD";
  private boolean active = false;
  private boolean closeRequested = false;
  private String closeReason;

  private final Queue<TransactionRequest> pendingTransactions = new ArrayDeque<>();

  @Override
  public void openAccount(CreateAccountRequest request) {
    this.accountId = request.getAccountId();
    this.ownerId = request.getOwnerId();
    this.accountType = request.getAccountType();
    if (request.getInitialBalance() != null) {
      this.balance = request.getInitialBalance().getAmount();
      this.currency = request.getInitialBalance().getCurrency();
    }

    String idempotencyKey = Workflow.randomUUID().toString();
    activities.createAccount(idempotencyKey, request);
    activities.logAuditEvent(accountId, "ACCOUNT_OPENED", "type=" + accountType);
    this.active = true;

    log.info("Account opened: {}", accountId);

    // Main processing loop — runs until close is requested
    while (!closeRequested) {
      Workflow.await(() -> !pendingTransactions.isEmpty() || closeRequested);

      while (!pendingTransactions.isEmpty() && active) {
        TransactionRequest txn = pendingTransactions.poll();
        processTransaction(txn);

        // Continue-as-new when history is large
        if (Workflow.getInfo().getHistoryLength() > CONTINUE_AS_NEW_THRESHOLD) {
          log.info("Continuing-as-new for account: {}", accountId);
          Workflow.continueAsNew(request);
        }
      }
    }

    activities.logAuditEvent(accountId, "ACCOUNT_CLOSED", closeReason);
    this.active = false;
    log.info("Account closed: {} reason={}", accountId, closeReason);
  }

  @Override
  public void submitTransaction(TransactionRequest request) {
    if (active) {
      pendingTransactions.add(request);
    } else {
      log.warn("Transaction {} rejected — account {} is not active", request.getTransactionId(), accountId);
    }
  }

  @Override
  public void closeAccount(String reason) {
    this.closeReason = reason;
    this.closeRequested = true;
  }

  @Override
  public AccountState getAccountState() {
    return new AccountState(accountId, ownerId, accountType, balance, currency, active);
  }

  // ---------------------------------------------------------------------------
  // Private helpers
  // ---------------------------------------------------------------------------

  private void processTransaction(TransactionRequest txn) {
    log.info("Processing transaction {} on account {}", txn.getTransactionId(), accountId);
    // Delegate heavy lifting to a child TransactionWorkflow
    TransactionWorkflow child =
        Workflow.newChildWorkflowStub(
            TransactionWorkflow.class,
            io.temporal.workflow.ChildWorkflowOptions.newBuilder()
                .setWorkflowId("TXN-" + txn.getTransactionId())
                .build());
    var result = child.processTransaction(txn);

    if (result.getStatus() == io.temporal.samples.ledger.model.TransactionStatus.COMPLETED) {
      // Update in-memory balance
      switch (txn.getType()) {
        case DEPOSIT:
          balance = balance.add(txn.getAmount().getAmount());
          break;
        case WITHDRAWAL:
        case FEE:
          balance = balance.subtract(txn.getAmount().getAmount());
          break;
        case TRANSFER:
          if (accountId.equals(txn.getSourceAccountId())) {
            balance = balance.subtract(txn.getAmount().getAmount());
          } else {
            balance = balance.add(txn.getAmount().getAmount());
          }
          break;
        default:
          break;
      }
      log.info("Balance updated for {}: {}", accountId, balance);
    }
  }
}
