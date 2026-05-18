package io.temporal.samples.ledger.workflow;

import io.temporal.activity.ActivityOptions;
import io.temporal.common.RetryOptions;
import io.temporal.samples.ledger.activities.LedgerActivities;
import io.temporal.samples.ledger.model.TransactionRequest;
import io.temporal.samples.ledger.model.TransactionResult;
import io.temporal.samples.ledger.model.TransactionStatus;
import io.temporal.workflow.Workflow;
import java.time.Duration;
import org.slf4j.Logger;

public class TransactionWorkflowImpl implements TransactionWorkflow {

  private static final Logger log = Workflow.getLogger(TransactionWorkflowImpl.class);

  private TransactionStatus status = TransactionStatus.PENDING;

  private final LedgerActivities activities =
      Workflow.newActivityStub(
          LedgerActivities.class,
          ActivityOptions.newBuilder()
              .setStartToCloseTimeout(Duration.ofMinutes(2))
              .setScheduleToCloseTimeout(Duration.ofHours(1))
              .setRetryOptions(
                  RetryOptions.newBuilder()
                      .setInitialInterval(Duration.ofSeconds(1))
                      .setMaximumInterval(Duration.ofSeconds(30))
                      .setBackoffCoefficient(2.0)
                      .build())
              .build());

  // Screening may be longer-running and must heartbeat
  private final LedgerActivities screeningActivities =
      Workflow.newActivityStub(
          LedgerActivities.class,
          ActivityOptions.newBuilder()
              .setStartToCloseTimeout(Duration.ofMinutes(10))
              .setScheduleToCloseTimeout(Duration.ofHours(2))
              .setHeartbeatTimeout(Duration.ofSeconds(30))
              .setRetryOptions(
                  RetryOptions.newBuilder()
                      .setInitialInterval(Duration.ofSeconds(2))
                      .setMaximumInterval(Duration.ofMinutes(2))
                      .build())
              .build());

  @Override
  public TransactionResult processTransaction(TransactionRequest request) {
    String txnId = request.getTransactionId();
    String idempotencyKey = Workflow.randomUUID().toString();

    log.info("Starting transaction: {}", txnId);
    activities.logAuditEvent(txnId, "STARTED", request.getType().name());

    // --- Validate ---
    status = TransactionStatus.VALIDATING;
    boolean valid = activities.validateTransaction(idempotencyKey, request);
    if (!valid) {
      status = TransactionStatus.FAILED;
      activities.logAuditEvent(txnId, "VALIDATION_FAILED", "Transaction rejected by validation");
      return new TransactionResult(txnId, TransactionStatus.FAILED, "Validation failed", null);
    }

    // --- Fraud / AML Screen ---
    boolean cleared = screeningActivities.screenTransaction(idempotencyKey, request);
    if (!cleared) {
      status = TransactionStatus.FAILED;
      activities.logAuditEvent(txnId, "SCREENING_FAILED", "Transaction flagged by screening");
      return new TransactionResult(txnId, TransactionStatus.FAILED, "Screening failed", null);
    }

    // --- Reserve funds (for transfers and withdrawals) ---
    status = TransactionStatus.PROCESSING;
    String reservationToken = null;
    if (request.getSourceAccountId() != null) {
      reservationToken =
          activities.reserveFunds(idempotencyKey, request.getSourceAccountId(), request.getAmount());
      activities.logAuditEvent(txnId, "FUNDS_RESERVED", reservationToken);
    }

    // --- Post ledger entry ---
    String ledgerEntryId;
    try {
      ledgerEntryId = activities.postLedgerEntry(idempotencyKey, request, reservationToken);
    } catch (Exception e) {
      // Release the hold if posting fails
      if (reservationToken != null) {
        activities.releaseFunds(idempotencyKey, reservationToken);
      }
      status = TransactionStatus.FAILED;
      activities.logAuditEvent(txnId, "POST_FAILED", e.getMessage());
      return new TransactionResult(txnId, TransactionStatus.FAILED, "Ledger post failed", null);
    }

    // --- Notify ---
    activities.notifyParties(idempotencyKey, request, "COMPLETED");
    activities.logAuditEvent(txnId, "COMPLETED", "ledgerEntryId=" + ledgerEntryId);

    status = TransactionStatus.COMPLETED;
    log.info("Transaction completed: {} entryId={}", txnId, ledgerEntryId);
    return new TransactionResult(txnId, TransactionStatus.COMPLETED, "Success", ledgerEntryId);
  }

  @Override
  public TransactionStatus getStatus() {
    return status;
  }
}
