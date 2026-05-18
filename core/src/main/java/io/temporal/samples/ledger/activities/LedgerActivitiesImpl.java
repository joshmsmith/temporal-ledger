package io.temporal.samples.ledger.activities;

import io.temporal.activity.Activity;
import io.temporal.samples.ledger.model.AccountState;
import io.temporal.samples.ledger.model.CreateAccountRequest;
import io.temporal.samples.ledger.model.Money;
import io.temporal.samples.ledger.model.TransactionRequest;
import io.temporal.samples.ledger.model.TransactionStatus;
import java.math.BigDecimal;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stub implementation of {@link LedgerActivities}.
 *
 * <p>All methods log their operation and simulate work with a short sleep. Replace each method body
 * with real integrations (database, payment rails, fraud API, notification service, etc.) for
 * production use.
 */
public class LedgerActivitiesImpl implements LedgerActivities {

  private static final Logger log = LoggerFactory.getLogger(LedgerActivitiesImpl.class);

  @Override
  public String createAccount(String idempotencyKey, CreateAccountRequest request) {
    log.info(
        "[createAccount] key={} accountId={} owner={} type={}",
        idempotencyKey,
        request.getAccountId(),
        request.getOwnerId(),
        request.getAccountType());
    sleep(200);
    return request.getAccountId();
  }

  @Override
  public boolean validateTransaction(String idempotencyKey, TransactionRequest request) {
    log.info(
        "[validateTransaction] key={} txnId={} type={} amount={}",
        idempotencyKey,
        request.getTransactionId(),
        request.getType(),
        request.getAmount());
    sleep(150);
    // TODO: call real validation service
    return true;
  }

  @Override
  public boolean screenTransaction(String idempotencyKey, TransactionRequest request) {
    log.info(
        "[screenTransaction] key={} txnId={}", idempotencyKey, request.getTransactionId());
    // Heartbeat so Temporal knows we are alive during longer checks
    Activity.getExecutionContext().heartbeat("screening");
    sleep(500);
    // TODO: call real fraud / AML screening API
    return true;
  }

  @Override
  public String reserveFunds(String idempotencyKey, String accountId, Money amount) {
    log.info(
        "[reserveFunds] key={} accountId={} amount={}", idempotencyKey, accountId, amount);
    sleep(150);
    // TODO: call real account store to place a hold
    return "HOLD-" + UUID.randomUUID();
  }

  @Override
  public String postLedgerEntry(
      String idempotencyKey, TransactionRequest request, String reservationToken) {
    log.info(
        "[postLedgerEntry] key={} txnId={} hold={}",
        idempotencyKey,
        request.getTransactionId(),
        reservationToken);
    sleep(300);
    // TODO: write double-entry records to ledger DB
    return "ENTRY-" + UUID.randomUUID();
  }

  @Override
  public void releaseFunds(String idempotencyKey, String reservationToken) {
    log.info("[releaseFunds] key={} hold={}", idempotencyKey, reservationToken);
    sleep(150);
    // TODO: release hold in account store
  }

  @Override
  public void reverseLedgerEntry(String idempotencyKey, String ledgerEntryId) {
    log.info("[reverseLedgerEntry] key={} entryId={}", idempotencyKey, ledgerEntryId);
    sleep(200);
    // TODO: write reversal record in ledger DB
  }

  @Override
  public void notifyParties(String idempotencyKey, TransactionRequest request, String status) {
    log.info(
        "[notifyParties] key={} txnId={} status={}", idempotencyKey, request.getTransactionId(), status);
    sleep(100);
    // TODO: send email / push notification
  }

  @Override
  public void logAuditEvent(String transactionId, String event, String detail) {
    log.info("[audit] txnId={} event={} detail={}", transactionId, event, detail);
    // TODO: persist to immutable audit log / SIEM
  }

  @Override
  public AccountState getAccountState(String accountId) {
    log.info("[getAccountState] accountId={}", accountId);
    sleep(100);
    // TODO: fetch from real account store
    return new AccountState(accountId, "owner-stub", "CHECKING", BigDecimal.ZERO, "USD", true);
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private static void sleep(long ms) {
    try {
      Thread.sleep(ms);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
