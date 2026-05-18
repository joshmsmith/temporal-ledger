package io.temporal.samples.ledger.activities;

import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;
import io.temporal.samples.ledger.model.AccountState;
import io.temporal.samples.ledger.model.CreateAccountRequest;
import io.temporal.samples.ledger.model.Money;
import io.temporal.samples.ledger.model.TransactionRequest;

/**
 * Activities that interact with external systems: the account store, ledger database, fraud
 * service, and notification service.
 *
 * <p>All mutating activities accept an {@code idempotencyKey} so retries are safe.
 */
@ActivityInterface
public interface LedgerActivities {

  /** Persist a newly opened account record. Returns the persisted account ID. */
  @ActivityMethod
  String createAccount(String idempotencyKey, CreateAccountRequest request);

  /** Validate the transaction — check limits, currency match, account status, etc. */
  @ActivityMethod
  boolean validateTransaction(String idempotencyKey, TransactionRequest request);

  /** Run a fraud / AML screening check. May be long-running; implementations should heartbeat. */
  @ActivityMethod
  boolean screenTransaction(String idempotencyKey, TransactionRequest request);

  /**
   * Reserve (hold) funds on the source account so the balance cannot be double-spent. Returns a
   * reservation token used to release or commit the hold.
   */
  @ActivityMethod
  String reserveFunds(String idempotencyKey, String accountId, Money amount);

  /**
   * Commit the double-entry ledger post: debit source, credit destination. Returns the ledger
   * entry ID.
   */
  @ActivityMethod
  String postLedgerEntry(String idempotencyKey, TransactionRequest request, String reservationToken);

  /** Release a previously reserved hold (called on failure / reversal paths). */
  @ActivityMethod
  void releaseFunds(String idempotencyKey, String reservationToken);

  /** Reverse a previously committed ledger entry. */
  @ActivityMethod
  void reverseLedgerEntry(String idempotencyKey, String ledgerEntryId);

  /** Notify account holders of a completed or failed transaction. */
  @ActivityMethod
  void notifyParties(String idempotencyKey, TransactionRequest request, String status);

  /** Write an immutable audit record for the transaction. */
  @ActivityMethod
  void logAuditEvent(String transactionId, String event, String detail);

  /** Fetch the current persisted state of an account. */
  @ActivityMethod
  AccountState getAccountState(String accountId);
}
