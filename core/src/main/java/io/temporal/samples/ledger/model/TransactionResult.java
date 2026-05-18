package io.temporal.samples.ledger.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/** The result returned when a transaction workflow completes. */
public class TransactionResult {

  private final String transactionId;
  private final TransactionStatus status;
  private final String message;
  private final String ledgerEntryId;

  @JsonCreator
  public TransactionResult(
      @JsonProperty("transactionId") String transactionId,
      @JsonProperty("status") TransactionStatus status,
      @JsonProperty("message") String message,
      @JsonProperty("ledgerEntryId") String ledgerEntryId) {
    this.transactionId = transactionId;
    this.status = status;
    this.message = message;
    this.ledgerEntryId = ledgerEntryId;
  }

  public String getTransactionId() {
    return transactionId;
  }

  public TransactionStatus getStatus() {
    return status;
  }

  public String getMessage() {
    return message;
  }

  public String getLedgerEntryId() {
    return ledgerEntryId;
  }
}
