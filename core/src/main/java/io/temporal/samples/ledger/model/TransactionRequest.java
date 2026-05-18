package io.temporal.samples.ledger.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/** Input for a ledger transaction (deposit, withdrawal, or transfer). */
public class TransactionRequest {

  private final String transactionId;
  private final TransactionType type;
  private final String sourceAccountId; // null for DEPOSIT
  private final String destinationAccountId; // null for WITHDRAWAL
  private final Money amount;
  private final String description;

  @JsonCreator
  public TransactionRequest(
      @JsonProperty("transactionId") String transactionId,
      @JsonProperty("type") TransactionType type,
      @JsonProperty("sourceAccountId") String sourceAccountId,
      @JsonProperty("destinationAccountId") String destinationAccountId,
      @JsonProperty("amount") Money amount,
      @JsonProperty("description") String description) {
    this.transactionId = transactionId;
    this.type = type;
    this.sourceAccountId = sourceAccountId;
    this.destinationAccountId = destinationAccountId;
    this.amount = amount;
    this.description = description;
  }

  public String getTransactionId() {
    return transactionId;
  }

  public TransactionType getType() {
    return type;
  }

  public String getSourceAccountId() {
    return sourceAccountId;
  }

  public String getDestinationAccountId() {
    return destinationAccountId;
  }

  public Money getAmount() {
    return amount;
  }

  public String getDescription() {
    return description;
  }
}
