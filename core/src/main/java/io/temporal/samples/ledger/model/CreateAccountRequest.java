package io.temporal.samples.ledger.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/** Input for creating a new ledger account. */
public class CreateAccountRequest {

  private final String accountId;
  private final String ownerId;
  private final String accountType; // e.g. "CHECKING", "SAVINGS", "ESCROW"
  private final Money initialBalance;

  @JsonCreator
  public CreateAccountRequest(
      @JsonProperty("accountId") String accountId,
      @JsonProperty("ownerId") String ownerId,
      @JsonProperty("accountType") String accountType,
      @JsonProperty("initialBalance") Money initialBalance) {
    this.accountId = accountId;
    this.ownerId = ownerId;
    this.accountType = accountType;
    this.initialBalance = initialBalance;
  }

  public String getAccountId() {
    return accountId;
  }

  public String getOwnerId() {
    return ownerId;
  }

  public String getAccountType() {
    return accountType;
  }

  public Money getInitialBalance() {
    return initialBalance;
  }
}
