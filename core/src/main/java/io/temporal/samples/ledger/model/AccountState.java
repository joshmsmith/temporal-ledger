package io.temporal.samples.ledger.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.math.BigDecimal;

/** A point-in-time snapshot of an account's state (returned by query). */
public class AccountState {

  private final String accountId;
  private final String ownerId;
  private final String accountType;
  private final BigDecimal balance;
  private final String currency;
  private final boolean active;

  @JsonCreator
  public AccountState(
      @JsonProperty("accountId") String accountId,
      @JsonProperty("ownerId") String ownerId,
      @JsonProperty("accountType") String accountType,
      @JsonProperty("balance") BigDecimal balance,
      @JsonProperty("currency") String currency,
      @JsonProperty("active") boolean active) {
    this.accountId = accountId;
    this.ownerId = ownerId;
    this.accountType = accountType;
    this.balance = balance;
    this.currency = currency;
    this.active = active;
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

  public BigDecimal getBalance() {
    return balance;
  }

  public String getCurrency() {
    return currency;
  }

  public boolean isActive() {
    return active;
  }
}
