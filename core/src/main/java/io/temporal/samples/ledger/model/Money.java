package io.temporal.samples.ledger.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.math.BigDecimal;

/** Immutable value object representing a monetary amount with currency. */
public class Money {

  private final BigDecimal amount;
  private final String currency;

  @JsonCreator
  public Money(
      @JsonProperty("amount") BigDecimal amount, @JsonProperty("currency") String currency) {
    this.amount = amount;
    this.currency = currency;
  }

  public BigDecimal getAmount() {
    return amount;
  }

  public String getCurrency() {
    return currency;
  }

  @Override
  public String toString() {
    return amount + " " + currency;
  }
}
