package io.temporal.samples.ledger.model;

/** The type of a ledger transaction. */
public enum TransactionType {
  DEPOSIT,
  WITHDRAWAL,
  TRANSFER,
  FEE,
  REVERSAL
}
