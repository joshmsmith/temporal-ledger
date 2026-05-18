package io.temporal.samples.ledger.model;

/** The status of a ledger transaction workflow. */
public enum TransactionStatus {
  PENDING,
  VALIDATING,
  PROCESSING,
  COMPLETED,
  FAILED,
  REVERSED
}
