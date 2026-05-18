package io.temporal.samples.ledger.workflow;

import io.temporal.workflow.QueryMethod;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;
import io.temporal.samples.ledger.model.TransactionRequest;
import io.temporal.samples.ledger.model.TransactionResult;
import io.temporal.samples.ledger.model.TransactionStatus;

/**
 * Workflow that durably processes a single ledger transaction: validate → screen → reserve →
 * post → notify.
 *
 * <p>WorkflowId convention: {@code TXN-<transactionId>}
 */
@WorkflowInterface
public interface TransactionWorkflow {

  @WorkflowMethod
  TransactionResult processTransaction(TransactionRequest request);

  /** Returns the current processing status (safe to call at any time). */
  @QueryMethod
  TransactionStatus getStatus();
}
