package io.temporal.samples.ledger.workflow;

import io.temporal.workflow.QueryMethod;
import io.temporal.workflow.SignalMethod;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;
import io.temporal.samples.ledger.model.AccountState;
import io.temporal.samples.ledger.model.CreateAccountRequest;
import io.temporal.samples.ledger.model.TransactionRequest;
import io.temporal.samples.ledger.model.TransactionResult;

/**
 * Long-running workflow that represents the lifecycle of a single ledger account.
 *
 * <p>An account workflow runs indefinitely (using continue-as-new to cap history size). It
 * processes incoming transaction requests sequentially, maintaining a consistent balance.
 *
 * <p>WorkflowId convention: {@code ACCT-<accountId>}
 */
@WorkflowInterface
public interface AccountWorkflow {

  @WorkflowMethod
  void openAccount(CreateAccountRequest request);

  /** Submit a transaction against this account (deposit, withdrawal, or transfer). */
  @SignalMethod
  void submitTransaction(TransactionRequest request);

  /** Close the account, rejecting any further transactions. */
  @SignalMethod
  void closeAccount(String reason);

  /** Query the current balance and status of the account. */
  @QueryMethod
  AccountState getAccountState();
}
