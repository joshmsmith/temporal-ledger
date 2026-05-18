package io.temporal.samples.ledger;

public final class LedgerConstants {

  public static final String TASK_QUEUE = "ledger";

  // Workflow type IDs
  public static final String TRANSACTION_WORKFLOW_ID_PREFIX = "TXN-";
  public static final String ACCOUNT_WORKFLOW_ID_PREFIX = "ACCT-";

  // Search attribute names (register via scripts/setup.sh)
  public static final String SEARCH_ATTR_ACCOUNT_ID = "LedgerAccountId";
  public static final String SEARCH_ATTR_TRANSACTION_STATUS = "LedgerTxnStatus";
  public static final String SEARCH_ATTR_TRANSACTION_TYPE = "LedgerTxnType";

  private LedgerConstants() {}
}
