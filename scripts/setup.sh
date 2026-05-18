#!/usr/bin/env bash
set -euo pipefail

echo "Registering custom search attributes for temporal-ledger..."

temporal operator search-attribute create --name LedgerAccountId       --type Keyword
temporal operator search-attribute create --name LedgerTxnStatus        --type Keyword
temporal operator search-attribute create --name LedgerTxnType          --type Keyword

echo "Done."
