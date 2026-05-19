from __future__ import annotations

import json
import os
import sqlite3
import threading
from datetime import datetime, timezone

from temporalio import activity

_DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "ledger.db")
_thread_local = threading.local()


def _db() -> sqlite3.Connection:
    """Return a thread-local SQLite connection, creating the schema on first use."""
    if not hasattr(_thread_local, "conn"):
        db_path = os.path.abspath(_DB_PATH)
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        conn = sqlite3.connect(db_path, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS ledger_entries (
                idempotency_key TEXT PRIMARY KEY,
                workflow_id     TEXT NOT NULL,
                entry_id        TEXT NOT NULL,
                amount          TEXT NOT NULL,
                entry_type      TEXT NOT NULL,
                state           TEXT NOT NULL,
                balance         TEXT NOT NULL,
                created_at      TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS approvals (
                idempotency_key TEXT PRIMARY KEY,
                approver_id     TEXT NOT NULL,
                entry_id        TEXT NOT NULL,
                amount          TEXT NOT NULL,
                reference       TEXT NOT NULL,
                notified_at     TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS confirmations (
                idempotency_key TEXT PRIMARY KEY,
                account_id      TEXT NOT NULL,
                amount          TEXT NOT NULL,
                balance         TEXT NOT NULL,
                confirmed_at    TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS fraud_audit (
                idempotency_key TEXT PRIMARY KEY,
                entry_id        TEXT NOT NULL,
                risk_score      REAL NOT NULL,
                flags_json      TEXT NOT NULL,
                passed          INTEGER NOT NULL,
                logged_at       TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS fund_reservations (
                idempotency_key TEXT PRIMARY KEY,
                workflow_id     TEXT NOT NULL,
                entry_id        TEXT NOT NULL,
                amount          TEXT NOT NULL,
                entry_type      TEXT NOT NULL,
                status          TEXT NOT NULL,  -- RESERVED or RELEASED
                reason          TEXT NOT NULL DEFAULT '',
                created_at      TEXT NOT NULL
            );
            """
        )
        conn.commit()
        _thread_local.conn = conn
    return _thread_local.conn


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


@activity.defn
def post_to_ledger_db(
    workflow_id: str,
    entry_id: str,
    amount: str,
    entry_type: str,
    state: str,
    new_balance: str,
) -> bool:
    """
    Upsert an approved ledger entry and its resulting balance.
    Idempotency key: (workflow_id, entry_id).
    Accepts plain string fields to avoid dataclass deserialization edge-cases.
    """
    idempotency_key = f"{workflow_id}:{entry_id}"
    conn = _db()
    conn.execute(
        """
        INSERT OR IGNORE INTO ledger_entries
            (idempotency_key, workflow_id, entry_id, amount, entry_type, state, balance, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (idempotency_key, workflow_id, entry_id, amount, entry_type, state, new_balance, _now()),
    )
    conn.commit()
    activity.logger.info(
        "post_to_ledger_db: entry=%s workflow=%s balance=%s",
        entry_id, workflow_id, new_balance,
    )
    return True


@activity.defn
def notify_approver(
    workflow_id: str,
    entry_id: str,
    approver_id: str,
    amount: str,
    reference: str,
) -> bool:
    """
    Record an approval notification.
    Idempotency key: (workflow_id, entry_id, approver_id).
    """
    idempotency_key = f"{workflow_id}:{entry_id}:{approver_id}"
    conn = _db()
    conn.execute(
        """
        INSERT OR IGNORE INTO approvals
            (idempotency_key, approver_id, entry_id, amount, reference, notified_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (idempotency_key, approver_id, entry_id, amount, reference, _now()),
    )
    conn.commit()
    activity.logger.info(
        "notify_approver: approver=%s entry=%s amount=%s",
        approver_id, entry_id, amount,
    )
    return True


@activity.defn
def send_payment_confirmation(
    workflow_id: str,
    entry_id: str,
    account_id: str,
    amount: str,
    new_balance: str,
) -> bool:
    """
    Record a payment confirmation sent to the account holder.
    Idempotency key: (workflow_id, entry_id).
    """
    idempotency_key = f"{workflow_id}:{entry_id}"
    conn = _db()
    conn.execute(
        """
        INSERT OR IGNORE INTO confirmations
            (idempotency_key, account_id, amount, balance, confirmed_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (idempotency_key, account_id, amount, new_balance, _now()),
    )
    conn.commit()
    activity.logger.info(
        "send_payment_confirmation: account=%s entry=%s new_balance=%s",
        account_id, entry_id, new_balance,
    )
    return True


@activity.defn
def log_fraud_clearance(
    workflow_id: str,
    entry_id: str,
    risk_score: float,
    flags: list,
    passed: bool,
) -> bool:
    """
    Write a fraud-check result to the audit log.
    Idempotency key: (workflow_id, entry_id).
    """
    idempotency_key = f"{workflow_id}:{entry_id}"
    conn = _db()
    conn.execute(
        """
        INSERT OR IGNORE INTO fraud_audit
            (idempotency_key, entry_id, risk_score, flags_json, passed, logged_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (idempotency_key, entry_id, risk_score, json.dumps(flags), 1 if passed else 0, _now()),
    )
    conn.commit()
    activity.logger.info(
        "log_fraud_clearance: entry=%s passed=%s risk_score=%s",
        entry_id, passed, risk_score,
    )
    return True


@activity.defn
def reserve_funds(
    workflow_id: str,
    entry_id: str,
    amount: str,
    entry_type: str,
) -> bool:
    """
    Record a funds reservation for a debit payment.
    Idempotency key: reserve:{workflow_id}:{entry_id}.
    """
    idempotency_key = f"reserve:{workflow_id}:{entry_id}"
    conn = _db()
    conn.execute(
        """
        INSERT OR IGNORE INTO fund_reservations
            (idempotency_key, workflow_id, entry_id, amount, entry_type, status, created_at)
        VALUES (?, ?, ?, ?, ?, 'RESERVED', ?)
        """,
        (idempotency_key, workflow_id, entry_id, amount, entry_type, _now()),
    )
    conn.commit()
    activity.logger.info(
        "reserve_funds: entry=%s amount=%s type=%s",
        entry_id, amount, entry_type,
    )
    return True


@activity.defn
def release_funds(
    workflow_id: str,
    entry_id: str,
    amount: str,
    entry_type: str,
    reason: str,
) -> bool:
    """
    Record a funds release (reservation voided due to rejection/fraud failure).
    Idempotency key: release:{workflow_id}:{entry_id}.
    """
    idempotency_key = f"release:{workflow_id}:{entry_id}"
    conn = _db()
    conn.execute(
        """
        INSERT OR IGNORE INTO fund_reservations
            (idempotency_key, workflow_id, entry_id, amount, entry_type, status, reason, created_at)
        VALUES (?, ?, ?, ?, ?, 'RELEASED', ?, ?)
        """,
        (idempotency_key, workflow_id, entry_id, amount, entry_type, reason, _now()),
    )
    conn.commit()
    activity.logger.info(
        "release_funds: entry=%s amount=%s reason=%s",
        entry_id, amount, reason,
    )
    return True


@activity.defn
def reconcile_external(
    workflow_id: str,
    account_id: str,
    balance: str,
) -> dict:
    """
    Reconcile the workflow's in-memory balance against the last posted entry in
    the ledger DB.  Returns {"matched": bool, "external_balance": str,
    "discrepancies": list}.
    """
    conn = _db()
    row = conn.execute(
        """
        SELECT balance FROM ledger_entries
        WHERE workflow_id = ?
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (workflow_id,),
    ).fetchone()

    external_balance = row[0] if row else "0.00"
    matched = external_balance == balance
    discrepancies = (
        [] if matched
        else [f"workflow_balance={balance}, db_balance={external_balance}"]
    )
    activity.logger.info(
        "reconcile_external: account=%s matched=%s workflow=%s db=%s",
        account_id, matched, balance, external_balance,
    )
    return {"matched": matched, "external_balance": external_balance, "discrepancies": discrepancies}
