from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Optional
import uuid


class EntryState(str, Enum):
    PENDING = "PENDING"
    FRAUD_CLEARED = "FRAUD_CLEARED"
    FRAUD_REJECTED = "FRAUD_REJECTED"
    AWAITING_APPROVAL = "AWAITING_APPROVAL"
    APPROVED = "APPROVED"
    POSTED = "POSTED"
    REJECTED = "REJECTED"
    VOIDED = "VOIDED"


class EntryType(str, Enum):
    DEBIT = "DEBIT"
    CREDIT = "CREDIT"


@dataclass
class LedgerInit:
    account_id: str
    currency: str = "USD"
    opening_balance: str = "0.00"
    # Carry in-flight entries across Continue-as-New boundaries.
    open_entries: list = field(default_factory=list)   # list[LedgerEntry]
    # Carry reserved balance across Continue-as-New boundaries.
    reserved_balance: str = "0.00"


@dataclass
class PaymentRequest:
    amount: str                      # Decimal as string
    entry_type: EntryType = EntryType.DEBIT
    reference: str = ""
    entry_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    metadata: dict = field(default_factory=dict)


@dataclass
class PaymentResult:
    entry_id: str
    state: EntryState
    new_balance: str                 # Decimal as string


@dataclass
class ApprovalRequest:
    entry_id: str
    approver_id: str


@dataclass
class ApprovalRequestResult:
    entry_id: str
    state: EntryState               # AWAITING_APPROVAL


@dataclass
class ApprovePaymentRequest:
    entry_id: str
    approved_by: str
    notes: str = ""


@dataclass
class ApprovePaymentResult:
    entry_id: str
    state: EntryState               # APPROVED or POSTED
    new_balance: str


@dataclass
class RejectPaymentRequest:
    entry_id: str
    rejected_by: str
    reason: str


@dataclass
class RejectPaymentResult:
    entry_id: str
    state: EntryState               # REJECTED


@dataclass
class VoidPaymentRequest:
    entry_id: str
    voided_by: str
    reason: str


@dataclass
class VoidPaymentResult:
    entry_id: str
    state: EntryState               # VOIDED
    new_balance: str


@dataclass
class FraudCheckResult:
    entry_id: str
    passed: bool
    risk_score: float
    flags: list = field(default_factory=list)   # e.g. ["velocity_breach"]
    checked_by: str = "fraud-service"


@dataclass
class FraudClearanceResult:
    entry_id: str
    new_state: EntryState           # FRAUD_CLEARED or FRAUD_REJECTED
    new_balance: str


@dataclass
class LedgerEntry:
    entry_id: str
    amount: str                     # Decimal as string
    entry_type: EntryType
    reference: str
    state: EntryState
    metadata: dict = field(default_factory=dict)
    fraud_metadata: Optional[FraudCheckResult] = None
    approver_id: Optional[str] = None
    approved_by: Optional[str] = None
    rejection_reason: Optional[str] = None


@dataclass
class LedgerState:
    """Carried forward across Continue-as-New boundaries."""
    account_id: str
    currency: str
    balance: str                    # Decimal as string
    entries: list = field(default_factory=list)     # list[LedgerEntry]
    reserved: str = "0.00"         # balance held pending approval


# --- Query response types ---

@dataclass
class BalanceResult:
    balance: str
    reserved: str
    currency: str


@dataclass
class EntriesResult:
    entries: list                   # list[LedgerEntry]


@dataclass
class PendingApprovalsResult:
    entries: list                   # list[LedgerEntry] with AWAITING_APPROVAL state
