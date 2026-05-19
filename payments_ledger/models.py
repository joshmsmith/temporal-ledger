from enum import Enum
from typing import Any, Dict, List, Optional
import uuid

from pydantic import BaseModel, Field


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


# ---- Define in dependency order so no forward references are needed ----

class FraudCheckResult(BaseModel):
    entry_id: str
    passed: bool
    risk_score: float
    flags: List[str] = Field(default_factory=list)
    checked_by: str = "fraud-service"


class LedgerEntry(BaseModel):
    entry_id: str
    amount: str                          # Decimal as string
    entry_type: EntryType
    reference: str
    state: EntryState
    metadata: Dict[str, Any] = Field(default_factory=dict)
    fraud_metadata: Optional[FraudCheckResult] = None
    approver_id: Optional[str] = None
    approved_by: Optional[str] = None
    rejection_reason: Optional[str] = None


class LedgerInit(BaseModel):
    account_id: str
    currency: str = "USD"
    opening_balance: str = "0.00"
    # Carry in-flight entries across Continue-as-New boundaries.
    open_entries: List[LedgerEntry] = Field(default_factory=list)
    # Carry reserved balance across Continue-as-New boundaries.
    reserved_balance: str = "0.00"


class PaymentRequest(BaseModel):
    amount: str                          # Decimal as string
    entry_type: EntryType = EntryType.DEBIT
    reference: str = ""
    entry_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    metadata: Dict[str, Any] = Field(default_factory=dict)


class PaymentResult(BaseModel):
    entry_id: str
    state: EntryState
    new_balance: str                     # Decimal as string


class ApprovalRequest(BaseModel):
    entry_id: str
    approver_id: str


class ApprovalRequestResult(BaseModel):
    entry_id: str
    state: EntryState                    # AWAITING_APPROVAL


class ApprovePaymentRequest(BaseModel):
    entry_id: str
    approved_by: str
    notes: str = ""


class ApprovePaymentResult(BaseModel):
    entry_id: str
    state: EntryState                    # APPROVED or POSTED
    new_balance: str


class RejectPaymentRequest(BaseModel):
    entry_id: str
    rejected_by: str
    reason: str


class RejectPaymentResult(BaseModel):
    entry_id: str
    state: EntryState                    # REJECTED


class VoidPaymentRequest(BaseModel):
    entry_id: str
    voided_by: str
    reason: str


class VoidPaymentResult(BaseModel):
    entry_id: str
    state: EntryState                    # VOIDED
    new_balance: str


class FraudClearanceResult(BaseModel):
    entry_id: str
    new_state: EntryState                # FRAUD_CLEARED or FRAUD_REJECTED
    new_balance: str


class LedgerState(BaseModel):
    """Carried forward across Continue-as-New boundaries."""
    account_id: str
    currency: str
    balance: str                         # Decimal as string
    entries: List[LedgerEntry] = Field(default_factory=list)
    reserved: str = "0.00"


# --- Query response types ---

class BalanceResult(BaseModel):
    balance: str
    reserved: str
    currency: str


class EntriesResult(BaseModel):
    entries: List[LedgerEntry] = Field(default_factory=list)


class PendingApprovalsResult(BaseModel):
    entries: List[LedgerEntry] = Field(default_factory=list)


class SinglePaymentResult(BaseModel):
    """Final result returned when a PaymentWorkflow completes."""
    entry_id: str
    state: EntryState
    approved_by: Optional[str] = None
    rejection_reason: Optional[str] = None
