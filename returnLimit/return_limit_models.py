from dataclasses import dataclass
from typing import Optional, Literal
from datetime import datetime


@dataclass
class TenorLimitCaps:
    tenor: int
    min_limit: float
    max_limit: float


@dataclass
class UseLimitTaskPayload:
    id: int
    prospect_id: str
    application_source: str
    tenor: int
    amount: float
    source_process: str
    agreement_number: str
    contract_status: str
    go_live_date: datetime


@dataclass
class JobPayloadUseLimit:
    customer_id: int
    queue_name: str
    tenor_limit_caps: TenorLimitCaps
    task_payload: UseLimitTaskPayload


@dataclass
class ReturnLimitRequest:
    customer_id: int
    prospect_id: str
    amount: float
    source_process: Literal["LOS", "CONFINS"]


@dataclass
class CustomerLimit:
    id: int
    customer_id: int
    limit_grant_type: Optional[str]
    given_limit_date: Optional[datetime]
    given_from_lob: Optional[str]
    active_date: Optional[datetime]
    active_from: Optional[str]
    expired_date: Optional[datetime]
    first_transaction_date: Optional[datetime]
    is_allowed_upgrade_limit: Optional[bool]
    limit_status_id: Optional[int]
    customer_status_id: Optional[int]
    category_limit_id: Optional[int]
    score: Optional[float]
    application_source_id: Optional[str]
    gross_limit_amount: float
    tenor_1_gross_limit_amount: float
    tenor_1_remaining_limit: float
    tenor_3_gross_limit_amount: float
    tenor_3_remaining_limit: float
    tenor_6_gross_limit_amount: float
    tenor_6_remaining_limit: float
    tenor_12_gross_limit_amount: float
    tenor_12_remaining_limit: float
    created_by: Optional[str]
    updated_by: Optional[str]
    source_order: Optional[str]
    source_value: Optional[str]
    created_at: Optional[datetime]
    updated_at: Optional[datetime]


@dataclass
class Mutation:
    id: int
    customer_id: int
    customer: dict
    prospect_id: str
    agreement_no: str
    contract_status: str
    go_live_date: datetime
    amount: float
    status: str
    is_first_transaction: bool
    is_first_trx_after_category: bool
    application_source_id: str
    transaction_type: str
    tenor: int
    previous_limit_amount: float


@dataclass
class LimitCalculationResult:
    tenor_1_remaining_limit: float
    tenor_3_remaining_limit: float
    tenor_6_remaining_limit: float
    tenor_12_remaining_limit: float


@dataclass
class TenorHideLimit:
    tenor_1: float
    tenor_3: float
    tenor_6: float
    tenor_12: float