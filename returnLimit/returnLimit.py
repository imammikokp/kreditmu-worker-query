# produce Cache
# class Limit:

# diatur dari config service
UseLimitDataRange = -2

print(f,"""
SELECT
  ID,
  BranchID,
  RTRIM(AgreementSyncKreditmu.CustomerID) AS CustomerID,
  ProspectID,
  AgreementNo,
  ApplicationID,
  ContractStatus,
  GoLiveDate,
  Tenor,
  (TotalOTR - DownPayment - DiscountOTRAmount) AS AF,
  IncomingSource,
  CustomerIDCore
FROM "AgreementSyncKreditmu"
WHERE
  IsProcess = 0
  AND (OutstandingPrincipal = 0 AND ContractStatus IN ("EXP", "CAN", "ICP", "RRD"))
  AND CAST (DtmUpd AS DATE) BETWEEN DATEADD(dd, {UseLimitDataRange}, CAST (GETDATE() AS DATE)) AND CAST (GETDATE() AS DATE)
ORDER BY CustomerID ASC;
""")

# update return customer step 1

# return limit sequence
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

# return
@dataclass
class JobPayloadUseLimit:
    customer_id: int
    queue_name: str
    tenor_limit_caps: TenorLimitCaps
    task_payload: UseLimitTaskPayload

job_payload_use_limit = JobPayloadUseLimit(
    customer_id=1,
    queue_name="UseLimitQueue",
    tenor_limit_caps=TenorLimitCaps(
        tenor=1,
        min_limit=100.0,
        max_limit=1000.0,
    ),
    task_payload=UseLimitTaskPayload(
        id=1,
        prospect_id="TEST-123",
        application_source="CFN",
        tenor=1,
        amount=100.0,
        source_process="CONFINS",
        agreement_number="TEST123",
        contract_status="LIV",
        go_live_date=datetime.now(),
    ),
)

@dataclass
class ReturnLimitRequest:
    customer_id: int
    prospect_id: str
    amount: float
    source_process: Literal["LOS", "CONFINS"]

# Contoh penggunaan
return_limit_request = ReturnLimitRequest(
    customer_id=job_payload_use_limit.customer_id,
    prospect_id=job_payload_use_limit.task_payload.prospect_id,
    amount=job_payload_use_limit.task_payload.amount,
    source_process=job_payload_use_limit.task_payload.source_process,
)


print("BEGIN;")
# step 3
print(f,"""
SELECT
  customer_limit.id,
  customer_limit.customer_id,
  customer_limit.limit_grant_type,
  customer_limit.given_limit_date,
  customer_limit.given_from_lob,
  customer_limit.active_date,
  customer_limit.active_from,
  customer_limit.expired_date,
  customer_limit.first_transaction_date,
  customer_limit.is_allowed_upgrade_limit,
  customer_limit.limit_status_id,
  customer_limit.customer_status_id,
  customer_limit.category_limit_id,
  customer_limit.score,
  customer_limit.application_source_id,
  customer_limit.gross_limit_amount,
  customer_limit.tenor_1_gross_limit_amount,
  customer_limit.tenor_1_remaining_limit,
  customer_limit.tenor_3_gross_limit_amount,
  customer_limit.tenor_3_remaining_limit,
  customer_limit.tenor_6_gross_limit_amount,
  customer_limit.tenor_6_remaining_limit,
  customer_limit.tenor_12_gross_limit_amount,
  customer_limit.tenor_12_remaining_limit,
  customer_limit.created_by,
  customer_limit.updated_by,
  customer_limit.source_order,
  customer_limit.source_value,
  customer_limit.created_at,
  customer_limit.updated_at
FROM "customer_limit"
WHERE customer_limit.customer_id = {return_limit_request.customer_id}
  AND "customer_limit"."deleted_at" IS NULL
ORDER BY "customer_limit"."id"
LIMIT 1
FOR UPDATE;
""")
# return
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

customer_limit_obj = CustomerLimit(
    id=1,
    customer_id=1,
    limit_grant_type=None,
    given_limit_date=None,
    given_from_lob=None,
    active_date=None,
    active_from=None,
    expired_date=None,
    first_transaction_date=None,
    is_allowed_upgrade_limit=None,
    limit_status_id=None,
    customer_status_id=None,
    category_limit_id=1,
    score=None,
    application_source_id=None,
    gross_limit_amount=5000000.0,
    tenor_1_gross_limit_amount=500000.0,
    tenor_1_remaining_limit=500000.0,
    tenor_3_gross_limit_amount=1250000.0,
    tenor_3_remaining_limit=1250000.0,
    tenor_6_gross_limit_amount=2500000.0,
    tenor_6_remaining_limit=2500000.0,
    tenor_12_gross_limit_amount=5000000.0,
    tenor_12_remaining_limit=5000000.0,
    created_by=None,
    updated_by=None,
    source_order=None,
    source_value=None,
    created_at=None,
    updated_at=None,
)

# step 4
print(f,"""
SELECT count(*)
FROM "mutations"
WHERE prospect_id = {return_limit_request.prospect_id}
  AND transaction_type = "RETURN_LIMIT"
  AND status = "CR"
  AND "mutations"."deleted_at" IS NULL;
 """)

# return 
countmutations = 0
# step 16
if countmutations >0 :
    print("ROLLBACK;")
    raise print("Mutation already exist")

print(f,"""
SELECT *
FROM "mutations"
WHERE mutations.prospect_id = $1
  AND mutations.customer_id = $2
  AND transaction_type = $3
  AND status = $4
  AND "mutations"."deleted_at" IS NULL
ORDER BY "mutations"."id"
LIMIT $5;
 """)

@dataclass
class Mutation:
    id: int
    customer_id: int
    customer: dict  # mapping ke CustomerLimit
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
    remaining_limit_amount: float
    cut_off_date: datetime
    source_cut_limit: str
    category_limi_id: int
    category_limit: dict  # mapping ke MCategoryLimit
    created_at: datetime
    created_by: str
    updated_at: datetime
    updated_by: str
    deleted_at: datetime

MutationObje = Mutation

MutationObje.amount = 25000.0

if MutationObje.amount != return_limit_request.amount:
    print("ROLLBACK;")
    raise print("Mutation amount not match")


