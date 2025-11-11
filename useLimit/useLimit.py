import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from dataclasses import dataclass
from typing import Optional
from datetime import date, datetime
from helper import validateTenorMapping

# produce Cache
# class Limit:

# diatur dari config service
UseLimitDataRange = 2

print(f"""
SELECT ID
	,BranchID
	,RTRIM(AgreementSyncKreditmu.CustomerID) AS CustomerID
	,ProspectID
	,AgreementNo
	,ApplicationID
	,ContractStatus
	,GoLiveDate
	,Tenor
	,(TotalOTR - DownPayment - DiscountOTRAmount) AS AF
	,IncomingSource
	,CustomerIDCore
FROM "AgreementSyncKreditmu"
WHERE IsProcess = 0
	AND IncomingSource = "MANUAL"
	AND (
		(
			OutstandingPrincipal > 0
			AND ContractStatus NOT IN (
				"EXP", "CAN", "RJC", "PRP"
				)
			)
		)
	AND (
		CAST(DtmUpd AS DATE) BETWEEN DATEADD(dd,{UseLimitDataRange} , CAST(GETDATE() AS DATE))
			AND CAST(GETDATE() AS DATE)
		)
ORDER BY CustomerID ASC
""")

# return
@dataclass
class UseLimitData:
    ID: int
    BranchID: str
    CustomerID: str
    ProspectID: Optional[str]
    AgreementNo: str
    ApplicationID: str
    ContractStatus: str
    GoLiveDate: Optional[date]
    Tenor: int
    AF: float
    IncomingSource: str
    CustomerIDCore: str

# Contoh objek dari hasil query
use_limit_data = UseLimitData(
    ID=1,
    BranchID="BR001",
    CustomerID="CUST001",
    ProspectID="TEST-1",
    AgreementNo="TEST1",
    ApplicationID="TEST-1",
    ContractStatus="LIV",
    GoLiveDate=date(2025, 10, 19),
    Tenor=1,
    AF=200000.0,
    IncomingSource="CFN",
    CustomerIDCore="1"
)

#worker consume
#  Worker Request

@dataclass
class TenorLimitCaps:
    Tenor: int
    MinLimit: float
    MaxLimit: float

@dataclass
class UseLimitTaskPayload:
    ID: int
    ProspectID: str
    ApplicationSource: str
    Tenor: int
    Amount: float
    SourceProcess: str
    AgreementNumber: str
    ContractStatus: str
    GoLiveDate: datetime

@dataclass
class JobPayloadUseLimit:
    CustomerID: int
    QueueName: str
    TenorLimitCaps: TenorLimitCaps
    TaskPayload: UseLimitTaskPayload

# mapping dari producer
print("""
SELECT * FROM "m_tenors" WHERE description = 1 AND "m_tenor"."deleted_at" IS NULL ORDER BY "m_tenor"."id" LIMIT 1
""")

# return
description = 1
minTenor1 = 150000.0
maxTenor1 = 500000.0


job_payload_use_limit = JobPayloadUseLimit(
    CustomerID=int(use_limit_data.CustomerIDCore),
    QueueName="useLimitManual",
    TenorLimitCaps=TenorLimitCaps(
        Tenor=description,  # dari use_limit_data.Tenor
        MinLimit=minTenor1,
        MaxLimit=maxTenor1,
    ),
    TaskPayload=UseLimitTaskPayload(
        ID=use_limit_data.ID,
        ProspectID=use_limit_data.ApplicationID,  # ProspectID = ApplicationID
        ApplicationSource="CFN",
        Tenor=validateTenorMapping(use_limit_data.Tenor),
        Amount=use_limit_data.AF,
        SourceProcess="CONFINS",
        AgreementNumber=use_limit_data.AgreementNo,
        ContractStatus=use_limit_data.ContractStatus,
        GoLiveDate=datetime.combine(use_limit_data.GoLiveDate, datetime.min.time()) if use_limit_data.GoLiveDate else datetime.now(),
    ),
)

# step 4
print("BEGIN;")

#  step 5 - 6
print(f"""
SELECT customer_limit.id
	,customer_limit.customer_id
	,customer_limit.category_limit_id
	,customer_limit.gross_limit_amount
	,customer_limit.tenor_1_gross_limit_amount
	,customer_limit.tenor_1_remaining_limit
	,customer_limit.tenor_3_gross_limit_amount
	,customer_limit.tenor_3_remaining_limit
	,customer_limit.tenor_6_gross_limit_amount
	,customer_limit.tenor_6_remaining_limit
	,customer_limit.tenor_12_gross_limit_amount
	,customer_limit.tenor_12_remaining_limit
FROM "customer_limit"
WHERE customer_limit.customer_id = {job_payload_use_limit.CustomerID}
	AND "customer_limit"."deleted_at" IS NULL
ORDER BY "customer_limit"."id" LIMIT 1
""")

# return
@dataclass
class CustomerLimitData:
    id: int
    customer_id: int
    category_limit_id: int
    gross_limit_amount: float
    tenor_1_gross_limit_amount: float
    tenor_1_remaining_limit: float
    tenor_3_gross_limit_amount: float
    tenor_3_remaining_limit: float
    tenor_6_gross_limit_amount: float
    tenor_6_remaining_limit: float
    tenor_12_gross_limit_amount: float
    tenor_12_remaining_limit: float


customer_limit_data = CustomerLimitData(
    id=1,
    customer_id=1,
    category_limit_id=1,
    gross_limit_amount=5000000.0,  # Dummy, atau hitung dari tenor_12
    tenor_1_gross_limit_amount=500000.0,
    tenor_1_remaining_limit=500000.0,
    tenor_3_gross_limit_amount=1250000.0,
    tenor_3_remaining_limit=1250000.0,
    tenor_6_gross_limit_amount=2500000.0,
    tenor_6_remaining_limit=2500000.0,
    tenor_12_gross_limit_amount=5000000.0,
    tenor_12_remaining_limit=5000000.0,
)

# step 8-9
if customer_limit_data is None:
    print("ROLLBACK")
    print("Customer limit data not found.")
    sys.exit(1)

# step 10 - 12
print(f"""SELECT count(*) FROM "mutations" WHERE mutations.prospect_id = {job_payload_use_limit.TaskPayload.ProspectID} AND mutations.transaction_type = 'USE_LIMIT' AND mutations.status = 'DB'""")
# return 
count = 0

# step 15
if count >0:
    # step 16
    print("ROLLBACK")
    print("Use limit mutation already exists.")
    sys.exit(1)

# step 18
def find_limit_tenor_for_use_limit(tenor: int, customer_limit: CustomerLimitData) -> float:
    limit_tenor = 0.0
    if tenor == 1:
        if customer_limit.tenor_1_remaining_limit > 0:
            limit_tenor = customer_limit.tenor_1_remaining_limit
    elif tenor == 3:
        if customer_limit.tenor_3_remaining_limit > 0:
            limit_tenor = customer_limit.tenor_3_remaining_limit
    elif tenor == 6:
        if customer_limit.tenor_6_remaining_limit > 0:
            limit_tenor = customer_limit.tenor_6_remaining_limit
    elif tenor == 12:
        if customer_limit.tenor_12_remaining_limit > 0:
            limit_tenor = customer_limit.tenor_12_remaining_limit
    else:
        raise ValueError("limit tenor not found")
    return limit_tenor

# Jalankan fungsi 
# STEP 18
tenor = job_payload_use_limit.TaskPayload.Tenor
limit = find_limit_tenor_for_use_limit(tenor, customer_limit_data)
print(f"Limit for tenor {tenor}: {limit}")

remaining = limit - job_payload_use_limit.TaskPayload.Amount


# step 19 - 20
@dataclass
class Mutation:
    customer_id: int
    prospect_id: str
    agreement_no: str
    contract_status: str
    go_live_date: Optional[datetime]
    amount: float
    status: str
    is_first_trx_after_category: bool
    application_source_id: str
    transaction_type: str
    tenor: int
    previous_limit_amount: float
    remaining_limit_amount: float
    source_cut_limit: str
    created_by: str
    category_limit_id: Optional[int]

# konstruksi objek
mutation_obj = Mutation(
    customer_id=job_payload_use_limit.CustomerID,
    prospect_id=job_payload_use_limit.TaskPayload.ProspectID,
    agreement_no=job_payload_use_limit.TaskPayload.AgreementNumber,
    contract_status=job_payload_use_limit.TaskPayload.ContractStatus,
    go_live_date=job_payload_use_limit.TaskPayload.GoLiveDate,
    amount=job_payload_use_limit.TaskPayload.Amount,
    status="DB",
    is_first_trx_after_category=False,
    application_source_id=job_payload_use_limit.TaskPayload.ApplicationSource,
    transaction_type="USE_LIMIT",
    tenor=job_payload_use_limit.TaskPayload.Tenor,
    previous_limit_amount=limit,
    remaining_limit_amount=remaining,
    source_cut_limit=job_payload_use_limit.TaskPayload.SourceProcess,
    created_by="WORKER",
    category_limit_id=customer_limit_data.category_limit_id,
)

print(f"""INSERT INTO "mutations" ("customer_id","prospect_id","agreement_no","contract_status","go_live_date","amount","status","is_first_trx_after_category","application_source_id","transaction_type","tenor","previous_limit_amount","remaining_limit_amount","source_cut_limit","category_limit_id","created_by") VALUES ({mutation_obj.customer_id},{mutation_obj.prospect_id},{mutation_obj.agreement_no},{mutation_obj.contract_status},{mutation_obj.go_live_date},{mutation_obj.amount},{mutation_obj.status},{mutation_obj.is_first_trx_after_category},{mutation_obj.application_source_id},{mutation_obj.transaction_type},{mutation_obj.tenor},{mutation_obj.previous_limit_amount},{mutation_obj.remaining_limit_amount},{mutation_obj.source_cut_limit},{mutation_obj.category_limit_id},{mutation_obj.created_by}) RETURNING "id";""")

# Step 22
@dataclass
class CalculateLimitRequest:
    tenor1_gross_limit_amount: float
    tenor1_remaining_limit: float
    tenor3_gross_limit_amount: float
    tenor3_remaining_limit: float
    tenor6_gross_limit_amount: float
    tenor6_remaining_limit: float
    tenor12_gross_limit_amount: float
    tenor12_remaining_limit: float


def calculate_limit(tenor: int, net_transaction: float, min_tenor1: float, body: CalculateLimitRequest) -> CalculateLimitRequest:
	# khusus untuk logic pemotongan limit worker, tidak ada penjagaan mengenai kecukupan limit konsumen, karena pemotongan limit
	# via worker hanya memproses dari pengajuan limit manual confins, jadi memungkinkan terjadi over limit

	# Jika transaksi WG / selain tenor 1
    if tenor != 1:
        tenor12_current_limit = body.tenor12_remaining_limit - net_transaction
        body.tenor12_remaining_limit = tenor12_current_limit

        tenor6_current_limit = body.tenor6_remaining_limit - net_transaction
        body.tenor6_remaining_limit = tenor6_current_limit

        tenor3_current_limit = body.tenor3_remaining_limit - net_transaction
        body.tenor3_remaining_limit = tenor3_current_limit

        # Kalkulasi untuk net limit tenor 1
        if tenor6_current_limit < body.tenor1_remaining_limit:
            body.tenor1_remaining_limit = tenor6_current_limit

    if tenor == 1:
        # Transaksi DG
        if net_transaction < min_tenor1:
            raise ValueError("error reject transaction")
        if net_transaction <= body.tenor1_remaining_limit:
            res1 = body.tenor1_remaining_limit - net_transaction
            res3 = body.tenor3_remaining_limit - net_transaction
            res6 = body.tenor6_remaining_limit - net_transaction
            res12 = body.tenor12_remaining_limit - net_transaction

            body.tenor1_remaining_limit = res1
            body.tenor3_remaining_limit = res3
            body.tenor6_remaining_limit = res6
            body.tenor12_remaining_limit = res12

    return body

calculate_limit_request = CalculateLimitRequest(
    tenor1_gross_limit_amount=customer_limit_data.tenor_1_gross_limit_amount,
    tenor1_remaining_limit=customer_limit_data.tenor_1_remaining_limit,
    tenor3_gross_limit_amount=customer_limit_data.tenor_3_gross_limit_amount,
    tenor3_remaining_limit=customer_limit_data.tenor_3_remaining_limit,
    tenor6_gross_limit_amount=customer_limit_data.tenor_6_gross_limit_amount,
    tenor6_remaining_limit=customer_limit_data.tenor_6_remaining_limit,
    tenor12_gross_limit_amount=customer_limit_data.tenor_12_gross_limit_amount,
    tenor12_remaining_limit=customer_limit_data.tenor_12_remaining_limit,
)

newLimit=calculate_limit(
    tenor=job_payload_use_limit.TaskPayload.Tenor,
    net_transaction=job_payload_use_limit.TaskPayload.Amount,
    min_tenor1=job_payload_use_limit.TenorLimitCaps.MinLimit,
    body=calculate_limit_request,
)

# step 23-24
print(f"""
UPDATE "customer_limit"
SET
  "tenor_12_gross_limit_amount" = {newLimit.tenor12_gross_limit_amount},
  "tenor_12_remaining_limit"    = {newLimit.tenor12_remaining_limit},
  "tenor_1_gross_limit_amount"  = {newLimit.tenor1_gross_limit_amount},
  "tenor_1_remaining_limit"     = {newLimit.tenor1_remaining_limit},
  "tenor_3_gross_limit_amount"  = {newLimit.tenor3_gross_limit_amount},
  "tenor_3_remaining_limit"     = {newLimit.tenor3_remaining_limit},
  "tenor_6_gross_limit_amount"  = {newLimit.tenor6_gross_limit_amount},
  "tenor_6_remaining_limit"     = {newLimit.tenor6_remaining_limit},
  "updated_at"                  = {datetime.now()},
  "updated_by"                  = "WORKER",
WHERE customer_limit.id = {customer_limit_data.id};
""")

# step 26
print(f"""
UPDATE "AgreementSyncKreditmu"
SET
  "DtmProcess" = true,
  "IsProcess"  = {datetime.now()}
WHERE ID = {job_payload_use_limit.TaskPayload.ID};
""")

# step 29
print("commit")