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

# diambil dari config service
minTenor1 = 150000.0
maxTenor1 = 500000.0


job_payload_use_limit = JobPayloadUseLimit(
    CustomerID=int(use_limit_data.CustomerIDCore),
    QueueName="useLimitManual",
    TenorLimitCaps=TenorLimitCaps(
        Tenor=1,  # Dummy, atau dari use_limit_data.Tenor
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

if count >0:
    print("ROLLBACK")
    print("Use limit mutation already exists.")
    sys.exit(1)

# step 15
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

# Jalankan fungsi find_limit_tenor_for_use_limit
tenor = job_payload_use_limit.TaskPayload.Tenor
limit = find_limit_tenor_for_use_limit(tenor, customer_limit_data)
print(f"Limit for tenor {tenor}: {limit}")












