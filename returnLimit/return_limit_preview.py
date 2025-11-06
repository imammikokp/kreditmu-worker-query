from dataclasses import dataclass
from typing import Optional, Literal, Dict
from datetime import datetime


# =========================
# Setup payload & dataclass
# =========================

# step 1: definisi caps tenor
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


@dataclass
class ReturnLimitRequest:
    customer_id: int
    prospect_id: str
    amount: float
    source_process: Literal["LOS", "CONFINS"]


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


@dataclass
class LimitCalculationResult:
    tenor_1_remaining_limit: float
    tenor_3_remaining_limit: float
    tenor_6_remaining_limit: float
    tenor_12_remaining_limit: float


class LimitCalculation:
    def __init__(self, customer_id: int, prospect_id: str, tenor: int, amount: float):
        self.customer_id = customer_id
        self.prospect_id = prospect_id
        self.tenor = tenor
        self.amount = amount

    def calculate_return_limit(
        self,
        tenor_1_gross: float,
        tenor_3_gross: float,
        tenor_6_gross: float,
        tenor_12_gross: float,
        tenor_1_remaining: float,
        tenor_3_remaining: float,
        tenor_6_remaining: float,
        tenor_12_remaining: float,
    ) -> LimitCalculationResult:
        # Tambahkan amount ke tenor yang relevan, dibatasi tidak melebihi gross dan tidak negatif
        new_t1 = tenor_1_remaining
        new_t3 = tenor_3_remaining
        new_t6 = tenor_6_remaining
        new_t12 = tenor_12_remaining

        if self.tenor == 1:
            new_t1 = tenor_1_remaining + self.amount
            new_t1 = max(0.0, min(new_t1, tenor_1_gross))
        elif self.tenor == 3:
            new_t3 = tenor_3_remaining + self.amount
            new_t3 = max(0.0, min(new_t3, tenor_3_gross))
        elif self.tenor == 6:
            new_t6 = tenor_6_remaining + self.amount
            new_t6 = max(0.0, min(new_t6, tenor_6_gross))
        elif self.tenor == 12:
            new_t12 = tenor_12_remaining + self.amount
            new_t12 = max(0.0, min(new_t12, tenor_12_gross))

        return LimitCalculationResult(
            tenor_1_remaining_limit=new_t1,
            tenor_3_remaining_limit=new_t3,
            tenor_6_remaining_limit=new_t6,
            tenor_12_remaining_limit=new_t12,
        )


def find_limit_tenor_for_return_limit(tenor: int, customer_limit: CustomerLimit) -> float:
    mapping: Dict[int, float] = {
        1: customer_limit.tenor_1_remaining_limit,
        3: customer_limit.tenor_3_remaining_limit,
        6: customer_limit.tenor_6_remaining_limit,
        12: customer_limit.tenor_12_remaining_limit,
    }
    return mapping.get(tenor, customer_limit.tenor_3_remaining_limit)


def find_new_limit_return_limit(tenor: int, calc: LimitCalculationResult) -> float:
    mapping: Dict[int, float] = {
        1: calc.tenor_1_remaining_limit,
        3: calc.tenor_3_remaining_limit,
        6: calc.tenor_6_remaining_limit,
        12: calc.tenor_12_remaining_limit,
    }
    return mapping.get(tenor, calc.tenor_3_remaining_limit)


@dataclass
class TenorHideLimit:
    tenor_1: float
    tenor_3: float
    tenor_6: float
    tenor_12: float


def hide_limit_balance(calc: LimitCalculationResult, h1: float, h3: float, h6: float, h12: float) -> LimitCalculationResult:
    # Jika remaining limit di bawah threshold hide, tampilkan 0
    return LimitCalculationResult(
        tenor_1_remaining_limit=calc.tenor_1_remaining_limit if calc.tenor_1_remaining_limit >= h1 else 0.0,
        tenor_3_remaining_limit=calc.tenor_3_remaining_limit if calc.tenor_3_remaining_limit >= h3 else 0.0,
        tenor_6_remaining_limit=calc.tenor_6_remaining_limit if calc.tenor_6_remaining_limit >= h6 else 0.0,
        tenor_12_remaining_limit=calc.tenor_12_remaining_limit if calc.tenor_12_remaining_limit >= h12 else 0.0,
    )


def preview_return_limit_sql():
    # Seed data agar skrip bisa langsung mencetak query
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

    return_limit_request = ReturnLimitRequest(
        customer_id=job_payload_use_limit.customer_id,
        prospect_id=job_payload_use_limit.task_payload.prospect_id,
        amount=job_payload_use_limit.task_payload.amount,
        source_process=job_payload_use_limit.task_payload.source_process,
    )

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
        gross_limit_amount=5_000_000.0,
        tenor_1_gross_limit_amount=500_000.0,
        tenor_1_remaining_limit=500_000.0,
        tenor_3_gross_limit_amount=1_250_000.0,
        tenor_3_remaining_limit=1_250_000.0,
        tenor_6_gross_limit_amount=2_500_000.0,
        tenor_6_remaining_limit=2_500_000.0,
        tenor_12_gross_limit_amount=5_000_000.0,
        tenor_12_remaining_limit=5_000_000.0,
        created_by=None,
        updated_by=None,
        source_order=None,
        source_value=None,
        created_at=None,
        updated_at=None,
    )

    print("BEGIN;")

    # step 3: SELECT customer_limit FOR UPDATE (konversi dari singleWithFilterTx)
    sql_customer_limit = """
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
WHERE customer_limit.customer_id = %s
  AND "customer_limit"."deleted_at" IS NULL
ORDER BY "customer_limit"."id"
LIMIT 1
FOR UPDATE;
"""
    params_customer_limit = [return_limit_request.customer_id]
    print("[RAW SQL] SELECT customer_limit FOR UPDATE:")
    print(sql_customer_limit)
    print("params =", params_customer_limit)

    # step 4: COUNT RETURN_LIMIT mutations (konversi dari CountMutationByProspectIdAndTransactionTypeAndStatus)
    sql_count_return_limit = """
SELECT count(*)
FROM "mutations"
WHERE prospect_id = %s
  AND transaction_type = %s
  AND status = %s
  AND "mutations"."deleted_at" IS NULL;
"""
    params_count_return_limit = [return_limit_request.prospect_id, "RETURN_LIMIT", "CR"]
    print("\n[RAW SQL] COUNT RETURN_LIMIT mutations:")
    print(sql_count_return_limit)
    print("params =", params_count_return_limit)

    # Mock hasil COUNT
    countmutations = 0

    # step 16: jika sudah ada, rollback
    if countmutations > 0:
        print("ROLLBACK;")
        print("[ERROR] Mutation already exist")
        return

    # step 5: SELECT mutation USE_LIMIT (konversi dari SingleWithFilter untuk USE_LIMIT)
    sql_select_use_limit = """
SELECT *
FROM "mutations"
WHERE mutations.prospect_id = %s
  AND mutations.customer_id = %s
  AND transaction_type = %s
  AND status = %s
  AND "mutations"."deleted_at" IS NULL
ORDER BY "mutations"."id"
LIMIT 1;
"""
    params_select_use_limit = [
        return_limit_request.prospect_id,
        return_limit_request.customer_id,
        "USE_LIMIT",
        "DB",
    ]
    print("\n[RAW SQL] SELECT mutation USE_LIMIT:")
    print(sql_select_use_limit)
    print("params =", params_select_use_limit)

    mutation_obj = Mutation(
        id=1,
        customer_id=return_limit_request.customer_id,
        customer={"id": customer_limit_obj.id},
        prospect_id=return_limit_request.prospect_id,
        agreement_no=job_payload_use_limit.task_payload.agreement_number,
        contract_status=job_payload_use_limit.task_payload.contract_status,
        go_live_date=job_payload_use_limit.task_payload.go_live_date,
        amount=job_payload_use_limit.task_payload.amount,
        status="DB",
        is_first_transaction=False,
        is_first_trx_after_category=False,
        application_source_id=job_payload_use_limit.task_payload.application_source,
        transaction_type="USE_LIMIT",
        tenor=job_payload_use_limit.task_payload.tenor,
        previous_limit_amount=customer_limit_obj.tenor_1_remaining_limit,
    )

    # step 6: validasi amount (Check amount use == amount return)
    if mutation_obj.amount != return_limit_request.amount:
        print("ROLLBACK;")
        print("[ERROR] Invalid return amount (use != return)")
        return

    # Snapshot limit saat ini
    tenor_1_gross = customer_limit_obj.tenor_1_gross_limit_amount
    tenor_3_gross = customer_limit_obj.tenor_3_gross_limit_amount
    tenor_6_gross = customer_limit_obj.tenor_6_gross_limit_amount
    tenor_12_gross = customer_limit_obj.tenor_12_gross_limit_amount

    tenor_1_remaining = customer_limit_obj.tenor_1_remaining_limit
    tenor_3_remaining = customer_limit_obj.tenor_3_remaining_limit
    tenor_6_remaining = customer_limit_obj.tenor_6_remaining_limit
    tenor_12_remaining = customer_limit_obj.tenor_12_remaining_limit

    # step 7: kalkulasi newRemaining via LimitCalculation (meniru calculate.CalculateReturnLimit)
    calculation = LimitCalculation(
        customer_id=return_limit_request.customer_id,
        prospect_id=return_limit_request.prospect_id,
        tenor=mutation_obj.tenor,
        amount=return_limit_request.amount,
    )
    calc_result = calculation.calculate_return_limit(
        tenor_1_gross=tenor_1_gross,
        tenor_3_gross=tenor_3_gross,
        tenor_6_gross=tenor_6_gross,
        tenor_12_gross=tenor_12_gross,
        tenor_1_remaining=tenor_1_remaining,
        tenor_3_remaining=tenor_3_remaining,
        tenor_6_remaining=tenor_6_remaining,
        tenor_12_remaining=tenor_12_remaining,
    )

    # previous_limit_amount per tenor (sesuai limitTenor)
    previous_limit_amount = find_limit_tenor_for_return_limit(mutation_obj.tenor, customer_limit_obj)

    # newRemaining per tenor
    new_remaining_for_tenor = find_new_limit_return_limit(mutation_obj.tenor, calc_result)

    # step 10: query m_tenor untuk mendapatkan config hide limit (print raw SQL)
    sql_select_tenor_config = """
SELECT tenor, min_net_limit_visible
FROM "m_tenor"
WHERE "m_tenor"."deleted_at" IS NULL
ORDER BY tenor;
"""
    print("\n[RAW SQL] SELECT m_tenor config (hide limits):")
    print(sql_select_tenor_config)
    print("params = []")

    # Mock konfigurasi hide limit per tenor (angka contoh)
    hide_cfg = TenorHideLimit(tenor_1=100.0, tenor_3=100.0, tenor_6=100.0, tenor_12=100.0)
    hidden_result = hide_limit_balance(
        calc_result,
        h1=hide_cfg.tenor_1,
        h3=hide_cfg.tenor_3,
        h6=hide_cfg.tenor_6,
        h12=hide_cfg.tenor_12,
    )

    # step 8: INSERT mutation RETURN_LIMIT (konversi dari StoreTx)
    sql_insert_return_limit = """
INSERT INTO "mutations" (
  customer_id,
  prospect_id,
  tenor,
  application_source_id,
  source_cut_limit,
  previous_limit_amount,
  remaining_limit_amount,
  status,
  transaction_type,
  is_first_trx_after_category,
  amount,
  category_limit_id,
  created_at,
  created_by
) VALUES (
  %s, %s, %s, %s, %s,
  %s, %s, %s, %s,
  %s, %s, %s, %s, %s
);
"""
    params_insert_return_limit = [
        return_limit_request.customer_id,
        return_limit_request.prospect_id,
        mutation_obj.tenor,
        mutation_obj.application_source_id,
        return_limit_request.source_process,
        previous_limit_amount,
        new_remaining_for_tenor,
        "CR",
        "RETURN_LIMIT",
        False,  # is_first_trx_after_category
        return_limit_request.amount,
        customer_limit_obj.category_limit_id,
        datetime.utcnow(),
        "SYSTEM",
    ]
    print("\n[RAW SQL] INSERT mutation RETURN_LIMIT:")
    print(sql_insert_return_limit)
    print("params =", params_insert_return_limit)

    # step 9: UPDATE selected fields di customer_limit (konversi dari UpdateSelectedFieldWithTx)
    sql_update_customer_limit = """
UPDATE "customer_limit" SET
  tenor_1_gross_limit_amount = %s,
  tenor_1_remaining_limit = %s,
  tenor_3_gross_limit_amount = %s,
  tenor_3_remaining_limit = %s,
  tenor_6_gross_limit_amount = %s,
  tenor_6_remaining_limit = %s,
  tenor_12_gross_limit_amount = %s,
  tenor_12_remaining_limit = %s,
  updated_at = %s,
  updated_by = %s
WHERE id = %s;
"""
    params_update_customer_limit = [
        tenor_1_gross,
        hidden_result.tenor_1_remaining_limit,
        tenor_3_gross,
        hidden_result.tenor_3_remaining_limit,
        tenor_6_gross,
        hidden_result.tenor_6_remaining_limit,
        tenor_12_gross,
        hidden_result.tenor_12_remaining_limit,
        datetime.utcnow(),
        "SYSTEM",
        customer_limit_obj.id,
    ]
    print("\n[RAW SQL] UPDATE customer_limit:")
    print(sql_update_customer_limit)
    print("params =", params_update_customer_limit)

    print("COMMIT;")


if __name__ == "__main__":
    preview_return_limit_sql()