import os
import sys
from datetime import datetime, timezone

# Pastikan bisa import modul dari direktori yang sama tanpa package
sys.path.append(os.path.dirname(__file__))

from return_limit_models import (
    TenorLimitCaps,
    UseLimitTaskPayload,
    JobPayloadUseLimit,
    ReturnLimitRequest,
    CustomerLimit,
    Mutation,
    TenorHideLimit,
)
from limit_calculation import (
    LimitCalculation,
    find_limit_tenor_for_return_limit,
    find_new_limit_return_limit,
)
from hide_limit import hide_limit_balance


def preview_return_limit_sql():
    job_payload_use_limit = JobPayloadUseLimit(
        customer_id=1,
        queue_name="UseLimitQueue",
        tenor_limit_caps=TenorLimitCaps(tenor=1, min_limit=100.0, max_limit=1000.0),
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

    countmutations = 0
    if countmutations > 0:
        print("ROLLBACK;")
        print("[ERROR] Mutation already exist")
        return

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
        agreement_no="TEST123",
        contract_status="LIV",
        go_live_date=datetime.now(),
        amount=return_limit_request.amount,
        status="DB",
        is_first_transaction=False,
        is_first_trx_after_category=False,
        application_source_id="CFN",
        transaction_type="USE_LIMIT",
        tenor=1,
        previous_limit_amount=customer_limit_obj.tenor_1_remaining_limit,
    )

    if mutation_obj.amount != return_limit_request.amount:
        print("ROLLBACK;")
        print("[ERROR] Invalid return amount (use != return)")
        return

    tenor_1_gross = customer_limit_obj.tenor_1_gross_limit_amount
    tenor_3_gross = customer_limit_obj.tenor_3_gross_limit_amount
    tenor_6_gross = customer_limit_obj.tenor_6_gross_limit_amount
    tenor_12_gross = customer_limit_obj.tenor_12_gross_limit_amount

    tenor_1_remaining = customer_limit_obj.tenor_1_remaining_limit
    tenor_3_remaining = customer_limit_obj.tenor_3_remaining_limit
    tenor_6_remaining = customer_limit_obj.tenor_6_remaining_limit
    tenor_12_remaining = customer_limit_obj.tenor_12_remaining_limit

    calculation = LimitCalculation(
        customer_id=return_limit_request.customer_id,
        prospect_id=return_limit_request.prospect_id,
        tenor=mutation_obj.tenor,
        amount=return_limit_request.amount,
    )
    calc_result = calculation.calculate_return_limit_go_style(
        tenor_1_gross=tenor_1_gross,
        tenor_3_gross=tenor_3_gross,
        tenor_6_gross=tenor_6_gross,
        tenor_12_gross=tenor_12_gross,
        tenor_3_remaining=tenor_3_remaining,
        tenor_6_remaining=tenor_6_remaining,
        tenor_12_remaining=tenor_12_remaining,
    )

    previous_limit_amount = find_limit_tenor_for_return_limit(mutation_obj.tenor, customer_limit_obj)
    new_remaining_for_tenor = find_new_limit_return_limit(mutation_obj.tenor, calc_result)

    # Ambil konfigurasi tenor dari m_tenor, mencetak raw SQL (tanpa error handling)
    # tenor_cfg = calculation.find_tenor_config()
    # # Bentuk konfigurasi hide limit sesuai hasil query
    # hide_cfg = TenorHideLimit(
    #     tenor_1=tenor_cfg.get(1, {}).get("hide_limit", 0.0),
    #     tenor_3=tenor_cfg.get(3, {}).get("hide_limit", 0.0),
    #     tenor_6=tenor_cfg.get(6, {}).get("hide_limit", 0.0),
    #     tenor_12=tenor_cfg.get(12, {}).get("hide_limit", 0.0),
    # )


    # hidden_result = hide_limit_balance(calc_result, h1=hide_cfg.tenor_1, h3=hide_cfg.tenor_3, h6=hide_cfg.tenor_6, h12=hide_cfg.tenor_12)

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
        False,
        return_limit_request.amount,
        customer_limit_obj.category_limit_id,
        datetime.now(timezone.utc),
        "SYSTEM",
    ]
    print("\n[RAW SQL] INSERT mutation RETURN_LIMIT:")
    print(sql_insert_return_limit)
    print("params =", params_insert_return_limit)

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
        calc_result.tenor_1_remaining_limit,
        tenor_3_gross,
        calc_result.tenor_3_remaining_limit,
        tenor_6_gross,
        calc_result.tenor_6_remaining_limit,
        tenor_12_gross,
        calc_result.tenor_12_remaining_limit,
        datetime.now(timezone.utc),
        "SYSTEM",
        customer_limit_obj.id,
    ]
    print("\n[RAW SQL] UPDATE customer_limit:")
    print(sql_update_customer_limit)
    print("params =", params_update_customer_limit)

    print("COMMIT;")


if __name__ == "__main__":
    preview_return_limit_sql()