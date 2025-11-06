from typing import Dict

from return_limit_models import CustomerLimit, LimitCalculationResult


class LimitCalculation:
    def __init__(self, customer_id: int, prospect_id: str, tenor: int, amount: float):
        self.customer_id = customer_id
        self.prospect_id = prospect_id
        self.tenor = tenor
        self.amount = amount

    def find_tenor_config(self, ctx=None) -> Dict[int, Dict[str, int]]:
        """
        Raw SQL setara dengan Go: Order("description asc").Find(
        ..., "is_active = ? and behaviour_min_max_limit = ?", true, "AMOUNT")

        Pada preview ini, kita cetak query dan seed hasilnya lokal.
        """
        sql = (
            'SELECT description, min_limit, max_limit\n'
            'FROM "m_tenor"\n'
            'WHERE is_active = %s\n'
            '  AND behaviour_min_max_limit = %s\n'
            '  AND "m_tenor"."deleted_at" IS NULL\n'
            'ORDER BY description ASC;'
        )
        params = [True, "AMOUNT"]
        print("\n[RAW SQL] SELECT m_tenor config (min/max limits):")
        print(sql)
        print("params =", params)

        # Seeded rows sebagai pengganti eksekusi DB
        rows = [
            {"description": 1, "min_limit": 20000, "max_limit": 500000},
            {"description": 3, "min_limit": 50000, "max_limit": 35000000},
            {"description": 6, "min_limit": 500000, "max_limit": 35000000},
            {"description": 12, "min_limit": 500000, "max_limit": 35000000},
        ]

        cfg: Dict[int, Dict[str, int]] = {
            int(r["description"]): {
                "min_limit": int(r["min_limit"]),
                "max_limit": int(r["max_limit"]),
            }
            for r in rows
        }
        return cfg

    def _find_limit_used(self, ctx=None) -> float:
        """Raw SQL equivalent of Go's findLimitUsed and seeded return for preview.
        Computes: SUM(DB, USE_LIMIT) - SUM(CR, RETURN_LIMIT) for this customer.
        """
        sql = (
            'SELECT customer_id ,\n'
            '(coalesce((select SUM(amount) from mutations where customer_id = %s and status = \"DB\" and transaction_type  = \"USE_LIMIT\"),0) \n'
            '- \n'
            '(coalesce((select SUM(amount) from mutations where customer_id = %s and status = \"CR\" and transaction_type  = \"RETURN_LIMIT\"),0))) as limit_used\n'
            'FROM mutations m WHERE customer_id = %s GROUP BY customer_id;'
        )
        params = [self.customer_id, self.customer_id, self.customer_id]
        print("\n[RAW SQL] SELECT net limit used:")
        print(sql)
        print("params =", params)

        # Preview seed: assume last use equals the amount being returned
        return float(self.amount)

    def _find_tenor1_outstanding_balance(self, ctx=None) -> Dict[str, float]:
        """Raw SQL equivalent of Go's findTenor1OutstandingBalance and seeded return for preview.
        Computes total_debit (DB, tenor=1) and total_credit (CR, tenor=1) for this customer.
        """
        sql = (
            'SELECT m.customer_id,\n'
            '       (SELECT SUM(amount) FROM "mutations"\n'
            '        WHERE customer_id = %s AND status = \"DB\" AND tenor = 1) AS total_debit,\n'
            '       (SELECT SUM(amount) FROM "mutations"\n'
            '        WHERE customer_id = %s AND status = \"CR\" AND tenor = 1) AS total_credit\n'
            'FROM "mutations" m\n'
            'WHERE m.customer_id = %s\n'
            'GROUP BY m.customer_id;'
        )
        params = [self.customer_id, self.customer_id, self.customer_id]
        print("\n[RAW SQL] SELECT tenor 1 outstanding balance:")
        print(sql)
        print("params =", params)

        # Preview seed: return zeros; outstanding = total_debit - total_credit
        return {"total_debit": 0.0, "total_credit": 0.0}

    def calculate_return_limit_go_style(
        self,
        tenor_1_gross: float = 0.0,
        tenor_3_gross: float = 0.0,
        tenor_6_gross: float = 0.0,
        tenor_12_gross: float = 0.0,
        tenor_3_remaining: float = 0.0,
        tenor_6_remaining: float = 0.0,
        tenor_12_remaining: float = 0.0,
    ) -> LimitCalculationResult:
        """Python conversion of Go CalculateReturnLimit (lines 175-256).
        Mirrors the same business rules, using seeded helpers for preview.
        """
        result = LimitCalculationResult(
            tenor_1_remaining_limit=0.0,
            tenor_3_remaining_limit=0.0,
            tenor_6_remaining_limit=0.0,
            tenor_12_remaining_limit=0.0,
        )

        tenor_cfg = self.find_tenor_config()

        total_limit_used = self._find_limit_used()
        limit_used_after_return = total_limit_used - float(self.amount)

        # Handle minus condition on tenor 12 remaining
        if tenor_12_remaining < 0:
            # Go: helper.InlineConditionFloat(remaining + amount > gross, gross, remaining + amount)
            calculate_limit_tenor3 = (
                tenor_3_gross
                if (tenor_3_remaining + float(self.amount)) > tenor_3_gross
                else (tenor_3_remaining + float(self.amount))
            )
            calculate_limit_tenor6 = (
                tenor_6_gross
                if (tenor_6_remaining + float(self.amount)) > tenor_6_gross
                else (tenor_6_remaining + float(self.amount))
            )
            calculate_limit_tenor12 = (
                tenor_12_gross
                if (tenor_12_remaining + float(self.amount)) > tenor_12_gross
                else (tenor_12_remaining + float(self.amount))
            )

            result.tenor_3_remaining_limit = calculate_limit_tenor3
            result.tenor_6_remaining_limit = calculate_limit_tenor6
            result.tenor_12_remaining_limit = calculate_limit_tenor12
        else:
            # Net limit per tenor before the return transaction occurs
            calculate_limit_tenor3 = tenor_3_gross - limit_used_after_return
            calculate_limit_tenor6 = tenor_6_gross - limit_used_after_return
            calculate_limit_tenor12 = tenor_12_gross - limit_used_after_return

            result.tenor_3_remaining_limit = calculate_limit_tenor3
            result.tenor_6_remaining_limit = calculate_limit_tenor6
            result.tenor_12_remaining_limit = calculate_limit_tenor12

        # Tenor 1 outstanding balance
        t1_balance = self._find_tenor1_outstanding_balance()
        outstanding_t1 = t1_balance["total_debit"] - t1_balance["total_credit"]

        if self.tenor == 1:
            calc_t1 = tenor_1_gross - outstanding_t1 + float(self.amount)

            if calc_t1 > calculate_limit_tenor6:
                if result.tenor_6_remaining_limit >= float(tenor_cfg[1]["max_limit"]):
                    result.tenor_1_remaining_limit = float(tenor_cfg[1]["max_limit"])
                else:
                    result.tenor_1_remaining_limit = calculate_limit_tenor6
            else:
                if calc_t1 >= float(tenor_cfg[1]["max_limit"]):
                    result.tenor_1_remaining_limit = float(tenor_cfg[1]["max_limit"])
                else:
                    result.tenor_1_remaining_limit = calc_t1
        else:
            # Return saldo tenor 1 for non-tenor-1 transactions
            calc_t1 = tenor_1_gross - outstanding_t1

            if calc_t1 > result.tenor_6_remaining_limit:
                if result.tenor_6_remaining_limit >= float(tenor_cfg[1]["max_limit"]):
                    result.tenor_1_remaining_limit = float(tenor_cfg[1]["max_limit"])
                else:
                    result.tenor_1_remaining_limit = result.tenor_6_remaining_limit
            else:
                if calc_t1 >= float(tenor_cfg[1]["max_limit"]):
                    result.tenor_1_remaining_limit = float(tenor_cfg[1]["max_limit"])
                else:
                    result.tenor_1_remaining_limit = calc_t1

        return result

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
        new_t1 = tenor_1_remaining
        new_t3 = tenor_3_remaining
        new_t6 = tenor_6_remaining
        new_t12 = tenor_12_remaining

        if self.tenor == 1:
            new_t1 = max(0.0, min(tenor_1_remaining + self.amount, tenor_1_gross))
        elif self.tenor == 3:
            new_t3 = max(0.0, min(tenor_3_remaining + self.amount, tenor_3_gross))
        elif self.tenor == 6:
            new_t6 = max(0.0, min(tenor_6_remaining + self.amount, tenor_6_gross))
        elif self.tenor == 12:
            new_t12 = max(0.0, min(tenor_12_remaining + self.amount, tenor_12_gross))

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
    return mapping.get(tenor, 0.0)


def find_new_limit_return_limit(tenor: int, calc: LimitCalculationResult) -> float:
    mapping: Dict[int, float] = {
        1: calc.tenor_1_remaining_limit,
        3: calc.tenor_3_remaining_limit,
        6: calc.tenor_6_remaining_limit,
        12: calc.tenor_12_remaining_limit,
    }
    return mapping.get(tenor, 0.0)