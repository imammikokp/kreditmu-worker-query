from typing import Dict

from return_limit_models import CustomerLimit, LimitCalculationResult


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
    return mapping.get(tenor, customer_limit.tenor_3_remaining_limit)


def find_new_limit_return_limit(tenor: int, calc: LimitCalculationResult) -> float:
    mapping: Dict[int, float] = {
        1: calc.tenor_1_remaining_limit,
        3: calc.tenor_3_remaining_limit,
        6: calc.tenor_6_remaining_limit,
        12: calc.tenor_12_remaining_limit,
    }
    return mapping.get(tenor, calc.tenor_3_remaining_limit)