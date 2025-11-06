from return_limit_models import LimitCalculationResult


def hide_limit_balance(calc: LimitCalculationResult, h1: float, h3: float, h6: float, h12: float) -> LimitCalculationResult:
    return LimitCalculationResult(
        tenor_1_remaining_limit=calc.tenor_1_remaining_limit if calc.tenor_1_remaining_limit >= h1 else 0.0,
        tenor_3_remaining_limit=calc.tenor_3_remaining_limit if calc.tenor_3_remaining_limit >= h3 else 0.0,
        tenor_6_remaining_limit=calc.tenor_6_remaining_limit if calc.tenor_6_remaining_limit >= h6 else 0.0,
        tenor_12_remaining_limit=calc.tenor_12_remaining_limit if calc.tenor_12_remaining_limit >= h12 else 0.0,
    )