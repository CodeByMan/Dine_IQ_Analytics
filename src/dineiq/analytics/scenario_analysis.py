"""Transparent, labeled what-if estimates for the eight SRS scenario types."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


SCENARIOS = {
    "increase_price": "Increase menu price",
    "reduce_price": "Reduce menu price",
    "change_discount": "Change discount percentage",
    "promotion_frequency": "Change promotion frequency",
    "remove_item": "Remove a menu item",
    "preparation_quantity": "Change preparation quantity",
    "increase_demand": "Change predicted demand",
    "wastage_assumption": "Change wastage assumption",
}

INDICATORS = ("revenue", "contribution_margin", "demand", "wastage_cost", "profitability")


@dataclass(frozen=True)
class ScenarioBaseline:
    """Observed or forecast-derived inputs for one item/location horizon."""

    forecast_demand: float
    selling_price: float
    unit_cost: float
    wastage_rate: float
    wastage_unit_cost: float
    preparation_quantity: float
    current_discount_pct: float = 0.0
    observed_price_elasticity: float | None = None


def _nonnegative(name: str, value: float) -> float:
    number = float(value)
    if number < 0:
        raise ValueError(f"{name} must be non-negative")
    return number


def _validate_baseline(baseline: ScenarioBaseline) -> None:
    for name in ("forecast_demand", "selling_price", "unit_cost", "wastage_rate",
                 "wastage_unit_cost", "preparation_quantity", "current_discount_pct"):
        _nonnegative(name, getattr(baseline, name))
    if baseline.wastage_rate > 1:
        raise ValueError("wastage_rate must be a fraction between 0 and 1")
    if baseline.current_discount_pct > 100:
        raise ValueError("current_discount_pct must be between 0 and 100")
    if baseline.observed_price_elasticity is not None and abs(float(baseline.observed_price_elasticity)) > 20:
        raise ValueError("observed_price_elasticity is outside the supported sanity range")


def _indicators(
    demand: float,
    served_units: float,
    effective_price: float,
    unit_cost: float,
    wastage_quantity: float,
    wastage_unit_cost: float,
) -> dict[str, float]:
    revenue = served_units * effective_price
    margin = served_units * (effective_price - unit_cost)
    waste_cost = wastage_quantity * wastage_unit_cost
    return {
        "revenue": revenue,
        "contribution_margin": margin,
        "demand": demand,
        "wastage_cost": waste_cost,
        "profitability": margin - waste_cost,
    }


def simulate_scenario(
    scenario: str, baseline: ScenarioBaseline, *, change_pct: float | None = None,
    new_discount_pct: float | None = None, demand_response_pct: float | None = None,
    new_wastage_rate_pct: float | None = None,
) -> dict[str, Any]:
    """Return baseline and scenario KPI estimates using explicit assumptions.

    Estimates are not causal claims. Promotion lift and missing price elasticity
    must be supplied as user assumptions; the function never invents them.
    """
    if scenario not in SCENARIOS:
        raise ValueError(f"Unsupported scenario: {scenario}")
    _validate_baseline(baseline)
    demand = float(baseline.forecast_demand)
    prep = float(baseline.preparation_quantity)
    rate = float(baseline.wastage_rate)
    base_effective_price = baseline.selling_price * (1 - baseline.current_discount_pct / 100)
    base_served = min(demand, prep)
    base_waste_qty = prep * rate
    current = _indicators(demand, base_served, base_effective_price, baseline.unit_cost,
                          base_waste_qty, baseline.wastage_unit_cost)

    effective_price = base_effective_price
    elasticity = baseline.observed_price_elasticity
    assumption_note = "Forecast and historical rates are descriptive inputs; outputs are estimates."

    if scenario in {"increase_price", "reduce_price"}:
        if change_pct is None or change_pct <= 0:
            raise ValueError("change_pct must be positive for a price increase/decrease scenario")
        delta_pct = float(change_pct) * (1 if scenario == "increase_price" else -1)
        effective_price = max(0.0, baseline.selling_price * (1 + delta_pct / 100)
                              * (1 - baseline.current_discount_pct / 100))
        response = elasticity * delta_pct if elasticity is not None else float(demand_response_pct or 0.0)
        demand = max(0.0, demand * (1 + response / 100))
        if elasticity is None:
            assumption_note += " No reliable elasticity was available; user-entered demand response was used."
    elif scenario == "change_discount":
        if new_discount_pct is None or not 0 <= new_discount_pct <= 100:
            raise ValueError("new_discount_pct must be between 0 and 100")
        effective_price = baseline.selling_price * (1 - float(new_discount_pct) / 100)
        price_delta_pct = ((effective_price / base_effective_price) - 1) * 100 if base_effective_price else 0.0
        response = elasticity * price_delta_pct if elasticity is not None else float(demand_response_pct or 0.0)
        demand = max(0.0, demand * (1 + response / 100))
        if elasticity is None:
            assumption_note += " No reliable elasticity was available; user-entered demand response was used."
    elif scenario == "promotion_frequency":
        if demand_response_pct is None:
            raise ValueError("demand_response_pct is required as the assumed effect of the promotion-frequency change")
        demand = max(0.0, demand * (1 + float(demand_response_pct) / 100))
        assumption_note += " Promotion response is an explicit user assumption, not a causal estimate."
    elif scenario == "remove_item":
        demand, prep = 0.0, 0.0
        assumption_note += " Assumes no replacement item, substitution, or retained demand."
    elif scenario == "preparation_quantity":
        if change_pct is None or change_pct <= -100:
            raise ValueError("preparation change must be greater than -100 percent")
        prep = max(0.0, prep * (1 + float(change_pct) / 100))
    elif scenario == "increase_demand":
        if change_pct is None or change_pct <= -100:
            raise ValueError("demand change must be greater than -100 percent")
        demand = max(0.0, demand * (1 + float(change_pct) / 100))
    elif scenario == "wastage_assumption":
        if new_wastage_rate_pct is None or not 0 <= new_wastage_rate_pct <= 100:
            raise ValueError("new_wastage_rate_pct must be between 0 and 100")
        rate = float(new_wastage_rate_pct) / 100

    served = min(demand, prep)
    if scenario == "wastage_assumption":
        waste_qty = prep * rate
    elif scenario == "remove_item":
        waste_qty = 0.0
    else:
        # Forecast-served difference is the scenario's estimated excess prep.
        waste_qty = max(0.0, prep - served)
        if scenario in {"increase_price", "reduce_price", "change_discount", "promotion_frequency"}:
            waste_qty = max(waste_qty, prep * baseline.wastage_rate)

    estimated = _indicators(demand, served, effective_price, baseline.unit_cost,
                             waste_qty, baseline.wastage_unit_cost)
    return {
        "scenario": scenario,
        "scenario_label": SCENARIOS[scenario],
        "status": "ESTIMATE — simulated output, not an actual result",
        "assumptions": assumption_note,
        "indicators": list(INDICATORS),
        "baseline": current,
        "estimate": estimated,
        "change": {name: estimated[name] - current[name] for name in INDICATORS},
    }
