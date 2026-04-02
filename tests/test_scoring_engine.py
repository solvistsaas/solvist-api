"""Smoke tests for the scoring engine."""
import pytest
from scoring.engine import compute_opportunity_score

EXPECTED_KEYS = {
    "installation_id", "company_id", "calculated_month",
    "total_score", "battery_score", "maintenance_score", "ev_score", "inverter_score",
    "primary_reason", "recommended_action", "close_probability",
    "priority_score", "recommendation_level", "is_opportunity",
    "expected_value", "value_breakdown", "sales_script_short",
    "sales_script_long", "opportunity_reason",
}

DEFAULT_WEIGHTS = {
    "size": 0.15, "age": 0.15, "battery": 0.25, "maintenance": 0.15,
    "ev": 0.08, "industrial": 0.07, "expansion": 0.10,
    "inverter": 0.05, "uncertainty": 0.05,
}


@pytest.fixture
def sample_installation():
    return {
        "id": "inst-001",
        "company_id": "company-001",
        "kwp": 10.0,
        "installation_year": 2015,
        "has_battery": False,
        "location_type": "residential",
        "has_maintenance_contract": False,
    }


# a) retorna un dict con las claves esperadas
def test_returns_expected_keys(sample_installation):
    result = compute_opportunity_score(sample_installation, DEFAULT_WEIGHTS, 2024, "2024-01")
    assert EXPECTED_KEYS.issubset(result.keys())


# b) total_score está entre 0-100
def test_total_score_in_range(sample_installation):
    result = compute_opportunity_score(sample_installation, DEFAULT_WEIGHTS, 2024, "2024-01")
    assert 0 <= result["total_score"] <= 100


# c) component scores contribuyen al total (suma ponderada parcial ≤ total_score)
def test_component_scores_consistent_with_total(sample_installation):
    result = compute_opportunity_score(sample_installation, DEFAULT_WEIGHTS, 2024, "2024-01")
    partial_weighted_sum = (
        result["battery_score"] * 0.25 +
        result["maintenance_score"] * 0.15 +
        result["ev_score"] * 0.08 +
        result["inverter_score"] * 0.05
    )
    # Suma parcial de 4 componentes debe ser <= total (resto de componentes contribuyen positivamente)
    assert partial_weighted_sum <= result["total_score"] + 5  # +5 tolerancia por redondeo


# d) con datos vacíos no lanza excepción (retorna defaults)
def test_empty_installation_no_exception():
    result = compute_opportunity_score({}, DEFAULT_WEIGHTS, 2024, "2024-01")
    assert isinstance(result, dict)
    assert 0 <= result["total_score"] <= 100


# e) weights son aceptados como parámetro sin crash
def test_custom_weights_accepted(sample_installation):
    # El motor acepta cualquier dict de weights como parámetro de API
    custom_weights = {"battery": 0.8, "age": 0.2}
    result = compute_opportunity_score(sample_installation, custom_weights, 2024, "2024-01")
    assert "total_score" in result
    assert 0 <= result["total_score"] <= 100
