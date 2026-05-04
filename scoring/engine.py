from __future__ import annotations
from typing import Dict, Optional, Any, List, Tuple
from textwrap import dedent
import logging

logger = logging.getLogger("solvist.scoring")

# ─── Opportunity Type Constants ───────────────────────────────────────────────
OPP_BATTERY_UPGRADE = "battery_upgrade"
OPP_INVERTER_REPLACEMENT = "inverter_replacement"
OPP_SYSTEM_EXPANSION = "system_expansion"
OPP_EV_CHARGER = "ev_charger"
OPP_MAINTENANCE = "maintenance"
OPP_INDUSTRIAL_BATTERY = "industrial_battery"

OPP_DISPLAY_NAMES: Dict[str, str] = {
    OPP_BATTERY_UPGRADE: "Battery Upgrade",
    OPP_INVERTER_REPLACEMENT: "Inverter Replacement",
    OPP_SYSTEM_EXPANSION: "System Expansion",
    OPP_EV_CHARGER: "EV Charger",
    OPP_MAINTENANCE: "Maintenance Contract",
    OPP_INDUSTRIAL_BATTERY: "Industrial Battery",
}

# ─── Helper Functions ─────────────────────────────────────────────────────────

def _get_field(installation: Dict, field_name: str, default: Any = None) -> Any:
    """Get field from installation, checking both top-level and raw_payload."""
    if field_name in installation and installation[field_name] is not None:
        return installation[field_name]
    raw_payload = installation.get("raw_payload", {})
    if isinstance(raw_payload, dict) and field_name in raw_payload and raw_payload[field_name] is not None:
        return raw_payload[field_name]
    return default


def _has_field(installation: Dict, field_name: str) -> bool:
    """Check if a field exists and is not None."""
    value = _get_field(installation, field_name, None)
    return value is not None


# ─── Scoring Components (Additive System) ─────────────────────────────────────

def score_system_size(kwp: float) -> Tuple[float, str]:
    """Score based on system size - larger systems = more opportunity value."""
    if kwp <= 0:
        return 0.0, "no_system"

    # Progressive scoring: small systems get base score, larger systems scale up
    # 1-5 kWp: base value, 5-20 kWp: linear growth, 20+ kWp: premium
    base_score = min(kwp * 10, 50)  # Up to 50 points for size
    scale_bonus = min(kwp * 2, 30) if kwp >= 5 else 0  # Bonus for larger systems
    score = base_score + scale_bonus

    return min(score, 100), f"kwp={kwp}"


def score_installation_age(installation_year: Optional[int], now_year: int) -> Tuple[float, str]:
    """Score based on system age - older systems need more attention."""
    if installation_year is None:
        return 30.0, "age_unknown"  # Give benefit of doubt for missing data

    age = now_year - installation_year
    if age < 0:
        age = 0  # Future installation

    # Progressive scoring for age
    score = 0.0
    if age >= 1:
        score += 10  # At least 1 year old
    if age >= 3:
        score += min(age * 5, 20)  # 3+ years: aging bonus
    if age >= 7:
        score += 15  # Mature systems need maintenance
    if age >= 10:
        score += 20  # Inverter replacement territory

    return min(score, 65), f"age={age}years"


def score_battery(has_battery: Optional[bool], kwp: float, location_type: str, installation_year: Optional[int] = None, now_year: int = 2024) -> Tuple[float, str]:
    """Score battery opportunity - includes industrial logic and age bonus."""
    if has_battery is True:
        return 0.0, "has_battery"

    if has_battery is None:
        return 0.0, "battery_unknown"

    # has_battery is False
    if location_type == "industrial":
        score = 90.0 if kwp >= 30 else 70.0
    else:
        score = 40.0
        if kwp >= 5:
            score += min(kwp * 2, 20)
        if location_type == "residential":
            score += 10

    # Age bonus
    if installation_year is not None:
        age = now_year - installation_year
        if age >= 5:
            score += 15
        elif age >= 3:
            score += 8

    return min(score, 100), f"no_battery+kwp={kwp}+loc={location_type}"


def score_maintenance(has_maintenance: Optional[bool], installation_year: Optional[int], now_year: int, kwp: float) -> Tuple[float, str]:
    """Score maintenance opportunity - no contract = recurring revenue."""
    if has_maintenance is True:
        return 0.0, "has_maintenance"

    score = 25.0  # Base score for no maintenance

    # Age bonus
    if installation_year is not None:
        age = now_year - installation_year
        if age >= 3:
            score += min(age * 3, 25)

    # Size bonus (larger systems = more complex maintenance)
    if kwp >= 10:
        score += 15

    # Unknown status bonus
    if has_maintenance is None:
        score += 10

    return min(score, 65), "no_maintenance"


def score_ev_charger(kwp: float, location_type: str) -> Tuple[float, str]:
    """Score EV charger opportunity."""
    if kwp < 3:
        return 0.0, "kwp_too_small"

    score = 15.0  # Base score for compatible system

    if kwp >= 5:
        score += 15
    if kwp >= 10:
        score += 10
    if location_type == "residential":
        score += 10

    return min(score, 50), f"kwp={kwp}"



def score_expansion(kwp: float, estimated_consumption: Optional[float]) -> Tuple[float, str]:
    """Score system expansion opportunity."""
    if estimated_consumption is None or estimated_consumption <= 0:
        # Unknown consumption still has expansion potential
        return 15.0, "consumption_unknown"

    if kwp <= 0:
        return 0.0, "no_system"

    # Coverage ratio: how much of consumption is covered
    coverage_ratio = (kwp * 1200) / estimated_consumption

    if coverage_ratio >= 1.0:
        return 5.0, f"fully_covered({coverage_ratio:.1%})"
    elif coverage_ratio >= 0.8:
        return 15.0, f"mostly_covered({coverage_ratio:.1%})"
    elif coverage_ratio >= 0.5:
        return 35.0, f"partially_covered({coverage_ratio:.1%})"
    else:
        return 50.0, f"under_sized({coverage_ratio:.1%})"


def score_inverter(installation_year: Optional[int], now_year: int, dc_ac_ratio: Optional[float]) -> Tuple[float, str]:
    """Score inverter replacement opportunity."""
    score = 0.0
    details = []

    if installation_year is not None:
        age = now_year - installation_year
        if age >= 10:
            score += 45
            details.append(f"age={age}")
        elif age >= 7:
            score += 25
            details.append(f"age={age}")
        elif age >= 5:
            score += 10
            details.append(f"age={age}")
    else:
        score += 15  # Unknown age = potential opportunity
        details.append("age_unknown")

    if dc_ac_ratio is not None and dc_ac_ratio > 1.2:
        score += min(int((dc_ac_ratio - 1.2) * 50), 25)
        details.append(f"dc_ac={dc_ac_ratio:.2f}")

    detail_str = "+".join(details) if details else "inverter_ok"
    return min(score, 70), detail_str


def score_warranty(installation_year: Optional[int], now_year: int) -> Tuple[float, str]:
    """Score warranty opportunity based on system age."""
    if installation_year is None:
        return 0.0, "age_unknown"

    age = now_year - installation_year
    if age >= 10:
        return 80.0, f"age={age}"
    elif age >= 7:
        return 50.0, f"age={age}"
    elif age >= 5:
        return 20.0, f"age={age}"
    else:
        return 0.0, f"age={age}"


# ─── Monetization: Dynamic Opportunity Value Calculation ──────────────────────

def estimate_opportunity_value(
    kwp: float,
    installation_year: Optional[int],
    has_battery: Optional[bool],
    has_maintenance: Optional[bool],
    location_type: str,
    estimated_consumption: Optional[float],
    component_scores: Dict[str, float],
    total_score: float,
    now_year: int,
) -> Tuple[float, Dict[str, float]]:
    """
    Calculate realistic € opportunity value based on installation characteristics.

    Returns:
        Tuple of (total_expected_value, breakdown_by_component)

    Logic:
        - Each opportunity component contributes independently
        - Value scales with kwp (larger systems = larger revenue)
        - Value is adjusted by confidence (total_score / 100)
    """
    breakdown = {}
    base_value = 0.0

    # 1. Battery Opportunity
    # - Average battery installation: ~800€ base + kwp scaling
    # - Larger systems need larger batteries
    battery_value = 0.0
    if has_battery is False or has_battery is None:
        battery_value = 800 + (kwp * 200)  # Base + kwp scaling
        if has_battery is None:
            battery_value *= 0.7  # Uncertainty discount
    breakdown["battery"] = round(battery_value, 2)
    base_value += battery_value

    # 2. Maintenance Opportunity
    # - Annual maintenance contract: ~30€/kwp/year
    # - Multi-year potential: 3 years
    maintenance_value = 0.0
    if has_maintenance is False or has_maintenance is None:
        maintenance_value = kwp * 30 * 3  # 30€/kwp/year × 3 years
        if has_maintenance is None:
            maintenance_value *= 0.7
    breakdown["maintenance"] = round(maintenance_value, 2)
    base_value += maintenance_value

    # 3. Inverter Replacement
    # - Inverters typically need replacement after 10-15 years
    # - Cost scales with system size
    inverter_value = 0.0
    if installation_year is not None:
        age = now_year - installation_year
        if age >= 8:
            # Progressive value increase with age
            inverter_value = kwp * 100 * (1 + (age - 8) * 0.1)  # Base 100€/kwp + age bonus
    else:
        # Unknown age - potential opportunity
        inverter_value = kwp * 50
    breakdown["inverter"] = round(inverter_value, 2)
    base_value += inverter_value

    # 4. System Expansion
    # - Under-sized systems can be expanded
    # - Base expansion value
    expansion_value = 0.0
    if estimated_consumption is not None and estimated_consumption > 0 and kwp > 0:
        coverage_ratio = (kwp * 1200) / estimated_consumption
        if coverage_ratio < 0.7:
            # Significant expansion opportunity
            expansion_value = 500 + kwp * 50
        elif coverage_ratio < 0.9:
            # Moderate expansion
            expansion_value = 300 + kwp * 30
    else:
        # Unknown consumption - potential opportunity
        expansion_value = 200
    breakdown["expansion"] = round(expansion_value, 2)
    base_value += expansion_value

    # 5. EV Charger
    # - Installation of EV charging point
    # - Only viable for systems >= 4kWp
    ev_value = 0.0
    if kwp >= 4:
        ev_value = 1200 + (kwp - 4) * 50  # Base + bonus for larger systems
        if location_type == "residential":
            ev_value *= 1.1  # Residential bonus (higher EV adoption)
    breakdown["ev_charger"] = round(ev_value, 2)
    base_value += ev_value

    # 6. Warranty Opportunity
    # - Out-of-warranty systems need extended coverage or component replacement
    warranty_value = 0.0
    if installation_year is not None:
        age = now_year - installation_year
        if age >= 7:
            warranty_value = 600.0
        elif age >= 5:
            warranty_value = 400.0
    breakdown["warranty"] = round(warranty_value, 2)
    base_value += warranty_value

    # Apply confidence multiplier based on score
    # Higher score = higher confidence = more realistic value
    confidence_multiplier = min(total_score / 100, 1.0)
    if confidence_multiplier < 0.3:
        confidence_multiplier = 0.3  # Minimum 30% confidence for any opportunity

    expected_value = base_value * confidence_multiplier

    # Ensure minimum viable value for any opportunity
    if expected_value < 100 and total_score >= 20:
        expected_value = 100  # Minimum 100€ for any scored opportunity

    breakdown["base_value"] = round(base_value, 2)
    breakdown["confidence"] = round(confidence_multiplier, 2)
    breakdown["expected_value"] = round(expected_value, 2)

    return expected_value, breakdown


# ─── Main Scoring Engine ───────────────────────────────────────────────────────

def compute_opportunity_score(
    installation: Dict,
    weights: Dict[str, float],
    now_year: int,
    calculated_month: str,
) -> Dict:
    """Pure-python opportunity scoring engine.

    Additive scoring system: each component contributes independently.
    Handles missing data gracefully - never returns 0 for valid installations.
    """
    installation_id = installation.get("id", "unknown")
    company_id = installation.get("company_id", "")

    # Debug: log detected fields
    logger.debug(f"Scoring installation {installation_id}")
    logger.debug(f"  Top-level fields: {list(installation.keys())}")
    if installation.get("raw_payload"):
        logger.debug(f"  raw_payload fields: {list(installation.get('raw_payload', {}).keys())}")

    # Extract fields with safe defaults
    kwp = float(_get_field(installation, "kwp") or _get_field(installation, "system_size_kwp") or 0)
    installation_year_raw = _get_field(installation, "installation_year")
    installation_year = int(installation_year_raw) if installation_year_raw else None
    has_battery = _get_field(installation, "has_battery")  # Can be True, False, or None
    location_type = str(_get_field(installation, "location_type") or "residential").lower()
    has_maintenance_contract = _get_field(installation, "has_maintenance_contract")  # Can be True, False, or None
    dc_ac_ratio = _get_field(installation, "dc_ac_ratio")
    estimated_consumption = _get_field(installation, "estimated_consumption")

    logger.debug(f"  Extracted: kwp={kwp}, year={installation_year}, battery={has_battery}, location={location_type}")

    # Calculate individual component scores (additive system)
    component_scores = {}
    component_details = {}

    # 1. System size score (always applicable if kwp > 0)
    component_scores["size"], component_details["size"] = score_system_size(kwp)

    # 2. Age-based score
    component_scores["age"], component_details["age"] = score_installation_age(installation_year, now_year)

    # 3. Battery opportunity (includes industrial logic)
    component_scores["battery"], component_details["battery"] = score_battery(
        has_battery, kwp, location_type, installation_year, now_year
    )

    # 4. Maintenance opportunity
    component_scores["maintenance"], component_details["maintenance"] = score_maintenance(
        has_maintenance_contract, installation_year, now_year, kwp
    )

    # 5. EV charger opportunity
    component_scores["ev"], component_details["ev"] = score_ev_charger(kwp, location_type)

    # 6. Expansion opportunity
    component_scores["expansion"], component_details["expansion"] = score_expansion(kwp, estimated_consumption)

    # 7. Inverter replacement
    component_scores["inverter"], component_details["inverter"] = score_inverter(
        installation_year, now_year, dc_ac_ratio
    )

    # 8. Warranty opportunity
    component_scores["warranty"], component_details["warranty"] = score_warranty(installation_year, now_year)

    # Log component breakdown
    for component, score in component_scores.items():
        logger.debug(f"  {component}: {score:.1f} ({component_details[component]})")

    # Calculate weighted total score (weights sum = 1.00)
    total_raw = (
        component_scores["battery"] * 0.30 +
        component_scores["age"] * 0.15 +
        component_scores["maintenance"] * 0.15 +
        component_scores["inverter"] * 0.12 +
        component_scores["expansion"] * 0.10 +
        component_scores["ev"] * 0.08 +
        component_scores["size"] * 0.08 +
        component_scores["warranty"] * 0.02
    )

    total_score = min(round(total_raw), 100)

    # Ensure minimum score for any valid installation with data
    if kwp > 0 and total_score < 15:
        total_score = 15  # Minimum viable opportunity

    logger.debug(f"  TOTAL SCORE: {total_score}")

    # Determine primary reason (highest scoring component)
    scored_reasons = {
        OPP_BATTERY_UPGRADE: component_scores["battery"],
        OPP_MAINTENANCE: component_scores["maintenance"],
        OPP_SYSTEM_EXPANSION: component_scores["expansion"],
        OPP_EV_CHARGER: component_scores["ev"],
        OPP_INVERTER_REPLACEMENT: component_scores["inverter"],
    }
    primary_reason = max(scored_reasons, key=lambda k: scored_reasons[k])

    # Close probability based on total score
    if total_score < 30:
        close_probability = 0.1
    elif total_score < 50:
        close_probability = 0.2
    elif total_score < 70:
        close_probability = 0.35
    elif total_score < 85:
        close_probability = 0.5
    else:
        close_probability = 0.65

    # Priority score combines total score with battery opportunity (highest value)
    priority_score = round(
        total_score * 0.5 +
        component_scores["battery"] * 0.3 +
        close_probability * 100 * 0.2,
        1
    )

    # Recommendation level
    if total_score >= 70:
        recommendation_level = "Candidato fuerte"
    elif total_score >= 50:
        recommendation_level = "Buena oportunidad"
    elif total_score >= 30:
        recommendation_level = "Oportunidad moderada"
    else:
        recommendation_level = "Baja prioridad"

    # Battery economics (for sales script)
    system_age = now_year - installation_year if installation_year else 5
    estimated_annual_export_kwh = round(kwp * 900 * 0.3) if has_battery is False else 0
    electricity_price_per_kwh = 0.22
    estimated_battery_savings = round(estimated_annual_export_kwh * electricity_price_per_kwh, 2)
    battery_cost_estimate = kwp * 400
    battery_payback_years = round(battery_cost_estimate / estimated_battery_savings, 1) if estimated_battery_savings > 0 else 0

    # Recommended action
    action_map = {
        OPP_BATTERY_UPGRADE: "Contactar cliente para propuesta de bateria de almacenamientos. Sistema sin bateria con potencial de ahorro identificado.",
        OPP_INVERTER_REPLACEMENT: "Revisar estado del inversor. Sistema con antiguedad elevada - posible sustitucion preventiva recomendada.",
        OPP_SYSTEM_EXPANSION: "Analizar ampliacion del sistema fotovoltaico. Consumo no cubierto detectado.",
        OPP_EV_CHARGER: "Proponer instalacion de punto de recarga para vehiculo electrico. Sistema compatible.",
        OPP_MAINTENANCE: "Ofrecer contrato de mantenimiento anual. Instalacion sin cobertura activa.",
        OPP_INDUSTRIAL_BATTERY: "Proponer sistema de almacenamiento industrial. Instalacion de gran potencia sin bateria.",
    }
    recommended_action = action_map.get(primary_reason, "Revisar instalacion y contactar cliente.")

    # Sales scripts
    opp_name = OPP_DISPLAY_NAMES.get(primary_reason, primary_reason)
    year_display = installation_year if installation_year else "desconocido"
    battery_display = "Si" if has_battery is True else ("No" if has_battery is False else "Desconocido")
    maintenance_display = "Si" if has_maintenance_contract is True else ("No" if has_maintenance_contract is False else "Desconocido")

    sales_script_short = (
        f"Sistema de {kwp} kWp instalado en {year_display}. "
        f"Oportunidad: {opp_name}. Score: {total_score}/100."
    )
    sales_script_long = dedent(f"""\
        Analisis de Oportunidad Comercial
        ==================================
        Sistema: {kwp} kWp | Ano: {year_display} | Antiguedad: {system_age} anos
        Tipo: {location_type.capitalize()}
        Bateria: {battery_display} | Mantenimiento: {maintenance_display}

        Oportunidad principal: {opp_name}
        Score total: {total_score}/100
        Probabilidad de cierre: {int(close_probability * 100)}%

        Desglose de score:
        - Tamano sistema: {component_scores['size']:.0f} ({component_details['size']})
        - Edad: {component_scores['age']:.0f} ({component_details['age']})
        - Bateria: {component_scores['battery']:.0f} ({component_details['battery']})
        - Mantenimiento: {component_scores['maintenance']:.0f} ({component_details['maintenance']})

        Accion: {recommended_action}
    """).strip()

    opportunity_reason = (
        f"Score {total_score}/100. Principal: {opp_name}. "
        f"Componentes: bateria={component_scores['battery']:.0f}, edad={component_scores['age']:.0f}."
    )

    # Determine if this is an opportunity (lower threshold for additive system)
    is_opportunity = total_score >= 20  # Lower threshold since scores are additive

    # Calculate dynamic opportunity value (€)
    expected_value, value_breakdown = estimate_opportunity_value(
        kwp=kwp,
        installation_year=installation_year,
        has_battery=has_battery,
        has_maintenance=has_maintenance_contract,
        location_type=location_type,
        estimated_consumption=estimated_consumption,
        component_scores=component_scores,
        total_score=total_score,
        now_year=now_year,
    )

    logger.debug(f"  EXPECTED VALUE: €{expected_value:.0f} (base: €{value_breakdown['base_value']:.0f}, confidence: {value_breakdown['confidence']:.0%})")

    return {
        "installation_id": installation_id,
        "company_id": company_id,
        "calculated_month": calculated_month,
        "total_score": total_score,
        "battery_score": round(component_scores["battery"]),
        "maintenance_score": round(component_scores["maintenance"]),
        "ev_score": round(component_scores["ev"]),
        "inverter_score": round(component_scores["inverter"]),
        "warranty_score": round(component_scores["warranty"]),
        "primary_reason": primary_reason,
        "recommended_action": recommended_action,
        "close_probability": close_probability,
        "priority_score": priority_score,
        "recommendation_level": recommendation_level,
        "is_opportunity": is_opportunity,
        "expected_value": round(expected_value, 2),
        "value_breakdown": value_breakdown,
        "estimated_annual_export_kwh": estimated_annual_export_kwh,
        "estimated_battery_savings": estimated_battery_savings,
        "battery_payback_years": battery_payback_years,
        "battery_opportunity_score": round(component_scores["battery"]),
        "sales_script_short": sales_script_short,
        "sales_script_long": sales_script_long,
        "opportunity_reason": opportunity_reason,
    }