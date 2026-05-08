"""
SOLVIST Scoring Engine — compute_opportunity_score()
Modificado: bloque de economía de batería reemplazado con datos de mercado PR validados 2025.

NOTA DE DESPLIEGUE: Copiar este archivo a scoring/engine.py en el repo solvist-api (Render).
El bloque modificado va desde "# ─── Cálculo con datos de mercado PR" hasta
el logger.debug() antes del return final.

INSTRUCCIONES DE PATCH:
1. En engine.py original, BORRAR desde la línea "# Battery economics (for sales script)"
   hasta la línea "logger.debug(f"  EXPECTED VALUE: €{expected_value:.0f}...")" inclusive.
2. PEGAR el bloque marcado entre ">>> START PATCH" y ">>> END PATCH".
3. En el return {} final, AÑADIR el campo: "battery_kwh_recommended": battery_kwh_recommended,
"""
from __future__ import annotations
from typing import Dict, Optional, Any, List, Tuple
from textwrap import dedent
import logging

logger = logging.getLogger("solvist.scoring")

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


def compute_opportunity_score(
    installation_id: str,
    company_id: str,
    kwp: float,
    installation_year: Optional[int],
    has_battery: Optional[bool],
    has_maintenance_contract: Optional[bool],
    location_type: str,
    estimated_consumption: Optional[float] = None,
    calculated_month: Optional[str] = None,
    # ... (rest of original params remain unchanged)
    **kwargs,
) -> Dict[str, Any]:
    """
    Compute opportunity score for a solar installation.
    Firma ORIGINAL — NO se modifica.
    """
    from datetime import datetime
    now_year = datetime.now().year

    # ─── Component scoring (UNCHANGED from original) ──────────────────────
    component_scores: Dict[str, float] = {}
    component_details: Dict[str, str] = {}

    # Size component
    if kwp >= 100:
        component_scores["size"] = 25
        component_details["size"] = "Large C&I system (≥100 kWp)"
    elif kwp >= 50:
        component_scores["size"] = 20
        component_details["size"] = "Medium C&I system (50-99 kWp)"
    elif kwp >= 20:
        component_scores["size"] = 15
        component_details["size"] = "Small C&I system (20-49 kWp)"
    else:
        component_scores["size"] = 10
        component_details["size"] = "Residential/small system (<20 kWp)"

    # Age component
    system_age = now_year - installation_year if installation_year else 5
    if system_age >= 8:
        component_scores["age"] = 20
        component_details["age"] = f"Old system ({system_age} years)"
    elif system_age >= 5:
        component_scores["age"] = 15
        component_details["age"] = f"Aging system ({system_age} years)"
    elif system_age >= 3:
        component_scores["age"] = 10
        component_details["age"] = f"Mid-age system ({system_age} years)"
    else:
        component_scores["age"] = 5
        component_details["age"] = f"Recent system ({system_age} years)"

    # Battery component
    if has_battery is False:
        component_scores["battery"] = 25
        component_details["battery"] = "No battery — high storage potential"
    elif has_battery is True:
        component_scores["battery"] = 5
        component_details["battery"] = "Has battery — upsell potential"
    else:
        component_scores["battery"] = 15
        component_details["battery"] = "Battery status unknown"

    # Maintenance component
    if has_maintenance_contract is False:
        component_scores["maintenance"] = 15
        component_details["maintenance"] = "No O&M contract — at-risk system"
    elif has_maintenance_contract is True:
        component_scores["maintenance"] = 5
        component_details["maintenance"] = "Has O&M contract"
    else:
        component_scores["maintenance"] = 10
        component_details["maintenance"] = "O&M contract unknown"

    # EV component (placeholder)
    component_scores["ev"] = 5
    component_details["ev"] = "EV potential not evaluated"

    # Inverter component
    if system_age >= 8:
        component_scores["inverter"] = 15
        component_details["inverter"] = "Inverter likely needs replacement"
    elif system_age >= 5:
        component_scores["inverter"] = 10
        component_details["inverter"] = "Inverter age warrants review"
    else:
        component_scores["inverter"] = 3
        component_details["inverter"] = "Inverter within lifespan"

    # Warranty component
    component_scores["warranty"] = 5
    component_details["warranty"] = "Warranty status not evaluated"

    # ─── Total score and primary reason (UNCHANGED) ──────────────────────
    total_score = sum(component_scores.values())
    total_score = min(total_score, 100)

    # Determine primary reason
    reason_scores = {
        OPP_BATTERY_UPGRADE: component_scores["battery"],
        OPP_INVERTER_REPLACEMENT: component_scores["inverter"],
        OPP_SYSTEM_EXPANSION: component_scores["size"],
        OPP_EV_CHARGER: component_scores["ev"],
        OPP_MAINTENANCE: component_scores["maintenance"],
        OPP_INDUSTRIAL_BATTERY: component_scores["battery"] if kwp >= 50 else 0,
    }
    primary_reason = max(reason_scores, key=reason_scores.get)

    # Close probability
    close_probability = min(0.95, total_score / 100)
    priority_score = round(total_score * close_probability, 1)

    if total_score >= 80:
        recommendation_level = "hot"
    elif total_score >= 60:
        recommendation_level = "warm"
    elif total_score >= 40:
        recommendation_level = "moderate"
    else:
        recommendation_level = "low"

    # ─── Value estimation (UNCHANGED — original estimate_opportunity_value) ──
    def estimate_opportunity_value(
        kwp, installation_year, has_battery, has_maintenance,
        location_type, estimated_consumption, component_scores,
        total_score, now_year,
    ):
        base_value = kwp * 350
        confidence = close_probability
        return base_value * confidence, {"base_value": base_value, "confidence": confidence}


    # ═══════════════════════════════════════════════════════════════════════════
    # >>> START PATCH — Reemplazar desde aquí hasta >>> END PATCH
    #
    # BORRAR en el engine.py original:
    #   Desde:  # Battery economics (for sales script)
    #   Hasta:  logger.debug(f"  EXPECTED VALUE: €{expected_value:.0f}...")
    #   (inclusive ambas líneas y todo entre ellas)
    #
    # PEGAR el bloque que empieza justo debajo de esta caja de comentarios.
    # ═══════════════════════════════════════════════════════════════════════════

    # ─── Cálculo con datos de mercado PR validados 2025 ──────────────────────
    from scoring.markets.pr import (
        calculate_expected_value_pr,
        get_recommended_action_pr,
        OPP_DISPLAY_NAMES_PR,
        BATTERY_COST_PER_KWH,
        BATTERY_KWH_PER_KWP,
        ELECTRICITY_PRICE_GSS,
        ANNUAL_PRODUCTION_KWH_PER_KWP,
        EXPORT_RATIO_WITHOUT_BATTERY,
        INVERTER_ALERT_YEAR_PR,
    )

    expected_value, value_breakdown = calculate_expected_value_pr(
        opportunity_type=primary_reason,
        kwp=kwp,
        installation_year=installation_year,
        has_battery=has_battery,
        location_type=location_type,
        now_year=now_year,
    )

    recommended_action, sales_script_long = get_recommended_action_pr(
        opportunity_type=primary_reason,
        kwp=kwp,
        installation_year=installation_year,
        has_battery=has_battery,
        location_type=location_type,
        expected_value=expected_value,
        breakdown=value_breakdown,
        now_year=now_year,
    )

    opp_name = OPP_DISPLAY_NAMES_PR.get(primary_reason, primary_reason)
    year_display = installation_year if installation_year else "unknown"
    system_age = now_year - installation_year if installation_year else 5
    battery_kwh_recommended = kwp * BATTERY_KWH_PER_KWP
    battery_cost_estimate = battery_kwh_recommended * BATTERY_COST_PER_KWH
    estimated_annual_export_kwh = round(
        kwp * ANNUAL_PRODUCTION_KWH_PER_KWP * EXPORT_RATIO_WITHOUT_BATTERY
    )
    estimated_battery_savings = round(
        estimated_annual_export_kwh * ELECTRICITY_PRICE_GSS, 2
    )
    battery_payback_years = (
        round(battery_cost_estimate / estimated_battery_savings, 1)
        if estimated_battery_savings > 0 else 0
    )
    sales_script_short = (
        f"{kwp} kWp system ({year_display}). "
        f"Opportunity: {opp_name}. "
        f"Estimated value: ${expected_value:,.0f}."
    )
    opportunity_reason = (
        f"Score {total_score}/100. Primary: {opp_name}. "
        f"Estimated value: ${expected_value:,.0f}."
    )

    # Determine if this is an opportunity (lower threshold for additive system)
    is_opportunity = total_score >= 20

    logger.debug(f"  EXPECTED VALUE PR: ${expected_value:.0f} | {opp_name}")

    # ═══════════════════════════════════════════════════════════════════════════
    # >>> END PATCH
    # ═══════════════════════════════════════════════════════════════════════════

    # ─── Return (UNCHANGED except battery_kwh_recommended added) ──────────
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
        "battery_kwh_recommended": battery_kwh_recommended,  # ← NUEVO CAMPO
    }
