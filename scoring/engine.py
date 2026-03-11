from __future__ import annotations
from typing import Dict, Optional
from textwrap import dedent

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

# ─── Scoring Engine ───────────────────────────────────────────────────────────

def compute_opportunity_score(
    installation: Dict,
    weights: Dict[str, float],
    now_year: int,
    calculated_month: str,
) -> Dict:
    """Pure-python opportunity scoring engine.
    Returns a result dict with all scoring fields.
    """
    kwp = float(installation.get("kwp") or 0)
    installation_year = int(installation.get("installation_year") or 2000)
    has_battery = bool(installation.get("has_battery", False))
    location_type = str(installation.get("location_type") or "residential").lower()
    has_maintenance_contract = bool(installation.get("has_maintenance_contract", False))
    dc_ac_ratio = float(installation.get("dc_ac_ratio") or 1.0)
    estimated_consumption = float(installation.get("estimated_consumption") or 0)
    company_id = installation.get("company_id", "")
    installation_id = installation.get("id", "")

    system_age = now_year - installation_year

    # ─── Battery Score ────────────────────────────────────────────────────────
    battery_score = 0.0
    if not has_battery:
        battery_score += 30
        if system_age >= 3:
            battery_score += min(system_age * 2, 30)
        if kwp >= 5:
            battery_score += min((kwp - 5) * 1.5, 20)
        if location_type == "residential":
            battery_score += 10
    battery_score = min(battery_score * weights.get("battery", 1.0), 100)

    # ─── Maintenance Score ────────────────────────────────────────────────────
    maintenance_score = 0.0
    if not has_maintenance_contract:
        maintenance_score += 20
        if system_age >= 5:
            maintenance_score += min((system_age - 5) * 3, 40)
        if kwp >= 10:
            maintenance_score += 15
    maintenance_score = min(maintenance_score * weights.get("maintenance", 1.0), 100)

    # ─── Expansion Score ─────────────────────────────────────────────────────
    expansion_score = 0.0
    if estimated_consumption > 0 and kwp > 0:
        coverage_ratio = (kwp * 1200) / estimated_consumption
        if coverage_ratio < 0.7:
            expansion_score += 40
        elif coverage_ratio < 0.9:
            expansion_score += 20
    if system_age >= 2:
        expansion_score += 10
    expansion_score = min(expansion_score * weights.get("expansion", 1.0), 100)

    # ─── EV Charger Score ─────────────────────────────────────────────────────
    ev_score = 0.0
    if kwp >= 5:
        ev_score += 20
    if kwp >= 10:
        ev_score += 20
    if location_type == "residential":
        ev_score += 15
    ev_score = min(ev_score * weights.get("ev", 1.0), 100)

    # ─── Industrial Battery Score ─────────────────────────────────────────────
    industrial_score = 0.0
    if location_type == "industrial":
        industrial_score += 40
        if kwp >= 50:
            industrial_score += 30
        if not has_battery:
            industrial_score += 20
    industrial_score = min(industrial_score * weights.get("industrial", 1.0), 100)

    # ─── Inverter Score ───────────────────────────────────────────────────────
    inverter_score = 0.0
    if system_age >= 10:
        inverter_score += 50
    elif system_age >= 7:
        inverter_score += 25
    if dc_ac_ratio > 1.3:
        inverter_score += 20
    inverter_score = min(inverter_score, 100)

    # ─── Total Score ──────────────────────────────────────────────────────────
    total_score = round(
        battery_score * 0.35
        + maintenance_score * 0.20
        + expansion_score * 0.20
        + ev_score * 0.10
        + industrial_score * 0.10
        + inverter_score * 0.05
    )
    total_score = min(total_score, 100)

    # ─── Primary Reason ───────────────────────────────────────────────────────
    scores = {
        OPP_BATTERY_UPGRADE: battery_score,
        OPP_MAINTENANCE: maintenance_score,
        OPP_SYSTEM_EXPANSION: expansion_score,
        OPP_EV_CHARGER: ev_score,
        OPP_INDUSTRIAL_BATTERY: industrial_score,
        OPP_INVERTER_REPLACEMENT: inverter_score,
    }
    primary_reason = max(scores, key=lambda k: scores[k])

    # ─── Close Probability ────────────────────────────────────────────────────
    if total_score < 40:
        close_probability = 0.1
    elif total_score <= 60:
        close_probability = 0.25
    elif total_score <= 80:
        close_probability = 0.45
    else:
        close_probability = 0.65

    # ─── Priority Score ───────────────────────────────────────────────────────
    priority_score = round((total_score * 0.4) + (battery_score * 0.3) + (close_probability * 100 * 0.3), 1)

    # ─── Recommendation Level ─────────────────────────────────────────────────
    if total_score >= 80:
        recommendation_level = "Candidato fuerte"
    elif total_score >= 60:
        recommendation_level = "Buena oportunidad"
    elif total_score >= 40:
        recommendation_level = "Oportunidad moderada"
    else:
        recommendation_level = "Baja prioridad"

    # ─── Battery Economics ────────────────────────────────────────────────────
    estimated_annual_export_kwh = round(kwp * 900 * 0.3) if not has_battery else 0
    electricity_price_per_kwh = 0.22
    estimated_battery_savings = round(estimated_annual_export_kwh * electricity_price_per_kwh, 2)
    battery_cost_estimate = kwp * 400
    battery_payback_years = round(battery_cost_estimate / estimated_battery_savings, 1) if estimated_battery_savings > 0 else 0
    battery_opportunity_score = round(battery_score)

    # ─── Recommended Action ───────────────────────────────────────────────────
    action_map = {
        OPP_BATTERY_UPGRADE: "Contactar cliente para propuesta de bateria de almacenamiento. Sistema sin bateria con potencial de ahorro identificado.",
        OPP_INVERTER_REPLACEMENT: "Revisar estado del inversor. Sistema con antiguedad elevada - posible sustitucion preventiva recomendada.",
        OPP_SYSTEM_EXPANSION: "Analizar ampliacion del sistema fotovoltaico. Consumo no cubierto detectado.",
        OPP_EV_CHARGER: "Proponer instalacion de punto de recarga para vehiculo electrico. Sistema compatible.",
        OPP_MAINTENANCE: "Ofrecer contrato de mantenimiento anual. Instalacion sin cobertura activa.",
        OPP_INDUSTRIAL_BATTERY: "Proponer sistema de almacenamiento industrial. Instalacion de gran potencia sin bateria.",
    }
    recommended_action = action_map.get(primary_reason, "Revisar instalacion y contactar cliente.")

    # ─── Sales Scripts ────────────────────────────────────────────────────────
    opp_name = OPP_DISPLAY_NAMES.get(primary_reason, primary_reason)
    sales_script_short = (
        f"Sistema de {kwp} kWp instalado en {installation_year}. "
        f"Oportunidad detectada: {opp_name}. Score: {total_score}/100."
    )
    sales_script_long = dedent(f"""\
        Analisis de Oportunidad Comercial
        ==================================
        Sistema: {kwp} kWp | Ano de instalacion: {installation_year} | Antiguedad: {system_age} anos
        Tipo de instalacion: {location_type.capitalize()}
        Bateria instalada: {'Si' if has_battery else 'No'}
        Contrato de mantenimiento: {'Si' if has_maintenance_contract else 'No'}

        Oportunidad principal: {opp_name}
        Score total: {total_score}/100
        Probabilidad de cierre estimada: {int(close_probability * 100)}%

        Ahorro anual estimado con bateria: EUR {estimated_battery_savings:,.0f}/ano
        Periodo de amortizacion estimado: {battery_payback_years} anos
        Exportacion anual estimada: {estimated_annual_export_kwh:,} kWh/ano

        Accion recomendada:
        {recommended_action}
    """).strip()

    opportunity_reason = (
        f"Score {total_score}/100. Vector principal: {opp_name}. "
        f"Antiguedad sistema: {system_age} anos."
    )

    is_opportunity = total_score >= 40

    return {
        "installation_id": installation_id,
        "company_id": company_id,
        "calculated_month": calculated_month,
        "total_score": total_score,
        "battery_score": round(battery_score),
        "maintenance_score": round(maintenance_score),
        "ev_score": round(ev_score),
        "inverter_score": round(inverter_score),
        "primary_reason": primary_reason,
        "recommended_action": recommended_action,
        "close_probability": close_probability,
        "priority_score": priority_score,
        "recommendation_level": recommendation_level,
        "is_opportunity": is_opportunity,
        "estimated_annual_export_kwh": estimated_annual_export_kwh,
        "estimated_battery_savings": estimated_battery_savings,
        "battery_payback_years": battery_payback_years,
        "battery_opportunity_score": battery_opportunity_score,
        "sales_script_short": sales_script_short,
        "sales_script_long": sales_script_long,
        "opportunity_reason": opportunity_reason,
    }
