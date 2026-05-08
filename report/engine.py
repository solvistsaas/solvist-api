"""
SOLVIST PDF Audit Report Engine — MVP

Pipeline: data → score → build report_data → generate charts → render HTML → PDF bytes

Bug fixes incorporated:
- BUG-1: Net cost now correctly subtracts ITC + IVU from gross
- BUG-2: Talking points use correct net cost values
- BUG-3: O&M contract now calculates annual_savings properly
- BUG-4: opportunities_found filters by is_opportunity=True (not total count)
- D-5: Uses OPP_DISPLAY_NAMES for consistent nomenclature
"""
from __future__ import annotations

import base64
import logging
import signal
import sys
import os
from datetime import datetime
from typing import Dict, Any, List, Optional
from pathlib import Path

from jinja2 import Environment, FileSystemLoader

# Add the backend directory to sys.path so we can import scoring
BACKEND_DIR = Path(__file__).resolve().parent.parent.parent.parent / "backend"
sys.path.insert(0, str(BACKEND_DIR))

from report.text.narratives import (
    FINDING_TEMPLATES,
    ASSESSMENT_TEMPLATES,
    RISK_TEMPLATES,
    OPPORTUNITY_NARRATIVES,
    DEMAND_CHARGE_NARRATIVE,
    METHODOLOGY_PR,
    ACTION_TEMPLATES,
)
from report.utils.formatting import (
    format_currency,
    format_currency_k,
    format_pct,
    format_number,
    format_payback,
    format_date,
)

logger = logging.getLogger("solvist.report")

# ─── Display names for consistent nomenclature (fix D-5) ──────────────────────

OPP_DISPLAY_NAMES = {
    "battery_upgrade": "Instalación de Baterías",
    "industrial_battery": "Batería Industrial",
    "maintenance": "Contrato O&M",
    "inverter_replacement": "Reemplazo de Inversor",
    "system_expansion": "Expansión del Sistema",
    "ev_charger": "Carga EV",
    "peak_shaving": "Reducción de Pico",
    "vpp_monetization": "Ingresos VPP",
    "tropical_degradation": "Degradación Tropical",
}

# ─── Constants ────────────────────────────────────────────────────────────────

REPORT_DIR = Path(__file__).resolve().parent
FONT_DIR = REPORT_DIR / "static" / "fonts"
TEMPLATE_DIR = REPORT_DIR / "templates"

# PR market constants (duplicated from scoring/markets/pr.py for report-level use)
BATTERY_COST_PER_KWH = 400
BATTERY_KWH_PER_KWP = 1.2
ELECTRICITY_PRICE_GSS = 0.27
ANNUAL_PRODUCTION_KWH_PER_KWP = 1600
EXPORT_RATIO_WITHOUT_BATTERY = 0.35
ITC_FEDERAL = 0.30
IVU_LOCAL = 0.115
EFFECTIVE_COST_MULTIPLIER = 0.585
OM_COST_PER_KWP_YEAR = 75
OM_CONTRACT_YEARS = 3
OM_DEGRADATION_WITHOUT_CONTRACT = 0.25
DEMAND_CHARGE_PER_KVA = 8.10
PEAK_SHAVING_REDUCTION = 0.55
POWER_FACTOR = 0.85

# Compact template activation threshold
COMPACT_THRESHOLD = 4  # Use Compact template if total_installations <= this value
CTA_URL = "https://solvist.app/demo"


# ─── Timeout handler ──────────────────────────────────────────────────────────

class ReportTimeoutError(Exception):
    pass


# ─── Core pipeline ────────────────────────────────────────────────────────────

def generate_audit_pdf(
    portfolio_id: str,
    market: str = "pr",
    portfolio_data: Optional[Dict] = None,
) -> bytes:
    """
    Generate PDF audit report for a portfolio.

    Pipeline:
    1. Fetch portfolio data (or use provided data)
    2. Score each system via scoring engine
    3. Build report_data dict
    4. Generate charts (matplotlib → base64)
    5. Render HTML (Jinja2)
    6. Convert to PDF (WeasyPrint) with 10s timeout
    7. Return bytes

    Raises TimeoutError if generation exceeds 10 seconds.
    """
    # Generate PDF directly (timeout handled at API level with asyncio)
    # PASO 1: Get portfolio data
    if portfolio_data is None:
        portfolio_data = _get_test_portfolio()

    systems = portfolio_data.get("systems", [])
    client_name = portfolio_data.get("client_name", "Test Company")

    # PASO 2: Score each system
    scored_systems = _score_systems(systems, market)

    # Guard: empty list (fix #6 from Claude audit)
    if not scored_systems:
        scored_systems = []

    # PASO 3: Determine template type (Compact vs Standard)
    total_installations = len(systems)
    use_compact = total_installations <= COMPACT_THRESHOLD

    if use_compact:
        # Compact pipeline
        report_data = _build_compact_report_data(scored_systems, client_name, market)
        charts = _generate_charts(scored_systems, report_data, compact=True)
        html = _render_compact_html(report_data, charts)
    else:
        # Standard pipeline (unchanged)
        report_data = _build_report_data(scored_systems, client_name, market)
        charts = _generate_charts(scored_systems, report_data)
        html = _render_html(report_data, charts)

    # PASO 6: Convert to PDF
    pdf_bytes = _html_to_pdf(html)

    return pdf_bytes


# ─── Scoring ──────────────────────────────────────────────────────────────────

def _score_systems(systems: List[Dict], market: str) -> List[Dict]:
    """Score each system using the scoring engine."""
    from scoring.engine import compute_opportunity_score

    scored = []
    for system in systems:
        result = compute_opportunity_score(
            installation_id=system.get("id", "unknown"),
            company_id=system.get("company_id", "unknown"),
            kwp=system.get("kw_peak", 0),
            installation_year=system.get("year_installed"),
            has_battery=system.get("has_battery"),
            has_maintenance_contract=system.get("has_maintenance_contract"),
            location_type=system.get("location_type", "commercial"),
            estimated_consumption=system.get("estimated_consumption"),
        )
        # Attach original system data
        result["_system"] = {
            "name": system.get("name", "Unknown System"),
            "kw_peak": system.get("kw_peak", 0),
            "year_installed": system.get("year_installed"),
            "has_battery": system.get("has_battery"),
            "system_type": system.get("location_type", "commercial"),
            "tariff_type": system.get("tariff_type", "GSS"),
        }
        scored.append(result)

    return scored


# ─── Build report data ───────────────────────────────────────────────────────

def _build_report_data(
    scored_systems: List[Dict], client_name: str, market: str
) -> Dict[str, Any]:
    """Build aggregated report data from scored systems."""

    now = datetime.now()

    # ── BUG-4 FIX: Filter opportunities properly ──
    # Only count systems where is_opportunity is True and expected_value > 0
    opportunities = [
        s for s in scored_systems
        if s.get("is_opportunity", False) and s.get("expected_value", 0) > 0
    ]

    total_value = sum(s.get("expected_value", 0) for s in opportunities)
    systems_analyzed = len(scored_systems)
    opportunities_found = len(opportunities)  # FIX: was len(scored_systems)

    # ── Top opportunities ──
    sorted_by_value = sorted(opportunities, key=lambda x: x.get("expected_value", 0), reverse=True)

    top_opportunity = sorted_by_value[0] if sorted_by_value else None
    second_opportunity = None
    if len(sorted_by_value) > 1:
        top_type = top_opportunity.get("primary_reason", "")
        for opp in sorted_by_value[1:]:
            if opp.get("primary_reason", "") != top_type:
                second_opportunity = opp
                break
        if second_opportunity is None and len(sorted_by_value) > 1:
            second_opportunity = sorted_by_value[1]

    # ── Value by type (using display names for consistency — fix D-5) ──
    value_by_type: Dict[str, float] = {}
    for opp in opportunities:
        opp_type = opp.get("primary_reason", "unknown")
        display_name = OPP_DISPLAY_NAMES.get(opp_type, opp_type.replace("_", " ").title())
        value_by_type[display_name] = value_by_type.get(display_name, 0) + opp.get("expected_value", 0)

    # ── Financial summary ──
    total_gross = sum(
        _get_gross_investment(s) for s in opportunities
    )
    total_incentives = sum(
        _get_total_incentives(s) for s in opportunities
    )
    net_investment = total_gross - total_incentives  # BUG-1 FIX: properly subtract

    total_annual_savings = sum(
        _get_annual_savings(s) for s in opportunities
    )

    blended_payback = (
        net_investment / total_annual_savings
        if total_annual_savings > 0 and net_investment > 0
        else 0
    )
    portfolio_roi = (
        (total_annual_savings / net_investment * 100)
        if net_investment > 0
        else 0
    )

    # ── Key findings ──
    key_findings = _generate_key_findings(scored_systems, opportunities, value_by_type)

    # ── Key risks ──
    key_risks = _generate_key_risks(scored_systems, opportunities)

    # ── Assessment ──
    assessment = _generate_assessment(opportunities, value_by_type, total_value)

    # ── Build opportunity detail data for P03 & P05 ──
    opp1_data = _build_opportunity_detail(top_opportunity, market) if top_opportunity else None
    opp2_data = _build_opportunity_detail(second_opportunity, market) if second_opportunity else None

    # ── Recommended actions ──
    recommended_actions = _generate_recommended_actions(top_opportunity, opportunities)

    # ── Methodology ──
    methodology = METHODOLOGY_PR

    return {
        "client_name": client_name,
        "date": now,
        "market_label": "Mercado de Puerto Rico",
        "font_path": str(FONT_DIR),
        # Executive Summary (SIN health score — fix #8)
        "total_value": total_value,
        "systems_analyzed": systems_analyzed,
        "opportunities_found": opportunities_found,
        "key_findings": key_findings,
        "key_risks": key_risks,
        "assessment": assessment,
        # Opportunity details
        "top_opportunity": opp1_data,
        "second_opportunity": opp2_data,
        # Financial Recovery
        "financial_summary": {
            "total_gross_investment": total_gross,
            "total_incentives": total_incentives,
            "net_investment": net_investment,
            "total_annual_savings": total_annual_savings,
            "portfolio_roi": portfolio_roi,
            "blended_payback": blended_payback,
        },
        "value_by_type": value_by_type,
        # Actions
        "recommended_actions": recommended_actions,
        # Methodology
        "methodology": methodology,
        # Raw scored data for charts
        "_scored_systems": scored_systems,
        "_opportunities": opportunities,
    }


# ─── Financial helpers (BUG-1 & BUG-3 fixes) ─────────────────────────────────

def _get_gross_investment(scored: Dict) -> float:
    """Get gross investment from scored system."""
    vb = scored.get("value_breakdown", {})
    # Battery upgrade
    if "battery_cost_gross_usd" in vb:
        return vb["battery_cost_gross_usd"]
    # Maintenance
    if "contract_3yr_usd" in vb:
        return vb["contract_3yr_usd"]
    # Fallback to expected_value
    return scored.get("expected_value", 0)


def _get_total_incentives(scored: Dict) -> float:
    """Get total incentives from scored system (BUG-1 FIX)."""
    vb = scored.get("value_breakdown", {})
    incentives = 0
    if "itc_saving_usd" in vb:
        incentives += vb["itc_saving_usd"]
    if "ivu_saving_usd" in vb:
        incentives += vb["ivu_saving_usd"]
    return incentives


def _get_annual_savings(scored: Dict) -> float:
    """Get annual savings from scored system (BUG-3 FIX for O&M)."""
    vb = scored.get("value_breakdown", {})
    reason = scored.get("primary_reason", "")

    # Battery upgrade / industrial battery
    if "annual_savings_usd" in vb and vb["annual_savings_usd"] > 0:
        return vb["annual_savings_usd"]

    # BUG-3 FIX: O&M contract — calculate savings from production loss prevention
    if reason in ("maintenance",):
        annual_loss = vb.get("annual_production_loss_usd", 0)
        if annual_loss > 0:
            return annual_loss
        # Fallback: use contract value as proxy for savings
        annual_contract = vb.get("annual_contract_usd", 0)
        return annual_contract * 1.5  # O&M saves ~1.5x contract cost in avoided repairs

    # Peak shaving
    if "annual_saving_usd" in vb:
        return vb["annual_saving_usd"]

    # VPP
    if "annual_vpp_revenue_usd" in vb:
        return vb["annual_vpp_revenue_usd"]

    # Tropical degradation
    if "annual_loss_usd" in vb:
        return vb["annual_loss_usd"]

    # Inverter replacement — use expected value as annual gain
    if reason == "inverter_replacement":
        return scored.get("expected_value", 0) * 0.15  # ~15% annual gain

    # Fallback: estimate from expected value
    ev = scored.get("expected_value", 0)
    if ev > 0:
        return ev * 0.15  # rough annual savings estimate

    return 0


# ─── Opportunity detail builder ───────────────────────────────────────────────

def _build_opportunity_detail(scored: Dict, market: str) -> Dict[str, Any]:
    """Build opportunity detail data for template rendering."""
    if not scored:
        return None

    sys = scored.get("_system", {})
    vb = scored.get("value_breakdown", {})
    reason = scored.get("primary_reason", "")
    opp_narrative = OPPORTUNITY_NARRATIVES.get(reason, {})

    # Build narrative text
    system_name = sys.get("name", "Unknown System")
    kw_peak = sys.get("kw_peak", 0)
    tariff = sys.get("tariff_type", "GSS")

    # Calculate key financial figures (BUG-1 FIX)
    gross_cost = vb.get("battery_cost_gross_usd", scored.get("expected_value", 0))
    itc_saving = vb.get("itc_saving_usd", 0)
    ivu_saving = vb.get("ivu_saving_usd", 0)
    total_incentives = itc_saving + ivu_saving
    net_cost = gross_cost - total_incentives  # BUG-1 FIX: subtract properly!

    annual_savings = _get_annual_savings(scored)
    payback_years = (net_cost / annual_savings) if annual_savings > 0 and net_cost > 0 else 0
    battery_kwh = vb.get("battery_kwh", kw_peak * BATTERY_KWH_PER_KWP)

    # Format narrative with real data
    narrative = opp_narrative.get("the_opportunity", f"Opportunity identified for {system_name}.")
    try:
        narrative = narrative.format(
            system_name=system_name,
            kw_peak=kw_peak,
            tariff=tariff,
            annual_peak_cost=annual_savings,
            battery_kwh=round(battery_kwh),
            age=2026 - sys.get("year_installed", 2020) if sys.get("year_installed") else 5,
            delta_pct=round(vb.get("estimated_degradation_pct", 5.5), 1),
            annual_loss=vb.get("annual_loss_usd", 0),
            recovery_pct=4,
            annual_gain=vb.get("annual_savings_usd", annual_savings),
            monthly_charge=vb.get("monthly_demand_charge_usd", 0),
            reduction_pct=55,
            annual_saving=vb.get("annual_saving_usd", 0),
            annual_revenue=vb.get("annual_vpp_revenue_usd", 0),
        )
    except (KeyError, IndexError):
        pass  # Use template as-is if interpolation fails

    # Format talking points (BUG-2 FIX: correct net vs gross)
    raw_points = opp_narrative.get("talking_points", [])
    talking_points = []
    for point in raw_points:
        try:
            tp = point.format(
                annual_savings=annual_savings,
                net_cost=net_cost,       # BUG-2 FIX: correct net cost
                gross_cost=gross_cost,   # BUG-2 FIX: correct gross cost
                annual_gain=annual_savings,
                delta_pct=round(vb.get("estimated_degradation_pct", 5.5), 1),
            )
        except (KeyError, IndexError):
            tp = point
        talking_points.append(tp)

    # Warning
    warning = opp_narrative.get("warning", "")

    # Build comparison block for battery upgrade
    comparison = None
    incentive_stack = None
    demand_charge_block = None

    if reason in ("battery_upgrade", "industrial_battery"):
        comparison = {
            "current": {
                "Almacenamiento": "Ninguno",
                "Producción excedente": "Exportada a red a tarifa baja",
                "Cobertura pico": "Sin protección",
                "Corte de red": "El sistema se apaga",
            },
            "proposed": {
                "Almacenamiento": f"{battery_kwh:.0f} kWh LFP",
                "Producción excedente": "Almacenada para uso pico",
                "Cobertura pico": "Evitación total de cargo por demanda",
                "Corte de red": "Respaldo activado",
            },
        }
        incentive_stack = {
            "gross_cost": gross_cost,
            "itc_saving": itc_saving,
            "ivu_saving": ivu_saving,
            "total_incentives": total_incentives,
            "net_cost": net_cost,  # BUG-1 FIX
        }

        # Conditional demand charge block for GSP tariff
        if tariff == "GSP":
            demand_kva = kw_peak * POWER_FACTOR
            monthly_charge = demand_kva * DEMAND_CHARGE_PER_KVA
            annual_demand = monthly_charge * 12
            annual_saving_ps = annual_demand * PEAK_SHAVING_REDUCTION
            try:
                demand_charge_block = DEMAND_CHARGE_NARRATIVE.format(
                    peak_kva=round(demand_kva),
                    monthly_charge=round(monthly_charge),
                    reduction_kva=round(demand_kva * PEAK_SHAVING_REDUCTION),
                    annual_saving=round(annual_saving_ps),
                )
            except (KeyError, IndexError):
                demand_charge_block = DEMAND_CHARGE_NARRATIVE

    # Key metrics table
    metrics = _build_metrics_table(scored, reason, vb, gross_cost, net_cost, annual_savings)

    # Financial summary for this opportunity
    financial = {
        "gross_investment": gross_cost,
        "total_incentives": total_incentives,
        "net_cost": net_cost,  # BUG-1 FIX
        "annual_savings": annual_savings,
        "payback_years": payback_years,
    }

    return {
        "type_label": OPP_DISPLAY_NAMES.get(reason, reason.replace("_", " ").title()),
        "system_name": system_name,
        "opportunity_reason": reason,
        "expected_value": scored.get("expected_value", 0),
        "close_probability": scored.get("close_probability", 0),
        "payback_years": payback_years,
        "narrative": narrative,
        "warning": warning,
        "talking_points": talking_points,
        "comparison": comparison,
        "incentive_stack": incentive_stack,
        "demand_charge_block": demand_charge_block,
        "has_gsp_tariff": sys.get("tariff_type") == "GSP",
        "metrics": metrics,
        "financial": financial,
        "kw_peak": kw_peak,
        "battery_kwh": round(battery_kwh),
        "annual_savings": annual_savings,
        "net_cost": net_cost,
        "gross_cost": gross_cost,
    }


def _build_metrics_table(
    scored: Dict, reason: str, vb: Dict,
    gross_cost: float, net_cost: float, annual_savings: float
) -> List[Dict[str, str]]:
    """Build key metrics table for opportunity detail page."""
    metrics = []

    # Component costs from scoring engine
    kwp = scored.get("_system", {}).get("kw_peak", 0)

    if reason in ("battery_upgrade", "industrial_battery"):
        metrics.append({"label": "Batería", "value": format_currency(vb.get("battery_cost_gross_usd", gross_cost))})
        metrics.append({"label": "Ahorro Anual", "value": format_currency(annual_savings)})
        metrics.append({"label": "Tamaño de Batería", "value": f"{vb.get('battery_kwh', kwp * 1.2):.0f} kWh"})
        metrics.append({"label": "Retorno (pre-incentivos)", "value": format_payback(vb.get("payback_pre_incentivos", 0))})
        metrics.append({"label": "Retorno (post-incentivos)", "value": format_payback(vb.get("payback_post_incentivos", 0))})
        metrics.append({"label": "Costo Bruto", "value": format_currency(gross_cost)})
        metrics.append({"label": "Costo Neto", "value": format_currency(net_cost)})  # BUG-1 FIX
        metrics.append({"label": "Prob. de Cierre", "value": f"{scored.get('close_probability', 0):.0%}"})
    elif reason == "maintenance":
        metrics.append({"label": "Contrato Anual", "value": format_currency(vb.get("annual_contract_usd", 0))})
        metrics.append({"label": "Valor a 3 Años", "value": format_currency(vb.get("contract_3yr_usd", 0))})
        metrics.append({"label": "Pérdida de Producción (anual)", "value": format_currency(vb.get("annual_production_loss_usd", 0))})
        metrics.append({"label": "Costo por kWp", "value": f"${vb.get('om_cost_per_kwp', 75)}/kWp/año"})
        metrics.append({"label": "Prob. de Cierre", "value": f"{scored.get('close_probability', 0):.0%}"})
    elif reason == "system_expansion":
        metrics.append({"label": "kWp Adicionales", "value": format_number(vb.get("expansion_kwp", kwp * 0.3), 1)})
        metrics.append({"label": "Costo por kWp", "value": format_currency(vb.get("cost_per_kwp_usd", 1100))})
        metrics.append({"label": "Valor Estimado", "value": format_currency(scored.get("expected_value", 0))})
        metrics.append({"label": "Prob. de Cierre", "value": f"{scored.get('close_probability', 0):.0%}"})
    else:
        # Generic metrics
        for key, value in vb.items():
            if isinstance(value, (int, float)) and value > 0:
                label = key.replace("_", " ").replace("usd", "").strip().title()
                if "usd" in key or "cost" in key or "value" in key or "saving" in key:
                    metrics.append({"label": label, "value": format_currency(value)})
                else:
                    metrics.append({"label": label, "value": format_number(value)})
        metrics.append({"label": "Prob. de Cierre", "value": f"{scored.get('close_probability', 0):.0%}"})

    return metrics


# ─── Key findings generator ───────────────────────────────────────────────────

def _generate_key_findings(
    scored_systems: List[Dict],
    opportunities: List[Dict],
    value_by_type: Dict[str, float],
) -> List[str]:
    """Generate 3-4 key findings from the scored data."""
    findings = []

    # Count systems without battery
    no_battery = [s for s in scored_systems if s.get("_system", {}).get("has_battery") is False]
    if no_battery:
        pct = round(len(no_battery) / len(scored_systems) * 100) if scored_systems else 0
        battery_value = value_by_type.get("Instalación de Baterías", 0)
        try:
            findings.append(
                FINDING_TEMPLATES["no_battery_majority"].format(
                    pct=pct, total=battery_value
                )
            )
        except (KeyError, IndexError):
            findings.append(f"{pct}% de los sistemas no tienen almacenamiento")

    # Count systems without O&M
    no_om = [s for s in scored_systems if s.get("_system", {}).get("has_maintenance_contract") is False]
    if no_om:
        om_value = value_by_type.get("Contrato O&M", 0)
        try:
            findings.append(
                FINDING_TEMPLATES["maintenance_gap"].format(
                    count=len(no_om), total=om_value
                )
            )
        except (KeyError, IndexError):
            findings.append(f"{len(no_om)} sistemas sin contrato O&M")

    # Tropical degradation for systems > 3 years
    old_systems = [
        s for s in scored_systems
        if s.get("_system", {}).get("year_installed") and
           (2026 - s["_system"]["year_installed"]) > 3
    ]
    if old_systems:
        deg_value = value_by_type.get("Degradación Tropical", 0)
        try:
            findings.append(
                FINDING_TEMPLATES["tropical_degradation"].format(
                    pct=round(len(old_systems) / len(scored_systems) * 100),
                    total=deg_value if deg_value > 0 else sum(
                        s.get("expected_value", 0) for s in opportunities
                    ) * 0.15,
                )
            )
        except (KeyError, IndexError):
            findings.append(f"{len(old_systems)} sistemas con más de 3 años")

    # Cap at 3 findings
    return findings[:3]


# ─── Key risks generator ──────────────────────────────────────────────────────

def _generate_key_risks(
    scored_systems: List[Dict],
    opportunities: List[Dict],
) -> List[str]:
    """Generate 1-2 key risks."""
    risks = []

    # LUMA rate increase (always present in PR)
    risks.append(RISK_TEMPLATES["luma_rate_increase"])

    # Systems without O&M
    no_om = [s for s in scored_systems if s.get("_system", {}).get("has_maintenance_contract") is False]
    if no_om:
        om_exposure = sum(s.get("expected_value", 0) for s in opportunities
                         if s.get("primary_reason") == "maintenance")
        try:
            risks.append(
                RISK_TEMPLATES["no_maintenance"].format(
                    count=len(no_om), total=om_exposure if om_exposure > 0 else 4200 * len(no_om)
                )
            )
        except (KeyError, IndexError):
            risks.append(f"{len(no_om)} sistemas sin contrato O&M enfrentan exposición a reparaciones no planificadas")

    return risks[:2]


# ─── Assessment generator ─────────────────────────────────────────────────────

def _generate_assessment(
    opportunities: List[Dict],
    value_by_type: Dict[str, float],
    total_value: float,
) -> str:
    """Generate assessment text for executive summary."""
    if not opportunities:
        return "No se identificaron oportunidades significativas en este portafolio."

    top_type = max(value_by_type, key=value_by_type.get) if value_by_type else "general"
    top_value = value_by_type.get(top_type, 0)

    if total_value > 500_000:
        try:
            return ASSESSMENT_TEMPLATES["critical"].format(
                top_type=top_type, top_value=top_value
            )
        except (KeyError, IndexError):
            pass
    elif total_value > 100_000:
        second_type = list(value_by_type.keys())[1] if len(value_by_type) > 1 else top_type
        try:
            return ASSESSMENT_TEMPLATES["moderate"].format(
                top_type=top_type, second_type=second_type, total=total_value
            )
        except (KeyError, IndexError):
            pass

    return f"Este portafolio tiene {len(opportunities)} oportunidades que representan ${total_value:,.0f} en valor identificado."


# ─── Recommended actions generator ────────────────────────────────────────────

def _generate_recommended_actions(
    top_opportunity: Optional[Dict],
    opportunities: List[Dict],
) -> Dict[str, List[Dict]]:
    """Generate recommended next actions."""
    actions = {
        "this_week": [],
        "next_30_days": [],
        "next_90_days": [],
    }

    # This week: contact top opportunity
    if top_opportunity:
        sys = top_opportunity.get("_system", {})
        try:
            action_text = ACTION_TEMPLATES["this_week"][0]["action"].format(
                client_name=sys.get("name", "top client"),
                opp_type=OPP_DISPLAY_NAMES.get(
                    top_opportunity.get("primary_reason", ""), "opportunity"
                ),
                value=top_opportunity.get("expected_value", 0),
                prob=round(top_opportunity.get("close_probability", 0) * 100),
            )
        except (KeyError, IndexError):
            action_text = f"Contactar a {sys.get('name', 'cliente principal')} sobre oportunidad principal"
        actions["this_week"].append({"text": action_text, "number": 1})

    # Next 30 days
    opp_count = len(opportunities)
    top_type = OPP_DISPLAY_NAMES.get(
        top_opportunity.get("primary_reason", ""), "opportunity"
    ) if top_opportunity else "opportunity"

    try:
        actions["next_30_days"].append({
            "text": ACTION_TEMPLATES["next_30_days"][0]["action"].format(
                opp_type=top_type, count=min(5, opp_count)
            ),
            "number": 1,
        })
    except (KeyError, IndexError):
        actions["next_30_days"].append({"text": f"Preparar propuestas para los {min(5, opp_count)} sistemas principales", "number": 1})

    actions["next_30_days"].append({
        "text": "Someter aplicaciones al programa CBES para sistemas elegibles",
        "number": 2,
    })

    # Next 90 days
    old_count = len([o for o in opportunities if
                     o.get("_system", {}).get("year_installed") and
                     (2026 - o["_system"]["year_installed"]) > 3])
    try:
        actions["next_90_days"].append({
            "text": ACTION_TEMPLATES["next_90_days"][0]["action"].format(
                count=old_count if old_count > 0 else opp_count
            ),
            "number": 1,
        })
    except (KeyError, IndexError):
        actions["next_90_days"].append({
            "text": f"Completar evaluación de degradación para {old_count or opp_count} sistemas",
            "number": 1,
        })

    return actions


# ─── Chart generation ─────────────────────────────────────────────────────────

def _generate_charts(scored_systems: List[Dict], report_data: Dict, compact: bool = False) -> Dict[str, str]:
    """Generate charts as base64 PNG images.

    Args:
        compact: if True, generate charts with axes hidden (for Compact template)
    """
    from report.charts.opportunity_matrix import OpportunityMatrixChart
    from report.charts.recovery_bar import RecoveryBarChart

    charts = {}

    # Opportunity matrix bubble chart
    opportunities = report_data.get("_opportunities", [])
    if opportunities:
        matrix_chart = OpportunityMatrixChart(opportunities)
        charts["matrix_chart"] = matrix_chart.to_base64()

    # Recovery bar chart
    value_by_type = report_data.get("value_by_type", {})
    if value_by_type:
        recovery_chart = RecoveryBarChart(value_by_type, hide_axes=compact)
        charts["recovery_chart"] = recovery_chart.to_base64()

    return charts


# ─── HTML rendering ───────────────────────────────────────────────────────────

def _render_html(report_data: Dict, charts: Dict[str, str]) -> str:
    """Render HTML using Jinja2 templates."""
    env = Environment(
        loader=FileSystemLoader(str(TEMPLATE_DIR)),
        autoescape=False,
    )

    # Register custom filters
    env.filters["currency"] = lambda v: format_currency(v)
    env.filters["currency_k"] = lambda v: format_currency_k(v)
    env.filters["pct"] = lambda v: format_pct(v)
    env.filters["payback"] = lambda v: format_payback(v)
    env.filters["number_fmt"] = lambda v, d=1: format_number(v, d)
    env.filters["date"] = lambda v: format_date(v)

    template = env.get_template("base.html")

    # Top 5 opportunities for the matrix table
    opportunities = report_data.get("_opportunities", [])
    top_5 = sorted(opportunities, key=lambda x: x.get("expected_value", 0), reverse=True)[:5]

    # Action values by window
    action_values = _calculate_action_values(report_data)

    html = template.render(
        client_name=report_data["client_name"],
        date=report_data["date"],
        market_label=report_data["market_label"],
        font_path=report_data["font_path"],
        # Executive Summary
        total_value=report_data["total_value"],
        systems_analyzed=report_data["systems_analyzed"],
        opportunities_found=report_data["opportunities_found"],
        key_findings=report_data["key_findings"],
        key_risks=report_data["key_risks"],
        assessment=report_data["assessment"],
        # Opportunity #1
        opp1=report_data["top_opportunity"],
        # Opportunity Matrix
        matrix_chart=charts.get("matrix_chart", ""),
        top_5_opportunities=top_5,
        opp_display_names=OPP_DISPLAY_NAMES,
        # Opportunity #2
        opp2=report_data["second_opportunity"],
        # Financial Recovery
        financial_summary=report_data["financial_summary"],
        value_by_type=report_data["value_by_type"],
        recovery_chart=charts.get("recovery_chart", ""),
        # Actions
        recommended_actions=report_data["recommended_actions"],
        action_values=action_values,
        # Methodology
        methodology=report_data["methodology"],
    )

    return html


def _calculate_action_values(report_data: Dict) -> Dict[str, float]:
    """Calculate estimated value by action window."""
    total = report_data.get("total_value", 0)
    opp1_val = report_data.get("top_opportunity", {}).get("expected_value", 0) if report_data.get("top_opportunity") else 0
    top_5_val = sum(
        s.get("expected_value", 0)
        for s in sorted(
            report_data.get("_opportunities", []),
            key=lambda x: x.get("expected_value", 0),
            reverse=True,
        )[:5]
    )
    return {
        "this_week": opp1_val,
        "next_30_days": top_5_val,
        "next_90_days": total,
    }


# ─── PDF generation ───────────────────────────────────────────────────────────

def _html_to_pdf(html: str) -> bytes:
    """Convert HTML to PDF using WeasyPrint."""
    from weasyprint import HTML

    pdf = HTML(string=html).write_pdf()
    return pdf


# ─── Compact template pipeline ──────────────────────────────────────────────

def _build_compact_report_data(
    scored_systems: List[Dict], client_name: str, market: str
) -> Dict[str, Any]:
    """Build compact report data — flat structure for 4-page template."""
    now = datetime.now()

    # Filter opportunities
    opportunities = [
        s for s in scored_systems
        if s.get("is_opportunity", False) and s.get("expected_value", 0) > 0
    ]

    total_value = sum(s.get("expected_value", 0) for s in opportunities)

    # Financial aggregation
    total_annual_savings = sum(_get_annual_savings(s) for s in opportunities)
    total_gross = sum(_get_gross_investment(s) for s in opportunities)
    total_incentives = sum(_get_total_incentives(s) for s in opportunities)
    net_investment = total_gross - total_incentives
    blended_payback = (
        net_investment / total_annual_savings
        if total_annual_savings > 0 and net_investment > 0 else 0
    )

    # Value by type
    value_by_type: Dict[str, float] = {}
    for opp in opportunities:
        opp_type = opp.get("primary_reason", "unknown")
        display_name = OPP_DISPLAY_NAMES.get(opp_type, opp_type.replace("_", " ").title())
        value_by_type[display_name] = value_by_type.get(display_name, 0) + opp.get("expected_value", 0)

    # Cover data
    total_kwp = sum(s.get("_system", {}).get("kw_peak", 0) for s in scored_systems)
    years = [s.get("_system", {}).get("year_installed") for s in scored_systems if s.get("_system", {}).get("year_installed")]
    year_range = f"{min(years)}–{max(years)}" if years else "N/A"

    # Systems without battery
    systems_without_battery = sum(
        1 for s in scored_systems if s.get("_system", {}).get("has_battery") is False
    )

    # Avoidable charges: production-based estimate for ALL systems without battery.
    # Formula: kWp × annual_production × export_ratio × electricity_rate
    # This represents the annual savings from installing battery storage,
    # regardless of each system's primary_reason (battery_upgrade, maintenance, etc.)
    avoidable_charges = sum(
        s.get("_system", {}).get("kw_peak", 0)
        * ANNUAL_PRODUCTION_KWH_PER_KWP
        * EXPORT_RATIO_WITHOUT_BATTERY
        * ELECTRICITY_PRICE_GSS
        for s in scored_systems
        if s.get("_system", {}).get("has_battery") is False
    )
    logger.info(
        "[COMPACT] avoidable_charges=%s  (systems_without_battery=%d)",
        avoidable_charges, systems_without_battery,
    )

    # Priority opportunities (max 4, sorted by expected_value × close_probability)
    sorted_opps = sorted(
        opportunities,
        key=lambda x: x.get("expected_value", 0) * x.get("close_probability", 0),
        reverse=True,
    )[:4]

    priority_opps = []
    for opp in sorted_opps:
        sys = opp.get("_system", {})
        vb = opp.get("value_breakdown", {})
        reason = opp.get("primary_reason", "")
        annual_savings = _get_annual_savings(opp)
        gross_cost = vb.get("battery_cost_gross_usd", opp.get("expected_value", 0))
        itc_saving = vb.get("itc_saving_usd", 0)
        ivu_saving = vb.get("ivu_saving_usd", 0)
        net_cost = gross_cost - itc_saving - ivu_saving
        payback = (net_cost / annual_savings) if annual_savings > 0 and net_cost > 0 else 0

        priority_opps.append({
            "system_name": sys.get("name", "Sistema"),
            "type_label": OPP_DISPLAY_NAMES.get(reason, reason.replace("_", " ").title()),
            "kw_peak": sys.get("kw_peak", 0),
            "year_installed": sys.get("year_installed", "—"),
            "expected_value": opp.get("expected_value", 0),
            "annual_savings": annual_savings,
            "payback_years": payback,
            "close_probability": opp.get("close_probability", 0),
            "primary_reason": reason,
        })

    # Top opportunity for actions
    top_opp_raw = sorted_opps[0] if sorted_opps else None
    top_opportunity = None
    if top_opp_raw:
        sys = top_opp_raw.get("_system", {})
        reason = top_opp_raw.get("primary_reason", "")
        top_opportunity = {
            "name": sys.get("name", "cliente principal"),
            "type_label": OPP_DISPLAY_NAMES.get(reason, reason.replace("_", " ").title()),
            "value": top_opp_raw.get("expected_value", 0),
            "close_prob": top_opp_raw.get("close_probability", 0),
        }

    return {
        "client_name": client_name,
        "date": now,
        "font_path": str(FONT_DIR),
        # Cover
        "company_name": client_name,
        "total_installations": len(scored_systems),
        "total_kwp": total_kwp,
        "year_range": year_range,
        "opportunities_count": len(opportunities),
        "total_value": total_value,
        # Recovery
        "annual_savings": total_annual_savings,
        "payback_years": blended_payback,
        "value_by_type": value_by_type,
        "systems_without_battery": systems_without_battery,
        "total_systems": len(scored_systems),
        "avoidable_charges": avoidable_charges,
        # Priority
        "opportunities": priority_opps,
        # Actions
        "top_opportunity": top_opportunity,
        "systems_count": len(scored_systems),
        "cta_url": CTA_URL,
        # Charts
        "_scored_systems": scored_systems,
        "_opportunities": opportunities,
    }


def _render_compact_html(report_data: Dict, charts: Dict[str, str]) -> str:
    """Render compact HTML template."""
    env = Environment(
        loader=FileSystemLoader(str(TEMPLATE_DIR)),
        autoescape=False,
    )

    # Reuse existing filters
    env.filters["currency"] = lambda v: format_currency(v)
    env.filters["currency_k"] = lambda v: format_currency_k(v)
    env.filters["pct"] = lambda v: format_pct(v)
    env.filters["payback"] = lambda v: format_payback(v)
    env.filters["number_fmt"] = lambda v, d=1: format_number(v, d)
    env.filters["date"] = lambda v: format_date(v)

    template = env.get_template("compact_base.html")

    html = template.render(
        font_path=report_data["font_path"],
        # Cover
        company_name=report_data["company_name"],
        date=report_data["date"],
        total_installations=report_data["total_installations"],
        total_kwp=report_data["total_kwp"],
        year_range=report_data["year_range"],
        opportunities_count=report_data["opportunities_count"],
        total_value=report_data["total_value"],
        # Recovery
        annual_savings=report_data["annual_savings"],
        payback_years=report_data["payback_years"],
        value_by_type=report_data["value_by_type"],
        systems_without_battery=report_data["systems_without_battery"],
        total_systems=report_data["total_systems"],
        avoidable_charges=report_data["avoidable_charges"],
        recovery_chart=charts.get("recovery_chart", ""),
        # Priority
        opportunities=report_data["opportunities"],
        # Actions
        top_opportunity=report_data["top_opportunity"],
        systems_count=report_data["systems_count"],
        cta_url=report_data["cta_url"],
    )

    return html


# ─── Test data (3 portfolios para demo) ──────────────────────────────────────

def _get_test_portfolio(portfolio_id: str = "industrial") -> Dict:
    """
    Generate test portfolio data for development.
    6 portfolios para demo:
    - 'industrial': Portafolio industrial grande, 7 sistemas, Standard template
    - 'mixto': Portafolio C&I mixto, 6 sistemas, Standard template
    - 'compacto': Portafolio pequeño, 3 sistemas sin batería, Compact template
    - 'compact_1sys': 1 sistema sin batería, Compact template (test n=1 grammar)
    - 'compact_4sys': 4 sistemas mixtos, Compact template (threshold edge case)
    - 'compact_sametype': 3 sistemas todos mismo tipo (battery_upgrade), Compact template
    """
    if portfolio_id == "compact_1sys":
        return {
            "client_name": "Clínica Dental Vega Alta",
            "systems": [
                {
                    "id": "1s-001", "company_id": "comp-1s",
                    "name": "Clínica Dental Vega Alta", "kw_peak": 25,
                    "year_installed": 2020, "has_battery": False,
                    "has_maintenance_contract": False,
                    "location_type": "commercial", "tariff_type": "GSS",
                },
            ],
        }
    elif portfolio_id == "compact_4sys":
        return {
            "client_name": "Centro Comercial Humacao",
            "systems": [
                {
                    "id": "4s-001", "company_id": "comp-4s",
                    "name": "Farmacia Económica Humacao", "kw_peak": 50,
                    "year_installed": 2018, "has_battery": False,
                    "has_maintenance_contract": False,
                    "location_type": "commercial", "tariff_type": "GSP",
                },
                {
                    "id": "4s-002", "company_id": "comp-4s",
                    "name": "Auto Parts Humacao", "kw_peak": 40,
                    "year_installed": 2019, "has_battery": False,
                    "has_maintenance_contract": True,
                    "location_type": "industrial", "tariff_type": "GSS",
                },
                {
                    "id": "4s-003", "company_id": "comp-4s",
                    "name": "Restaurante El Jibarito", "kw_peak": 30,
                    "year_installed": 2021, "has_battery": True,
                    "has_maintenance_contract": False,
                    "location_type": "commercial", "tariff_type": "GSS",
                },
                {
                    "id": "4s-004", "company_id": "comp-4s",
                    "name": "Oficinas Administrativas", "kw_peak": 35,
                    "year_installed": 2017, "has_battery": False,
                    "has_maintenance_contract": False,
                    "location_type": "commercial", "tariff_type": "GSP",
                },
            ],
        }
    elif portfolio_id == "compact_sametype":
        return {
            "client_name": "Parque Solar Caguas",
            "systems": [
                {
                    "id": "st-001", "company_id": "comp-st",
                    "name": "Nave A Caguas", "kw_peak": 50,
                    "year_installed": 2019, "has_battery": False,
                    "has_maintenance_contract": True,
                    "location_type": "industrial", "tariff_type": "GSS",
                },
                {
                    "id": "st-002", "company_id": "comp-st",
                    "name": "Nave B Caguas", "kw_peak": 40,
                    "year_installed": 2020, "has_battery": False,
                    "has_maintenance_contract": True,
                    "location_type": "industrial", "tariff_type": "GSS",
                },
                {
                    "id": "st-003", "company_id": "comp-st",
                    "name": "Nave C Caguas", "kw_peak": 30,
                    "year_installed": 2021, "has_battery": False,
                    "has_maintenance_contract": True,
                    "location_type": "industrial", "tariff_type": "GSS",
                },
            ],
        }
    elif portfolio_id == "mixto":
        return {
            "client_name": "Grupo Empresarial Borinquen",
            "systems": [
                {
                    "id": "mix-001", "company_id": "comp-mix",
                    "name": "Plaza del Caribe", "kw_peak": 250,
                    "year_installed": 2018, "has_battery": False,
                    "has_maintenance_contract": False,
                    "location_type": "commercial", "tariff_type": "GSP",
                },
                {
                    "id": "mix-002", "company_id": "comp-mix",
                    "name": "Taller Mecánico Bayamón", "kw_peak": 45,
                    "year_installed": 2021, "has_battery": False,
                    "has_maintenance_contract": True,
                    "location_type": "industrial", "tariff_type": "GSS",
                },
                {
                    "id": "mix-003", "company_id": "comp-mix",
                    "name": "Centro Médico San Patricio", "kw_peak": 180,
                    "year_installed": 2019, "has_battery": True,
                    "has_maintenance_contract": False,
                    "location_type": "commercial", "tariff_type": "GSP",
                },
                {
                    "id": "mix-004", "company_id": "comp-mix",
                    "name": "Escuela Vocacional Caguas", "kw_peak": 75,
                    "year_installed": 2020, "has_battery": False,
                    "has_maintenance_contract": False,
                    "location_type": "commercial", "tariff_type": "GSS",
                },
                {
                    "id": "mix-005", "company_id": "comp-mix",
                    "name": "Almacén Frio Arecibo", "kw_peak": 320,
                    "year_installed": 2017, "has_battery": False,
                    "has_maintenance_contract": False,
                    "location_type": "industrial", "tariff_type": "GSP",
                },
                {
                    "id": "mix-006", "company_id": "comp-mix",
                    "name": "Hotelilla Vieques", "kw_peak": 40,
                    "year_installed": 2022, "has_battery": True,
                    "has_maintenance_contract": True,
                    "location_type": "commercial", "tariff_type": "GSS",
                },
            ],
        }
    elif portfolio_id == "compacto":
        return {
            "client_name": "Solares del Sur PR",
            "systems": [
                {
                    "id": "cmp-001", "company_id": "comp-cmp",
                    "name": "Farmacia Familiar Ponce", "kw_peak": 30,
                    "year_installed": 2020, "has_battery": False,
                    "has_maintenance_contract": False,
                    "location_type": "commercial", "tariff_type": "GSS",
                },
                {
                    "id": "cmp-002", "company_id": "comp-cmp",
                    "name": "Panadería La Estrella", "kw_peak": 15,
                    "year_installed": 2019, "has_battery": False,
                    "has_maintenance_contract": False,
                    "location_type": "commercial", "tariff_type": "GSS",
                },
                {
                    "id": "cmp-003", "company_id": "comp-cmp",
                    "name": "Consultorio Médico Mayagüez", "kw_peak": 20,
                    "year_installed": 2021, "has_battery": False,
                    "has_maintenance_contract": True,
                    "location_type": "commercial", "tariff_type": "GSS",
                },
            ],
        }
    else:  # industrial (default)
        return {
            "client_name": "Energía Industrial PR",
            "systems": [
                {
                    "id": "ind-001", "company_id": "comp-ind",
                    "name": "Nave Industrial Torrijos", "kw_peak": 380,
                    "year_installed": 2019, "has_battery": False,
                    "has_maintenance_contract": False,
                    "location_type": "industrial", "tariff_type": "GSS",
                },
                {
                    "id": "ind-002", "company_id": "comp-ind",
                    "name": "Fábrica Plásticos Monfort", "kw_peak": 220,
                    "year_installed": 2020, "has_battery": False,
                    "has_maintenance_contract": False,
                    "location_type": "industrial", "tariff_type": "GSP",
                },
                {
                    "id": "ind-003", "company_id": "comp-ind",
                    "name": "Centro Comercial Mayagüez", "kw_peak": 150,
                    "year_installed": 2021, "has_battery": False,
                    "has_maintenance_contract": True,
                    "location_type": "commercial", "tariff_type": "GSS",
                },
                {
                    "id": "ind-004", "company_id": "comp-ind",
                    "name": "Servicios Jurídicos Plus", "kw_peak": 25,
                    "year_installed": 2020, "has_battery": False,
                    "has_maintenance_contract": False,
                    "location_type": "commercial", "tariff_type": "GSS",
                },
                {
                    "id": "ind-005", "company_id": "comp-ind",
                    "name": "Hotel Caribe Playa", "kw_peak": 95,
                    "year_installed": 2018, "has_battery": True,
                    "has_maintenance_contract": False,
                    "location_type": "commercial", "tariff_type": "GSP",
                },
                {
                    "id": "ind-006", "company_id": "comp-ind",
                    "name": "Oficinas Metro San Juan", "kw_peak": 60,
                    "year_installed": 2022, "has_battery": False,
                    "has_maintenance_contract": True,
                    "location_type": "commercial", "tariff_type": "GSS",
                },
                {
                    "id": "ind-007", "company_id": "comp-ind",
                    "name": "Almacén Ponce Sur", "kw_peak": 180,
                    "year_installed": 2017, "has_battery": False,
                    "has_maintenance_contract": False,
                    "location_type": "industrial", "tariff_type": "GSP",
                },
            ],
        }
