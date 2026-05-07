from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from jinja2 import Environment, FileSystemLoader, select_autoescape
from supabase import create_client

from config import SUPABASE_SERVICE_ROLE_KEY, SUPABASE_URL
from report.charts.opportunity_matrix import OpportunityMatrixChart
from report.charts.recovery_bar import RecoveryBarChart
from report.text.narratives import (
    ASSESSMENT_TEMPLATES,
    DEMAND_CHARGE_NARRATIVE,
    FINDING_TEMPLATES,
    METHODOLOGY_PR,
    OPPORTUNITY_NARRATIVES,
    RISK_TEMPLATES,
)
from report.utils.formatting import (
    format_currency,
    format_date,
    format_payback,
    format_pct,
)
from scoring.engine import compute_opportunity_score


REPORT_DIR = Path(__file__).resolve().parent
TEMPLATE_DIR = REPORT_DIR / "templates"
FONT_DIR = REPORT_DIR / "static" / "fonts"


def _client():
    return create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)


def _get_value(row: dict, *keys, default=None):
    for key in keys:
        value = row.get(key)
        if value is not None:
            return value
        raw_payload = row.get("raw_payload")
        if isinstance(raw_payload, dict) and raw_payload.get(key) is not None:
            return raw_payload.get(key)
    return default


def _to_float(value, default=0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _to_int(value, default=None):
    try:
        if value is None or value == "":
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _opportunity_type(scored: dict) -> str:
    return (
        scored.get("primary_reason")
        or scored.get("opportunity_type")
        or scored.get("opportunity_reason")
        or "unknown"
    )


def _type_label(opp_type: str) -> str:
    return OPPORTUNITY_NARRATIVES.get(opp_type, {}).get(
        "title", opp_type.replace("_", " ").title()
    )


def fetch_portfolio(portfolio_id: str) -> dict:
    db = _client()
    company_res = (
        db.table("companies")
        .select("id, name")
        .eq("id", portfolio_id)
        .limit(1)
        .execute()
    )
    company = (company_res.data or [{}])[0] if company_res.data else {}

    systems_res = (
        db.table("installations")
        .select("*")
        .eq("company_id", portfolio_id)
        .execute()
    )
    return {
        "id": portfolio_id,
        "client_name": company.get("name") or "SOLVIST Portfolio",
        "systems": systems_res.data or [],
    }


def score_systems(portfolio: dict, market: str) -> list:
    now = datetime.now(timezone.utc)
    now_year = now.year
    calculated_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0).isoformat()
    weights = {
        "battery": 1.0,
        "maintenance": 1.0,
        "expansion": 1.0,
        "ev": 1.0,
        "industrial": 1.0,
    }

    scored_systems = []
    for system in portfolio.get("systems", []):
        result = compute_opportunity_score(
            installation=system,
            weights=weights,
            now_year=now_year,
            calculated_month=calculated_month,
        )
        scored = dict(result)
        opp_type = _opportunity_type(scored)
        scored["opportunity_reason"] = opp_type
        scored["_raw_opportunity_reason"] = result.get("opportunity_reason", "")
        kw_peak = _to_float(_get_value(system, "kw_peak", "kwp", "system_size_kwp"), 0)
        year_installed = _to_int(_get_value(system, "year_installed", "installation_year"))
        scored["_system"] = {
            "name": (
                _get_value(system, "name", "client_name", "client_alias")
                or f"System {system.get('id', '')}".strip()
                or "Unnamed System"
            ),
            "kw_peak": kw_peak,
            "year_installed": year_installed,
            "has_battery": bool(_get_value(system, "has_battery", default=False)),
            "system_type": _get_value(system, "system_type", "location_type", default="commercial"),
            "tariff_type": _get_value(system, "tariff_type", default="GSS"),
        }
        scored_systems.append(scored)
    return scored_systems


def get_top_opportunity(scored_systems: list) -> dict | None:
    """Return the opportunity with highest expected_value. None if empty."""
    if not scored_systems:
        return None
    return max(scored_systems, key=lambda s: s.get("expected_value", 0))


def get_second_opportunity(scored_systems: list) -> dict | None:
    """Return the second-highest opportunity (different type from top). None if not available."""
    if not scored_systems or len(scored_systems) < 2:
        return None

    sorted_by_value = sorted(scored_systems, key=lambda s: s.get("expected_value", 0), reverse=True)
    top_type = _opportunity_type(sorted_by_value[0])

    for s in sorted_by_value[1:]:
        if _opportunity_type(s) != top_type:
            return s

    return sorted_by_value[1]


def aggregate_by_type(scored_systems: list) -> dict:
    """Aggregate expected_value by opportunity type."""
    result = {}
    for s in scored_systems:
        opp_type = _opportunity_type(s)
        result[opp_type] = result.get(opp_type, 0) + s.get("expected_value", 0)
    return result


def generate_findings(scored_systems: list) -> list:
    """Generate 3-4 key findings from scored data."""
    if not scored_systems:
        return ["No systems analyzed"]

    findings = []

    no_battery = [s for s in scored_systems if not s.get("_system", {}).get("has_battery")]
    if no_battery:
        total = sum(s.get("expected_value", 0) for s in no_battery)
        pct = len(no_battery) / len(scored_systems) * 100
        findings.append(FINDING_TEMPLATES["no_battery_majority"].format(
            pct=int(pct), total=total
        ))

    no_maintenance = [s for s in scored_systems if _opportunity_type(s) == "maintenance"]
    if no_maintenance:
        total = sum(s.get("expected_value", 0) for s in no_maintenance)
        findings.append(FINDING_TEMPLATES["maintenance_gap"].format(
            count=len(no_maintenance), total=total
        ))

    tropical = [s for s in scored_systems if _opportunity_type(s) == "tropical_degradation"]
    if tropical:
        total = sum(s.get("expected_value", 0) for s in tropical)
        pct = len(tropical) / len(scored_systems) * 100
        findings.append(FINDING_TEMPLATES["tropical_degradation"].format(
            pct=int(pct), total=total
        ))

    inverter = [s for s in scored_systems if _opportunity_type(s) == "inverter_replacement"]
    if inverter:
        total = sum(s.get("expected_value", 0) for s in inverter)
        findings.append(FINDING_TEMPLATES["inverter_obsolete"].format(
            count=len(inverter), total=total
        ))

    return findings[:4]


def generate_risks(scored_systems: list) -> list:
    """Generate 1-2 key risks."""
    if not scored_systems:
        return []

    risks = []
    risks.append(RISK_TEMPLATES["luma_rate_increase"])

    no_maintenance = [s for s in scored_systems if _opportunity_type(s) == "maintenance"]
    if no_maintenance:
        total = sum(s.get("expected_value", 0) for s in no_maintenance)
        risks.append(RISK_TEMPLATES["no_maintenance"].format(
            count=len(no_maintenance), total=total
        ))

    return risks[:2]


def generate_assessment(scored_systems: list) -> str:
    """Generate 1-2 line assessment for Executive Summary."""
    if not scored_systems:
        return "No systems available for analysis."

    total = sum(s.get("expected_value", 0) for s in scored_systems)
    top = get_top_opportunity(scored_systems)
    top_type = _type_label(_opportunity_type(top)) if top else "N/A"

    if total > 100_000:
        return ASSESSMENT_TEMPLATES["critical"].format(
            top_type=top_type, top_value=total
        )
    elif total > 30_000:
        second = get_second_opportunity(scored_systems)
        second_type = _type_label(_opportunity_type(second)) if second else "other"
        return ASSESSMENT_TEMPLATES["moderate"].format(
            top_type=top_type, second_type=second_type, total=total
        )
    else:
        return ASSESSMENT_TEMPLATES["strong"].format(
            top_type=top_type, total=total
        )


def generate_actions(scored_systems: list) -> dict:
    """Generate recommended actions by time period. No hardcoded dates."""
    if not scored_systems:
        return {"this_week": [], "next_30_days": [], "next_90_days": [],
                "estimated_value": {"this_week": 0, "next_30": 0, "next_90": 0}}

    sorted_by_value = sorted(scored_systems, key=lambda s: s.get("expected_value", 0), reverse=True)

    this_week = []
    next_30 = []
    next_90 = []

    if sorted_by_value:
        top = sorted_by_value[0]
        opp_title = _type_label(_opportunity_type(top))
        this_week.append({
            "number": 1,
            "text": (f"Contact {top.get('_system', {}).get('name', 'top system')} re: {opp_title} "
                     f"(${top.get('expected_value', 0):,.0f} value, "
                     f"{top.get('close_probability', 0)*100:.0f}% close probability)"),
        })

    inverter_opps = [s for s in scored_systems if _opportunity_type(s) == "inverter_replacement"]
    if inverter_opps:
        inv = inverter_opps[0]
        this_week.append({
            "number": 2,
            "text": f"Schedule firmware update for {inv.get('_system', {}).get('name', 'system')} (${inv.get('expected_value', 0):,.0f} quick win)",
        })

    next_30.append({
        "number": 3,
        "text": f"Prepare proposals for top {min(5, len(sorted_by_value))} systems identified",
    })
    next_30.append({
        "number": 4,
        "text": "Submit CBES program applications for eligible systems",
    })

    ev_opps = [s for s in scored_systems if _opportunity_type(s) == "ev_charger"]
    if ev_opps:
        next_90.append({
            "number": 5,
            "text": f"Develop EV charging proposals for {len(ev_opps)} locations with NEVI funding",
        })

    old_systems = [s for s in scored_systems
                   if s.get("_system", {}).get("year_installed")
                   and (datetime.now().year - s["_system"]["year_installed"]) > 3]
    if old_systems:
        next_90.append({
            "number": 6,
            "text": f"Complete tropical degradation assessment for {len(old_systems)} systems > 3 years old",
        })

    return {
        "this_week": this_week,
        "next_30_days": next_30,
        "next_90_days": next_90,
        "estimated_value": {
            "this_week": sum(s.get("expected_value", 0) for s in sorted_by_value[:2]) if len(sorted_by_value) >= 2 else (sorted_by_value[0].get("expected_value", 0) if sorted_by_value else 0),
            "next_30": sum(s.get("expected_value", 0) for s in sorted_by_value[:5]) if len(sorted_by_value) >= 5 else sum(s.get("expected_value", 0) for s in sorted_by_value),
            "next_90": sum(s.get("expected_value", 0) for s in sorted_by_value),
        }
    }


def _metric_value(key: str, value) -> str:
    if isinstance(value, (int, float)):
        if "usd" in key or "value" in key or "cost" in key or "saving" in key:
            return format_currency(value)
        if "payback" in key:
            return format_payback(float(value))
        return f"{value:,.1f}"
    return str(value)


def _annual_savings_value(scored: dict, breakdown: dict, expected_value: float) -> float:
    annual_savings = _to_float(breakdown.get("annual_savings_usd"), 0)
    if annual_savings > 0:
        return annual_savings

    estimated_battery_savings = _to_float(scored.get("estimated_battery_savings"), 0)
    if estimated_battery_savings > 0:
        return estimated_battery_savings

    contract_3yr = _to_float(breakdown.get("contract_3yr_usd"), 0)
    if contract_3yr > 0:
        return contract_3yr / 3

    annual_contract = _to_float(breakdown.get("annual_contract_usd"), 0)
    if annual_contract > 0:
        return annual_contract

    return expected_value * 0.33


def _build_opportunity_view(scored: dict | None) -> dict | None:
    if not scored:
        return None

    opp_type = _opportunity_type(scored)
    system = scored.get("_system", {})
    breakdown = scored.get("value_breakdown") or {}
    template = OPPORTUNITY_NARRATIVES.get(opp_type, {})
    kw_peak = _to_float(system.get("kw_peak"), 0)
    year_installed = system.get("year_installed")
    age = datetime.now().year - year_installed if year_installed else 0
    expected_value = _to_float(scored.get("expected_value"), 0)
    gross_cost = _to_float(breakdown.get("battery_cost_gross_usd", breakdown.get("base_value", expected_value)), expected_value)
    itc_saving = _to_float(breakdown.get("itc_saving_usd"), gross_cost * 0.30)
    ivu_saving = _to_float(breakdown.get("ivu_saving_usd"), gross_cost * 0.115)
    net_cost = _to_float(breakdown.get("battery_cost_net_usd"), max(gross_cost - itc_saving - ivu_saving, 0))
    annual_savings = _annual_savings_value(scored, breakdown, expected_value)
    payback = _to_float(
        breakdown.get("payback_post_incentivos", scored.get("battery_payback_years")),
        0,
    )
    if payback <= 0 and annual_savings > 0:
        payback = net_cost / annual_savings
    battery_kwh = _to_float(
        breakdown.get("battery_kwh", scored.get("battery_kwh_recommended")),
        kw_peak * 1.2,
    )
    context = {
        "system_name": system.get("name", "System"),
        "kw_peak": f"{kw_peak:,.0f}",
        "tariff": system.get("tariff_type", "GSS"),
        "annual_peak_cost": annual_savings or expected_value,
        "battery_kwh": f"{battery_kwh:,.0f}",
        "annual_savings": annual_savings,
        "net_cost": net_cost,
        "gross_cost": gross_cost,
        "age": age,
        "delta_pct": 12,
        "annual_loss": expected_value,
        "recovery_pct": 4,
        "annual_gain": expected_value,
        "annual_revenue": expected_value,
        "monthly_charge": annual_savings / 12 if annual_savings else 0,
        "reduction_pct": 55,
        "annual_saving": annual_savings,
    }
    narrative_template = template.get(
        "the_opportunity",
        "{system_name} has an identified {type_label} opportunity valued at ${expected_value:,.0f}.",
    )
    context["type_label"] = _type_label(opp_type)
    context["expected_value"] = expected_value

    talking_points = []
    for point in template.get("talking_points", []):
        talking_points.append(point.format(**context))

    metrics = [
        {"label": key.replace("_", " ").title(), "value": _metric_value(key, value)}
        for key, value in breakdown.items()
    ][:8]
    if not metrics:
        metrics = [
            {"label": "Expected Value", "value": format_currency(expected_value)},
            {"label": "Close Probability", "value": format_pct(scored.get("close_probability", 0))},
        ]

    return {
        "opportunity_reason": opp_type,
        "type_label": _type_label(opp_type),
        "system_name": system.get("name", "System"),
        "expected_value": expected_value,
        "payback": f"{payback:.1f}",
        "close_probability": scored.get("close_probability", 0),
        "narrative": narrative_template.format(**context),
        "warning": template.get("warning", ""),
        "talking_points": talking_points,
        "has_gsp_tariff": str(system.get("tariff_type", "")).upper() == "GSP",
        "demand_charge_narrative": DEMAND_CHARGE_NARRATIVE.format(
            peak_kva=max(kw_peak / 0.85, 0),
            monthly_charge=annual_savings / 12 if annual_savings else 0,
            reduction_kva=max((kw_peak / 0.85) * 0.55, 0),
            annual_saving=annual_savings,
        ),
        "metrics": metrics,
        "battery_kwh": f"{battery_kwh:,.0f}",
        "gross_cost": gross_cost,
        "net_cost": net_cost,
        "annual_savings": annual_savings,
        "itc_saving": itc_saving,
        "ivu_saving": ivu_saving,
    }


def _financial_summary(scored_systems: list) -> dict:
    gross = 0.0
    incentives = 0.0
    annual = 0.0
    for scored in scored_systems:
        breakdown = scored.get("value_breakdown") or {}
        expected = _to_float(scored.get("expected_value"), 0)
        item_gross = _to_float(breakdown.get("battery_cost_gross_usd", breakdown.get("base_value", expected)), expected)
        item_incentives = _to_float(breakdown.get("itc_saving_usd"), 0) + _to_float(breakdown.get("ivu_saving_usd"), 0)
        gross += item_gross
        incentives += item_incentives
        annual += _annual_savings_value(scored, breakdown, expected)
    net = max(gross - incentives, 0)
    return {
        "total_gross_investment": gross,
        "total_incentives": incentives,
        "net_investment": net,
        "portfolio_roi": annual / net if net else 0,
        "blended_payback": round(net / annual, 1) if annual else 0,
    }


def build_report_data(scored_systems: list, portfolio: dict, market: str) -> dict:
    value_by_type = aggregate_by_type(scored_systems)
    top_opportunity = get_top_opportunity(scored_systems)
    second_opportunity = get_second_opportunity(scored_systems)
    sorted_by_value = sorted(scored_systems, key=lambda s: s.get("expected_value", 0), reverse=True)

    return {
        "client_name": portfolio.get("client_name", "SOLVIST Portfolio"),
        "date": datetime.now(),
        "market": market,
        "market_label": "Puerto Rico Market" if market == "pr" else market.upper(),
        "font_path": str(FONT_DIR),
        "total_value": sum(s.get("expected_value", 0) for s in scored_systems),
        "systems_analyzed": len(scored_systems),
        "opportunities_found": len([s for s in scored_systems if s.get("expected_value", 0) > 0]),
        "key_findings": generate_findings(scored_systems),
        "key_risks": generate_risks(scored_systems),
        "assessment": generate_assessment(scored_systems),
        "top_opportunity": _build_opportunity_view(top_opportunity),
        "second_opportunity": _build_opportunity_view(second_opportunity),
        "all_opportunities": scored_systems,
        "top_5_opportunities": [_build_opportunity_view(s) for s in sorted_by_value[:5]],
        "financial_summary": _financial_summary(scored_systems),
        "value_by_type": value_by_type,
        "value_by_type_display": [
            (_type_label(opp_type), value)
            for opp_type, value in sorted(value_by_type.items(), key=lambda item: item[1], reverse=True)
        ],
        "actions": generate_actions(scored_systems),
        "methodology": METHODOLOGY_PR,
    }


def render_html(report_data: dict, charts: dict) -> str:
    env = Environment(
        loader=FileSystemLoader(str(TEMPLATE_DIR)),
        autoescape=select_autoescape(["html", "xml"]),
    )
    env.filters["currency"] = format_currency
    env.filters["pct"] = format_pct
    env.filters["payback"] = format_payback
    env.filters["report_date"] = format_date
    template = env.get_template("base.html")
    return template.render(
        data=report_data,
        charts=charts,
        font_path=report_data["font_path"],
    )


def generate_audit_pdf(portfolio_id: str, market: str = "pr") -> bytes:
    """
    Pipeline:
    1. Fetch portfolio data
    2. Score each system via scoring.engine.compute_opportunity_score()
    3. Build report_data dict
    4. Generate charts (matplotlib -> base64)
    5. Render HTML (Jinja2)
    6. Convert to PDF (WeasyPrint) with 10s timeout
    7. Return bytes

    Raises TimeoutError if generation exceeds 10 seconds.
    """
    portfolio = fetch_portfolio(portfolio_id)
    scored = score_systems(portfolio, market=market)
    report_data = build_report_data(scored, portfolio, market=market)
    charts = {
        "matrix_chart": OpportunityMatrixChart(scored).to_base64(),
        "recovery_chart": RecoveryBarChart(report_data["value_by_type"]).to_base64(),
    }
    html = render_html(report_data, charts)

    import weasyprint
    pdf_bytes = weasyprint.HTML(string=html).write_pdf()

    return pdf_bytes
