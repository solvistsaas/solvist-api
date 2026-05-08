"""
Módulo de scoring para mercado Puerto Rico (PR)
Datos validados 2025:
- Perplexity Deep Research + Gemini + documentos LUMA Energy
- NREL PR100, IEC 62804, Kilowatt PR 2026
- OIG federal: 57% instalaciones C&I con deficiencia técnica
"""
from __future__ import annotations
from typing import Tuple

# ─── Constantes de mercado Puerto Rico 2025 ──────────────────────────────────

MARKET = "PR"
CURRENCY = "USD"
CURRENCY_SYMBOL = "$"

BATTERY_COST_PER_KWH = 400
BATTERY_KWH_PER_KWP = 1.2
ELECTRICITY_PRICE_GSS = 0.27
ELECTRICITY_PRICE_GSP = 0.22
DEMAND_CHARGE_PER_KVA = 8.10
ANNUAL_PRODUCTION_KWH_PER_KWP = 1600
EXPORT_RATIO_WITHOUT_BATTERY = 0.35
POWER_FACTOR = 0.85
ITC_FEDERAL = 0.30
IVU_LOCAL = 0.115
EFFECTIVE_COST_MULTIPLIER = 0.585

OM_COST_PER_KWP_YEAR = 75
OM_CONTRACT_YEARS = 3
OM_DEGRADATION_WITHOUT_CONTRACT = 0.25

INVERTER_LIFESPAN_PR = 8
INVERTER_ALERT_YEAR_PR = 5
INVERTER_REPLACEMENT_COST = {10: 4500, 50: 14500, 100: 26000}

EV_KWP_PER_POINT = 20
EV_VALUE_PER_POINT_AC = 4000
NEVI_SUBSIDY_RATIO = 0.80

PEAK_SHAVING_REDUCTION = 0.55
PEAK_SHAVING_VALUE_YEARS = 5

VPP_PAYMENT_PER_KWH = 1.00
VPP_EVENTS_PER_YEAR = 87
VPP_HOURS_PER_EVENT = 2
VPP_AVAILABILITY = 0.80

TROPICAL_DEGRADATION_ANNUAL = 0.055
TROPICAL_DEGRADATION_CAP = 0.25
PID_RECOVERY_COST_PER_INVERTER = 850

POST_HURRICANE_AUDIT_VALUE = 2500

# ─── Tipos de oportunidad Puerto Rico ────────────────────────────────────────

OPP_BATTERY_UPGRADE = "battery_upgrade"
OPP_INDUSTRIAL_BATTERY = "industrial_battery"
OPP_MAINTENANCE = "maintenance"
OPP_INVERTER_REPLACEMENT = "inverter_replacement"
OPP_SYSTEM_EXPANSION = "system_expansion"
OPP_EV_CHARGER = "ev_charger"
OPP_PEAK_SHAVING = "peak_shaving"
OPP_VPP_MONETIZATION = "vpp_monetization"
OPP_TROPICAL_DEGRADATION = "tropical_degradation"

OPP_DISPLAY_NAMES_PR = {
    OPP_BATTERY_UPGRADE: "Battery Storage Installation",
    OPP_INDUSTRIAL_BATTERY: "Industrial Battery System",
    OPP_MAINTENANCE: "O&M Contract",
    OPP_INVERTER_REPLACEMENT: "Inverter Replacement/Review",
    OPP_SYSTEM_EXPANSION: "System Expansion",
    OPP_EV_CHARGER: "EV Charging Infrastructure",
    OPP_PEAK_SHAVING: "Peak Demand Reduction",
    OPP_VPP_MONETIZATION: "VPP Revenue (CBES Program)",
    OPP_TROPICAL_DEGRADATION: "Tropical Degradation Audit",
}

# ─── Helpers ─────────────────────────────────────────────────────────────────

def _get_inverter_replacement_cost(kwp: float) -> float:
    if kwp <= 15:
        return INVERTER_REPLACEMENT_COST[10]
    elif kwp <= 75:
        return INVERTER_REPLACEMENT_COST[50]
    else:
        return INVERTER_REPLACEMENT_COST[100]

def _estimated_demand_kva(kwp: float) -> float:
    return kwp * POWER_FACTOR

# ─── Cálculo de expected_value por tipo ──────────────────────────────────────

def calculate_expected_value_pr(
    opportunity_type: str,
    kwp: float,
    installation_year: int | None,
    has_battery: bool | None,
    location_type: str,
    now_year: int,
) -> Tuple[float, dict]:
    age = (now_year - installation_year) if installation_year else 5
    breakdown = {}

    if opportunity_type in (OPP_BATTERY_UPGRADE, OPP_INDUSTRIAL_BATTERY):
        battery_kwh = kwp * BATTERY_KWH_PER_KWP
        battery_cost_gross = battery_kwh * BATTERY_COST_PER_KWH
        battery_cost_net = battery_cost_gross * EFFECTIVE_COST_MULTIPLIER
        annual_export_kwh = kwp * ANNUAL_PRODUCTION_KWH_PER_KWP * EXPORT_RATIO_WITHOUT_BATTERY
        annual_savings = annual_export_kwh * ELECTRICITY_PRICE_GSS
        payback_pre = round(battery_cost_gross / annual_savings, 1) if annual_savings > 0 else 0
        payback_post = round(battery_cost_net / annual_savings, 1) if annual_savings > 0 else 0
        breakdown = {
            "battery_kwh": round(battery_kwh, 1),
            "battery_cost_gross_usd": round(battery_cost_gross),
            "battery_cost_net_usd": round(battery_cost_net),
            "annual_savings_usd": round(annual_savings),
            "payback_pre_incentivos": payback_pre,
            "payback_post_incentivos": payback_post,
            "itc_saving_usd": round(battery_cost_gross * ITC_FEDERAL),
            "ivu_saving_usd": round(battery_cost_gross * IVU_LOCAL),
        }
        return round(battery_cost_gross, 2), breakdown

    elif opportunity_type == OPP_PEAK_SHAVING:
        demand_kva = _estimated_demand_kva(kwp)
        monthly_demand_charge = demand_kva * DEMAND_CHARGE_PER_KVA
        annual_demand_charge = monthly_demand_charge * 12
        annual_saving = annual_demand_charge * PEAK_SHAVING_REDUCTION
        total_value = annual_saving * PEAK_SHAVING_VALUE_YEARS
        breakdown = {
            "estimated_demand_kva": round(demand_kva, 1),
            "monthly_demand_charge_usd": round(monthly_demand_charge),
            "annual_demand_charge_usd": round(annual_demand_charge),
            "annual_saving_usd": round(annual_saving),
            "reduction_pct": int(PEAK_SHAVING_REDUCTION * 100),
        }
        return round(total_value, 2), breakdown

    elif opportunity_type == OPP_VPP_MONETIZATION:
        battery_kwh = kwp * BATTERY_KWH_PER_KWP
        utilizable_kwh = battery_kwh * VPP_AVAILABILITY
        annual_revenue = utilizable_kwh * VPP_PAYMENT_PER_KWH * VPP_EVENTS_PER_YEAR * VPP_HOURS_PER_EVENT
        breakdown = {
            "battery_kwh_estimated": round(battery_kwh, 1),
            "utilizable_kwh": round(utilizable_kwh, 1),
            "annual_vpp_revenue_usd": round(annual_revenue),
            "events_per_year": VPP_EVENTS_PER_YEAR,
            "payment_per_kwh_usd": VPP_PAYMENT_PER_KWH,
        }
        return round(annual_revenue * 3, 2), breakdown

    elif opportunity_type == OPP_TROPICAL_DEGRADATION:
        degradation_pct = min(TROPICAL_DEGRADATION_CAP, age * TROPICAL_DEGRADATION_ANNUAL)
        annual_production_loss_kwh = kwp * ANNUAL_PRODUCTION_KWH_PER_KWP * degradation_pct
        annual_loss_usd = annual_production_loss_kwh * ELECTRICITY_PRICE_GSS
        recovery_value = annual_loss_usd * 3
        breakdown = {
            "estimated_degradation_pct": round(degradation_pct * 100, 1),
            "annual_production_loss_kwh": round(annual_production_loss_kwh),
            "annual_loss_usd": round(annual_loss_usd),
            "system_age_years": age,
        }
        return round(recovery_value, 2), breakdown

    elif opportunity_type == OPP_MAINTENANCE:
        annual_value = kwp * OM_COST_PER_KWP_YEAR
        contract_value = annual_value * OM_CONTRACT_YEARS
        annual_production_loss = kwp * ANNUAL_PRODUCTION_KWH_PER_KWP * OM_DEGRADATION_WITHOUT_CONTRACT * ELECTRICITY_PRICE_GSS
        breakdown = {
            "annual_contract_usd": round(annual_value),
            "contract_3yr_usd": round(contract_value),
            "annual_production_loss_usd": round(annual_production_loss),
            "om_cost_per_kwp": OM_COST_PER_KWP_YEAR,
        }
        return round(contract_value, 2), breakdown

    elif opportunity_type == OPP_INVERTER_REPLACEMENT:
        replacement_cost = _get_inverter_replacement_cost(kwp)
        breakdown = {
            "inverter_age_years": age,
            "replacement_cost_usd": replacement_cost,
            "action": "full_replacement" if age >= INVERTER_ALERT_YEAR_PR else "firmware_voltage_config",
            "luma_voltage_instability": True,
        }
        return float(replacement_cost), breakdown

    elif opportunity_type == OPP_SYSTEM_EXPANSION:
        expansion_kwp = kwp * 0.30
        cost_per_kwp = 1100
        value = expansion_kwp * cost_per_kwp
        breakdown = {
            "expansion_kwp": round(expansion_kwp, 1),
            "cost_per_kwp_usd": cost_per_kwp,
        }
        return round(value, 2), breakdown

    elif opportunity_type == OPP_EV_CHARGER:
        points = max(1, int(kwp / EV_KWP_PER_POINT))
        value = points * EV_VALUE_PER_POINT_AC
        nevi_subsidy = value * NEVI_SUBSIDY_RATIO
        breakdown = {
            "ev_points_estimated": points,
            "value_per_point_usd": EV_VALUE_PER_POINT_AC,
            "nevi_subsidy_potential_usd": round(nevi_subsidy),
        }
        return float(value), breakdown

    return 0.0, {}

# ─── Narrativas por tipo de oportunidad ──────────────────────────────────────

def get_recommended_action_pr(
    opportunity_type: str,
    kwp: float,
    installation_year: int | None,
    has_battery: bool | None,
    location_type: str,
    expected_value: float,
    breakdown: dict,
    now_year: int,
) -> Tuple[str, str]:
    age = (now_year - installation_year) if installation_year else 5
    year_display = installation_year if installation_year else "unknown"
    battery_kwh = breakdown.get("battery_kwh", kwp * 1.2)
    annual_savings = breakdown.get("annual_savings_usd", 0)
    payback_post = breakdown.get("payback_post_incentivos", 0)
    demand_kva = breakdown.get("estimated_demand_kva", kwp * 0.85)
    monthly_demand = breakdown.get("monthly_demand_charge_usd", 0)
    annual_saving_ps = breakdown.get("annual_saving_usd", 0)
    annual_vpp = breakdown.get("annual_vpp_revenue_usd", 0)
    degradation_pct = breakdown.get("estimated_degradation_pct", 0)
    annual_loss = breakdown.get("annual_loss_usd", 0)
    ev_points = breakdown.get("ev_points_estimated", 1)
    annual_contract = breakdown.get("annual_contract_usd", 0)
    annual_loss_om = breakdown.get("annual_production_loss_usd", 0)
    replacement_cost = breakdown.get("replacement_cost_usd", expected_value)
    expansion_kwp = breakdown.get("expansion_kwp", kwp * 0.3)
    nevi = breakdown.get("nevi_subsidy_potential_usd", 0)
    gross = breakdown.get("battery_cost_gross_usd", expected_value)
    net = breakdown.get("battery_cost_net_usd", expected_value * 0.585)
    itc = breakdown.get("itc_saving_usd", 0)
    ivu = breakdown.get("ivu_saving_usd", 0)

    if opportunity_type in (OPP_BATTERY_UPGRADE, OPP_INDUSTRIAL_BATTERY):
        recommended_action = (
            f"System: {kwp} kWp installed in {year_display} ({age} years), no battery. "
            f"Recommended storage: {battery_kwh:.0f} kWh (1.2 kWh/kWp C&I standard). "
            f"Gross cost: ${gross:,.0f}. After ITC (30%) + IVU exemption (11.5%): "
            f"effective cost ${net:,.0f} — {int((1-net/gross)*100) if gross > 0 else 0}% reduction. "
            f"ITC savings: ${itc:,.0f}. IVU savings: ${ivu:,.0f}. "
            f"Estimated annual savings: ${annual_savings:,.0f}/year. "
            f"Payback post-incentives: {payback_post} years. "
            f"ITC federal guarantee expires 2032 — window is open now."
        )
        sales_script = (
            f"Good morning [name], I'm calling because we've analyzed your {year_display} "
            f"solar installation and found a significant opportunity. "
            f"With a {battery_kwh:.0f} kWh battery, you could save ${annual_savings:,.0f}/year. "
            f"After ITC (30%) and IVU exemption, effective cost is ${net:,.0f} — not ${gross:,.0f}. "
            f"Payback: {payback_post} years. Would you have 15 minutes this week?"
        )

    elif opportunity_type == OPP_PEAK_SHAVING:
        recommended_action = (
            f"System: {kwp} kWp. Estimated demand: {demand_kva:.0f} kVA. "
            f"Under GSP tariff, demand charge: ${monthly_demand:,.0f}/month "
            f"(${monthly_demand*12:,.0f}/year) based on your 15-minute peak. "
            f"A properly configured battery reduces this peak by 55%, "
            f"saving approximately ${annual_saving_ps:,.0f}/year in demand charges alone. "
            f"This is the fastest ROI available in your portfolio."
        )
        sales_script = (
            f"Good morning [name], do you know what your demand charge is on your LUMA bill? "
            f"For a system like yours, that charge is approximately ${monthly_demand:,.0f}/month "
            f"based solely on your highest 15-minute peak. "
            f"A battery configured for peak shaving cuts that by over half — "
            f"saving ${annual_saving_ps:,.0f}/year."
        )

    elif opportunity_type == OPP_VPP_MONETIZATION:
        recommended_action = (
            f"System has battery storage — eligible for LUMA CBES VPP program. "
            f"LUMA pays $1.00/kWh dispatched during grid events. "
            f"With {battery_kwh:.0f} kWh and {VPP_EVENTS_PER_YEAR} events/year: "
            f"estimated annual revenue ${annual_vpp:,.0f}/year. "
            f"Battery becomes a productive asset, not just emergency backup. "
            f"LUMA goal: enroll 60,000+ additional clients by 2026."
        )
        sales_script = (
            f"Good morning [name], is your battery enrolled in LUMA's CBES program? "
            f"LUMA pays $1.00/kWh during peak events — "
            f"approximately ${annual_vpp:,.0f}/year for a system like yours. "
            f"Your battery can pay for its own maintenance just from CBES income."
        )

    elif opportunity_type == OPP_TROPICAL_DEGRADATION:
        recommended_action = (
            f"Installation from {year_display} ({age} years) in tropical coastal environment. "
            f"Estimated performance degradation: {degradation_pct:.0f}% "
            f"(vs 2.5-3% theoretical catalog degradation). "
            f"Factors: PID from 80-90% sustained humidity, salt corrosion, "
            f"Arrhenius thermal acceleration (rooftop temps 65-75C in summer). "
            f"Estimated annual production loss: ${annual_loss:,.0f}/year. "
            f"Source: OIG federal audits — 57% of C&I installations have documented deficiencies. "
            f"Technical audit with IV-curve testing and thermography recommended."
        )
        sales_script = (
            f"Good morning [name], according to federal OIG audits, "
            f"57% of commercial solar installations in Puerto Rico have at least one "
            f"significant technical deficiency. "
            f"Your {year_display} installation has been under 80-90% humidity and salt exposure "
            f"for {age} years. We estimate {degradation_pct:.0f}% below rated capacity "
            f"— ${annual_loss:,.0f}/year in lost production."
        )

    elif opportunity_type == OPP_MAINTENANCE:
        recommended_action = (
            f"Installation of {age} years with no active O&M contract documented. "
            f"Puerto Rico's tropical environment causes 20-30% performance degradation "
            f"without preventive maintenance. "
            f"Estimated production loss: ${annual_loss_om:,.0f}/year. "
            f"Annual O&M contract: ${annual_contract:,.0f}/year (${OM_COST_PER_KWP_YEAR}/kWp). "
            f"Includes demineralized cleaning, IR thermography, inverter firmware, performance report. "
            f"Also required by insurers under Act 130-2025 for valid hurricane claims."
        )
        sales_script = (
            f"Good morning [name], your {year_display} installation has been running "
            f"{age} years in Puerto Rico's climate without documented maintenance. "
            f"That means approximately ${annual_loss_om:,.0f}/year in lost production. "
            f"Annual O&M contract: ${annual_contract:,.0f}/year. "
            f"Also: without maintenance records, your insurer can deny hurricane damage claims "
            f"under Act 130-2025."
        )

    elif opportunity_type == OPP_INVERTER_REPLACEMENT:
        if age >= INVERTER_ALERT_YEAR_PR:
            recommended_action = (
                f"Inverter with {age} years of operation in tropical field conditions. "
                f"In Puerto Rico, string inverter real lifespan is 5-8 years "
                f"(vs 10-15 years continental US) due to salt corrosion and humidity. "
                f"Inverters account for 43% of all solar system failures in Puerto Rico. "
                f"LUMA voltage instability (3-15 events/week) accelerates component stress. "
                f"Replacement cost for {kwp} kWp system: ${replacement_cost:,.0f}. "
                f"New grid-forming inverters also enable VPP/CBES participation."
            )
            sales_script = (
                f"Good morning [name], your inverter from {year_display} has {age} years "
                f"of operation in Puerto Rico's climate. "
                f"Inverters here typically last 5-8 years — not 15 years like continental ratings. "
                f"Inverter failures account for 43% of all system issues on the island. "
                f"Proactive replacement now: ${replacement_cost:,.0f}. "
                f"New grid-forming models also qualify for CBES revenue."
            )
        else:
            recommended_action = (
                f"Inverter with {age} years. LUMA voltage reconfiguration recommended. "
                f"LUMA network instability (3-15 voltage events/week) causes involuntary "
                f"disconnections during peak production hours (10am-2pm), "
                f"resulting in 15-25% daily production loss. "
                f"Reconfiguring voltage window to 75-115% nominal eliminates most events. "
                f"Visit cost: ~$300-500."
            )
            sales_script = (
                f"Good morning [name], do you know how many times your inverter "
                f"disconnects weekly due to LUMA voltage fluctuations? "
                f"Most systems in Puerto Rico lose 15-25% of daily production "
                f"during peak sun hours. "
                f"A $300-500 reconfiguration visit recovers that production immediately."
            )

    elif opportunity_type == OPP_SYSTEM_EXPANSION:
        recommended_action = (
            f"System: {kwp} kWp from {year_display}. Client profile suggests uncovered consumption. "
            f"Recommended expansion: {expansion_kwp:.0f} kWp additional. "
            f"Estimated cost: ${expected_value:,.0f} ($1,100/kWp — includes "
            f"Jones Act logistics, structural engineering and LUMA interconnection). "
            f"ITC 30% applies to expansion if battery is included."
        )
        sales_script = (
            f"Good morning [name], your {kwp} kWp system from {year_display} "
            f"may no longer cover your full consumption. "
            f"An expansion of {expansion_kwp:.0f} kWp would cost approximately ${expected_value:,.0f}. "
            f"If you add storage simultaneously, the entire project qualifies for the 30% federal tax credit."
        )

    elif opportunity_type == OPP_EV_CHARGER:
        recommended_action = (
            f"Commercial {location_type} client with compatible solar installation. "
            f"Recommended: {ev_points} AC Level 2 charging point{'s' if ev_points > 1 else ''} (22 kW). "
            f"Estimated installed cost: ${expected_value:,.0f}. "
            f"NEVI program covers up to 80% on AFC corridors (${nevi:,.0f} potential subsidy). "
            f"30C federal tax credit: additional 30% for businesses. "
            f"Solar + EV integration maximizes self-consumption."
        )
        sales_script = (
            f"Good morning [name], with NEVI federal funding available, "
            f"your solar installation is perfectly positioned to add "
            f"{ev_points} charging point{'s' if ev_points > 1 else ''}. "
            f"Estimated cost ${expected_value:,.0f} with up to ${nevi:,.0f} covered by NEVI. "
            f"Your solar panels charge your vehicles for free during the day."
        )

    else:
        recommended_action = f"Review {kwp} kWp installation ({year_display}) and contact client."
        sales_script = f"Good morning [name], calling to review your {year_display} solar installation."

    return recommended_action, sales_script
