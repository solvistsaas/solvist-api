"""
Templates de texto para el PDF.
No se usa LLM. Se usa interpolacion con datos reales del scoring engine.
Cada template recibe datos y devuelve strings listos para el HTML.

REGLA CRITICA: sales_script_long NO se renderiza directamente.
Solo se usan estos templates controlados para garantizar control editorial.
"""

# === KEY FINDINGS ===

FINDING_TEMPLATES = {
    "no_battery_majority": (
        "{pct}% of systems lack battery storage - "
        "${total:,.0f} in recoverable peak charges"
    ),
    "maintenance_gap": (
        "{count} systems without active O&M contract - "
        "${total:,.0f} in avoidable repair risk"
    ),
    "tropical_degradation": (
        "{pct}% of systems show tropical degradation above warranty - "
        "${total:,.0f} in hidden revenue loss"
    ),
    "inverter_obsolete": (
        "{count} inverters running outdated firmware - "
        "${total:,.0f} in recoverable production"
    ),
    "ev_opportunity": (
        "{count} sites eligible for NEVI-funded EV charging - "
        "${total:,.0f} in first-year revenue"
    ),
}


# === HEALTH ASSESSMENT (for Executive Summary context) ===

ASSESSMENT_TEMPLATES = {
    "critical": (
        "This portfolio has significant untapped potential. "
        "{top_type} opportunities alone represent ${top_value:,.0f} "
        "in recoverable revenue."
    ),
    "moderate": (
        "This portfolio shows solid fundamentals with meaningful "
        "optimization potential. {top_type} and {second_type} "
        "represent ${total:,.0f} in identified value."
    ),
    "strong": (
        "This portfolio is well-optimized. Remaining opportunities "
        "in {top_type} represent ${total:,.0f} in incremental value."
    ),
}


# === KEY RISKS ===

RISK_TEMPLATES = {
    "luma_rate_increase": (
        "LUMA Energy rate changes expected - "
        "GSS peak rate may increase 8-12%"
    ),
    "itc_window": (
        "Federal ITC at 30% through 2032 - "
        "potential phase-down could increase net cost by ${delta:,.0f}"
    ),
    "no_maintenance": (
        "{count} systems without O&M contract face "
        "${total:,.0f} in unplanned repair exposure"
    ),
    "tropical_damage": (
        "Tropical conditions accelerating degradation - "
        "PID risk HIGH for systems near coast"
    ),
}


# === OPPORTUNITY-SPECIFIC NARRATIVES ===

OPPORTUNITY_NARRATIVES = {
    "battery_upgrade": {
        "title": "Battery Upgrade",
        "the_opportunity": (
            "{system_name} operates a {kw_peak} kWp solar system without "
            "battery storage. Under LUMA's {tariff} tariff, this creates "
            "${annual_peak_cost:,.0f}/yr in avoidable peak demand charges. "
            "Adding a {battery_kwh} kWh LFP battery system eliminates "
            "these charges and provides backup power during grid outages."
        ),
        "warning": (
            "LUMA Energy rate changes expected. "
            "Current GSS peak rate may increase 8-12%."
        ),
        "talking_points": [
            "Your system generates ${annual_savings:,.0f}/yr in energy you're not storing",
            "Federal ITC covers 30% - net cost is ${net_cost:,.0f}, not ${gross_cost:,.0f}",
            "CBES program has limited application slots available",
        ],
    },
    "maintenance": {
        "title": "O&M Contract",
        "the_opportunity": (
            "{system_name} has no active maintenance contract. Without "
            "preventive care, tropical conditions accelerate degradation "
            "and increase unplanned repair costs. An O&M contract provides "
            "95% production guarantee and 24-hour emergency response."
        ),
        "warning": (
            "Average unplanned repair cost in PR is $4,200 - "
            "3x the annual O&M contract cost."
        ),
        "talking_points": [
            "A single inverter failure costs more than 2 years of O&M coverage",
            "95% production guarantee - we pay you if performance drops",
            "Preventive maintenance extends system life by 3-5 years in tropical climate",
        ],
    },
    "tropical_degradation": {
        "title": "Tropical Degradation",
        "the_opportunity": (
            "{system_name} is {age} years old in Puerto Rico's tropical "
            "climate. Actual degradation likely exceeds manufacturer warranty "
            "by {delta_pct}%, representing ${annual_loss:,.0f}/yr in hidden "
            "revenue loss. Early intervention prevents cascade failure."
        ),
        "warning": (
            "Standard 25-year warranty assumes 0.5%/yr linear degradation. "
            "Puerto Rico tropical conditions typically produce 2-3x faster degradation."
        ),
        "talking_points": [
            "Your panels are likely producing {delta_pct}% less than the manufacturer promised",
            "PID from humidity is irreversible - early detection prevents cascade",
            "A maintenance check now can identify panels before damage becomes total",
        ],
    },
    "inverter_replacement": {
        "title": "Inverter Reconfiguration",
        "the_opportunity": (
            "{system_name}'s inverter is running below optimal efficiency. "
            "A firmware update and threshold adjustment can recover "
            "{recovery_pct}% production - ${annual_gain:,.0f}/yr in "
            "additional revenue with minimal implementation cost."
        ),
        "warning": (
            "Inverter firmware has not been updated since installation. "
            "Manufacturer has released updates with 2-4% efficiency improvements."
        ),
        "talking_points": [
            "A firmware update alone can recover 2-4% production - that's ${annual_gain:,.0f}/yr",
            "This is the fastest ROI in your portfolio - weeks, not months",
            "No equipment cost, just expertise",
        ],
    },
    "ev_charger": {
        "title": "EV Charging",
        "the_opportunity": (
            "{system_name} has parking infrastructure suitable for EV charging. "
            "NEVI funding covers up to 80% of installation costs, making this "
            "a low-risk revenue opportunity with ${annual_revenue:,.0f}/yr "
            "potential at moderate utilization."
        ),
        "warning": (
            "NEVI program funding is allocated on a first-come basis."
        ),
        "talking_points": [
            "NEVI covers up to 80% of installation costs",
            "Each L2 charger generates revenue from Day 1",
            "EV drivers spend 45 min on-site - that's foot traffic for tenants",
        ],
    },
    "peak_shaving": {
        "title": "Peak Shaving",
        "the_opportunity": (
            "{system_name} is on LUMA GSP tariff with demand charges of "
            "$8.10/kVA. Current peak demand creates ${monthly_charge:,.0f}/mo "
            "in demand charges. Battery storage can reduce peak by {reduction_pct}%, "
            "saving ${annual_saving:,.0f}/yr."
        ),
        "warning": (
            "LUMA GSP tariff demand charges are structured in 3 tiers. "
            "Shaving from Tier 3 to Tier 2 yields 40% savings."
        ),
        "talking_points": [
            "Demand charges are 35-45% of your electricity bill",
            "A battery can shave your peak, dropping you to a lower tier",
            "This saving is immediate - first bill after installation",
        ],
    },
}


# === DEMAND CHARGE BLOCK (condicional dentro de battery_upgrade) ===

DEMAND_CHARGE_NARRATIVE = (
    "System is on LUMA GSP tariff with demand charges of $8.10/kVA. "
    "Current peak demand of {peak_kva} kVA generates ${monthly_charge:,.0f}/mo "
    "in demand charges alone. Battery storage can reduce peak demand "
    "by {reduction_kva} kVA, saving ${annual_saving:,.0f}/yr."
)


# === METHODOLOGY (estatico para PR) ===

METHODOLOGY_PR = {
    "data_sources": [
        "LUMA Energy tariff schedules",
        "Puerto Rico Energy Bureau regulations",
        "Federal ITC guidelines (IRA 2022)",
        "NREL PV degradation models (tropical)",
        "CBES/VPP program specifications",
        "OIG Federal Audit - 57% system deficiency rate",
        "IEC 62804 - PID testing standards",
    ],
    "financial_assumptions": {
        "discount_rate": "8%",
        "itc_rate": "30%",
        "degradation_y1": "5.5% (tropical adjusted)",
        "degradation_subsequent": "1.2%/yr",
        "battery_cost": "$400/kWh (LFP)",
        "electricity_escalation": "3.5%/yr",
        "itc_window": "Through 2032 (potential phase-down)",
    },
    "glossary": {
        "ITC": "Investment Tax Credit (Federal)",
        "LUMA": "LUMA Energy (grid operator, PR)",
        "CBES": "Critical Battery Energy Storage",
        "GSS": "General Service Schedule (LUMA)",
        "GSP": "General Service Peak (LUMA)",
        "IVU": "Impuesto sobre Ventas y Uso",
        "PID": "Potential Induced Degradation",
        "VPP": "Virtual Power Plant",
        "NEVI": "National Electric Vehicle Infrastructure Program",
        "LFP": "Lithium Iron Phosphate",
    },
}


# === RECOMMENDED ACTIONS (template structure) ===
# NOTA: No se incluyen fechas hardcodeadas.
# Solo acciones sin deadline especifico.

ACTION_TEMPLATES = {
    "this_week": [
        {
            "action": "Contact {client_name} re: {opp_type} (${value:,.0f} value, {prob}% close probability)",
            "type": "call",
        },
    ],
    "next_30_days": [
        {
            "action": "Prepare {opp_type} proposals for top {count} systems identified",
            "type": "proposal",
        },
        {
            "action": "Submit CBES program applications for eligible systems",
            "type": "application",
        },
    ],
    "next_90_days": [
        {
            "action": "Develop EV charging proposals for {count} locations with NEVI funding",
            "type": "proposal",
        },
        {
            "action": "Complete tropical degradation assessment for systems > 3 years old",
            "type": "assessment",
        },
    ],
}
