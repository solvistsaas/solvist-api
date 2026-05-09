# report/i18n.py — Fuente única de verdad para strings en español

LABELS = {
    # Títulos de sección
    "executive_summary": "Resumen Ejecutivo",
    "opportunity_matrix": "Matriz de Oportunidades",
    "financial_recovery": "Recuperación Financiera",
    "next_actions": "Próximas Acciones Recomendadas",
    "methodology": "Metodología",

    # Métricas
    "value": "VALOR",
    "close_prob": "CONV.",
    "payback": "RETORNO",
    "annual_savings": "AHORRO ANUAL",
    "net_investment": "INVERSIÓN NETA",
    "gross_investment": "INVERSIÓN BRUTA",
    "roi": "RENTABILIDAD",
    "score": "PUNTUACIÓN",
    "priority": "PRIORIDAD",

    # Unidades
    "years": "años",
    "months": "meses",
    "kwp": "kWp",
    "kwh": "kWh",
    "kva": "kVA",

    # Assessment levels
    "hot": "Prioritario",
    "warm": "Relevante",
    "moderate": "Moderado",
    "low": "Bajo potencial",

    # Tipos de oportunidad
    "battery_upgrade": "Instalación de Baterías",
    "industrial_battery": "Batería Industrial",
    "maintenance": "Contrato O&M",
    "inverter_replacement": "Reemplazo de Inversor",
    "system_expansion": "Expansión del Sistema",
    "ev_charger": "Cargador EV",
    "peak_shaving": "Reducción de Pico",
    "vpp_monetization": "Ingresos VPP",
    "tropical_degradation": "Degradación Tropical",

    # Key Findings → Hallazgos
    "key_findings": "Hallazgos Principales",
    "risks": "Riesgos Identificados",
    "top_opportunity": "Oportunidad Principal",
    "secondary_opportunity": "Oportunidad Secundaria",
    "portfolio_summary": "Resumen de Cartera",
    "systems_analyzed": "Sistemas Analizados",
    "total_value": "Valor Total Detectado",
    "conversion_rate": "Tasa de Conversión",

    # Acciones
    "this_week": "Esta Semana",
    "next_30_days": "Próximos 30 Días",
    "next_90_days": "Próximos 90 Días",

    # Misc
    "confidential": "Confidencial",
    "prepared_by": "Elaborado por",
    "page": "Página",
    "of": "de",
}

OPP_DISPLAY_NAMES = {
    "battery_upgrade": "Instalación de Baterías",
    "industrial_battery": "Batería Industrial",
    "maintenance": "Contrato O&M",
    "inverter_replacement": "Reemplazo de Inversor",
    "system_expansion": "Expansión del Sistema",
    "ev_charger": "Cargador EV",
    "peak_shaving": "Reducción de Pico",
    "vpp_monetization": "Ingresos VPP",
    "tropical_degradation": "Degradación Tropical",
}


def t(key: str) -> str:
    """Traducción segura. Si no existe la key, devuelve la key en mayúsculas como fallback visible."""
    return LABELS.get(key, key.upper().replace("_", " "))


def opp_name(opp_type: str) -> str:
    """Nombre display de tipo de oportunidad."""
    return OPP_DISPLAY_NAMES.get(opp_type, opp_type.replace("_", " ").title())
