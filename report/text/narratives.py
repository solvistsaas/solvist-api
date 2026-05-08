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
        "{pct}% de los sistemas no tienen almacenamiento de batería — "
        "${total:,.0f} en cargos de pico recuperables"
    ),
    "maintenance_gap": (
        "{count} sistemas sin contrato O&M activo — "
        "${total:,.0f} en riesgo de reparación evitable"
    ),
    "tropical_degradation": (
        "{pct}% de los sistemas muestran degradación tropical por encima de garantía — "
        "${total:,.0f} en pérdida de ingresos oculta"
    ),
    "inverter_obsolete": (
        "{count} inversores con firmware desactualizado — "
        "${total:,.0f} en producción recuperable"
    ),
    "ev_opportunity": (
        "{count} instalaciones elegibles para carga EV con fondos NEVI — "
        "${total:,.0f} en ingresos el primer año"
    ),
}


# === HEALTH ASSESSMENT (para el Executive Summary) ===

ASSESSMENT_TEXTS = {
    "excellent": (
        "La cartera presenta un potencial de recuperación excepcional "
        "con oportunidades de alto valor listas para activar."
    ),
    "good": (
        "La cartera muestra oportunidades de valor significativo "
        "distribuidas en múltiples sistemas."
    ),
    "moderate": (
        "La cartera presenta oportunidades moderadas con margen de mejora "
        "en eficiencia y rentabilidad."
    ),
    "low": (
        "La cartera muestra potencial limitado en el horizonte actual. "
        "Se recomienda revisión técnica preventiva."
    ),
}

# Alias de compatibilidad
ASSESSMENT_TEMPLATES = ASSESSMENT_TEXTS


# === KEY RISKS ===

RISK_TEMPLATES = {
    "luma_rate_increase": (
        "Cambios de tarifa LUMA Energy previstos — "
        "tarifa pico GSS podría subir entre 8 y 12%"
    ),
    "itc_window": (
        "ITC federal al 30% vigente hasta 2032 — "
        "reducción potencial aumentaría el costo neto en ${delta:,.0f}"
    ),
    "no_maintenance": (
        "{count} sistemas sin contrato O&M expuestos "
        "a ${total:,.0f} en reparaciones no planificadas"
    ),
    "tropical_damage": (
        "Condiciones tropicales aceleran la degradación — "
        "riesgo de PID ALTO en sistemas próximos a la costa"
    ),
}


# === OPPORTUNITY-SPECIFIC NARRATIVES ===

OPPORTUNITY_NARRATIVES = {
    "battery_upgrade": {
        "title": "Actualización de Batería",
        "the_opportunity": (
            "{system_name} opera un sistema solar de {kw_peak} kWp sin "
            "almacenamiento de energía. Bajo la tarifa {tariff} de LUMA, esto genera "
            "${annual_peak_cost:,.0f}/año en cargos de demanda de pico evitables. "
            "Añadir un sistema de batería LFP de {battery_kwh} kWh elimina "
            "estos cargos y proporciona energía de respaldo durante cortes de red."
        ),
        "warning": (
            "Se prevén cambios en las tarifas de LUMA Energy. "
            "La tarifa pico GSS actual podría incrementarse entre un 8 y un 12%."
        ),
        "talking_points": [
            "Tu sistema genera ${annual_savings:,.0f}/año en energía que no estás almacenando",
            "El ITC federal cubre el 30% — el costo neto es ${net_cost:,.0f}, no ${gross_cost:,.0f}",
            "El programa CBES tiene cupos de solicitud limitados disponibles",
        ],
    },
    "industrial_battery": {
        "title": "Batería Industrial",
        "the_opportunity": (
            "{system_name} cuenta con una instalación de {kw_peak} kWp con potencial "
            "de almacenamiento industrial no aprovechado. Un sistema LFP de {battery_kwh} kWh "
            "optimizaría el autoconsumo, reduciría la dependencia de LUMA y habilitaría "
            "participación en programas CBES con ingresos adicionales desde el primer año."
        ),
        "warning": (
            "Se prevén cambios en las tarifas de LUMA Energy. "
            "La tarifa pico GSS actual podría incrementarse entre un 8 y un 12%."
        ),
        "talking_points": [
            "Tu sistema genera ${annual_savings:,.0f}/año en energía que no estás almacenando",
            "El ITC federal cubre el 30% — el costo neto es ${net_cost:,.0f}, no ${gross_cost:,.0f}",
            "El programa CBES tiene cupos de solicitud limitados disponibles",
        ],
    },
    "maintenance": {
        "title": "Contrato O&M",
        "the_opportunity": (
            "{system_name} no tiene contrato de mantenimiento activo. Sin "
            "cuidado preventivo, las condiciones tropicales aceleran la degradación "
            "y aumentan los costos de reparación no planificados. Un contrato O&M "
            "proporciona garantía de producción del 95% y respuesta de emergencia en 24 horas."
        ),
        "warning": (
            "El costo promedio de reparación no planificada en PR es de $4.200 — "
            "3 veces el costo anual del contrato O&M."
        ),
        "talking_points": [
            "Un solo fallo de inversor cuesta más que 2 años de cobertura O&M",
            "Garantía de producción del 95% — te compensamos si el rendimiento baja",
            "El mantenimiento preventivo extiende la vida del sistema 3-5 años en clima tropical",
        ],
    },
    "tropical_degradation": {
        "title": "Degradación Tropical",
        "the_opportunity": (
            "{system_name} tiene {age} años en el clima tropical de Puerto Rico. "
            "La degradación real probablemente supera la garantía del fabricante "
            "en un {delta_pct}%, lo que representa ${annual_loss:,.0f}/año en "
            "pérdida de ingresos oculta. La intervención temprana previene fallos en cascada."
        ),
        "warning": (
            "La garantía estándar de 25 años asume una degradación lineal de 0,5%/año. "
            "Las condiciones tropicales de Puerto Rico producen típicamente una degradación 2-3 veces más rápida."
        ),
        "talking_points": [
            "Tus paneles probablemente producen un {delta_pct}% menos de lo prometido por el fabricante",
            "El PID por humedad es irreversible — la detección temprana previene el fallo en cascada",
            "Una revisión técnica ahora puede identificar los paneles antes de que el daño sea total",
        ],
    },
    "inverter_replacement": {
        "title": "Reconfiguración de Inversor",
        "the_opportunity": (
            "El inversor de {system_name} opera por debajo de la eficiencia óptima. "
            "Una actualización de firmware y ajuste de umbrales puede recuperar "
            "un {recovery_pct}% de producción — ${annual_gain:,.0f}/año en "
            "ingresos adicionales con un costo de implementación mínimo."
        ),
        "warning": (
            "El firmware del inversor no ha sido actualizado desde la instalación. "
            "El fabricante ha publicado actualizaciones con mejoras de eficiencia del 2-4%."
        ),
        "talking_points": [
            "Una actualización de firmware puede recuperar un 2-4% de producción — son ${annual_gain:,.0f}/año",
            "Este es el ROI más rápido de tu cartera — semanas, no meses",
            "Sin costo de equipo, solo experiencia técnica",
        ],
    },
    "ev_charger": {
        "title": "Carga EV",
        "the_opportunity": (
            "{system_name} tiene infraestructura de estacionamiento apta para carga EV. "
            "La financiación NEVI cubre hasta el 80% de los costos de instalación, "
            "lo que lo convierte en una oportunidad de ingresos de bajo riesgo con "
            "${annual_revenue:,.0f}/año de potencial con utilización moderada."
        ),
        "warning": (
            "Los fondos del programa NEVI se asignan por orden de llegada."
        ),
        "talking_points": [
            "NEVI cubre hasta el 80% de los costos de instalación",
            "Cada cargador L2 genera ingresos desde el primer día",
            "Los conductores de EV permanecen 45 minutos en el sitio — tráfico para los inquilinos",
        ],
    },
    "peak_shaving": {
        "title": "Reducción de Pico",
        "the_opportunity": (
            "{system_name} está en la tarifa GSP de LUMA con cargos de demanda de "
            "$8.10/kVA. La demanda pico actual genera ${monthly_charge:,.0f}/mes "
            "en cargos de demanda. El almacenamiento de batería puede reducir el pico "
            "en un {reduction_pct}%, ahorrando ${annual_saving:,.0f}/año."
        ),
        "warning": (
            "Los cargos de demanda de la tarifa GSP de LUMA están estructurados en 3 niveles. "
            "Reducir del Nivel 3 al Nivel 2 genera un ahorro del 40%."
        ),
        "talking_points": [
            "Los cargos de demanda representan el 35-45% de tu factura eléctrica",
            "Una batería puede reducir tu pico y bajarte a un nivel inferior",
            "Este ahorro es inmediato — desde la primera factura tras la instalación",
        ],
    },
    "vpp_monetization": {
        "title": "Monetización VPP",
        "the_opportunity": (
            "El sistema de almacenamiento de {system_name} cumple los requisitos técnicos "
            "para participar en el programa CBES de LUMA Energy. La monetización como recurso "
            "VPP genera ingresos adicionales sin inversión extra, con pagos vinculados "
            "a disponibilidad y eventos de despacho programados."
        ),
        "warning": (
            "El programa CBES tiene cupos de solicitud limitados — la inscripción temprana asegura la participación."
        ),
        "talking_points": [
            "Ingresos adicionales sin ninguna inversión nueva",
            "Los pagos están vinculados a disponibilidad y despacho — sin riesgo operativo",
            "El programa CBES está abierto ahora mismo a solicitudes",
        ],
    },
    "system_expansion": {
        "title": "Expansión del Sistema",
        "the_opportunity": (
            "{system_name} presenta capacidad disponible para ampliar su instalación de "
            "{kw_peak} kWp. El perfil energético del cliente es compatible con una expansión "
            "que maximizaría el aprovechamiento del recurso solar "
            "y mejoraría el retorno sobre la infraestructura existente."
        ),
        "warning": (
            "La ventana ITC al 30% vigente hasta 2032 hace que este sea el momento óptimo para ampliar."
        ),
        "talking_points": [
            "La expansión aprovecha la infraestructura de conexión existente — reducción de costos del 25%",
            "ITC al 30% cubre la ampliación igual que la instalación original",
            "Más producción solar, mayor ahorro y mejor payback",
        ],
    },
}


# === DEMAND CHARGE BLOCK (condicional dentro de battery_upgrade) ===

DEMAND_CHARGE_NARRATIVE = (
    "El sistema está en la tarifa GSP de LUMA con cargos de demanda de $8,10/kVA. "
    "La demanda pico actual de {peak_kva} kVA genera ${monthly_charge:,.0f}/mes "
    "solo en cargos de demanda. El almacenamiento de batería puede reducir la demanda pico "
    "en {reduction_kva} kVA, ahorrando ${annual_saving:,.0f}/año."
)


# === METHODOLOGY (estático para PR) ===

METHODOLOGY_PR = {
    "data_sources": [
        "Tarifas de LUMA Energy",
        "Regulaciones de la Oficina de Energía de Puerto Rico",
        "Directrices ITC federal (IRA 2022)",
        "Modelos de degradación fotovoltaica NREL (tropical)",
        "Especificaciones del programa CBES/VPP",
        "Auditoría Federal OIG — tasa de deficiencia del 57% en sistemas",
        "IEC 62804 — estándares de ensayo PID",
    ],
    "financial_assumptions": {
        "Tasa de Descuento": "8%",
        "Crédito ITC Federal": "30%",
        "Degradación Año 1": "5,5% (ajustado tropical)",
        "Degradación Años Siguientes": "1,2%/año",
        "Coste de Batería LFP": "$400/kWh (LFP)",
        "Escalación Tarifaria": "3,5%/año",
        "Vigencia ITC": "Hasta 2032 (posible reducción gradual)",
    },
    "glossary": {
        "ITC": "Crédito Fiscal por Inversión (Federal)",
        "LUMA": "LUMA Energy (operador de red, PR)",
        "CBES": "Almacenamiento de Energía en Batería Crítica",
        "GSS": "Servicio General Estándar (LUMA)",
        "GSP": "Servicio General Pico (LUMA)",
        "IVU": "Impuesto sobre Ventas y Uso",
        "PID": "Degradación Inducida por Potencial",
        "VPP": "Central Eléctrica Virtual",
        "NEVI": "Programa Nacional de Infraestructura para Vehículos Eléctricos",
        "LFP": "Fosfato de Hierro y Litio",
    },
}


# === RECOMMENDED ACTIONS (template structure) ===

ACTION_TEMPLATES = {
    "this_week": [
        {
            "action": "Contactar a {client_name} sobre {opp_type} (${value:,.0f} de valor, {prob}% probabilidad de cierre)",
            "type": "call",
        },
    ],
    "next_30_days": [
        {
            "action": "Preparar propuestas de {opp_type} para los {count} sistemas principales identificados",
            "type": "proposal",
        },
        {
            "action": "Enviar solicitudes al programa CBES para los sistemas elegibles",
            "type": "application",
        },
    ],
    "next_90_days": [
        {
            "action": "Desarrollar propuestas de carga EV para {count} ubicaciones con fondos NEVI",
            "type": "proposal",
        },
        {
            "action": "Completar evaluación de degradación tropical para sistemas de más de 3 años",
            "type": "assessment",
        },
    ],
}


# === CONTEXTUAL NARRATIVE BUILDER ===

def build_opportunity_narrative(
    opp_type: str,
    client_name: str,
    kwp: float,
    install_year: int,
    value_breakdown: dict = None,
    has_battery: bool = False,
    age_years: int = None,
) -> str:
    vb = value_breakdown or {}
    age = age_years if age_years is not None else (2026 - int(install_year))
    battery_kwh = vb.get("battery_kwh", round(kwp * 1.2))

    narratives = {
        "battery_upgrade": (
            f"{client_name} opera un sistema de {kwp:.0f} kWp instalado en {install_year} "
            f"sin almacenamiento energético. El perfil de generación actual exporta el 35% "
            f"de la producción a la red sin retorno económico. La integración de una batería LFP "
            f"de {battery_kwh:.0f} kWh permitiría capturar ese excedente, reducir la exposición "
            f"a tarifas GSS y generar ahorro neto desde el primer año."
        ),
        "industrial_battery": (
            f"{client_name} cuenta con una instalación de {kwp:.0f} kWp con potencial de "
            f"almacenamiento industrial no aprovechado. Un sistema LFP de {battery_kwh:.0f} kWh "
            f"optimizaría el autoconsumo, reduciría la dependencia de LUMA y habilitaría "
            f"participación en programas CBES con ingresos adicionales desde el primer año."
        ),
        "maintenance": (
            f"El sistema de {client_name} ({kwp:.0f} kWp, {install_year}) opera sin contrato "
            f"de mantenimiento activo. En condiciones tropicales de Puerto Rico, la ausencia de "
            f"revisiones periódicas genera una pérdida de rendimiento estimada del 20-25% anual. "
            f"Un contrato O&M estructurado protege la inversión y garantiza producción óptima."
        ),
        "inverter_replacement": (
            f"El inversor del sistema de {client_name} ({kwp:.0f} kWp) acumula {age} años de "
            f"operación en condiciones tropicales. La vida útil real en Puerto Rico es de 5-8 años "
            f"por temperatura y humedad. La sustitución preventiva evita paradas no planificadas "
            f"y pérdidas de producción superiores al coste de reemplazo."
        ),
        "system_expansion": (
            f"{client_name} presenta capacidad disponible para ampliar su instalación de "
            f"{kwp:.0f} kWp. El perfil energético del cliente es compatible con una expansión "
            f"de aproximadamente {kwp * 0.3:.0f} kWp adicionales, maximizando el aprovechamiento "
            f"del recurso solar y mejorando el retorno sobre la infraestructura existente."
        ),
        "ev_charger": (
            f"El perfil comercial de {client_name} es compatible con la instalación de "
            f"infraestructura de carga EV. La generación solar de {kwp:.0f} kWp puede absorber "
            f"la demanda de carga durante horas de máxima producción, con subsidio NEVI "
            f"aplicable hasta el 80% de la inversión en equipos."
        ),
        "peak_shaving": (
            f"{client_name} opera bajo tarifa GSP con cargo por demanda activo. La gestión "
            f"inteligente de pico mediante almacenamiento reduce el cargo de demanda mensual "
            f"hasta un 55%, con retorno sobre inversión inferior a 3 años en condiciones "
            f"estándar del mercado puertorriqueño."
        ),
        "vpp_monetization": (
            f"El sistema de almacenamiento de {client_name} reúne los requisitos técnicos "
            f"para participar en el programa CBES de LUMA Energy. La monetización como recurso "
            f"VPP genera ingresos adicionales sin inversión adicional, con pagos vinculados "
            f"a disponibilidad y eventos de despacho programados."
        ),
        "tropical_degradation": (
            f"El sistema de {client_name} acumula {age} años de operación en condiciones "
            f"tropicales. La degradación acelerada por temperatura, humedad y salitre reduce "
            f"la producción efectiva de forma progresiva. Una auditoría técnica y tratamiento "
            f"preventivo recuperan el rendimiento y extienden la vida útil del sistema."
        ),
    }

    return narratives.get(
        opp_type,
        f"{client_name} presenta oportunidades de optimización en su instalación de "
        f"{kwp:.0f} kWp. El análisis técnico identifica margen de mejora en eficiencia "
        f"y rentabilidad del sistema actual."
    )
