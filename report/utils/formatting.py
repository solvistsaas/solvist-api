"""Formatting utilities for PDF report generation."""

from datetime import datetime

MESES_ES = {
    1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril", 5: "Mayo", 6: "Junio",
    7: "Julio", 8: "Agosto", 9: "Septiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre"
}


def format_currency(value, symbol: str = "$") -> str:
    """Format currency value with thousands separator."""
    if value is None:
        return "N/D"
    v = float(value)
    if v >= 1000:
        return f"{symbol}{v:,.0f}"
    return f"{symbol}{v:.0f}"


def format_currency_k(value: float) -> str:
    """Format large numbers as $XXK."""
    if value >= 1_000_000:
        return f"${value/1_000_000:,.1f}M"
    return f"${value/1_000:,.0f}K"


def format_pct(value) -> str:
    """Format percentage."""
    if value is None:
        return "N/D"
    v = float(value)
    if v <= 1.0:
        v = v * 100
    return f"{v:.0f}%"


def format_number(value: float, decimals: int = 1) -> str:
    """Format number with comma thousands."""
    if decimals == 0:
        return f"{value:,.0f}"
    return f"{value:,.{decimals}f}"


def format_payback(years) -> str:
    """Format payback period in years or months."""
    if years is None or years <= 0:
        return "N/D"
    if years < 1:
        months = round(float(years) * 12)
        return f"{months} meses"
    return f"{float(years):.1f} años"


def format_roi(net_investment, annual_savings) -> str:
    """Format ROI percentage, capped at 'Ver detalle' if > 300%."""
    try:
        if not net_investment or float(net_investment) <= 0:
            return "N/D"
        roi = (float(annual_savings) / float(net_investment)) * 100
        if roi > 300:
            return "Ver detalle"
        return f"{roi:.0f}%"
    except (TypeError, ZeroDivisionError):
        return "N/D"


def format_date(date_obj) -> str:
    """Format date for cover page."""
    if date_obj is None:
        date_obj = datetime.now()
    return f"{MESES_ES[date_obj.month]} {date_obj.year}"
