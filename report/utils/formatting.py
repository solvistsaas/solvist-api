"""Formatting utilities for PDF report generation."""


def format_currency(value: float, market: str = "pr") -> str:
    """Format currency for PR market ($ prefix, comma thousands)."""
    if market == "pr":
        return f"${value:,.0f}"
    # TODO: EUR for ES market
    return f"${value:,.0f}"


def format_currency_k(value: float) -> str:
    """Format large numbers as $XXK."""
    if value >= 1_000_000:
        return f"${value/1_000_000:,.1f}M"
    return f"${value/1_000:,.0f}K"


def format_pct(value: float) -> str:
    """Format percentage (0.0-1.0 -> XX%)."""
    return f"{value * 100:.0f}%"


def format_number(value: float, decimals: int = 1) -> str:
    """Format number with comma thousands."""
    if decimals == 0:
        return f"{value:,.0f}"
    return f"{value:,.{decimals}f}"


def format_payback(years: float) -> str:
    """Format payback period."""
    if years < 1:
        return f"{years * 12:.0f} months"
    return f"{years:.1f} yrs"


def format_date(date_obj) -> str:
    """Format date for cover page."""
    return date_obj.strftime("%B %Y")
