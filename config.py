import logging
import os
from pathlib import Path

from dotenv import load_dotenv

# Load environment variables from .env if it exists
# In production, Render will provide these directly as env vars.
load_dotenv()

BASE_DIR = Path(__file__).resolve().parent
logger = logging.getLogger("solvist.config")

# Core credentials
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_ANON_KEY = os.getenv("SUPABASE_ANON_KEY")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

# Extra services
STRIPE_SECRET_KEY = os.getenv("STRIPE_SECRET_KEY")
RESEND_API_KEY = os.getenv("RESEND_API_KEY")

# Env configuration
ENVIRONMENT = os.getenv("ENVIRONMENT", "development")

# Support multiple CORS sources
# ALLOWED_ORIGIN can be a comma-separated list of origins
_raw_origins = os.getenv("ALLOWED_ORIGIN") or os.getenv("CORS_ORIGINS") or "http://localhost:3000"
ALLOWED_ORIGINS = [origin.strip() for origin in _raw_origins.split(",") if origin.strip()]

# Optional regex for Vercel preview deployments (e.g. https://*.vercel.app)
CORS_ORIGIN_REGEX = os.getenv("CORS_ORIGIN_REGEX", None)

# Legacy single-origin alias (keep for backwards compat)
ALLOWED_ORIGIN = ALLOWED_ORIGINS[0] if ALLOWED_ORIGINS else "http://localhost:3000"

def _mask_key(value: str) -> str:
    if not value:
        return "missing"
    if len(value) <= 12:
        return "*" * len(value)
    return f"{value[:8]}...{value[-4:]}"


if not SUPABASE_URL or not SUPABASE_ANON_KEY or not SUPABASE_SERVICE_ROLE_KEY:
    # Fail fast if we can't talk to the DB
    raise RuntimeError(
        "Critical: Missing required Supabase configuration. "
        "Expected SUPABASE_URL, SUPABASE_ANON_KEY, SUPABASE_SERVICE_ROLE_KEY."
    )

logger.info(
    "Supabase env loaded (anon=%s, service_role=%s)",
    _mask_key(SUPABASE_ANON_KEY),
    _mask_key(SUPABASE_SERVICE_ROLE_KEY),
)
