"""
Solvist API v4.0.0 — Production-Ready
Fixes implemented from security audit:
  #1 + #2 — Per-request JWT-scoped Supabase client (activates RLS auth.uid())
  #3       — Per-tenant rate limiting via slowapi
  #4       — Postgres-side date filter in revenue-at-risk
  #5       — Async audit logging middleware
"""

from __future__ import annotations

import os
import uuid
import logging
from datetime import datetime, timezone, timedelta
from typing import Annotated

from fastapi import FastAPI, Depends, HTTPException, Request, Response, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
from slowapi import Limiter
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware
from slowapi.util import get_remote_address
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("solvist")

# ─── Env ──────────────────────────────────────────────────────────────────────
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_ANON_KEY = os.getenv("SUPABASE_ANON_KEY") or os.getenv("SUPABASE_KEY")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

if not SUPABASE_URL or not SUPABASE_ANON_KEY or not SUPABASE_SERVICE_KEY:
    raise RuntimeError(
        "Missing env vars: SUPABASE_URL, SUPABASE_ANON_KEY (or SUPABASE_KEY), SUPABASE_SERVICE_KEY"
    )

# ─── Admin client (service role) — ONLY for JWT verification ──────────────────
# Never used for tenant data queries.
admin_client: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)


def scoped_client(jwt: str) -> Client:
    """
    FIX #1 + #2 — Create a per-request Supabase client with the user's JWT.
    This populates auth.uid() in RLS policies, making RLS enforcement real.
    A new client is cheap; the network connection is the actual cost.
    """
    client = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)
    client.postgrest.auth(jwt)
    return client


# ─── App & Rate Limiter ────────────────────────────────────────────────────────
# FIX #3 — Rate limiting keyed by user_id (set after auth) or IP as fallback.
def _tenant_key(request: Request) -> str:
    tenant: TenantContext | None = getattr(request.state, "tenant", None)
    return tenant.user_id if tenant else get_remote_address(request)


limiter = Limiter(key_func=_tenant_key, default_limits=["60/minute"])

app = FastAPI(title="Solvist API", version="4.0.0")

app.state.limiter = limiter
app.add_exception_handler(
    RateLimitExceeded,
    lambda req, exc: Response("Rate limit exceeded", status_code=429),
)
app.add_middleware(SlowAPIMiddleware)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[os.getenv("ALLOWED_ORIGIN", "*")],  # Set ALLOWED_ORIGIN in prod
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ─── FIX #5 — Audit Logging Middleware ────────────────────────────────────────
@app.middleware("http")
async def audit_middleware(request: Request, call_next):
    start = datetime.now(timezone.utc)
    response = await call_next(request)
    tenant: TenantContext | None = getattr(request.state, "tenant", None)

    if tenant:  # Only log authenticated requests
        duration_ms = int((datetime.now(timezone.utc) - start).total_seconds() * 1000)
        log_entry = {
            "id": str(uuid.uuid4()),
            "user_id": tenant.user_id,
            "company_id": tenant.company_id,
            "method": request.method,
            "endpoint": str(request.url.path),
            "status_code": response.status_code,
            "ip": request.headers.get("x-forwarded-for", get_remote_address(request)),
            "duration_ms": duration_ms,
            "created_at": start.isoformat(),
        }
        try:
            # Fire-and-forget via service role (audit log is not tenant-scoped)
            admin_client.table("audit_log").insert(log_entry).execute()
        except Exception as exc:
            logger.warning("Audit log write failed: %s", exc)

    return response


# ─── Auth & Tenant Resolution ──────────────────────────────────────────────────
bearer_scheme = HTTPBearer(auto_error=False)


class TenantContext(BaseModel):
    user_id: str
    company_id: str
    jwt: str  # Carried so route handlers can build a scoped_client

    class Config:
        frozen = True


async def get_tenant(
    request: Request,
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(bearer_scheme)],
) -> TenantContext:
    if credentials is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing Authorization header.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    token = credentials.credentials

    # Verify JWT via admin client (service role bypasses RLS for auth check only)
    try:
        auth_response = admin_client.auth.get_user(token)
        user_id = auth_response.user.id
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # Use a JWT-scoped client for the user lookup so RLS on `users` is enforced
    db = scoped_client(token)
    try:
        result = (
            db.table("users")
            .select("company_id")
            .eq("id", user_id)
            .single()
            .execute()
        )
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User not registered in the platform.",
        )

    company_id = (result.data or {}).get("company_id")
    if not company_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User has no associated company.",
        )

    tenant = TenantContext(user_id=user_id, company_id=company_id, jwt=token)

    # Attach to request.state so audit middleware can read it
    request.state.tenant = tenant

    return tenant


Tenant = Annotated[TenantContext, Depends(get_tenant)]


# ─── Root ──────────────────────────────────────────────────────────────────────
@app.get("/")
def root():
    return {"status": "Solvist API running", "version": "4.0.0"}


@app.get("/health")
def health():
    """Ping endpoint for uptime monitoring and load balancer checks."""
    try:
        admin_client.table("companies").select("id").limit(1).execute()
        return {"status": "ok", "db": "connected"}
    except Exception:
        raise HTTPException(status_code=503, detail="Database unreachable.")


# ─── Top Priority ──────────────────────────────────────────────────────────────
@app.get("/top-priority")
@limiter.limit("30/minute")
def top_priority(request: Request, tenant: Tenant):
    db = scoped_client(tenant.jwt)
    response = (
        db.table("clients")
        .select("*")
        .eq("company_id", tenant.company_id)
        .order("score", desc=True)
        .limit(20)
        .execute()
    )
    return response.data or []


# ─── Hot Leads ─────────────────────────────────────────────────────────────────
@app.get("/hot-leads")
@limiter.limit("30/minute")
def hot_leads(request: Request, tenant: Tenant):
    db = scoped_client(tenant.jwt)
    response = (
        db.table("clients")
        .select("*")
        .eq("company_id", tenant.company_id)
        .gte("score", 85)
        .gte("close_probability", 0.6)
        .neq("status", "Closed")
        .execute()
    )
    return response.data or []


# ─── Revenue At Risk ───────────────────────────────────────────────────────────
@app.get("/revenue-at-risk")
@limiter.limit("20/minute")
def revenue_at_risk(request: Request, tenant: Tenant):
    """
    FIX #4 — Date arithmetic pushed to Postgres via .lte("created_at", ...).
    No more in-memory filtering of potentially thousands of rows.
    """
    cutoff = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
    db = scoped_client(tenant.jwt)
    response = (
        db.table("clients")
        .select("*")
        .eq("company_id", tenant.company_id)
        .eq("status", "New")
        .gte("score", 80)
        .lte("created_at", cutoff)  # Postgres evaluates this — no Python loop
        .execute()
    )
    data = response.data or []

    # Annotate days_in_pipeline (cheap: only matching rows come back)
    now = datetime.now(timezone.utc)
    for c in data:
        try:
            created = datetime.fromisoformat(c["created_at"].replace("Z", "+00:00"))
            c["days_in_pipeline"] = (now - created).days
        except (KeyError, ValueError):
            c["days_in_pipeline"] = None

    return data


# ─── Commercial Dashboard ──────────────────────────────────────────────────────
@app.get("/commercial-dashboard")
@limiter.limit("30/minute")
def commercial_dashboard(request: Request, tenant: Tenant):
    db = scoped_client(tenant.jwt)
    data = (
        db.table("clients")
        .select("estimated_investment,expected_value,status,closed_value,score,close_probability")
        .eq("company_id", tenant.company_id)
        .execute()
        .data
    ) or []

    total_pipeline = sum((c.get("estimated_investment") or 0) for c in data)
    weighted_forecast = sum((c.get("expected_value") or 0) for c in data)
    closed_revenue = sum(
        (c.get("closed_value") or 0) for c in data if c.get("status") == "Closed"
    )
    hot_count = len([
        c for c in data
        if c.get("score", 0) >= 85
        and c.get("close_probability", 0) >= 0.6
        and c.get("status") != "Closed"
    ])

    return {
        "total_systems": len(data),
        "total_pipeline_value": round(total_pipeline, 2),
        "weighted_forecast": round(weighted_forecast, 2),
        "closed_revenue": round(closed_revenue, 2),
        "hot_leads_count": hot_count,
    }


# ─── Update Status ─────────────────────────────────────────────────────────────
class StatusUpdate(BaseModel):
    status: str
    closed_value: float | None = None


@app.post("/api/client/{client_id}/status")
@limiter.limit("30/minute")
def update_status(request: Request, client_id: str, payload: StatusUpdate, tenant: Tenant):
    db = scoped_client(tenant.jwt)

    # Ownership check — confirms client belongs to tenant before writing
    check = (
        db.table("clients")
        .select("id")
        .eq("id", client_id)
        .eq("company_id", tenant.company_id)
        .single()
        .execute()
    )

    if not check.data:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Client not found or access denied.",
        )

    update_data: dict = {"status": payload.status}

    if payload.status == "Closed":
        if payload.closed_value is None:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="Closed deals must include closed_value.",
            )
        update_data["closed_value"] = payload.closed_value

    db.table("clients").update(update_data).eq("id", client_id).execute()

    return {"message": "Status updated successfully"}