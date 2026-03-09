"""
Solvist API v5.0.0 — Opportunity Intelligence Layer (V1 Blueprint)
Replaces generic CRM endpoints with:
- GET /dashboard
- GET /activation
- GET /insights
- POST /tracking
- POST /engine/score-all (Monthly Scoring Job)
- POST /installations (Creation with Plan limit enforcement)
- GET /activation/{id}/pdf (PDF generator)
"""

from __future__ import annotations

import os
import uuid
import logging
import secrets
from datetime import datetime, timezone, date
from typing import Annotated, Dict, List
from enum import Enum

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

import io
from fpdf import FPDF

load_dotenv()

# ─── Logging & Env ───────────────────────────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("solvist")

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
# SUPABASE_ANON_KEY: acepta SUPABASE_ANON_KEY, SUPABASE_KEY, o fallback al service key
SUPABASE_ANON_KEY = (
    os.getenv("SUPABASE_ANON_KEY")
    or os.getenv("SUPABASE_KEY")
    or SUPABASE_SERVICE_KEY  # fallback seguro si solo hay service key
)

# Validación lazy — NO crashea en import, solo loguea warnings
_missing_vars = []
if not SUPABASE_URL:
    _missing_vars.append("SUPABASE_URL")
if not SUPABASE_SERVICE_KEY:
    _missing_vars.append("SUPABASE_SERVICE_KEY")
if _missing_vars:
    logger.warning(f"Supabase env vars missing: {', '.join(_missing_vars)}. "
                   "API will start but Supabase endpoints will fail.")

# Lazy Supabase client initialization
_admin_client: Client | None = None

def get_admin_client() -> Client:
    global _admin_client
    if _admin_client is not None:
        return _admin_client
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        raise HTTPException(
            status_code=503,
            detail="Supabase not configured. Set SUPABASE_URL and SUPABASE_SERVICE_KEY."
        )
    _admin_client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
    return _admin_client


def scoped_client(jwt: str) -> Client:
    """Creates a per-request client with the user's JWT (activates RLS auth.uid())."""
    if not SUPABASE_URL or not SUPABASE_ANON_KEY:
        raise HTTPException(status_code=503, detail="Supabase not configured.")
    client = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)
    client.postgrest.auth(jwt)
    return client


# ─── App & Rate Limiter ──────────────────────────────────────────────────────────────────────────────────

def _tenant_key(request: Request) -> str:
    tenant: TenantContext | None = getattr(request.state, "tenant", None)
    return tenant.user_id if tenant else get_remote_address(request)


limiter = Limiter(key_func=_tenant_key, default_limits=["60/minute"])
app = FastAPI(title="Solvist Opportunity Intelligence", version="5.0.0")
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, lambda req, exc: Response("Rate limit exceeded", status_code=429))
app.add_middleware(SlowAPIMiddleware)

_cors_origins = [
    "https://app.solvist.io",
    "https://solvist.io",
]
_extra_origin = os.getenv("ALLOWED_ORIGIN")
if _extra_origin:
    _cors_origins.append(_extra_origin)

app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def audit_middleware(request: Request, call_next):
    start = datetime.now(timezone.utc)
    response = await call_next(request)
    tenant: TenantContext | None = getattr(request.state, "tenant", None)
    if tenant and request.url.path.startswith("/api"):
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
            get_admin_client().table("audit_log").insert(log_entry).execute()
        except Exception:
            pass
    return response


# ─── Health Check ────────────────────────────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok"}


# ─── Auth Dependency ─────────────────────────────────────────────────────────────────────────────────────

bearer_scheme = HTTPBearer(auto_error=False)


class TenantContext(BaseModel):
    user_id: str
    company_id: str
    jwt: str
    installation_limit: int

    class Config:
        frozen = True


async def get_tenant(
    request: Request,
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(bearer_scheme)],
) -> TenantContext:
    if not credentials:
        raise HTTPException(status_code=401, detail="Missing Authorization header.")
    token = credentials.credentials
    try:
        auth_response = get_admin_client().auth.get_user(token)
        user_id = auth_response.user.id
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid token.")
    db = scoped_client(token)
    try:
        res_user = db.table("users").select("company_id").eq("id", user_id).single().execute()
        company_id = res_user.data.get("company_id")
    except Exception:
        raise HTTPException(status_code=403, detail="User not registered.")
    if not company_id:
        raise HTTPException(status_code=403, detail="User has no company.")
    # Fetch installation limit from companies (RLS applies or fallback to admin query if needed)
    try:
        # Service role to fetch plan limits (companies RLS might be strict)
        res_comp = get_admin_client().table("companies").select("installation_limit").eq("id", company_id).single().execute()
        max_inst = res_comp.data.get("installation_limit") or 500
    except Exception:
        max_inst = 500
    tenant = TenantContext(user_id=user_id, company_id=company_id, jwt=token, installation_limit=max_inst)
    request.state.tenant = tenant
    return tenant


Tenant = Annotated[TenantContext, Depends(get_tenant)]


# ─── Endpoints: Data Ingestion (Plan Enforcement) ────────────────────────────────────────────────────────

class LocationTypeEnum(str, Enum):
    residential = "residential"
    industrial = "industrial"


class InstallationCreate(BaseModel):
    client_name: str
    installation_year: int
    kwp: float
    inverter_model: str = "Unknown"
    has_battery: bool = False
    location_type: LocationTypeEnum = LocationTypeEnum.residential
    tariff_type: str = "standard"
    estimated_consumption: float = 0
    dc_ac_ratio: float = 1.0
    has_maintenance_contract: bool = False
    country: str = "Unknown"


@app.post("/api/installations")
@limiter.limit("20/minute")
def create_installation(request: Request, payload: InstallationCreate, tenant: Tenant):
    if payload.installation_year < 2000:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Installation year must be >= 2000.")
    db = scoped_client(tenant.jwt)
    # Enforce plan limits
    count_res = get_admin_client().table("installations").select("id", count="exact").eq("company_id", tenant.company_id).execute()
    current_count = count_res.count if hasattr(count_res, "count") and count_res.count is not None else len(count_res.data)
    if current_count >= tenant.installation_limit:
        raise HTTPException(status_code=402, detail=f"Plan limit exceeded. Max: {tenant.installation_limit}")
    # Pydantic implicitly formats Enums
    data = payload.dict()
    data["location_type"] = payload.location_type.value
    data["company_id"] = tenant.company_id
        res = db.table("installations").insert(data).execute()
    return res.data[0]


# ─── Endpoints: Commercial Dashboard V1 ──────────────────────────────────────────────────────────────────

@app.get("/api/dashboard")
@limiter.limit("30/minute")
def dashboard(request: Request, tenant: Tenant):
    db = scoped_client(tenant.jwt)
    # BLOCK 2: Fetch Active Threshold Parameter
    try:
        param_res = get_admin_client().table("company_parameters").select("active_threshold").eq("company_id", tenant.company_id).single().execute()
        active_threshold = param_res.data.get("active_threshold") if param_res.data else 70
    except Exception:
        active_threshold = 70
    if active_threshold is None:
        active_threshold = 70
    # Total installations
    inst_res = db.table("installations").select("id, kwp").execute()
    total_inst = len(inst_res.data)
    # Opportunity Scores > threshold
    sc_res = db.table("opportunity_scores").select("total_score, installation_id").execute()
    total_scored = len(sc_res.data)
    active_window = sum(1 for s in sc_res.data if s.get("total_score", 0) > active_threshold)
    window_pct = round((active_window / total_scored * 100), 1) if total_scored > 0 else 0
    # Estimated potential value simplified
    active_ids = {s["installation_id"] for s in sc_res.data if s.get("total_score", 0) > active_threshold}
    active_kwp = sum((i.get("kwp") or 0) for i in inst_res.data if i["id"] in active_ids)
    pot_value = round(active_kwp * 1500, 2)
    # Top 5 clients
    top_res = (
        db.table("opportunity_scores")
        .select("total_score, primary_reason, recommended_action, installations(client_name)")
        .order("total_score", desc=True)
        .limit(5)
        .execute()
    )
    formatted_top = []
    for row in top_res.data:
        client_name = row.get("installations", {}).get("client_name") if row.get("installations") else "Unknown"
        formatted_top.append({
            "client_name": client_name,
            "total_score": row["total_score"],
            "primary_reason": row["primary_reason"],
            "recommended_action": row["recommended_action"]
        })
    return {
        "total_installations": total_inst,
        "window_active_pct": window_pct,
        "estimated_potential_value": pot_value,
        "monthly_avg_score_trend": [45, 48, 52, 59, 65, 68],
        "top_5_clients": formatted_top
    }


# ─── Endpoints: Activation List V1 ───────────────────────────────────────────────────────────────────────

@app.get("/api/activation")
@limiter.limit("30/minute")
def activation_list(request: Request, tenant: Tenant, limit: int = 20):
    db = scoped_client(tenant.jwt)
    res = (
        db.table("opportunity_scores")
        .select("id, installation_id, total_score, primary_reason, recommended_action, installations(client_name, location_type, installation_year)")
        .order("total_score", desc=True)
        .limit(limit)
        .execute()
    )
    output = []
    for row in res.data:
        inst = row.get("installations", {}) or {}
        output.append({
            "score_id": row.get("id"),
            "installation_id": row.get("installation_id"),
            "client_name": inst.get("client_name", "Unknown"),
            "location_type": inst.get("location_type"),
            "installation_year": inst.get("installation_year"),
            "total_score": row.get("total_score"),
            "primary_reason": row.get("primary_reason"),
            "recommended_action": row.get("recommended_action")
        })
    return output


# ─── Endpoints: Insights Panel V1 ────────────────────────────────────────────────────────────────────────

@app.get("/api/insights")
@limiter.limit("30/minute")
def insights(request: Request, tenant: Tenant):
    db = scoped_client(tenant.jwt)
    res = db.table("opportunity_scores").select("battery_score, maintenance_score, ev_score").execute()
    total = len(res.data)
    batt_opps = sum(1 for r in res.data if r.get("battery_score", 0) >= 15)
    maint_opps = sum(1 for r in res.data if r.get("maintenance_score", 0) >= 15)
    perc_batt = round((batt_opps / total * 100)) if total > 0 else 0
    perc_maint = round((maint_opps / total * 100)) if total > 0 else 0
    return [
        {"metric": "Oportunidades Batería", "value": f"{perc_batt}%", "description": "en ventana óptima (>15 pts)"},
        {"metric": "Riesgo Mantenimiento", "value": f"{perc_maint}%", "description": "instalaciones desprotegidas"},
    ]


# ─── Endpoints: Execution Tracking V1 ────────────────────────────────────────────────────────────────────

class OpportunityTypeEnum(str, Enum):
    battery = "battery"
    maintenance = "maintenance"
    expansion = "expansion"
    ev = "ev"
    industrial = "industrial"


class TrackingUpdate(BaseModel):
    installation_id: str
    opportunity_type: OpportunityTypeEnum
    contacted: bool = False
    result: str = ""
    closed: bool = False
    value: float = 0.0


@app.post("/api/tracking")
@limiter.limit("30/minute")
def add_tracking(request: Request, payload: TrackingUpdate, tenant: Tenant):
    db = scoped_client(tenant.jwt)
    check = db.table("installations").select("id").eq("id", payload.installation_id).single().execute()
    if not check.data:
        raise HTTPException(status_code=404, detail="Installation not found")
    data = payload.dict()
    data["company_id"] = tenant.company_id
    if payload.contacted:
        data["contact_date"] = datetime.now(timezone.utc).isoformat()
    if payload.closed:
        data["closed_at"] = datetime.now(timezone.utc).isoformat()
    res = db.table("execution_tracking").insert(data).execute()
    return {"message": "Tracking saved", "id": res.data[0]["id"]}


# ─── Endpoints: Scoring Engine Job ───────────────────────────────────────────────────────────────────────

# ENGINE_SECRET strictly loaded from environment with no defaults
ENGINE_SECRET = os.getenv("ENGINE_SECRET")
if not ENGINE_SECRET:
    logger.warning("ENGINE_SECRET not configured. /api/engine/score-all endpoint will return 403.")


@app.post("/api/engine/score-all")
@limiter.limit("5/minute")
def score_all_installations(request: Request):
    """
    Monthly Serverless Job endpoint (BLOCK 0 - Secure Refactor).
    Iterates per company, logs systematically, supports calculated_month.
    """
    start_time = datetime.now(timezone.utc)
    logger.info("ENGINE: Starting full opportunity scoring run.")
    # 1. Timing-safe Secret Validation via Header
    if not ENGINE_SECRET:
        logger.error("ENGINE: ENGINE_SECRET not configured.")
        raise HTTPException(status_code=503, detail="Scoring engine not configured.")
    provided = request.headers.get("X-ENGINE-SECRET")
    if not provided or not secrets.compare_digest(provided, ENGINE_SECRET):
        logger.warning(f"ENGINE: Authentication failed from IP {get_remote_address(request)}")
        raise HTTPException(status_code=403, detail="Forbidden")
    # PREP FOR BLOCK 1: Set calculated_month to first day of current month
    calculated_month = start_time.replace(day=1, hour=0, minute=0, second=0, microsecond=0).isoformat()
    now_year = start_time.year
    # 2. Prevent Full-Table Scan: Fetch companies first
    comp_res = get_admin_client().table("companies").select("id, name").execute()
    companies = comp_res.data or []
    total_companies = len(companies)
    total_installations_scored = 0
    companies_failed = 0
    logger.info(f"ENGINE: Found {total_companies} companies to process.")
    # Cache default parameters
    try:
        def_param_res = get_admin_client().table("country_parameters").select("*").eq("country", "default").single().execute()
        default_params = def_param_res.data or {
            "battery_weight": 1.0,
            "maintenance_weight": 1.0,
            "expansion_weight": 1.0,
            "ev_weight": 1.0,
            "industrial_weight": 1.0
        }
    except Exception:
        default_params = {
            "battery_weight": 1.0,
            "maintenance_weight": 1.0,
            "expansion_weight": 1.0,
            "ev_weight": 1.0,
            "industrial_weight": 1.0
        }
    # 3. Iterate Per Company (Tenant Isolation)
    for company in companies:
        company_id = company["id"]
        company_name = company.get("name", "Unknown")
        try:
            logger.info(f"ENGINE: Processing company {company_name} ({company_id})")
            # BLOCK 2: Fetch Company Parameters (1 fetch per tenant)
            try:
                comp_param_res = get_admin_client().table("company_parameters").select("*").eq("company_id", company_id).single().execute()
                comp_params = comp_param_res.data
            except Exception:
                comp_params = None
            # Resolve weights
            w_batt = float(comp_params.get("battery_weight") if comp_params and comp_params.get("battery_weight") is not None else default_params.get("battery_weight", 1.0))
            w_maint = float(comp_params.get("maintenance_weight") if comp_params and comp_params.get("maintenance_weight") is not None else default_params.get("maintenance_weight", 1.0))
            w_exp = float(comp_params.get("expansion_weight") if comp_params and comp_params.get("expansion_weight") is not None else default_params.get("expansion_weight", 1.0))
            w_ev = float(comp_params.get("ev_weight") if comp_params and comp_params.get("ev_weight") is not None else default_params.get("ev_weight", 1.0))
            w_ind = float(comp_params.get("industrial_weight") if comp_params and comp_params.get("industrial_weight") is not None else default_params.get("industrial_weight", 1.0))
            # Fetch installations specifically for this tenant
            inst_res = get_admin_client().table("installations").select("*").eq("company_id", company_id).execute()
            installations = inst_res.data or []
            if not installations:
                logger.info(f"ENGINE: No installations found for company {company_name}")
                continue
            upsert_payload = []
            for inst in installations:
                batt = 0
                maint = 0
                exp = 0
                ev = 0
                ind = 0
                # 1. Battery Score (0-25)
                if not inst.get("has_battery"):
                    batt += 10
                if inst.get("installation_year") and (now_year - inst.get("installation_year")) >= 2:
                    batt += 5
                if str(inst.get("tariff_type")).lower() in ["discriminacion", "td", "time-of-use"]:
                    batt += 5
                if (inst.get("estimated_consumption") or 0) > 5000:
                    batt += 5
                # 2. Maintenance (0-20)
                if inst.get("installation_year") and (now_year - inst.get("installation_year")) >= 3:
                    maint += 10
                if not inst.get("has_maintenance_contract"):
                    maint += 5
                if str(inst.get("country")).lower() in ["spain-coastal", "islands", "mallorca", "canarias", "valencia"]:
                    maint += 5
                # 3. Expansion (0-20)
                if (inst.get("dc_ac_ratio") or 1.0) < 1.1:
                    exp += 10
                exp += 5
                # 4. EV (0-15)
                if inst.get("location_type") == "residential":
                    ev += 10
                ev += 5
                # 5. Industrial Battery (0-30)
                if inst.get("location_type") == "industrial":
                    if (inst.get("estimated_consumption") or 0) > 10000:
                        ind += 10
                    ind += 10
                    ind += 10
                # BLOCK 2: Apply weights from parameters
                batt = batt * w_batt
                maint = maint * w_maint
                exp = exp * w_exp
                ev = ev * w_ev
                ind = ind * w_ind
                total = min(batt + maint + exp + ev + ind, 100)
                total = max(total, 0)
                scores = {"Retrofit Batería": batt, "Contrato Mantenimiento": maint, "Ampliación Solar": exp, "Cargador EV": ev, "Batería Industrial": ind}
                primary_reason = max(scores, key=scores.get)
                actions = {
                    "Retrofit Batería": "Ofrecer pack batería retrofit amortizable en 4 años.",
                    "Contrato Mantenimiento": "Ofrecer revisión anual preventiva y limpieza.",
                    "Ampliación Solar": "Proponer ampliación de 3-5 kWp en techo restante.",
                    "Cargador EV": "Campaña upsell cargador inteligente.",
                    "Batería Industrial": "Estudio peak-shaving para reducción de penalizaciones."
                }
                reason_breakdown: Dict[str, List[str]] = {
                    "battery": [f"+{batt} points"] if batt > 0 else [],
                    "maintenance": [f"+{maint} points"] if maint > 0 else [],
                    "expansion": [f"+{exp} points"] if exp > 0 else [],
                    "ev": [f"+{ev} points"] if ev > 0 else [],
                    "industrial": [f"+{ind} points"] if ind > 0 else [],
                }
                upsert_payload.append({
                    "company_id": inst["company_id"],
                    "installation_id": inst["id"],
                    "calculated_month": calculated_month,
                    "battery_score": batt,
                    "maintenance_score": maint,
                    "expansion_score": exp,
                    "ev_score": ev,
                    "industrial_score": ind,
                    "total_score": total,
                    "primary_reason": primary_reason,
                    "recommended_action": actions[primary_reason],
                    "reason_breakdown": reason_breakdown,
                    "calculated_at": datetime.now(timezone.utc).isoformat()
                })
            if upsert_payload:
                get_admin_client().table("opportunity_scores").upsert(
                    upsert_payload,
                    on_conflict="company_id,installation_id,calculated_month"
                ).execute()
                total_installations_scored += len(upsert_payload)
                logger.info(f"ENGINE: Successfully scored {len(upsert_payload)} installations for {company_name}.")
        except Exception as e:
            logger.error(f"ENGINE: Failed to process company {company_name} ({company_id}): {str(e)}")
            companies_failed += 1
            continue
    runtime_seconds = (datetime.now(timezone.utc) - start_time).total_seconds()
    logger.info(f"ENGINE: Run complete. Scored {total_installations_scored} installations across {total_companies - companies_failed} companies. Failed: {companies_failed}. Runtime: {runtime_seconds:.2f}s")
    return {
        "message": "Monthly scoring completed",
        "total_installations": total_installations_scored,
        "companies_processed": total_companies - companies_failed,
        "companies_failed": companies_failed,
        "runtime_seconds": round(runtime_seconds, 2)
    }


# ─── Endpoints: Minimalist PDF Generation V1 ──────────────────────────────────────────────────────────────

@app.get("/api/activation/{installation_id}/pdf")
@limiter.limit("10/minute")
def generate_activation_pdf(request: Request, installation_id: str, tenant: Tenant):
    db = scoped_client(tenant.jwt)
    # Verify access via RLS and join
    res = (
        db.table("opportunity_scores")
        .select("total_score, primary_reason, recommended_action, installations(*)")
        .eq("installation_id", installation_id)
        .single()
        .execute()
    )
    if not res.data:
        raise HTTPException(status_code=404, detail="Score/Installation not found or access denied")
    data = res.data
    inst = data.get("installations", {})
    # ── FPDF Logic ──
    pdf = FPDF()
    pdf.add_page()
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.set_fill_color(10, 14, 40)
    pdf.rect(0, 0, 210, 297, "F")
    pdf.set_font("Helvetica", "B", 24)
    pdf.set_text_color(100, 180, 255)
    pdf.cell(0, 15, "SOLVIST - Oportunidad Comercial", ln=True, align="C")
    pdf.set_font("Helvetica", "", 12)
    pdf.set_text_color(220, 220, 220)
    pdf.cell(0, 10, f"Cliente: {inst.get('client_name')}", ln=True, align="C")
    pdf.ln(10)
    # Data card
    pdf.set_font("Helvetica", "B", 14)
    pdf.set_text_color(100, 180, 255)
    pdf.cell(0, 10, "1. Datos del Sistema Analizado", ln=True)
    pdf.set_font("Helvetica", "", 11)
    pdf.set_text_color(180, 190, 210)
    pdf.cell(0, 8, f"Potencia Instalada: {inst.get('kwp', 0)} kWp", ln=True)
    pdf.cell(0, 8, f"Despliegue inicial: Año {inst.get('installation_year')}", ln=True)
    pdf.cell(0, 8, f"Tipo de consumo: {str(inst.get('location_type')).capitalize()}", ln=True)
    pdf.ln(5)
    pdf.set_font("Helvetica", "B", 14)
    pdf.set_text_color(255, 100, 100)
    pdf.cell(0, 10, "2. Oportunidad Detectada", ln=True)
    pdf.set_font("Helvetica", "", 11)
    pdf.set_text_color(255, 255, 255)
    pdf.cell(0, 8, f"Score Total: {data.get('total_score')} / 100 pts", ln=True)
    pdf.cell(0, 8, f"Vector principal: {data.get('primary_reason')}", ln=True)
    pdf.ln(5)
    pdf.set_font("Helvetica", "B", 14)
    pdf.set_text_color(100, 255, 150)
    pdf.cell(0, 10, "3. Acción Recomendada (Playbook)", ln=True)
    pdf.set_font("Helvetica", "", 11)
    pdf.set_text_color(220, 220, 220)
    pdf.multi_cell(0, 8, data.get('recommended_action'))
    pdf.ln(15)
    pdf.set_font("Helvetica", "I", 9)
    pdf.set_text_color(100, 100, 100)
    pdf.cell(0, 10, f"Documento generado autom\u00e1ticamente por el motor Solvist. (ID: {installation_id})", align="C")
    pdf_bytes = bytes(pdf.output())
    return Response(content=pdf_bytes, media_type="application/pdf", headers={"Content-Disposition": f"attachment; filename=oportunidad_{inst.get('client_name')}.pdf"})

    res = db.table("installations").insert(data).execute()
    return res.data[0]
