from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from supabase import create_client
from pydantic import BaseModel
from datetime import datetime, timezone
import os

app = FastAPI()

# =========================
# CORS
# =========================
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# =========================
# SUPABASE CONNECTION
# =========================
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
    raise Exception("Supabase environment variables not set")

supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)


# =========================
# ROOT
# =========================
@app.get("/")
def root():
    return {"status": "Solvist API running"}


# =========================
# HELPER: PRIORITY SCORE
# =========================
def calculate_priority(client, max_expected):

    expected_norm = 0
    if max_expected > 0:
        expected_norm = client.get("expected_value", 0) / max_expected

    priority = (
        (client.get("score", 0) * 0.5)
        + (client.get("close_probability", 0) * 100 * 0.3)
        + (expected_norm * 100 * 0.2)
    )

    return round(priority, 2)


# =========================
# SMART PRIORITY RANKING
# =========================
@app.get("/top-priority")
def top_priority():

    response = supabase.table("clients").select("*").execute()
    data = response.data

    if not data:
        return []

    max_expected = max((c.get("expected_value") or 0) for c in data)

    for client in data:
        client["priority_score"] = calculate_priority(client, max_expected)

    ranked = sorted(data, key=lambda x: x["priority_score"], reverse=True)

    return ranked[:20]


# =========================
# HOT LEADS
# =========================
@app.get("/hot-leads")
def hot_leads():

    response = supabase.table("clients").select("*").execute()
    data = response.data

    hot = [
        c for c in data
        if c.get("score", 0) >= 85
        and c.get("close_probability", 0) >= 0.6
        and c.get("status") != "Closed"
    ]

    return hot


# =========================
# REVENUE AT RISK
# =========================
@app.get("/revenue-at-risk")
def revenue_at_risk():

    response = supabase.table("clients").select("*").execute()
    data = response.data

    now = datetime.now(timezone.utc)

    at_risk = []

    for c in data:
        created = datetime.fromisoformat(c["created_at"].replace("Z", "+00:00"))
        days_old = (now - created).days

        if (
            c.get("score", 0) >= 80
            and c.get("status") == "New"
            and days_old >= 7
        ):
            c["days_in_pipeline"] = days_old
            at_risk.append(c)

    return at_risk


# =========================
# COMMERCIAL DASHBOARD
# =========================
@app.get("/commercial-dashboard")
def commercial_dashboard():

    response = supabase.table("clients").select("*").execute()
    data = response.data

    total_systems = len(data)
    total_pipeline = sum((c.get("estimated_investment") or 0) for c in data)
    weighted_forecast = sum((c.get("expected_value") or 0) for c in data)
    closed_revenue = sum(
        (c.get("closed_value") or 0)
        for c in data
        if c.get("status") == "Closed"
    )

    hot_count = len([
        c for c in data
        if c.get("score", 0) >= 85
        and c.get("close_probability", 0) >= 0.6
        and c.get("status") != "Closed"
    ])

    return {
        "total_systems": total_systems,
        "total_pipeline_value": round(total_pipeline, 2),
        "weighted_forecast": round(weighted_forecast, 2),
        "closed_revenue": round(closed_revenue, 2),
        "hot_leads_count": hot_count,
    }


# =========================
# UPDATE STATUS (VALIDATION)
# =========================
class StatusUpdate(BaseModel):
    status: str
    closed_value: float | None = None


@app.post("/api/client/{client_id}/status")
def update_status(client_id: str, payload: StatusUpdate):

    update_data = {
        "status": payload.status
    }

    if payload.status == "Closed":
        if payload.closed_value is None:
            raise Exception("Closed deals must include closed_value")
        update_data["closed_value"] = payload.closed_value

    supabase.table("clients") \
        .update(update_data) \
        .eq("id", client_id) \
        .execute()

    return {"message": "Status updated successfully"}