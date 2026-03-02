from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from supabase import create_client
from pydantic import BaseModel
import os

app = FastAPI()

# --- CORS ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Supabase connection ---
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
    raise Exception("Supabase environment variables not set")

supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)


# =========================
# TOP 20 OPPORTUNITIES
# =========================
@app.get("/top20-simple")
def top20_simple():
    response = (
        supabase
        .table("clients")
        .select("*")
        .order("score", desc=True)
        .limit(20)
        .execute()
    )

    return response.data


# =========================
# EXECUTIVE SUMMARY
# =========================
@app.get("/executive-summary")
def executive_summary():
    response = supabase.table("clients").select("*").execute()
    data = response.data

    total_systems = len(data)
    total_potential = sum((c.get("estimated_investment") or 0) for c in data)
    total_expected = sum((c.get("expected_value") or 0) for c in data)
    total_closed = sum(
        (c.get("closed_value") or 0)
        for c in data
        if c.get("status") == "Closed"
    )

    return {
        "total_systems": total_systems,
        "total_potential_revenue": round(total_potential, 2),
        "total_expected_value": round(total_expected, 2),
        "total_closed_revenue": round(total_closed, 2),
    }


# =========================
# UPDATE CLIENT STATUS
# =========================
class StatusUpdate(BaseModel):
    status: str
    closed_value: float | None = None


@app.post("/api/client/{client_id}/status")
def update_status(client_id: str, payload: StatusUpdate):

    update_data = {
        "status": payload.status
    }

    if payload.status == "Closed" and payload.closed_value is not None:
        update_data["closed_value"] = payload.closed_value

    supabase.table("clients") \
        .update(update_data) \
        .eq("id", client_id) \
        .execute()

    return {"message": "Status updated successfully"}