from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd

app = FastAPI()

# --- CORS ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

CURRENT_YEAR = 2026

# --- Load data ---
df = pd.read_csv("demo_clients.csv")

df["years_since_install"] = CURRENT_YEAR - df["install_year"]
df["years_since_contact"] = CURRENT_YEAR - df["last_contact_year"]

# --- SCORING LOGIC ---

def score_row(row):
    battery_score = 0
    upgrade_score = 0
    maintenance_score = 0

    # Battery
    if not row["has_battery"]:
        battery_score += 20
    if row["years_since_install"] >= 5:
        battery_score += 15
    if row["location_type"] == "coastal":
        battery_score += 20

    # Upgrade
    if row["system_kw"] < 6:
        upgrade_score += 20
    if row["years_since_install"] >= 3:
        upgrade_score += 20

    # Maintenance
    if row["years_since_contact"] >= 4:
        maintenance_score += 25
    if row["client_type"] == "commercial":
        maintenance_score += 20

    total_score = max(battery_score, upgrade_score, maintenance_score)

    if total_score == battery_score:
        main_opportunity = "Battery"
        estimated_value = 8000
        close_probability = 0.35 if total_score >= 60 else 0.25
        effort_score = 4 if total_score >= 60 else 3

    elif total_score == upgrade_score:
        main_opportunity = "Upgrade"
        estimated_value = 6000
        close_probability = 0.30
        effort_score = 3

    else:
        main_opportunity = "Maintenance"
        estimated_value = 900
        close_probability = 0.05
        effort_score = 5

    expected_value = estimated_value * close_probability
    priority_score = expected_value / effort_score

    return pd.Series([
        main_opportunity,
        total_score,
        estimated_value,
        close_probability,
        expected_value,
        effort_score,
        priority_score
    ])

df[[
    "main_opportunity",
    "total_score",
    "estimated_value",
    "close_probability",
    "expected_value",
    "effort_score",
    "priority_score"
]] = df.apply(score_row, axis=1)

# --- ENDPOINTS ---

@app.get("/top20-simple")
def top20_simple():
    result = df.sort_values(by="priority_score", ascending=False).head(20)
    return result[[
        "client_name",
        "main_opportunity",
        "total_score",
        "expected_value",
        "priority_score"
    ]].to_dict(orient="records")


@app.get("/executive-summary")
def executive_summary():
    total_clients = len(df)
    total_pipeline_value = df["estimated_value"].sum()
    total_expected_value = df["expected_value"].sum()
    high_priority_clients = len(df[df["priority_score"] >= 600])
    top_opportunity_type = df["main_opportunity"].value_counts().idxmax()

    return {
        "total_clients": int(total_clients),
        "total_pipeline_value": float(total_pipeline_value),
        "total_expected_value": float(total_expected_value),
        "high_priority_clients": int(high_priority_clients),
        "top_opportunity_type": top_opportunity_type
    }