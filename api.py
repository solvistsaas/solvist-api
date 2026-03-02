from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
from datetime import datetime

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

# --- Load CSV ---
df = pd.read_csv("demo_clients.csv")

# --- Derived fields ---
df["years_since_install"] = CURRENT_YEAR - df["install_year"]
df["years_since_contact"] = CURRENT_YEAR - df["last_contact_year"]

# --- SCORING ENGINE V1 ---
def score_row(row):
    score = 0
    drivers = []

    # 1️⃣ AGE SCORE (max 35 pts)
    if 2 <= row["years_since_install"] <= 5:
        score += 35
        drivers.append("Optimal upgrade age window")
    elif row["years_since_install"] > 5:
        score += 20
        drivers.append("System mature for expansion")

    # 2️⃣ CONTACT GAP SCORE (max 35 pts)
    if row["years_since_contact"] >= 2:
        score += 35
        drivers.append(f"{row['years_since_contact']} years without contact")
    elif row["years_since_contact"] >= 1:
        score += 20
        drivers.append("Commercial inactivity detected")

    # 3️⃣ SYSTEM SIZE SCORE (max 15 pts)
    if row["system_kw"] >= 6:
        score += 15
        drivers.append("High capacity system")

    # 4️⃣ COUNTRY CONTEXT (simple placeholder logic)
    if row["location_type"] == "coastal":
        score += 15
        drivers.append("High energy pressure region")

    # Clamp
    if score > 100:
        score = 100

    # --- CLOSE PROBABILITY ---
    if score >= 72:
        close_probability = 0.25
    elif score >= 60:
        close_probability = 0.15
    else:
        close_probability = 0.05

    # --- ESTIMATED INVESTMENT ---
    estimated_upgrade_kw = row["system_kw"] * 0.25
    estimated_battery_kwh = row["system_kw"] * 1.2

    upgrade_price_per_kw = 1500
    battery_price_per_kwh = 900

    estimated_investment = round(
        (estimated_upgrade_kw * upgrade_price_per_kw) +
        (estimated_battery_kwh * battery_price_per_kwh),
        2
    )

    expected_value = round(estimated_investment * close_probability, 2)

    return pd.Series([
        score,
        drivers[:3],
        close_probability,
        estimated_investment,
        expected_value
    ])

df[[
    "score",
    "drivers",
    "close_probability",
    "estimated_investment",
    "expected_value"
]] = df.apply(score_row, axis=1)

# --- ENDPOINTS ---

@app.get("/top20-simple")
def top20_simple():
    result = df.sort_values(by="score", ascending=False).head(20)
    return result[[
        "client_name",
        "score",
        "drivers",
        "estimated_investment",
        "expected_value"
    ]].to_dict(orient="records")


@app.get("/executive-summary")
def executive_summary():
    total_systems = len(df)
    total_potential = round(df["estimated_investment"].sum(), 2)
    total_expected = round(df["expected_value"].sum(), 2)

    high_priority = len(df[df["score"] >= 72])

    return {
        "total_systems": int(total_systems),
        "activation_candidates": int(high_priority),
        "total_potential_revenue": total_potential,
        "total_expected_value": total_expected
    }