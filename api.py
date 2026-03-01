from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
from datetime import datetime

app = FastAPI()

# 🔥 CORS CONFIGURATION
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production we can restrict this
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

CURRENT_YEAR = 2026

# ---------- SCORING CONSTANTS ----------
BATTERY_AVG_VALUE = 8000
UPGRADE_AVG_VALUE = 6000
MAINTENANCE_AVG_VALUE = 900

BATTERY_CLOSE_PROB = 0.35
UPGRADE_CLOSE_PROB = 0.25
MAINTENANCE_CLOSE_PROB = 0.05

EFFORT_BATTERY = 4
EFFORT_UPGRADE = 3
EFFORT_MAINTENANCE = 5


def load_and_score():
    df = pd.read_csv("demo_clients.csv")

    df["years_since_install"] = CURRENT_YEAR - df["install_year"]
    df["years_since_contact"] = CURRENT_YEAR - df["last_contact_year"]

    def score_battery(row):
        score = 0
        if not row["has_battery"]:
            score += 10
        if row["years_since_install"] >= 2:
            score += 5
        if row["system_kw"] >= 5:
            score += 5
        if row["location_type"] == "coastal":
            score += 5
        return score

    def score_upgrade(row):
        score = 0
        if row["years_since_install"] >= 3:
            score += 10
        if row["system_kw"] <= 5:
            score += 10
        if row["years_since_contact"] >= 2:
            score += 5
        return score

    def score_maintenance(row):
        score = 0
        if row["years_since_contact"] >= 3:
            score += 15
        if row["client_type"] == "commercial":
            score += 5
        return score

    df["battery_score"] = df.apply(score_battery, axis=1)
    df["upgrade_score"] = df.apply(score_upgrade, axis=1)
    df["maintenance_score"] = df.apply(score_maintenance, axis=1)

    df["total_score"] = (
        df["battery_score"]
        + df["upgrade_score"]
        + df["maintenance_score"]
    )

    def main_opportunity(row):
        scores = {
            "Battery": row["battery_score"],
            "Upgrade": row["upgrade_score"],
            "Maintenance": row["maintenance_score"],
        }
        return max(scores, key=scores.get)

    df["main_opportunity"] = df.apply(main_opportunity, axis=1)

    def estimated_value(row):
        if row["main_opportunity"] == "Battery":
            return BATTERY_AVG_VALUE
        elif row["main_opportunity"] == "Upgrade":
            return UPGRADE_AVG_VALUE
        else:
            return MAINTENANCE_AVG_VALUE

    df["estimated_value"] = df.apply(estimated_value, axis=1)
    df["close_probability"] = df["main_opportunity"].map({
        "Battery": BATTERY_CLOSE_PROB,
        "Upgrade": UPGRADE_CLOSE_PROB,
        "Maintenance": MAINTENANCE_CLOSE_PROB,
    })

    df["expected_value"] = df["estimated_value"] * df["close_probability"]

    df["effort_score"] = df["main_opportunity"].map({
        "Battery": EFFORT_BATTERY,
        "Upgrade": EFFORT_UPGRADE,
        "Maintenance": EFFORT_MAINTENANCE,
    })

    df["priority_score"] = df["expected_value"] / df["effort_score"]

    return df.sort_values(by="priority_score", ascending=False)


@app.get("/top20")
def get_top20():
    df = load_and_score()
    return df.head(20).to_dict(orient="records")


@app.get("/top20-simple")
def get_top20_simple():
    df = load_and_score()
    top = df.head(20)[[
        "client_name",
        "main_opportunity",
        "total_score",
        "expected_value",
        "priority_score"
    ]]
    return top.to_dict(orient="records")