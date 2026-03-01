from fastapi import FastAPI
import pandas as pd

app = FastAPI()

CURRENT_YEAR = 2026

COUNTRY_CONFIG = {
    "Spain": {
        "kwh_price": 0.22,
        "vat": 0.21,
        "battery_multiplier": 1.0
    }
}

ACTIVE_COUNTRY = "Spain"
config = COUNTRY_CONFIG[ACTIVE_COUNTRY]


def run_activation_engine():
    df = pd.read_csv("demo_clients.csv")
    df.columns = df.columns.str.strip()
    df["has_battery"] = df["has_battery"].astype(str).str.upper() == "TRUE"

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
        if row["system_kw"] <= 8:
            score += 10
        if row["years_since_install"] >= 3:
            score += 5
        if row["years_since_contact"] >= 2:
            score += 5
        return score

    def score_maintenance(row):
        score = 0
        if row["years_since_install"] >= 3:
            score += 10
        if row["location_type"] == "coastal":
            score += 5
        if row["years_since_contact"] >= 2:
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

    BASE_BATTERY_VALUE = 8000
    BASE_UPGRADE_VALUE = 6000
    BASE_MAINTENANCE_VALUE = 900

    def estimated_value(row):
        if row["main_opportunity"] == "Battery":
            return BASE_BATTERY_VALUE * config["battery_multiplier"]
        elif row["main_opportunity"] == "Upgrade":
            return BASE_UPGRADE_VALUE
        else:
            return BASE_MAINTENANCE_VALUE

    df["estimated_value"] = df.apply(estimated_value, axis=1)
    df["estimated_value_with_vat"] = df["estimated_value"] * (1 + config["vat"])

    def close_probability(score):
        if score >= 60:
            return 0.35
        elif score >= 50:
            return 0.25
        elif score >= 40:
            return 0.15
        else:
            return 0.05

    df["close_probability"] = df["total_score"].apply(close_probability)
    df["expected_value"] = df["estimated_value_with_vat"] * df["close_probability"]

    def effort_score(row):
        effort = 3
        if row["years_since_install"] <= 2:
            effort -= 1
        if row["years_since_contact"] <= 1:
            effort -= 1
        if row["client_type"] == "commercial":
            effort += 1
        if row["years_since_contact"] >= 3:
            effort += 1
        return max(1, min(5, effort))

    df["effort_score"] = df.apply(effort_score, axis=1)
    df["priority_score"] = df["expected_value"] / df["effort_score"]

    top_20 = df.sort_values(by="priority_score", ascending=False).head(20)

    return top_20.to_dict(orient="records")


@app.get("/top20")
def get_top_20():
    return run_activation_engine()