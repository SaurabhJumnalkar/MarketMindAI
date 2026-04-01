import pandas as pd
import numpy as np

# from datetime import date
import os
import logging

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)


def generate_enterprise_data():
    logger.info("🚀 Starting GlobalTech Solutions Data Generation (Enterprise Scale)...")

    np.random.seed(42)

    # ==========================================
    # 1. DIMENSION TABLES (EXPANDED)
    # ==========================================

    # Expanded Geography (15 Regions)
    geos = [
        {"Geo_ID": "GEO-01", "Region": "AMER", "Country": "USA"},
        {"Geo_ID": "GEO-02", "Region": "AMER", "Country": "Canada"},
        {"Geo_ID": "GEO-03", "Region": "AMER", "Country": "Mexico"},
        {"Geo_ID": "GEO-04", "Region": "AMER", "Country": "Brazil"},
        {"Geo_ID": "GEO-05", "Region": "AMER", "Country": "Argentina"},
        {"Geo_ID": "GEO-06", "Region": "EMEA", "Country": "UK"},
        {"Geo_ID": "GEO-07", "Region": "EMEA", "Country": "Germany"},
        {"Geo_ID": "GEO-08", "Region": "EMEA", "Country": "France"},
        {"Geo_ID": "GEO-09", "Region": "EMEA", "Country": "Italy"},
        {"Geo_ID": "GEO-10", "Region": "EMEA", "Country": "Spain"},
        {"Geo_ID": "GEO-11", "Region": "APAC", "Country": "India"},
        {"Geo_ID": "GEO-12", "Region": "APAC", "Country": "Japan"},
        {"Geo_ID": "GEO-13", "Region": "APAC", "Country": "China"},
        {"Geo_ID": "GEO-14", "Region": "APAC", "Country": "Australia"},
        {"Geo_ID": "GEO-15", "Region": "APAC", "Country": "Singapore"},
    ]
    df_geo = pd.DataFrame(geos)

    # Expanded Business Units (50 Cost Centers dynamically generated)
    bus = []
    bb_categories = [
        ("Technology Solutions", "Cloud Services", "Enterprise Cloud Migration"),
        ("Technology Solutions", "Cloud Services", "Cloud Security"),
        ("Technology Solutions", "On-Premises", "Hardware Infrastructure"),
        ("Professional Services", "Consulting", "AI Strategy"),
        ("Professional Services", "Support", "Global IT Support"),
    ]

    bu_counter = 101
    for bb_plus, bb, bb_minus in bb_categories:
        for i in range(1, 11):  # Generate 10 distinct Cost Centers per department
            bus.append(
                {
                    "BU_ID": f"BU-{bu_counter}",
                    "BB_Plus_1": bb_plus,
                    "BB": bb,
                    "BB_Minus_1": bb_minus,
                    "Cost_Center": f"CC-{bu_counter}-{i:02d}",  # noqa E231
                }
            )
            bu_counter += 1

    df_bu = pd.DataFrame(bus)

    # Dim KeyFigure (Same 22)
    kfs = [
        {"KF_ID": "KF-001", "Name": "Revenue (Gross)", "Category": "P&L", "Sign": 1},
        {"KF_ID": "KF-002", "Name": "Revenue (Net)", "Category": "P&L", "Sign": 1},
        {
            "KF_ID": "KF-003",
            "Name": "OPEX - Internal Charging",
            "Category": "OPEX",
            "Sign": -1,
        },
        {
            "KF_ID": "KF-004",
            "Name": "OPEX - External Charging",
            "Category": "OPEX",
            "Sign": -1,
        },
        {"KF_ID": "KF-005", "Name": "OPEX - Rent", "Category": "OPEX", "Sign": -1},
        {"KF_ID": "KF-006", "Name": "OPEX - Car Fleet", "Category": "OPEX", "Sign": -1},
        {
            "KF_ID": "KF-007",
            "Name": "OPEX - Education & Training",
            "Category": "OPEX",
            "Sign": -1,
        },
        {"KF_ID": "KF-008", "Name": "OPEX - Marketing", "Category": "OPEX", "Sign": -1},
        {
            "KF_ID": "KF-009",
            "Name": "OPEX - Travel & Entertainment",
            "Category": "OPEX",
            "Sign": -1,
        },
        {"KF_ID": "KF-010", "Name": "OPEX - Utilities", "Category": "OPEX", "Sign": -1},
        {
            "KF_ID": "KF-011",
            "Name": "OPEX - IT Expenses",
            "Category": "OPEX",
            "Sign": -1,
        },
        {"KF_ID": "KF-012", "Name": "Headcount Cost", "Category": "OPEX", "Sign": -1},
        {"KF_ID": "KF-013", "Name": "EBITDA", "Category": "P&L", "Sign": 1},
        {
            "KF_ID": "KF-014",
            "Name": "Depreciation",
            "Category": "Non-Operating",
            "Sign": -1,
        },
        {
            "KF_ID": "KF-015",
            "Name": "Amortization",
            "Category": "Non-Operating",
            "Sign": -1,
        },
        {"KF_ID": "KF-016", "Name": "EBIT", "Category": "P&L", "Sign": 1},
        {
            "KF_ID": "KF-017",
            "Name": "Interest Expenses - Loans",
            "Category": "Non-Operating",
            "Sign": -1,
        },
        {
            "KF_ID": "KF-018",
            "Name": "Interest Expenses - Leases",
            "Category": "Non-Operating",
            "Sign": -1,
        },
        {
            "KF_ID": "KF-019",
            "Name": "Other Operating Income",
            "Category": "P&L",
            "Sign": 1,
        },
        {"KF_ID": "KF-020", "Name": "Net Income", "Category": "P&L", "Sign": 1},
        {"KF_ID": "KF-021", "Name": "CAPEX - Software", "Category": "CAPEX", "Sign": 1},
        {"KF_ID": "KF-022", "Name": "CAPEX - Hardware", "Category": "CAPEX", "Sign": 1},
    ]
    df_kf = pd.DataFrame(kfs)

    # Dim Date (2020 - 2025)
    dates = pd.date_range(start="2020-01-01", end="2025-12-31", freq="MS")
    df_date = pd.DataFrame(
        {
            "Date_ID": dates.strftime("%Y%m").astype(int),
            "Year": dates.year,
            "Month": dates.month,
            "Month_Name": dates.strftime("%B"),
            "Quarter": "Q" + dates.quarter.astype(str),
        }
    )

    # ==========================================
    # 2. FACT TABLE GENERATION (The Engine)
    # ==========================================
    logger.info(
        f"⚙️ Crunching financials for {len(geos)} Countries and {len(bus)} Cost Centers..."
    )
    fact_records = []
    record_id_counter = 100000

    for current_date in dates:
        date_id = int(current_date.strftime("%Y%m"))
        year = current_date.year
        month = current_date.month

        # Terminal Progress bar
        if month == 1:
            logger.info(f"⏳ Generating data for Year: {year}...")

        growth_factor = 1.0 + ((year - 2020) * 0.05)
        seasonality = 1.0 + (np.sin(month / 12 * np.pi) * 0.1)

        for geo in geos:
            for bu in bus:
                # 1. Generate Base Revenue (Scaled down slightly so single cost centers aren't doing $500M a month)
                base_rev = np.random.normal(150000, 25000) * growth_factor * seasonality

                # THE ANOMALY: 2025 European Hardware Crash
                if (
                    year == 2025  # noqa W503
                    and month >= 8  # noqa W503
                    and geo["Region"] == "EMEA"  # noqa W503
                    and "Hardware" in bu["BB"]  # noqa W503
                ):
                    base_rev *= 0.30

                # 2. Calculate P&L Logic
                gross_rev = base_rev
                net_rev = gross_rev * np.random.uniform(0.92, 0.98)

                opex = {
                    "KF-003": net_rev * 0.05,
                    "KF-004": net_rev * 0.04,
                    "KF-005": 5000 * growth_factor,
                    "KF-006": 1500,
                    "KF-007": net_rev * 0.02,
                    "KF-008": net_rev * 0.08,
                    "KF-009": net_rev * 0.03,
                    "KF-010": 2000,
                    "KF-011": net_rev * 0.06,
                    "KF-012": net_rev * 0.35,
                }

                total_opex = sum(opex.values())
                ebitda = net_rev - total_opex

                depreciation = ebitda * 0.05 if ebitda > 0 else 5000
                amortization = ebitda * 0.02 if ebitda > 0 else 2000
                ebit = ebitda - (depreciation + amortization)

                interest = ebit * 0.05 if ebit > 0 else 5000
                other_income = np.random.uniform(0, 3000)
                net_income = ebit - interest + other_income

                capex_soft = gross_rev * 0.04
                capex_hard = gross_rev * 0.06

                actuals_map = {
                    "KF-001": gross_rev,
                    "KF-002": net_rev,
                    **opex,
                    "KF-013": ebitda,
                    "KF-014": depreciation,
                    "KF-015": amortization,
                    "KF-016": ebit,
                    "KF-017": interest,
                    "KF-018": interest * 0.2,
                    "KF-019": other_income,
                    "KF-020": net_income,
                    "KF-021": capex_soft,
                    "KF-022": capex_hard,
                }

                # 4. Write rows
                for kf_id, actual_val in actuals_map.items():
                    forecast_val = actual_val * np.random.uniform(0.90, 1.15)

                    if (
                        year == 2025  # noqa W503
                        and month >= 8  # noqa W503
                        and geo["Region"] == "EMEA"  # noqa W503
                        and "Hardware" in bu["BB"]  # noqa W503
                    ):
                        forecast_val = actual_val * 3.0

                    fact_records.append(
                        {
                            "Record_ID": f"REC-{record_id_counter}",
                            "Date_ID": date_id,
                            "BU_ID": bu["BU_ID"],
                            "Geo_ID": geo["Geo_ID"],
                            "KeyFigure_ID": kf_id,
                            "Actuals": round(actual_val, 2),
                            "Forecast": round(forecast_val, 2),
                        }
                    )
                    record_id_counter += 1

    df_fact = pd.DataFrame(fact_records)

    # ==========================================
    # 3. EXPORT TO CSV
    # ==========================================
    logger.info(
        "💾 Saving massive datasets to data/seed/ ... (This may take a few seconds)"
    )
    os.makedirs("data/seed", exist_ok=True)

    df_geo.to_csv("data/seed/dim_geography.csv", index=False)
    df_bu.to_csv("data/seed/dim_business_unit.csv", index=False)
    df_kf.to_csv("data/seed/dim_keyfigure.csv", index=False)
    df_date.to_csv("data/seed/dim_date.csv", index=False)
    df_fact.to_csv("data/seed/fact_financials.csv", index=False)

    logger.info("========================================")
    logger.info("🎉 ENTERPRISE DATA GENERATION COMPLETE!")
    logger.info(f"📊 Total Fact Rows Created: {len(df_fact)}")
    logger.info("========================================")


if __name__ == "__main__":
    generate_enterprise_data()
