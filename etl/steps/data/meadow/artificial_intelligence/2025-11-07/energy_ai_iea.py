"""Load a snapshot and create a meadow dataset."""

import pandas as pd
from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    """Load IEA Energy and AI snapshot and create a meadow dataset."""
    paths.log.info("energy_ai_iea.start")

    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("energy_ai_iea.xlsx")

    # Process World Data sheet
    df_world = process_world_data(snap)

    # Process Regional Data sheet
    df_regional = process_regional_data(snap)

    df = pd.concat([df_world, df_regional])

    tb = Table(pd.DataFrame(df), short_name="energy_ai_iea")

    # Set index - year, country, and metric form the primary key
    tb = tb.format(["country", "year", "metric", "data_center_category", "infrastructure_type", "scenario"])
    # Add metadata
    for col in tb.columns:
        tb[col].metadata.origins = [snap.metadata.origin]

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()

    paths.log.info("energy_ai_iea.end")


def process_world_data(snap) -> pd.DataFrame:
    """Process World Data sheet into long format."""
    # Read the raw sheet
    df = snap.read(sheet_name="World Data", header=None, safe_types=False)

    # Historical years from columns 3-5
    # Scenario projections in columns 7-8, 10-11, 13-14, 16-17
    historical_years = [2020, 2023, 2024]

    data_rows = []

    # Process each metric section
    current_metric = None
    current_infrastructure = None  # Total or IT

    for idx in range(3, len(df)):
        row = df.iloc[idx, :]

        # Check if this is a metric header
        metric_name = str(row[2]) if pd.notna(row[2]) else None

        if metric_name and metric_name in [
            "Installed capacity (GW)",
            "Power usage effectiveness",
            "Load factor (%)",
            "Electricity consumption (TWh)",
        ]:
            current_metric = metric_name
            continue

        # Check if this is a category (Total, Hyperscale, etc.)
        category = str(row[2]) if pd.notna(row[2]) else None

        if not category or category == "nan":
            continue

        # Determine infrastructure type based on category position
        if category == "Total":
            current_infrastructure = "Total"
            # Extract Total values as aggregate data
            for i, year in enumerate(historical_years):
                value = row[3 + i]
                if pd.notna(value):
                    data_rows.append(
                        {
                            "year": year,
                            "metric": current_metric,
                            "data_center_category": "Total",
                            "infrastructure_type": current_infrastructure,
                            "scenario": "historical",
                            "value": value,
                        }
                    )

            # Scenario projections for Total
            scenario_columns = {
                "Base": [(7, 2030), (8, 2035)],
                "Lift-Off": [(10, 2030), (11, 2035)],
                "High Efficiency": [(13, 2030), (14, 2035)],
                "Headwinds": [(16, 2030), (17, 2035)],
            }

            for scenario_name, year_cols in scenario_columns.items():
                for col_idx, year in year_cols:
                    value = row[col_idx]
                    if pd.notna(value):
                        data_rows.append(
                            {
                                "year": year,
                                "metric": current_metric,
                                "data_center_category": "Total",
                                "infrastructure_type": current_infrastructure,
                                "scenario": scenario_name.lower().replace(" ", "_"),
                                "value": value,
                            }
                        )
            continue

        elif category == "IT":
            current_infrastructure = "IT"
            # Extract IT values as aggregate data
            for i, year in enumerate(historical_years):
                value = row[3 + i]
                if pd.notna(value):
                    data_rows.append(
                        {
                            "year": year,
                            "metric": current_metric,
                            "data_center_category": "IT",
                            "infrastructure_type": current_infrastructure,
                            "scenario": "historical",
                            "value": value,
                        }
                    )

            # Scenario projections for IT
            scenario_columns = {
                "Base": [(7, 2030), (8, 2035)],
                "Lift-Off": [(10, 2030), (11, 2035)],
                "High Efficiency": [(13, 2030), (14, 2035)],
                "Headwinds": [(16, 2030), (17, 2035)],
            }

            for scenario_name, year_cols in scenario_columns.items():
                for col_idx, year in year_cols:
                    value = row[col_idx]
                    if pd.notna(value):
                        data_rows.append(
                            {
                                "year": year,
                                "metric": current_metric,
                                "data_center_category": "IT",
                                "infrastructure_type": current_infrastructure,
                                "scenario": scenario_name.lower().replace(" ", "_"),
                                "value": value,
                            }
                        )
            continue

        elif category in ["Hyperscale", "Colocation and service provider", "Enterprise"]:
            # Historical years (2020, 2023, 2024)
            for i, year in enumerate(historical_years):
                value = row[3 + i]
                if pd.notna(value):
                    data_rows.append(
                        {
                            "year": year,
                            "metric": current_metric,
                            "data_center_category": category.lower().replace(" and ", "_").replace(" ", "_"),
                            "infrastructure_type": current_infrastructure,
                            "scenario": "historical",
                            "value": value,
                        }
                    )

            # Scenario projections (2030, 2035)
            scenario_columns = {
                "Base": [(7, 2030), (8, 2035)],
                "Lift-Off": [(10, 2030), (11, 2035)],
                "High Efficiency": [(13, 2030), (14, 2035)],
                "Headwinds": [(16, 2030), (17, 2035)],
            }

            for scenario_name, year_cols in scenario_columns.items():
                for col_idx, year in year_cols:
                    value = row[col_idx]
                    if pd.notna(value):
                        data_rows.append(
                            {
                                "year": year,
                                "metric": current_metric,
                                "data_center_category": category.lower().replace(" and ", "_").replace(" ", "_"),
                                "infrastructure_type": current_infrastructure,
                                "scenario": scenario_name.lower().replace(" ", "_"),
                                "value": value,
                            }
                        )

    # Convert to DataFrame and then to Table
    df = Table(pd.DataFrame(data_rows), short_name="energy_ai_iea")
    df["country"] = "World"

    return df


def process_regional_data(snap) -> pd.DataFrame:
    """Process Regional Data sheet into long format."""
    # Read the raw sheet
    df = snap.read(sheet_name="Regional Data", header=None, safe_types=False)

    # Extract year headers (row 3: 2020, 2023, 2024, 2030)
    years = df.iloc[3, 2:7].values  # Columns 2-6
    years = [int(y) if pd.notna(y) and y != 0 else None for y in years]

    data_rows = []
    current_metric = None

    for idx in range(4, len(df)):
        row = df.iloc[idx, :]

        # Check if this is a metric header
        metric_name = str(row[1]) if pd.notna(row[1]) else None

        # Check for specific metric headers that define new sections
        if metric_name and metric_name in [
            "Total installed capacity (GW)",
            "IT installed capacity (GW)",
            "Power usage effectiveness",
            "Load factor (%)",
            "Total electricity consumption (TWh)",
            "IT electricity consumption (TWh)",
        ]:
            current_metric = metric_name
            continue

        # Extract country/region
        country = str(row[1]) if pd.notna(row[1]) else None

        if not country or country == "nan" or not current_metric:
            continue

        # Rename metrics to distinguish from World Data sheet and remove "Total" prefix
        metric_name = current_metric
        if current_metric == "Power usage effectiveness":
            metric_name = "Regional power usage effectiveness"
        elif current_metric == "Load factor (%)":
            metric_name = "Regional load factor (%)"
        elif current_metric.startswith("Total "):
            metric_name = current_metric.replace("Total ", "Regional total ", 1)
        elif current_metric.startswith("IT "):
            metric_name = current_metric.replace("IT ", "Regional IT ", 1)

        # Extract values for each year
        for i, year in enumerate(years):
            if year is not None:
                value = row[2 + i]
                if pd.notna(value) and value != 0:
                    # Set scenario based on year: "base" for 2030, "historical" for others
                    scenario = "base" if year == 2030 else "historical"
                    infrastructure_type = "IT" if "IT" in current_metric else "Total"
                    data_rows.append(
                        {
                            "year": year,
                            "country": country,
                            "metric": metric_name,
                            "value": value,
                            "infrastructure_type": infrastructure_type,
                            "scenario": scenario,
                        }
                    )

    # Convert to DataFrame and then to Table
    df = pd.DataFrame(data_rows)
    df["data_center_category"] = "Total"

    print(df)

    return df
