"""Load a snapshot and create a meadow dataset."""

import re

import pandas as pd

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("famines.xlsx")

    # Load data from snapshot.
    tb = snap.read(sheet_name="0. Spreadsheet for disseminatio")

    #
    # Process data.
    #
    # Find the index of the specific entry where summary table by cause is provided in the 'Unnamed: 0' column
    trigger_index = tb[
        tb["Unnamed: 0"] == "Number of famines in which the following factor was a trigger cause, among others:"
    ].index[0]

    # Select all rows up to that index
    tb = tb.iloc[:trigger_index]

    columns = [
        "Date",
        "Place",
        "Sub region",
        "Global region",
        "Conventional title",
        "WPF authoritative mortality estimate",
        "Cause: immediate trigger",
        "Cause: contributing factors",
        "Cause: structural factors",
    ]
    tb = tb[columns]

    # Remove rows with NaN values
    tb = tb.dropna(axis=0, how="all")

    # Ensure the 'Date' column is treated as a string
    tb["Date"] = tb["Date"].astype(str)

    # Clean the 'Date' column
    tb["Date"] = tb["Date"].apply(clean_and_expand_years)

    # Fill NaN values in 'Conventional title' with 'Place' and 'Date'
    tb["Conventional title"] = tb.apply(
        lambda row: f"{row['Place']} ({row['Date']})"
        if pd.isna(row["Conventional title"])
        else f"{row['Place']} ({row['Date']})"
        if row["Conventional title"] == "Famine"
        else f"{row['Conventional title']} ({row['Place']} {row['Date']})"
        if row["Conventional title"] == "Hungerplan (German)"
        else row["Conventional title"],
        axis=1,
    )

    tb["WPF authoritative mortality estimate"] = tb["WPF authoritative mortality estimate"].astype(str)

    # Keep only the number in 'WPF authoritative mortality estimate'
    tb["WPF authoritative mortality estimate"] = tb["WPF authoritative mortality estimate"].str.extract(r"([\d,]+)")

    # Remove commas from the extracted numbers and convert to integer
    tb["WPF authoritative mortality estimate"] = (
        tb["WPF authoritative mortality estimate"].str.replace(",", "").astype(float)
    )

    # Combine famines for the African Red Sea Region and Hungerplan as the mortality estimate exists for just the total rather than each entry
    tb = combine_entries(tb)
    print(tb["Conventional title"].unique())

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["date", "conventional_title"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()


def clean_and_expand_years(year):
    # Correct cases like '1934, 1936-7' to '1934, 1936-1937'
    year = re.sub(
        r"(\d{4}),\s*(\d{4})-(\d{1})\b", lambda x: f"{x.group(1)}, {x.group(2)}-{x.group(2)[:3]}{x.group(3)}", year
    )
    # Correct abbreviated years to full years (e.g., 1998-9 -> 1998-1999)
    year = re.sub(r"(\d{4})-(\d{1})\b", lambda x: f"{x.group(1)}-{x.group(1)[:3]}{x.group(2)}", year)
    # Correct spaces or commas between years (e.g., 1932- 34 -> 1932-1934)
    year = re.sub(r"(\d{4})[,\s-]+(\d{2})\b", lambda x: f"{x.group(1)}-{x.group(1)[:2]}{x.group(2)}", year)
    # Remove any trailing symbols like '*' if present
    year = re.sub(r"[^\d-]", "", year)

    # Handle a specific incorrect value
    if year == "19341936-1937":
        return "1934,1936,1937"

    # Expand ranges to include all years (e.g., 1870-1872 -> 1870,1871,1872)
    if "-" in year:
        start, end = map(int, year.split("-"))
        expanded_range = ",".join(str(y) for y in range(start, end + 1))
        return expanded_range

    # Return the cleaned year if it's not a range
    return year


def combine_entries(df):
    """
    Combine multiple entries in the DataFrame into a single entry.
    """
    # Filter rows to combine the Haraame Cune ("the years of eating the unclean") and African Red Sea Region (Sudan, Northern Ethiopia, Eritrea, Djibouti)) as the mortality estimate only exists for the total
    famines = [
        'Haraame Cune ("the years of eating the unclean")',
        "African Red Sea Region (Sudan, Northern Ethiopia, Eritrea, Djibouti) (1913,1914)",
        "African Red Sea Region (Sudan, Northern Ethiopia, Eritrea, Djibouti) (1914,1915,1916,1917,1918,1919)",
    ]
    # Filter rows to combine
    rows_to_combine = df[df["Conventional title"].apply(lambda x: any(sub in x for sub in famines))]

    # Combine dates
    combined_dates = ",".join(rows_to_combine["Date"].unique())

    # Calculate the sum of the 'WPF authoritative mortality estimate' column
    mortality_estimate = rows_to_combine["WPF authoritative mortality estimate"].sum()

    # Create new combined entry
    new_entry = {
        "Date": combined_dates,
        "Place": rows_to_combine.iloc[0]["Place"],
        "Sub region": rows_to_combine.iloc[0]["Sub region"],
        "Global region": rows_to_combine.iloc[0]["Global region"],
        "Conventional title": 'African Red Sea Region and Haraame Cune ("the years of eating the unclean")',
        "Cause: immediate trigger": rows_to_combine.iloc[0]["Cause: immediate trigger"],
        "Cause: contributing factors": rows_to_combine.iloc[0]["Cause: contributing factors"],
        "Cause: structural factors": rows_to_combine.iloc[0]["Cause: structural factors"],
        "WPF authoritative mortality estimate": mortality_estimate,
    }

    # Add new combined entry
    df = df._append(new_entry, ignore_index=True)
    # Additional logic to combine "Hungerplan" entries
    places = ["USSR (Russia)", "USSR (Ukraine)", "USSR (Russia and Western Soviet States)"]
    famine = "Hungerplan (German)"

    hungerplan_rows = df[
        df["Conventional title"].apply(lambda x: any(sub in x for sub in famine)) & df["Place"].isin(places)
    ]
    if not hungerplan_rows.empty:
        combined_dates_hungerplan = ",".join(hungerplan_rows["Date"].unique())
        combined_mortality_estimate_hungerplan = hungerplan_rows["WPF authoritative mortality estimate"].sum()

        new_hungerplan_entry = {
            "Date": combined_dates_hungerplan,
            "Place": hungerplan_rows.iloc[0]["Place"],
            "Sub region": hungerplan_rows.iloc[0]["Sub region"],
            "Global region": hungerplan_rows.iloc[0]["Global region"],
            "Conventional title": "Hungerplan (Russia, Ukraine and Western Soviet States)",
            "Cause: immediate trigger": hungerplan_rows.iloc[0]["Cause: immediate trigger"],
            "Cause: contributing factors": hungerplan_rows.iloc[0]["Cause: contributing factors"],
            "Cause: structural factors": hungerplan_rows.iloc[0]["Cause: structural factors"],
            "WPF authoritative mortality estimate": combined_mortality_estimate_hungerplan,
        }

        # Add new combined Hungerplan entry
        df = df._append(new_hungerplan_entry, ignore_index=True)

    return df
