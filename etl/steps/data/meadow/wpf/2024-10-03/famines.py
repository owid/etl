"""Load a snapshot and create a meadow dataset."""

import re

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


NAMES = {
    "Persia (Iran)": "Persia",
    "China": "China",
    "India": "India",
    "Brazil": "Brazil",
    "Congo Free State (Democratic Republic of Congo)": "Congo Free State",
    "Sudan and Ethiopia": "Sudan, Ethiopia",
    "Russia": "Russia",
    "Ottoman Empire (Turkey)": "Ottoman Empire",
    "Cuba": "Cuba",
    "East Africa (Kenya, Uganda, Tanzania)": "East Africa",
    "Philippines": "Philippines",
    "Tanganyika (German East Africa, Tanzania)": "Tanzania",
    "British Somaliland": "Somaliland",
    "African Red Sea Region (Sudan, Northern Ethiopia, Eritrea, Djibouti)": "African Red Sea Region",
    "Ottoman Empire (Turkey, Iraq, Iran, Syria)": "Ottoman Empire",
    "Sahel (Upper Senegal and Niger (contemporary Burkina Faso and Mali), the Military Territory of Niger, and Chad)": "Sahel",
    "German East Africa (Tanzania, Mozambique, Rwanda, Burundi)": "German East Africa",
    "Serbia and the Balkans": "Serbia, Balkans",
    "Ottoman Empire (Turkey, Armenians)": "Ottoman Empire",
    "Austria-Hungary (Poland)": "Poland",
    "Greater Syria": "Greater Syria",
    "Russia and Ukraine": "Russia, Ukraine",
    "Germany": "Germany",
    "Persia": "Persia",
    "Armenia": "Armenia",
    "USSR (Ukraine)": "Ukraine",
    "USSR (Russia, Kazakhstan)": "Russia, Kazakhstan",
    "Germany/USSR": "Germany, USSR",
    "USSR (Russia)": "Russia",
    "USSR (Russia and Western Soviet States)": "USSR",
    "Greece": "Greece",
    "East Asia": "East Asia",
    "Indonesia": "Indonesia",
    "India (India, West Bengal, Bangladesh)": "India, Bangladesh",
    "Vietnam": "Vietnam",
    "Eastern Europe": "Eastern Europe",
    "USSR (Moldova, Ukraine, Russia, Belarus)": "USSR",
    "Ethiopia": "Ethiopia",
    "Nigeria (Biafra)": "Nigeria",
    "Sahel (Mauritania, Mali, Niger)": "Sahel",
    "Bangladesh": "Bangladesh",
    "East Timor": "East Timor",
    "Cambodia": "Cambodia",
    "Mozambique": "Mozambique",
    "Sudan": "Sudan",
    "Sudan\n(South Sudan)": "South Sudan",
    "Somalia": "Somalia",
    "Sudan\n(including South Sudan)": "Sudan",
    "North Korea": "North Korea",
    "DRC": "Democratic Republic of Congo",
    "Uganda": "Uganda",
    "Syria": "Syria",
    "South Sudan": "South Sudan",
    "Nigeria": "Nigeria",
    "Yemen": "Yemen",
    "CAR": "Central African Republic",
}


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

    tb["WPF authoritative mortality estimate"] = tb["WPF authoritative mortality estimate"].astype(str)

    # Keep only the number in 'WPF authoritative mortality estimate'
    tb["WPF authoritative mortality estimate"] = tb["WPF authoritative mortality estimate"].str.extract(r"([\d,]+)")

    # Remove commas from the extracted numbers and convert to integer
    tb["WPF authoritative mortality estimate"] = (
        tb["WPF authoritative mortality estimate"].str.replace(",", "").astype(float)
    )
    # Simplify names - will later use it for titles with years combined
    tb["simplified_place"] = tb["Place"].replace(NAMES, regex=False)

    # Combine famines for the African Red Sea Region and Hungerplan as the mortality estimate exists for just the total rather than each entry
    tb = combine_entries(tb)

    tb = tb.rename(columns={"Place": "country"})

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["date", "country"])

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
    famines = ["British Somaliland", "African Red Sea Region (Sudan, Northern Ethiopia, Eritrea, Djibouti)"]

    # Filter rows to combine
    rows_to_combine = df[df["Place"].apply(lambda x: any(sub in x for sub in famines))]

    # Combine dates and causes
    combined_dates = ",".join(rows_to_combine["Date"].unique())
    combined_trigger = ",".join(rows_to_combine["Cause: immediate trigger"])
    combined_contributing_cause = ",".join(rows_to_combine["Cause: contributing factors"])
    combined_structural_cause = ",".join(rows_to_combine["Cause: structural factors"])

    # Calculate the sum of the 'WPF authoritative mortality estimate' column
    mortality_estimate = rows_to_combine["WPF authoritative mortality estimate"].sum()

    # Create new combined entry
    new_entry = {
        "Date": combined_dates,
        "Place": "Somaliland, African Red Sea Region",
        "Cause: immediate trigger": combined_trigger,
        "Cause: contributing factors": combined_contributing_cause,
        "Cause: structural factors": combined_structural_cause,
        "WPF authoritative mortality estimate": mortality_estimate,
        "simplified_place": "Somaliland, African Red Sea Region",
    }

    # Add new combined entry
    df = df._append(new_entry, ignore_index=True)
    df = df.drop(rows_to_combine.index)

    # Additional logic to combine "Hungerplan" entries
    places = ["USSR (Russia)", "USSR (Ukraine)", "USSR (Russia and Western Soviet States)"]
    dates = ["1941,1942,1943,1944"]

    hungerplan_rows = df[df["Place"].isin(places) & df["Date"].isin(dates)]

    if not hungerplan_rows.empty:
        # Combine dates and causes
        combined_dates_hungerplan = ",".join(hungerplan_rows["Date"].unique())
        combined_mortality_estimate_hungerplan = hungerplan_rows["WPF authoritative mortality estimate"].sum()
        combined_trigger = ",".join(hungerplan_rows["Cause: immediate trigger"])
        combined_contributing_cause = ",".join(hungerplan_rows["Cause: contributing factors"])
        combined_structural_cause = ",".join(hungerplan_rows["Cause: structural factors"])

        new_hungerplan_entry = {
            "Date": combined_dates_hungerplan,
            "Place": "USSR",
            "Cause: immediate trigger": combined_trigger,
            "Cause: contributing factors": combined_contributing_cause,
            "Cause: structural factors": combined_structural_cause,
            "WPF authoritative mortality estimate": combined_mortality_estimate_hungerplan,
            "simplified_place": "USSR (Hungerplan)",
        }

        # Add new combined Hungerplan entry
        df = df._append(new_hungerplan_entry, ignore_index=True)
        df = df.drop(hungerplan_rows.index)

    return df
