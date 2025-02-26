"""Load a snapshot and create a meadow dataset."""

import re

from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


NAMES = {
    "Persia (Iran)": "Persia",
    "China": "China",
    "India": "India",
    "Brazil": "Brazil",
    "Congo Free State (Democratic Republic of Congo)": "Congo Free State",
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
    "Germany": "Germany",
    "Persia": "Persia",
    "Armenia": "Armenia",
    "USSR (Ukraine)": "Ukraine",
    "Germany/USSR": "Germany, USSR",
    "USSR (Russia)": "Russia",
    "USSR (Russia and Western Soviet States)": "USSR",
    "Greece": "Greece",
    "East Asia": "Japan",
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
    "USSR (southern Russia & Ukraine)": "USSR (Southern Russia & Ukraine)",
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("famines.xlsx")

    # Load data from snapshot.
    tb = snap.read(safe_types=False, sheet_name="0. Spreadsheet for disseminatio")

    #
    # Process data.
    #

    columns = [
        "Date_unify",
        "Place",
        "Sub region",
        "WPF authoritative mortality estimate",
        "Principal Cause",
    ]
    tb = tb[columns]
    tb = tb.rename(columns={"Date_unify": "Date"})

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

    # Add 'Sub region' to 'Place' column if 'Sub region' is "Ghettos, Concentration camps"
    tb["simplified_place"] = tb.apply(
        lambda row: f"{row['Place']} (ghettos and concentration camps)"
        if row["Sub region"] == "Ghettos, Concentration camps"
        else row["simplified_place"],
        axis=1,
    )

    # Combine famines for the African Red Sea Region as the mortality estimate exists for just the total rather than each entry
    tb = combine_entries(tb)

    # Drop the 'Sub region' column as it's no longer needed
    tb = tb.drop(columns=["Sub region"])

    tb = tb.rename(columns={"Place": "country"})

    # Ensure there are no NaNs in the mortality estimates
    assert (
        not tb["WPF authoritative mortality estimate"].isna().any()
    ), "There are NaN values in the mortality estimates"

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["date", "simplified_place"])

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


def combine_entries(tb: Table) -> Table:
    """
    Combine famines for the African Red Sea Region as the mortality estimate exists for just the total rather than each entry.
    """
    # Filter rows to combine British Somaliland and African Red Sea Region (Sudan, Northern Ethiopia, Eritrea, Djibouti) as the mortality estimate only exists for the total
    famines = ["British Somaliland", "African Red Sea Region (Sudan, Northern Ethiopia, Eritrea, Djibouti)"]
    rows_to_combine = tb[tb["Place"].apply(lambda x: any(sub in x for sub in famines))]

    # Combine dates and principle causes
    combined_dates = ",".join(rows_to_combine["Date"].unique())

    # Filter out NaN values and join the remaining strings
    combined_cause = ",".join(rows_to_combine["Principal Cause"].dropna())

    # Calculate the sum of the 'WPF authoritative mortality estimate' column
    mortality_estimate = rows_to_combine["WPF authoritative mortality estimate"].sum()

    # Create new combined entry
    new_entry = {
        "Date": combined_dates,
        "Place": "Somaliland, African Red Sea Region",
        "Principal Cause": combined_cause,
        "WPF authoritative mortality estimate": mortality_estimate,
        "simplified_place": "Somaliland, African Red Sea Region",
    }

    # Add new combined entry
    tb = tb._append(new_entry, ignore_index=True)
    tb = tb.drop(rows_to_combine.index)

    return tb
