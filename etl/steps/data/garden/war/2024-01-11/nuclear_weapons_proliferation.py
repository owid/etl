"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Latest year to be assumed for the content of the data, when intervals are open, e.g. "2000-", or "1980-".
LATEST_YEAR = 2017

# Labels to be used on the status column.
LABEL_DOES_NOT_CONSIDER = "Does not consider"
LABEL_CONSIDERS = "Considers"
LABEL_PURSUES = "Pursues"
LABEL_POSSESSES = "Possesses"


def extract_year_ranges(years_ranges):
    # Extract years from a string that contains a list of years and year ranges.
    # Examples: "1964-66,72-75,80-", "1980-", "1953-62,82-87", "1970-2003", "1975-90", "2002-07".
    years = []
    if len(years_ranges) > 0:
        for years_range in years_ranges.split(","):
            if "-" in years_range:
                start, end = years_range.split("-")
                if len(end) == 0:
                    end = str(LATEST_YEAR)
                if len(start) == 2:
                    start = "19" + start
                if len(end) == 2:
                    end = start[0:2] + end
                years.extend(list(range(int(start), int(end) + 1)))
            else:
                years.append(int(years_range))

    return years


def correct_historical_regions(tb: Table) -> Table:
    # Convert columns to string type, to avoid issues with categorical columns.
    tb = tb.astype(str)

    # Merge the years of nuclear weapons exploration of Germany and West Germany.
    tb.loc[tb["country"] == "Germany", "explore"] = (
        tb[tb["country"] == "Germany"]["explore"].astype(str).item()
        + ","
        + tb[tb["country"] == "West Germany"]["explore"].astype(str).item()
    )
    # Remove row for West Germany.
    tb = tb[tb["country"] != "West Germany"].reset_index(drop=True)

    # Assign the years of nuclear weapons exploration and pursuit of Yugoslavia to Serbia.
    # To do that, simply rename the country.
    tb.loc[tb["country"] == "Yugoslavia", "country"] = "Serbia"

    return tb


def add_all_non_nuclear_countries(tb: Table, tb_regions: Table) -> Table:
    tb = tb.copy()
    # Get the list of all other currently existing countries that are not included in the data.
    countries_missing = sorted(
        set(tb_regions[(tb_regions["region_type"] == "country") & (~tb_regions["is_historical"])]["name"])
        - set(tb["country"])
    )

    # Add rows for those countries, with empty values.
    for country in countries_missing:
        tb.loc[len(tb)] = {"country": country, "explore": "", "pursue": "", "acquire": ""}

    return tb


def add_all_years(tb: Table) -> Table:
    tb = tb.copy()
    # Convert column where years are given as ranges into a list of years.
    for column in tb.drop(columns="country").columns:
        tb[column] = tb[column].astype(str).apply(extract_year_ranges)

    # Create a column that contains a list of all years in the data (the same list of all years for all countries).
    # For completeness, include the year right before the first in the data.
    all_years = sorted(set(tb["explore"].sum()))
    tb["year"] = [all_years + [min(all_years) - 1] for i in range(len(tb))]

    # Explode that column to create a row for each combination of country-year.
    tb = tb.explode("year").reset_index(drop=True)

    return tb


def add_status_column(tb: Table) -> Table:
    tb = tb.copy()

    tb["status"] = LABEL_DOES_NOT_CONSIDER
    for i, row in tb.iterrows():
        year = int(row["year"])
        if year in row["acquire"]:
            tb.loc[i, "status"] = LABEL_POSSESSES
            # A country that possesses nuclear weapons must be coded as pursuing and exploring nuclear weapons.
            assert (year in row["pursue"]) and (year in row["explore"])
        elif year in row["pursue"]:
            tb.loc[i, "status"] = LABEL_PURSUES
            # A country that pursues nuclear weapons must be coded as exploring nuclear weapons, but not possessing.
            assert (year in row["explore"]) and (year not in row["acquire"])
        elif year in row["explore"]:
            tb.loc[i, "status"] = LABEL_CONSIDERS
            # A country that considers nuclear weapons must not be coded as possessing or pursuing nuclear weapons.
            assert (year not in row["pursue"]) and (year not in row["acquire"])

    # Add metadata to the new status column.
    tb["status"] = tb["status"].copy_metadata(tb["explore"])

    return tb


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its main table.
    ds_meadow = paths.load_dataset("nuclear_weapons_proliferation")
    tb = ds_meadow["nuclear_weapons_proliferation"].reset_index()

    # Load regions dataset and read its main table.
    ds_regions = paths.load_dataset("regions")
    tb_regions = ds_regions["regions"]

    #
    # Process data.
    #
    # Harmonize country names.
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # Trace back nuclear weapons status for current countries (Germany, Serbia), and remove historical regions
    # (West Germany, Yugoslavia).
    tb = correct_historical_regions(tb=tb)

    # Add rows for countries that are not in the data (i.e. countries that do not even consider nuclear weapons).
    tb = add_all_non_nuclear_countries(tb=tb, tb_regions=tb_regions)

    # Add rows for years (years were given as intervals, e.g. "1964-66,72-75,80-").
    tb = add_all_years(tb=tb)

    # Create a column that contains the status of each country-year combination.
    tb = add_status_column(tb=tb)

    # Drop unnecessary columns.
    tb = tb.drop(columns=["explore", "pursue", "acquire"])

    # Set an appropriate index and sort conveniently.
    tb = tb.set_index(["country", "year"], verify_integrity=True).sort_index().sort_index(axis=1)

    # Create a new table with the total count of countries in each status.
    tb_counts = (
        tb.reset_index()
        .groupby(["status", "year"], as_index=False)
        .count()
        .pivot(index="year", columns="status", join_column_levels_with="_")
    )

    # Rename columns conveniently.
    tb_counts = tb_counts.underscore().rename(
        columns={
            "country_does_not_consider": "n_countries_not_considering",
            "country_considers": "n_countries_considering",
            "country_pursues": "n_countries_pursuing",
            "country_possesses": "n_countries_possessing",
        },
        errors="raise",
    )

    # Fill missing values with zeros and set an appropriate type.
    tb_counts = tb_counts.fillna(0).astype(int)

    # Set an appropriate index and sort conveniently.
    tb_counts = tb_counts.set_index(["year"], verify_integrity=True).sort_index().sort_index(axis=1)

    # Rename table conveniently.
    tb_counts.metadata.short_name = "nuclear_weapons_proliferation_counts"

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb, tb_counts], check_variables_metadata=True)
    ds_garden.save()
