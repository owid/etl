"""Load a meadow dataset and create a garden dataset."""

import itertools

import owid.catalog.processing as pr
import pandas as pd
from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Labels to be used on the status column.
LABEL_DOES_NOT_CONSIDER = "Does not consider"
LABEL_CONSIDERS = "Considers"
LABEL_PURSUES = "Pursues"
LABEL_POSSESSES = "Possesses"


# Latest year to be assumed for the content of the data, when intervals are open, e.g. "2000-", or "1980-".
LATEST_YEAR = 2017

# Labels to be used on the status column.
LABEL_DOES_NOT_CONSIDER = "Does not consider"
LABEL_CONSIDERS = "Considers"
LABEL_PURSUES = "Pursues"
LABEL_POSSESSES = "Possesses"


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


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load Bleek garden dataset and read its main table.
    ds_bleek = paths.load_dataset("nuclear_weapons_proliferation")
    tb = ds_bleek["nuclear_weapons_proliferation"].reset_index()

    # Load Nuclear Threat Initiative garden dataset and read its main table.
    ds_nti = paths.load_dataset("nuclear_threat_initiative_overview")
    tb_nti = ds_nti["nuclear_threat_initiative_overview"].reset_index()

    # Load regions dataset and read its main table.
    ds_regions = paths.load_dataset("regions")
    tb_regions = ds_regions["regions"]

    #
    # Process data.
    #
    # Combine the two tables.
    tb = pr.concat([tb, tb_nti], ignore_index=True)

    # Add all years and countries (including those that do not even consider nuclear weapons).
    all_years = sorted(set(tb["year"]))
    all_countries = sorted(
        set(tb_regions[(tb_regions["region_type"] == "country") & (~tb_regions["is_historical"])]["name"])
    )

    # Create a DataFrame with all combinations of countries and years, and a common column of status 0.
    tb_added = Table(
        pd.DataFrame(list(itertools.product(all_countries, all_years)), columns=["country", "year"]).assign(
            **{"status": 0}
        )
    )

    # Combine the two tables, and remove repeated rows (keeping the values of the original table).
    tb = pr.concat([tb.astype({"year": int}), tb_added], ignore_index=True).drop_duplicates(
        subset=["country", "year"], keep="first"
    )

    # Use labels for status, instead of numbers.
    tb["status"] = tb["status"].replace(
        {0: LABEL_DOES_NOT_CONSIDER, 1: LABEL_CONSIDERS, 2: LABEL_PURSUES, 3: LABEL_POSSESSES}
    )

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
