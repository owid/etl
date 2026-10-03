"""Load the World Bank income groups meadow dataset and produce a garden dataset
mapping each country to its income group.
"""

import json

import owid.catalog.processing as pr
import pandas as pd
from owid.catalog import Table

from etl.helpers import PathFinder

paths = PathFinder(__file__)

# Countries not present in the World Bank source but assigned to "High-income countries"
# in line with how OWID has historically classified them.
EXTRA_HIGH_INCOME_COUNTRIES = [
    "Falkland Islands",
    "Guernsey",
    "Jersey",
    "Saint Helena",
    "Montserrat",
    "Northern Cyprus",
    "Wallis and Futuna",
    "Anguilla",
]


def run() -> None:
    ds_meadow = paths.load_dataset("wb_income")
    tb = ds_meadow["wb_income_group"]

    # Drop supranational regions (rows without a region).
    tb = tb.dropna(subset=["region"])

    # Keep and rename relevant columns.
    tb = tb.reset_index()[["economy", "income_group"]].rename(columns={"economy": "country"})

    # Harmonize country names using the local mapping file.
    with open(paths.directory / "wb_income.country_mapping.json") as f:
        country_mapping = json.load(f)
    tb["country"] = tb["country"].map(lambda c: country_mapping.get(c, c))

    # Harmonize income group names.
    with open(paths.directory / "wb_income.income_mapping.json") as f:
        income_mapping = json.load(f)
    tb["income_group"] = tb["income_group"].map(lambda v: income_mapping.get(v, v))

    # Append extra countries hard-coded as "High-income countries".
    tb_extra = Table(
        pd.DataFrame({"country": EXTRA_HIGH_INCOME_COUNTRIES, "income_group": "High-income countries"})
    ).copy_metadata(tb)

    tb = pr.concat([tb, tb_extra]).sort_values("country").reset_index(drop=True)
    tb = tb.set_index("country", verify_integrity=True)
    tb.metadata.short_name = "wb_income_group"

    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)
    ds_garden.save()
