"""Load a meadow dataset and create a garden dataset."""

import pandas as pd
from owid.catalog.tables import Table, concat

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


# We will infer indicator values for some countries based on their historical equivalences.
COUNTRIES_IMPUTE = [
    {
        "country": "Colombia",
        "country_impute": "Great Colombia (former)",
        "year_min": 1821,
        "year_max": 1830,
    },
    {
        "country": "Costa Rica",
        "country_impute": "Federal Republic of Central America (former)",
        "year_min": 1824,
        "year_max": 1837,
    },
    {
        "country": "Czechia",
        "country_impute": "Czechoslovakia",
        "year_min": 1918,
        "year_max": 1992,
    },
    {
        "country": "Ecuador",
        "country_impute": "Great Colombia (former)",
        "year_min": 1821,
        "year_max": 1829,
    },
    {
        "country": "El Salvador",
        "country_impute": "Federal Republic of Central America (former)",
        "year_min": 1824,
        "year_max": 1838,
    },
    {
        "country": "Guatemala",
        "country_impute": "Federal Republic of Central America (former)",
        "year_min": 1824,
        "year_max": 1838,
    },
    {
        "country": "Honduras",
        "country_impute": "Federal Republic of Central America (former)",
        "year_min": 1824,
        "year_max": 1838,
    },
    {
        "country": "Nicaragua",
        "country_impute": "Federal Republic of Central America (former)",
        "year_min": 1824,
        "year_max": 1837,
    },
    {
        "country": "North Korea",
        "country_impute": "Korea (former)",
        "year_min": 1800,
        "year_max": 1910,
    },
    {
        "country": "Panama",
        "country_impute": "Great Colombia (former)",
        "year_min": 1821,
        "year_max": 1830,
    },
    {
        "country": "Slovakia",
        "country_impute": "Czechoslovakia",
        "year_min": 1918,
        "year_max": 1992,
    },
    {
        "country": "South Korea",
        "country_impute": "Korea (former)",
        "year_min": 1800,
        "year_max": 1910,
    },
    {
        "country": "Venezuela",
        "country_impute": "Great Colombia (former)",
        "year_min": 1821,
        "year_max": 1829,
    },
    {
        "country": "Russia",
        "country_impute": "USSR",
        "year_min": 1922,
        "year_max": 1991,
    },
    {
        "country": "Ethiopia",
        "country_impute": "Ethiopia (former)",
        "year_min": 1952,
        "year_max": 1992,
    },
    {
        "country": "Eritrea",
        "country_impute": "Ethiopia (former)",
        "year_min": 1952,
        "year_max": 1992,
    },
    {
        "country": "Pakistan",
        "country_impute": "Pakistan (former)",
        "year_min": 1947,
        "year_max": 1971,
    },
    {
        "country": "Bangladesh",
        "country_impute": "Pakistan (former)",
        "year_min": 1947,
        "year_max": 1970,
    },
]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("bmr")

    # Read table from meadow dataset.
    tb = ds_meadow["bmr"].reset_index()

    #
    # Process data.
    #
    tb = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )

    # Identify duplicate observations
    ## "Germany" in 1945 and 1990 (those years we have West and East Germany)
    ## "Yugoslavia" in 1991 (the country was dissolved) and "Yugoslavia/Serbia" in 2006 (the country was dissolved)
    tb = tb[~((tb["ccode"] == 260) & (tb["year"] == 1945))]
    tb = tb[~((tb["ccode"] == 260) & (tb["year"] == 1990))]
    tb = tb[~((tb["ccode"] == 345) & (tb["year"] == 1991))]
    tb = tb[~((tb["ccode"] == 347) & (tb["year"] == 2006))]

    # Keep relevant columns
    tb = tb[["country", "year", "democracy_omitteddata", "democracy_femalesuffrage"]]
    tb = tb.rename(
        columns={
            "democracy_omitteddata": "regime_bmr",
            "democracy_femalesuffrage": "regime_womsuffr_bmr",
        }
    )

    # Impute missing values
    tb = add_imputes(tb)

    # Refine
    ## Set NaNs to womsuff
    tb.loc[tb["regime_bmr"].isna(), "regime_womsuffr_bmr"] = pd.NA
    ## Add country codes
    tb["ccode"] = tb["country"].astype("category").cat.codes

    tb = tb.sort_values(["country", "year"])

    ## Add democracy age / experience
    ### Count the number of years since the country first became a democracy. Transition NaN -> 1 is considered as 0 -> 1.
    tb["dem_age_bmr_owid"] = tb.groupby(["country", tb["regime_bmr"].fillna(0).eq(0).cumsum()])["regime_bmr"].cumsum()
    tb["dem_age_bmr_owid"] = tb["dem_age_bmr_owid"].astype(float)
    ## Add democracy age (including women's suffrage) / experience
    ### Count the number of years since the country first became a democracy. Transition NaN -> 1 is considered as 0 -> 1.
    tb["dem_ws_age_bmr_owid"] = tb.groupby(["country", tb["regime_womsuffr_bmr"].fillna(0).eq(0).cumsum()])[
        "regime_bmr"
    ].cumsum()
    tb["dem_ws_age_bmr_owid"] = tb["dem_ws_age_bmr_owid"].astype(float)

    # Set index.
    tb = tb.set_index(["country", "year"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def add_imputes(tb) -> Table:
    """Add imputed values to the table."""
    tb_imputed = []
    for impute in COUNTRIES_IMPUTE:
        # Get relevant rows
        tb_ = tb.loc[
            (tb["country"] == impute["country_impute"])
            & (tb["year"] >= impute["year_min"])
            & (tb["year"] <= impute["year_max"])
        ].copy()
        # Sanity checks
        assert tb_.shape[0] > 0, f"No data found for {impute['country_impute']}"
        assert tb_["year"].max() == impute["year_max"], f"Missing years (max check) for {impute['country_impute']}"
        assert tb_["year"].min() == impute["year_min"], f"Missing years (min check) for {impute['country_impute']}"

        # Tweak them
        tb_ = tb_.rename(
            columns={
                "country": "regime_imputed_country_bmr_owid",
            }
        ).assign(
            **{
                "country": impute["country"],
                "regime_imputed_bmr_owid": 1,
            }
        )
        tb_imputed.append(tb_)

    tb = concat(tb_imputed + [tb], ignore_index=True)

    # Re-order columns
    tb = tb[
        [
            "country",
            "year",
            "regime_bmr",
            "regime_womsuffr_bmr",
            "regime_imputed_country_bmr_owid",
            "regime_imputed_bmr_owid",
        ]
    ]
    return tb
