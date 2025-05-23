"""Load a meadow dataset and create a garden dataset."""

import pandas as pd

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


FLAGS_EXPECTED = [
    {
        "countries": [
            "Chile",
            "South Korea",
        ],
        "flags": {
            0,
        },
    },
    {
        "countries": [
            "Czech Republic",
            "Denmark",
            "France",
            "Greece",
            "Italy",
            "Lithuania",
            "Netherlands",
            "Norway",
            "Spain",
            "Switzerland",
            "England/Wales",
        ],
        "flags": {
            1,
        },
    },
    {
        "countries": [
            "Australia",
            "United States",
            "Uruguay",
        ],
        "flags": {
            2,
        },
    },
    {
        "countries": [
            "Austria",
            "Canada",
            "Finland",
            "Germany",
            "Japan",
        ],
        "flags": {
            1,
            99,
        },
    },
    {
        "countries": {
            "Iceland",
            "New Zealand",
            "Sweden",
            "Scotland",
        },
        "flags": {
            0,
            1,
        },
    },
]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("multiple_births")

    # Read table from meadow dataset.
    tb = ds_meadow.read("multiple_births")

    # Harmonize country names
    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
        excluded_countries_file=paths.excluded_countries_path,
    )

    # Sanity check
    check_stillbirths(tb)

    # Adapt flags
    tb = adapt_stillbirths_flags(tb)

    # Add triplets or more category
    tb["triplets_plus_deliveries"] = tb["multiple_deliveries"] - tb["twin_deliveries"]
    tb["triplets_plus_rate"] = (1_000 * tb["triplets_plus_deliveries"] / tb["total_deliveries"]).round(2)

    # Estimate singleton_rate
    tb["singleton_rate"] = (1_000 * tb["singletons"] / tb["total_deliveries"]).round(2)

    # Estimate share
    columns = ["singleton", "twinning", "triplets_plus", "multiple"]
    for col in columns:
        tb[f"{col}_share"] = 0.1 * tb[f"{col}_rate"]

    # Estimate ratios
    tb["children_delivery_ratio"] = (
        1_000 * (tb["multiple_children"] + tb["singletons"]) / tb["total_deliveries"]
    ).round(3)
    tb["children_multiple_delivery_ratio"] = (1_000 * tb["multiple_children"] / tb["multiple_deliveries"]).round(3)
    tb["multiple_to_singleton_ratio"] = (1_000 * tb["multiple_deliveries"] / tb["singletons"]).round(3)

    # Remove outliers
    tb = remove_outliers(tb)

    # Keep relevant columns
    tb = tb[
        [
            # Index
            "country",
            "year",
            # Absolute numbers
            "singletons",
            "multiple_deliveries",
            "twin_deliveries",
            "triplets_plus_deliveries",
            "total_deliveries",
            # Rates
            "singleton_rate",
            "multiple_rate",
            "twinning_rate",
            "triplets_plus_rate",
            # Shares
            "singleton_share",
            "multiple_share",
            "twinning_share",
            "triplets_plus_share",
            # Ratios
            "children_delivery_ratio",
            "children_multiple_delivery_ratio",
            "multiple_to_singleton_ratio",
            # Births
            "multiple_children",
        ]
    ]

    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def check_stillbirths(tb):
    """Datapoints (country-year) are given using different methodologies.

    This is communicated in the 'stillbirths' column, which can vary from country to country (and year to year):

    0: Stillbirths not included
    1: Stillbirths included
    2: Mixed (stillbirths included in some cases only)
    99: Unsure

    Reference: https://www.twinbirths.org/en/data-metadata/, Table 1
    """
    # Check that the stillbirths flags are as expected.
    for expected in FLAGS_EXPECTED:
        countries_expected = expected["countries"]
        flags_expected = expected["flags"]

        flags_actual = set(tb.loc[tb["country"].isin(countries_expected), "stillbirths"].unique())

        assert (
            flags_actual == flags_expected
        ), f"Expected flags {flags_expected} for countries {countries_expected} are not as expected! Found: {flags_actual}"

    # Check Overlaps
    ## There are overlaps in New Zealand and Sweden
    x = tb.groupby(["country", "year"], as_index=False).stillbirths.nunique()
    countries_overlap_expected = {"New Zealand", "Sweden"}
    countries_overlap_actually = set(x.loc[x["stillbirths"] != 1, "country"].unique())
    assert (
        countries_overlap_actually == countries_overlap_expected
    ), f"Expected countries with overlaps {countries_overlap_expected} are not as expected! Found: {countries_overlap_actually}"


def adapt_stillbirths_flags(tb):
    # Iceland: Remove even there is no replacement. Keep only 1.
    country = "Iceland"
    flag = (tb["country"] == country) & (tb["stillbirths"] == 0)
    assert len(tb.loc[flag]) == 5, f"Unexpected number of values for {country}"
    tb = tb.loc[~flag]

    # If there is 1 and 0, keep 1.
    flag = tb.sort_values("stillbirths").duplicated(subset=["country", "year"], keep="last")
    assert set(tb.loc[flag, "stillbirths"].unique()) == {
        0
    }, "Removed rows because of duplicate country-year values should only be stillbirths=0!"
    tb = tb.loc[~flag]

    # Sweden: Remove, ensure there is actually redundancy. Keep 1.
    assert set(tb.loc[tb["country"] == "Sweden", "stillbirths"].unique()) == {1}, "Unexpected stillbirths=0 for Sweden!"

    return tb


def get_summary_methodology_sb(tb):
    tbx = tb.groupby("country", as_index=False)["stillbirths"].agg(["nunique", "unique"])

    # Only one method
    tbx1 = tbx.loc[tbx["nunique"] == 1]
    tbx1["unique"] = tbx1["unique"].apply(lambda x: x[0])
    tbx1 = tbx1[["country", "unique"]].sort_values("unique")

    # Multiple methods
    tbx2 = tbx.loc[tbx["nunique"] > 1]
    countries_mult = set(tbx2["country"].unique())
    tb[tb["country"].isin(countries_mult)].groupby(["country", "stillbirths"]).agg({"year": ("min", "max")})


def remove_outliers(tb):
    flag = (tb["country"] == "England and Wales") & (tb["year"] == 1938)
    assert (
        tb.loc[flag, "children_multiple_delivery_ratio"] >= 4000
    ).all(), "Unexpected outlier for England and Wales in 1938"
    tb.loc[flag, ["multiple_children", "children_multiple_delivery_ratio", "children_delivery_ratio"]] = pd.NA
    flag = (tb["country"] == "England and Wales") & (tb["year"] == 1939)
    assert (
        tb.loc[flag, "children_multiple_delivery_ratio"] <= 1500
    ).all(), "Unexpected outlier for England and Wales in 1938"
    tb.loc[flag, ["multiple_children", "children_multiple_delivery_ratio", "children_delivery_ratio"]] = pd.NA

    return tb
