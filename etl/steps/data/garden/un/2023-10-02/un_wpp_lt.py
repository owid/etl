"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
# Column rename and selection
COLUMNS_RENAME = {
    "time": "year",
    "agegrpstart": "age",
}
COLUMNS_INDEX = [
    "location",
    "year",
    "sex",
    "age",
]
COLUMNS_INDICATORS = [
    "central_death_rate",
    "probability_of_death",
    "probability_of_survival",
    "number_survivors",
    "number_deaths",
    "number_person_years_lived",
    "survivorship_ratio",
    "number_person_years_remaining",
    "life_expectancy",
    "average_survival_length",
]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("un_wpp_lt")

    # Read table from meadow dataset.
    paths.log.info("load tables, concatenate.")
    tb = pr.concat(
        [
            ds_meadow["un_wpp_lt_all"].reset_index(),
            ds_meadow["un_wpp_lt_f"].reset_index(),
            ds_meadow["un_wpp_lt_m"].reset_index(),
        ],
        short_name=paths.short_name,
    ).reset_index()

    #
    # Process data.
    #
    # Sanity check
    assert (tb["agegrpspan"].isin([1, -1])).all() and (
        tb.loc[tb["agegrpspan"] == -1, "agegrpstart"] == 100
    ).all(), "Age group span should always be of 1, except for 100+ (-1)"

    # Rename columns, select columns
    tb = tb.rename(columns=COLUMNS_RENAME)

    # Change 100 -> 100+
    tb.loc[tb["age"] == 100, "age"] = "100+"

    # Scale central death rates
    paths.log.info("scale indicators to make them more.")
    tb["central_death_rate"] = tb["central_death_rate"] * 1000
    tb["probability_of_death"] = tb["probability_of_death"] * 100
    tb["probability_of_survival"] = tb["probability_of_survival"] * 100

    # Harmonize country names.
    paths.log.info("harmonise country names.")
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path, country_col="location")

    # Harmonize sex sex
    tb["sex"] = tb["sex"].map({"Total": "both", "Male": "male", "Female": "female"})
    assert tb["sex"].notna().all(), "NaNs detected after mapping sex values!"

    # DTypes
    tb = tb.astype(
        {
            "age": str,
        }
    )

    # Set index
    tb = tb.set_index(COLUMNS_INDEX, verify_integrity=True)[COLUMNS_INDICATORS]

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
