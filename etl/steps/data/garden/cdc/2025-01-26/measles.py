"""Load a meadow dataset and create a garden dataset."""

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("measles")

    # Read table from meadow dataset.
    tb = ds_meadow.read("measles")
    tb = tb.drop(columns=["notes", "disease_code", "year_code", "regions_states_code"])
    tb["disease"] = tb["disease"].str.replace("Measles, ", "")
    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
        excluded_countries_file=paths.excluded_countries_path,
        warn_on_missing_countries=False,
    )
    # Separate the national data into a separate table.
    msk = tb["country"] == "United States"
    tb_usa = tb[msk]
    tb = tb[~msk]
    # Check the values are the same
    assert sum(tb["case_count"].astype("Int64")) == sum(tb_usa["case_count"].astype("Int64"))

    #
    # Process data.
    #

    tb = tb.format(["country", "year"])
    tb_usa = tb_usa.format(["country", "year"])
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb, tb_usa], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
