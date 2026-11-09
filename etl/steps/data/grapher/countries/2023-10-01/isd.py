"""Load a garden dataset and create a grapher dataset."""

from shared import fill_gaps_with_zeroes

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("isd")

    # Read table from garden dataset.
    tb_regions = ds_garden["isd_regions"]
    tb_countries = ds_garden["isd_countries"]

    #
    # Process data.
    #
    tb_regions = tb_regions.rename_index_names({"region": "country"})
    tb_countries = tb_countries.reset_index().rename(columns={"id": "is_present"})
    tb_countries["is_present"] = 1
    tb_countries["is_present"].metadata = tb_regions["number_countries"].m

    # Fill zeroes
    column_index = ["year", "country"]
    tb_countries = fill_gaps_with_zeroes(tb_countries, columns=column_index)
    # Store the sovereign state flag as No/Yes, so the Swimlane chart can show it.
    # NOTE: On each update, check that the flag is still 0/1. The recoding fails on any other value.
    unknown = set(tb_countries["is_present"].dropna().unique()) - {0, 1}
    assert not unknown, f"`is_present` has values other than 0 and 1: {unknown}"
    metadata = tb_countries["is_present"].metadata
    tb_countries["is_present"] = tb_countries["is_present"].map({0: "No", 1: "Yes"}).astype("category")
    tb_countries["is_present"].metadata = metadata
    tb_countries["is_present"].metadata.type = "string"
    tb_countries["is_present"].metadata.sort = ["No", "Yes"]

    tb_countries = tb_countries.set_index(column_index, verify_integrity=True)

    #
    # Save outputs.
    #
    tables = [
        tb_regions,
        tb_countries,
    ]
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(
        dest_dir,
        tables=tables,
        check_variables_metadata=True,
        default_metadata=ds_garden.metadata,
    )

    # Save changes in the new grapher dataset.
    ds_grapher.save()
