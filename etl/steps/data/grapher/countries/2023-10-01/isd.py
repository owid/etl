"""Load a garden dataset and create a grapher dataset."""

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
    tb_countries = (
        tb_countries.reset_index()
        .set_index(["year", "country"], verify_integrity=True)
        .rename(columns={"id": "is_present"})
    )
    tb_countries["is_present"] = 1
    tb_countries["is_present"].m.origins = tb_regions["number_countries"].m.origins

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
