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
    ds_meadow = paths.load_dataset("sdgs_urbanization")

    # Read table from meadow dataset.
    tb = ds_meadow["un_sdg"].reset_index()

    metadata = tb.metadata

    #
    # Process data.
    #
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    average_by_country = (
        tb.groupby(["country", "year", "seriesdescription"], observed=True)["value"].mean().reset_index()
    )

    average_by_country = average_by_country.dropna(subset=["value"])
    pivot_tb = average_by_country.pivot(
        index=["country", "year"], columns="seriesdescription", values="value"
    ).reset_index()
    pivot_tb = pivot_tb.underscore().set_index(["country", "year"], verify_integrity=True)

    pivot_tb.metadata = metadata

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir,
        tables=[pivot_tb],
        check_variables_metadata=True,
        default_metadata=ds_meadow.metadata,
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
