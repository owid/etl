"""Load a meadow dataset and create a garden dataset."""

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot

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

    # Load the snapshot's origin directly: `DatasetMeta` has no `origins` field,
    # so the per-snapshot origin doesn't survive the meadow save/load round-trip.
    # The snapshot is already a transitive dep of this step via the meadow.
    base_origin = Snapshot("un/2023-08-16/un_sdg.feather").metadata.origin
    assert base_origin is not None, "Expected un_sdg snapshot to declare an origin"

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
    pivot_tb = pivot_tb.format(["country", "year"])

    pivot_tb.metadata = metadata

    for col in pivot_tb.columns:
        pivot_tb[col].metadata.origins = [base_origin]

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
