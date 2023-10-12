"""Create a dummy dataset with indicators that have very different metadata situations."""

from owid.catalog import Origin, Source, Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    # Create a dummy data table.
    tb = Table({"country": ["France", "Portugal", "Portugal"], "year": [2022, 2022, 2023]}, short_name=paths.short_name)

    # Create an origin with almost no metadata.
    origin_with_no_metadata = Origin(
        title="Origin with almost no metadata", producer="Producer with almost no metadata"
    )

    # Create an empty source.
    source_empty = Source()

    # Indicator has no metadata.
    indicator = "origin_no_metadata"
    tb[indicator] = [1, 2, 3]
    tb[indicator].metadata.origins = [origin_with_no_metadata]

    # Indicator that has only an empty source.
    indicator = "only_source_empty"
    tb[indicator] = [1, 2, 3]
    tb[indicator].metadata.sources = [source_empty]

    # Set an appropriate index.
    tb = tb.set_index(["country", "year"], verify_integrity=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    ds_garden.save()
