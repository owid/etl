"""Load a meadow dataset and create a garden dataset."""

from shapely import wkt

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("nat_earth_110")

    # Read table from meadow dataset.
    tb = ds_meadow["nat_earth_110"].reset_index()
    # Convert the geometry string column to a Shapely object.
    tb["geometry_wkt"] = tb["geometry"].apply(wkt.loads)

    tb["latitude_centroid"] = tb["geometry_wkt"].apply(calculate_centroid_latitude)
    tb = tb.drop("geometry_wkt", axis=1)
    #
    # Process data.
    #
    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
        country_col="name",
    )
    tb = tb.set_index(
        [
            "name",
        ],
        verify_integrity=True,
    )

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def calculate_centroid_latitude(geometry):
    return geometry.centroid.y
