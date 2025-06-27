"""Load a meadow dataset and create a garden dataset."""

from shapely import wkt

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("wb_admin_boundaries")

    # Read table from meadow dataset.
    tb = ds_meadow.read("wb_admin_boundaries")

    #
    # Process data.
    #
    # Get centroids
    # Convert the geometry string column to a Shapely object.
    tb["geometry_wkt"] = tb["geometry"].apply(wkt.loads)

    tb["latitude_centroid"] = tb["geometry_wkt"].apply(calculate_centroid_latitude)
    # Convert from string to float
    tb["latitude_centroid"] = tb["latitude_centroid"].astype(float)
    # Create an absolute version
    tb["absolute_latitude"] = tb["latitude_centroid"].abs()

    tb = tb.drop("geometry_wkt", axis=1)

    # Harmonize country names.
    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
        excluded_countries_file=paths.excluded_countries_path,
        country_col="name",
    )

    # Improve table format.
    tb["index"] = tb.index
    tb = tb.format(["index"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()


def calculate_centroid_latitude(geometry):
    return geometry.centroid.y
