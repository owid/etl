"""Load a meadow dataset and create a garden dataset."""

import geopandas as gpd
import pandas as pd

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# GeoJson file with country boundaries
URL_TINY_CTY = "https://d2ad6b4ur7yvpq.cloudfront.net/naturalearth-3.3.0/ne_50m_admin_0_tiny_countries.geojson"


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("migrant_stock")

    # Read table from meadow dataset.
    tb = ds_meadow["migrant_stock_dest_origin"].reset_index()

    # read natural earth data
    world = gpd.read_file(gpd.datasets.get_path("naturalearth_lowres"))  # type: ignore
    tiny_countries = gpd.read_file(URL_TINY_CTY)

    # use web mercator projection
    world = world.to_crs("EPSG:3857")[["name", "geometry"]]
    tiny_countries = tiny_countries.to_crs("EPSG:3857")[["admin", "geometry"]]
    tiny_countries = tiny_countries.rename(columns={"admin": "name"})

    # add tiny countries to world
    world = pd.concat([world, tiny_countries], ignore_index=True)

    # harmonize country names
    world = geo.harmonize_countries(
        df=world,  # type: ignore
        country_col="name",
        countries_file=paths.country_mapping_path,
    )

    # Calculate distance matrix (in km)
    distance_matrix = (
        world.geometry.apply(lambda geom1: world.geometry.apply(lambda geom2: geom1.distance(geom2))) / 1e3
    )

    # Add country names to the distance matrix
    distance_df = distance_matrix.copy()
    distance_df.index = world["name"]
    distance_df.columns = world["name"]

    ## Add distances to migration flows table
    # Remove "Other" and countries without distance data from country destination or country origin columns
    cty_no_data = [
        "Other",
        "Tokelau",
        "Cape Verde",
        "Montserrat",
        "Bonaire Sint Eustatius and Saba",
        "French Guiana",
        "Guadeloupe",
        "Martinique",
        "Reunion",
        "Channel Islands",
        "Mayotte",
    ]
    cty_data = [cty for cty in tb["country_origin"].unique() if cty not in cty_no_data]
    tb = tb[(tb["country_destination"].isin(cty_data)) & (tb["country_origin"].isin(cty_data))]

    # Add distance to the table
    tb["distance"] = tb.apply(lambda row: distance_df.loc[row["country_origin"], row["country_destination"]], axis=1)
    tb["distance"] = tb["distance"].apply(get_min_distance).astype("Float64")

    migrant_groups = tb.groupby(["country_origin", "year"])
    med_distance = migrant_groups.apply(calc_median).reset_index()
    med_distance["median_distance"] = med_distance[0].apply(lambda x: x[0])
    med_distance["total_journeys"] = med_distance[0].apply(lambda x: x[1])
    med_distance = med_distance.drop(columns=[0])

    tb = med_distance.format(["country_origin", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def get_min_distance(distance):
    """Get the minimum distance between two countries."""
    if isinstance(distance, pd.Series):
        return distance.min()
    elif isinstance(distance, pd.DataFrame):
        return distance.min().min()
    else:
        return distance


def calc_median(group):
    """Calculate the median distance for each country origin and year."""
    group = group.sort_values(by="distance")
    group["cumulative_journeys"] = group["migrants_all_sexes"].cumsum()
    total_journeys = group["migrants_all_sexes"].sum()
    median_journey = total_journeys / 2
    median_dist = group[group["cumulative_journeys"] >= median_journey].iloc[0]["distance"]
    return median_dist, total_journeys
