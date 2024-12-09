"""Load a meadow dataset and create a garden dataset."""

import warnings

import geopandas as gpd
import pandas as pd
import structlog
from geopy.distance import geodesic
from shapely import wkt
from shapely.ops import nearest_points
from tqdm import tqdm

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

LOG = structlog.get_logger()


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("migrant_stock")
    ds_nat_earth = paths.load_dataset("nat_earth_110")

    # Read table from meadow dataset.
    tb = ds_meadow["migrant_stock_dest_origin"].reset_index()
    tb_countries = ds_nat_earth["nat_earth_110"].reset_index()

    # Read natural earth data
    # Convert the geometry string column to a Shapely object.
    tb_countries["geometry"] = tb_countries["geometry"].apply(wkt.loads)
    world = gpd.GeoDataFrame(tb_countries, geometry="geometry")

    # use World Geodetic System 1984 as projection
    world = world.set_crs("EPSG:4326")

    # harmonize country names
    world = geo.harmonize_countries(
        df=world,  # type: ignore
        country_col="name",
        countries_file=paths.country_mapping_path,
    )

    # Calculate distance matrix (in km) (catch warnings to ignore "invalid value encountered" warning)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=RuntimeWarning)
        distance_matrix = calculate_distance_matrix(world)

    ## Add distances to migration flows table
    # Remove countries not included in nat earth data and "Other" from country destination or country origin columns
    cty_no_data = [
        "Other",
        "Tokelau",
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
    tb["distance"] = tb.apply(
        lambda row: distance_matrix.loc[row["country_origin"], row["country_destination"]], axis=1
    )
    tb["distance"] = tb["distance"].apply(get_min_distance).astype("Float64")

    migrant_groups = tb.groupby(["country_origin", "year"])
    med_distance = migrant_groups.apply(calc_median).reset_index()
    med_distance["median_distance"] = med_distance[0].apply(lambda x: x[0])
    med_distance["total_emigrants"] = med_distance[0].apply(lambda x: x[1])
    med_distance = med_distance.drop(columns=[0]).copy_metadata(tb)

    med_distance.metadata.dataset.short_name = "migration_distance"
    med_distance.metadata.short_name = "migration_distance"

    for col in med_distance.columns:
        med_distance[col].metadata.origins = tb["country_origin"].m.origins + tb_countries["name"].m.origins

    med_distance = med_distance.format(["country_origin", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[med_distance], check_variables_metadata=True, default_metadata=None)

    # Save changes in the new garden dataset.
    ds_garden.save()


def get_min_distance(distance):
    """Get the minimum distance between two countries.
    Sometimes the distance is a DataFrame or Series (if country is represented by multiple polygons). Then we take the minimum distance."""
    if isinstance(distance, pd.Series):
        return distance.min()
    elif isinstance(distance, pd.DataFrame):
        return distance.min().min()
    else:
        return distance


def calc_median(group, col="distance"):
    """Calculate the median distance for each country origin and year."""
    group = group.sort_values(by=col)
    group["cumulative_journeys"] = group["migrants_all_sexes"].cumsum()
    total_journeys = group["migrants_all_sexes"].sum()
    median_journey = total_journeys / 2
    median_dist = group[group["cumulative_journeys"] >= median_journey].iloc[0]["distance"]
    return median_dist, total_journeys


def calculate_distance_matrix(world):
    # Create an empty distance matrix
    distance_matrix = pd.DataFrame(index=world["name"], columns=world["name"])

    for i, row1 in tqdm(world.iterrows(), total=len(world), desc="Calculating distance matrix"):
        for j, row2 in world.iterrows():
            if i == j:
                distance_matrix.iloc[i, j] = 0  # Distance to itself
            elif i > j:
                distance_matrix.iloc[i, j] = distance_matrix.iloc[j, i]  # Distance is symmetric
            else:
                # Get the nearest points between two geometries
                point1, point2 = nearest_points(row1.geometry, row2.geometry)  # type: ignore

                # Calculate geodesic distance between the nearest points
                distance_matrix.iloc[i, j] = geodesic((point1.y, point1.x), (point2.y, point2.x)).kilometers  # type: ignore

    return distance_matrix
