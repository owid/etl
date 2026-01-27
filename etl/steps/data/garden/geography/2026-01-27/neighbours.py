"""Garden step to compute neighbouring countries based on shared borders.

This step uses geoBoundaries data to determine which countries share borders,
and enriches the data with population information from neighbours.
"""

import geopandas as gpd
from owid.catalog import Table
from shapely import make_valid, wkt
from shapely.geometry import GeometryCollection, MultiPolygon, Polygon

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # (~1.2 seconds)
    # Load geoboundaries dataset for border geometries.
    ds_geo = paths.load_dataset("geoboundaries_cgaz")
    tb_geo = ds_geo.read("geoboundaries_cgaz")

    # Load population dataset
    ds_pop = paths.load_dataset("population")
    tb_pop = ds_pop.read("population")

    #
    # Process data.
    #
    # (~32 seconds)
    # Prepare geometries (expensive operation, do once).
    gdf = prepare_geometries(tb_geo)

    # (~1.5 minutes)
    # Compute border distances between ALL country pairs.
    tb = compute_all_border_distances(gdf)

    # (~10 seconds)
    # Compute border shares for neighbouring countries only.
    tb_borders = compute_neighbours(gdf)

    # Compute neighbour population shares.
    tb_borders = compute_neighbour_population_share(tb_borders, tb_pop)

    # Merge: add border_share to all pairs (will be NaN for non-neighbours).
    tb = combine_tables(tb_borders, tb)

    # Normalize scores so they sum to 1 per country.
    tb = _normalize_scores(tb)

    # Keep relevant columns
    tb = tb[
        ["country", "other_country", "score_neighbour_border", "score_border_distance", "score_neighbour_population"]
    ]

    # Add weighted combined scores
    tb.loc[:, "score_neighbour_c1"] = (
        tb.loc[:, "score_neighbour_border"] * 0.7 + tb.loc[:, "score_border_distance"] * 0.3
    )
    tb.loc[:, "score_neighbour_c2"] = (
        tb.loc[:, "score_neighbour_border"] * 0.5 + tb.loc[:, "score_border_distance"] * 0.5
    )
    tb.loc[:, "score_neighbour_c3"] = (
        tb.loc[:, "score_neighbour_border"] * 0.3 + tb.loc[:, "score_border_distance"] * 0.7
    )

    # Create neighbour lists table.
    tb_list = create_neighbour_lists(tb)

    # Format tables
    tb = tb.format(["country", "other_country"], short_name=paths.short_name)
    tb_list = tb_list.format(["country"], short_name="neighbours_list")

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=[tb, tb_list])
    ds_garden.save()


def combine_tables(tb_borders, tb):
    """Combine border shares and neighbour population into the full centroid distances table."""
    # Convert subset to Table to ensure proper merge.
    borders_subset = Table(tb_borders[["country", "neighbour", "border_share", "score_neighbour_population"]]).rename(
        columns={"neighbour": "other_country"}
    )
    tb = tb.merge(borders_subset, on=["country", "other_country"], how="left")

    # Fill NaN values with 0 for non-neighbours.
    tb["border_share"] = tb["border_share"].fillna(0)
    tb["score_neighbour_population"] = tb["score_neighbour_population"].fillna(0)

    return tb


def prepare_geometries(tb_geo: Table) -> gpd.GeoDataFrame:
    """Parse and prepare geometries from the geo table.

    This is an expensive operation (~23 seconds) so it should be done once
    and the result reused by other functions.

    Returns a GeoDataFrame with valid, simplified geometries.
    """
    # Convert to GeoDataFrame.
    tb_geo = tb_geo.reset_index()

    # Parse geometry as WKT (the actual format in our data). (~5s)
    tb_geo["geometry"] = tb_geo["geometry"].apply(lambda x: wkt.loads(x) if x is not None else None)

    # Create GeoDataFrame.
    gdf = gpd.GeoDataFrame(tb_geo, geometry="geometry", crs="EPSG:4326")

    # Filter to keep only ADM0 (sovereign states) - exclude disputed territories.
    gdf = gdf[(gdf["territory_type"] == "ADM0") | (gdf["country"].isin(["Palestine"]))].copy()

    # Remove rows with null geometries.
    gdf = gdf[gdf.geometry.notna()].copy()

    # (~ 20 seconds) Fix invalid geometries (e.g., Austria has self-intersecting polygons).
    # Apply make_valid to all geometries to handle self-intersections.
    gdf["geometry"] = gdf.geometry.apply(make_valid)

    # Extract polygon parts from GeometryCollections (make_valid can create these).
    # GeometryCollections don't have a valid .boundary property, so we need to
    # convert them to MultiPolygons by extracting only the polygon parts.
    gdf["geometry"] = gdf.geometry.apply(_extract_polygons)

    # After make_valid, filter out any empty or null geometries.
    gdf = gdf[gdf.geometry.notna() & ~gdf.geometry.is_empty].copy()

    # Reset index for clean join operations.
    gdf = gdf.reset_index(drop=True)

    # Create simplified geometries for faster spatial operations.
    simplify_tolerance = 0.005  # ~0.5 km tolerance
    gdf["simplified"] = gdf.geometry.simplify(simplify_tolerance, preserve_topology=True)

    return gdf


def _extract_polygons(geom):
    """Extract polygon parts from a geometry, handling GeometryCollections."""
    if geom is None:
        return None
    if isinstance(geom, (Polygon, MultiPolygon)):
        return geom
    if isinstance(geom, GeometryCollection):
        polygons = [g for g in geom.geoms if isinstance(g, (Polygon, MultiPolygon))]
        if not polygons:
            return None
        if len(polygons) == 1:
            return polygons[0]
        # Flatten any MultiPolygons
        all_polys = []
        for p in polygons:
            if isinstance(p, Polygon):
                all_polys.append(p)
            else:
                all_polys.extend(p.geoms)
        return MultiPolygon(all_polys) if all_polys else None
    return geom


def compute_neighbours(gdf: gpd.GeoDataFrame) -> Table:
    """Compute which countries share borders using geometry data.

    Uses spatial join with R-tree indexing for O(n log n) performance
    instead of nested loops O(n²).

    Returns a table with country pairs that share a border and their border_share ratio.
    """
    # Use a small buffer to account for small gaps in borders (tolerance for touching).
    # Buffer in degrees (~111m per 0.001 degree at equator).
    buffer_distance = 0.01  # ~1.1 km buffer

    # Buffer the simplified geometries for spatial join.
    gdf = gdf.copy()
    gdf["buffered"] = gdf["simplified"].buffer(buffer_distance)
    gdf_buffered = gdf.set_geometry("buffered")

    # Use spatial join with R-tree indexing (much faster than nested loops).
    # This finds all pairs where buffered geometry of left intersects geometry of right.
    joined = gpd.sjoin(
        gdf_buffered[["country", "buffered"]],
        gdf[["country", "geometry"]],
        how="inner",
        predicate="intersects",
    )

    # Filter out self-matches.
    joined = joined[joined["country_left"] != joined["country_right"]].copy()

    # Rename columns for clarity.
    neighbours_df = joined[["country_left", "country_right"]].rename(
        columns={"country_left": "country", "country_right": "neighbour"}
    )

    # Remove duplicates.
    neighbours_df = neighbours_df.drop_duplicates()

    # Compute border_share: ratio of shared border to country's total border length.
    # Use simplified geometries for faster computation (still accurate for relative shares).
    geom_lookup = gdf.set_index("country")["simplified"].to_dict()
    # Filter out None and empty geometries from lookup.
    geom_lookup = {c: g for c, g in geom_lookup.items() if g is not None and not g.is_empty}
    boundary_lengths = {c: g.boundary.length for c, g in geom_lookup.items()}

    border_shares = []
    for _, row in neighbours_df.iterrows():
        country = row["country"]
        neighbour = row["neighbour"]

        country_geom = geom_lookup.get(country)
        neighbour_geom = geom_lookup.get(neighbour)

        if country_geom is None or neighbour_geom is None:
            border_shares.append(None)
            continue

        # Compute intersection of the two geometries.
        try:
            intersection = country_geom.intersection(neighbour_geom)
            # Shared border length (for polygons, intersection of touching polygons is a line).
            shared_length = intersection.length
            # Total border length of the country.
            total_length = boundary_lengths.get(country, 0)

            if total_length > 0:
                border_share = shared_length / total_length
            else:
                border_share = 0.0
        except Exception:
            border_share = None

        border_shares.append(border_share)

    neighbours_df["border_share"] = border_shares

    # Convert to Table.
    tb_neighbours = Table(neighbours_df)

    return tb_neighbours


def compute_neighbour_population_share(tb_neighbours: Table, tb_pop: Table) -> Table:
    """Compute the population share of each neighbour among all neighbours.

    For each country, calculates what fraction of total neighbour population
    each individual neighbour represents. Scores sum to 1 per country.

    Args:
        tb_neighbours: Table with country-neighbour pairs and border_share.
        tb_pop: Population table with country, year, population columns.

    Returns:
        Table with added score_neighbour_population column.
    """
    # Get most recent population for each country (use 2024 as reference year).
    reference_year = 2024
    pop_recent = tb_pop[tb_pop["year"] == reference_year][["country", "population"]].copy()

    # Create population lookup.
    pop_lookup = pop_recent.set_index("country")["population"].to_dict()

    # Add neighbour population to the table.
    tb_neighbours = tb_neighbours.copy()
    tb_neighbours["neighbour_population"] = tb_neighbours["neighbour"].map(pop_lookup)

    # Compute total neighbour population per country.
    total_neighbour_pop = tb_neighbours.groupby("country")["neighbour_population"].transform("sum")

    # Compute score: neighbour population / total neighbour population.
    tb_neighbours["score_neighbour_population"] = tb_neighbours["neighbour_population"] / total_neighbour_pop

    # Handle edge cases (countries with no population data for any neighbour).
    tb_neighbours["score_neighbour_population"] = tb_neighbours["score_neighbour_population"].fillna(0)

    # Drop the intermediate column.
    tb_neighbours = tb_neighbours.drop(columns=["neighbour_population"])

    return tb_neighbours


def compute_all_border_distances(gdf: gpd.GeoDataFrame) -> Table:
    """Compute minimum border-to-border distances between ALL country pairs.

    Returns a table with columns: country, other_country, border_distance, score_distance.
    The score_distance is normalized so that for each country, scores sum to 1
    (closer countries get higher scores).

    Uses Shapely's .distance() method which computes minimum Euclidean distance
    between two geometries. For neighboring countries (touching borders), this
    returns 0.

    With ~159 countries, this produces ~25k pairs. Uses combinations (symmetric)
    to reduce computations by 50%.
    """
    from itertools import combinations

    # Use more aggressive simplification for distance calculation only.
    # Tolerance 0.05 (~5km) reduces vertices from 572k to 76k, speeding up
    # distance calculation from ~90s to ~1.3s while still correctly detecting neighbors.
    distance_tolerance = 0.05
    gdf = gdf.copy()
    gdf["distance_geom"] = gdf["simplified"].simplify(distance_tolerance, preserve_topology=True)

    # Create geometry lookup from distance-optimized geometries.
    geom_lookup = gdf.set_index("country")["distance_geom"].to_dict()
    geom_lookup = {c: g for c, g in geom_lookup.items() if g is not None and not g.is_empty}
    countries = list(geom_lookup.keys())

    # Compute using combinations (symmetric: A→B == B→A) - 50% fewer computations.
    distance_map = {}
    for country_a, country_b in combinations(countries, 2):
        dist = geom_lookup[country_a].distance(geom_lookup[country_b])
        distance_map[(country_a, country_b)] = dist
        distance_map[(country_b, country_a)] = dist

    # Build full table with all pairs.
    rows = []
    for country in countries:
        for other_country in countries:
            if country != other_country:
                rows.append(
                    {
                        "country": country,
                        "other_country": other_country,
                        "border_distance": distance_map[(country, other_country)],
                    }
                )

    distances_df = Table(rows)

    # Normalize to score (inverse distance, sum to 1 per country).
    epsilon = 1e-10
    distances_df["inverse_distance"] = 1 / (distances_df["border_distance"] + epsilon)
    distances_df["score_distance"] = distances_df.groupby("country")["inverse_distance"].transform(
        lambda x: x / x.sum() if x.sum() > 0 else 0
    )
    distances_df = distances_df.drop(columns=["inverse_distance"])

    return distances_df


def _normalize_scores(df):
    """Normalize scores so they sum to 1 per country.

    - score_neighbour_border: proportional to border_share (0 for non-neighbours)
    - score_border_distance: already computed, just rename
    """
    # Score for borders: normalize border_share to sum to 1 per country.
    # For countries with no land borders (islands), all scores will be 0.
    df["score_neighbour_border"] = df.groupby("country")["border_share"].transform(
        lambda x: x / x.sum() if x.sum() > 0 else 0
    )

    # Rename score_distance to score_border_distance.
    df = df.rename(columns={"score_distance": "score_border_distance"})

    return df


def create_neighbour_lists(tb: Table) -> Table:
    """Create table with sorted neighbour lists for each country.

    For each country, creates three lists of neighbours sorted by different criteria:
    - neighbours_1: sorted by border share score
    - neighbours_2: sorted by combined score (50% border + 50% distance)
    - neighbours_3: sorted by population share score

    Each list contains at least 10 countries. If fewer than 10 actual neighbours
    exist, remaining slots are filled using border distance (nearest countries).

    Lists are stored as semicolon-separated strings.
    """
    results = []
    for country in tb["country"].unique():
        country_data = tb[tb["country"] == country]

        # Get neighbours sorted by each score.
        neighbours_1 = _get_top_neighbours(country_data, "score_neighbour_border", 10)
        neighbours_2 = _get_top_neighbours(country_data, "score_neighbour_c2", 10)
        neighbours_3 = _get_top_neighbours(country_data, "score_neighbour_c3", 10)
        neighbours_4 = _get_top_neighbours(country_data, "score_neighbour_population", 10)

        # Get top 10 by border distance (no fallback needed).
        neighbours_10 = (
            country_data.sort_values("score_border_distance", ascending=False)["other_country"].head(10).tolist()
        )

        # Convert lists to semicolon-separated strings for storage.
        results.append(
            {
                "country": country,
                "neighbours_1": ";".join(neighbours_1),
                "neighbours_2": ";".join(neighbours_2),
                "neighbours_3": ";".join(neighbours_3),
                "neighbours_4": ";".join(neighbours_4),
                "neighbours_10": ";".join(neighbours_10),
            }
        )

    return Table(results)


def _get_top_neighbours(country_data, score_col: str, min_count: int = 10) -> list:
    """Get top neighbours by score, filling with centroid distance if needed.

    Args:
        country_data: DataFrame filtered to a single country's pairs.
        score_col: Column name to sort by (descending).
        min_count: Minimum number of neighbours to return.

    Returns:
        List of country names, sorted by score then filled by distance.
    """
    # Sort by score (descending).
    sorted_by_score = country_data.sort_values(score_col, ascending=False)

    # Get neighbours with non-zero score.
    with_score = sorted_by_score[sorted_by_score[score_col] > 0]["other_country"].tolist()

    # If not enough, fill with border distance.
    if len(with_score) < min_count:
        # Get remaining by border distance (excluding already selected).
        remaining = sorted_by_score[~sorted_by_score["other_country"].isin(with_score)]
        by_distance = remaining.sort_values("score_border_distance", ascending=False)
        fill_count = min_count - len(with_score)
        with_score.extend(by_distance["other_country"].head(fill_count).tolist())

    return with_score[:min_count]
