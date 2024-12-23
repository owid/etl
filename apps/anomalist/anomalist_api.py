import random
import tempfile
import time
from pathlib import Path
from typing import List, Literal, Optional, Tuple, cast, get_args

import numpy as np
import pandas as pd
import structlog
from sqlalchemy.engine import Engine
from sqlalchemy.exc import NoResultFound
from sqlalchemy.orm import Session

from apps.anomalist.detectors import (
    AnomalyDetector,
    AnomalyTimeChange,
    AnomalyUpgradeChange,
    AnomalyUpgradeMissing,
    get_long_format_score_df,
)
from apps.anomalist.gp_detector import AnomalyGaussianProcessOutlier
from apps.wizard.utils.cached import load_latest_population
from apps.wizard.utils.paths import WIZARD_ANOMALIES_RELATIVE
from etl import grapher_model as gm
from etl.config import OWID_ENV
from etl.db import get_engine, read_sql
from etl.files import create_folder, upload_file_to_server
from etl.grapher.io import variable_data_table_from_catalog

log = structlog.get_logger()

# Name of index columns for dataframe.
INDEX_COLUMNS = ["entity_name", "year"]

# TODO: this is repeated in detector classes, is there a way to DRY this?
ANOMALY_TYPE = Literal["time_change", "upgrade_change", "upgrade_missing", "gp_outlier"]

# Define mapping of available anomaly types to anomaly detectors.
ANOMALY_DETECTORS = {
    detector.anomaly_type: detector
    for detector in [
        AnomalyTimeChange,
        AnomalyUpgradeChange,
        AnomalyUpgradeMissing,
        AnomalyGaussianProcessOutlier,
    ]
}


def load_detector(anomaly_type: ANOMALY_TYPE) -> AnomalyDetector:
    """Load detector."""
    return ANOMALY_DETECTORS[anomaly_type]


def get_variables_views_in_charts(
    variable_ids: Optional[List[int]] = None,
) -> pd.DataFrame:
    # Assumed base url for all charts.
    base_url = "https://ourworldindata.org/grapher/"

    # SQL query to join variables, charts, and analytics pageviews data
    query = f"""\
    SELECT
        v.id AS variable_id,
        c.id AS chart_id,
        cc.slug AS chart_slug,
        ap.views_7d,
        ap.views_14d,
        ap.views_365d
    FROM
        charts c
    JOIN
        chart_dimensions cd ON c.id = cd.chartId
    JOIN
        variables v ON cd.variableId = v.id
    JOIN
        chart_configs cc ON c.configId = cc.id
    LEFT JOIN
        analytics_pageviews ap ON ap.url = CONCAT('{base_url}', cc.slug)
    """
    if variable_ids is not None:
        query += f"WHERE v.id IN ({', '.join([str(v_id) for v_id in variable_ids])})"
    query += """\
        ORDER BY
        v.id ASC;
    """
    df = read_sql(query)
    # Handle potential duplicated rows
    df = df.drop_duplicates().reset_index(drop=True)

    if len(df) == 0:
        df = pd.DataFrame(columns=["variable_id", "chart_id", "chart_slug", "views_7d", "views_14d", "views_365d"])

    return df.astype(
        {
            "views_7d": "Int64",
            "views_14d": "Int64",
            "views_365d": "Int64",
        }
    ).fillna(0)


def renormalize_score(
    score: pd.Series, min_value: float, min_score: float, max_value: float, max_score: float
) -> pd.Series:
    # Normalize the population score to the range 0, 1.
    factor = (max_score - min_score) / (np.log(max_value) - np.log(min_value))
    constant = min_score - factor * np.log(min_value)
    # Clip population between the minimum and maximum defined above.
    score_clipped = score.clip(lower=min_value, upper=max_value)
    # Renormalize score.
    score_renormalized = factor * np.log(score_clipped) + constant  # type: ignore
    # Sanity checks.
    if len(score_renormalized[score_renormalized.notnull()]) > 0:
        # NOTE: Sometimes the input score may be empty (e.g. because indicators don't have analytics data, or are not used in charts) or nan (e.g. when renormalizing the population score on an array of entities with no population data, like "Northern Hemisphere").
        assert np.float32(score_renormalized.min()) >= min_score, f"Expected a minimum score >= {min_score}."
        assert np.float32(score_renormalized.max()) <= max_score, f"Expected a maximum score <= {max_score}."

    return score_renormalized


# Function to format population numbers.
def pretty_print_number(number):
    if pd.isna(number):
        return "?"
    elif int(number) >= 1e9:
        return f"{number/1e9:.1f}B"
    elif number >= 1e6:
        return f"{number/1e6:.1f}M"
    elif number >= 1e3:
        return f"{number/1e3:.1f}k"
    else:
        return f"{int(number)}"


def debug_population_score_examples(df_score_population: pd.DataFrame) -> None:
    # Prepare an empty list to store the output.
    output_list = []

    # For each target score, find the closest entity (for a given reference year).
    reference_year = 2023
    _df = df_score_population[df_score_population["year"] == reference_year].copy()
    for target_score in [round(0.1 * i, 1) for i in range(1, 11)]:
        # No exact matches, find entities closest to the target score
        _df["score_diff"] = (_df["score_population"] - target_score).abs()
        # Sort by score_diff ascending and population descending
        sorted_df = _df.sort_values(by=["score_diff", "population"], ascending=[True, False])
        closest_row = sorted_df.iloc[0]
        score_str = f"{closest_row['score_population']:.1f}"
        population_str = pretty_print_number(closest_row["population"])
        output_list.append(f"* {closest_row['entity_name']} (population {population_str}): ~{score_str}")

    # Also, for reference, include some important reference regions.
    output_list.append("\nOther references:")
    for region in [
        "Africa",
        "Asia",
        "Europe",
        "North America",
        "South America",
        "Oceania",
        "Brazil",
        "China",
        "India",
        "Russia",
        "United States",
    ]:
        _df_country = _df[_df["entity_name"] == region]
        score_str = f"{_df_country['score_population'].item():.2f}"
        output_list.append(
            f"* {region} (population {pretty_print_number(_df_country['population'].item())}): ~{score_str}"
        )

    # Print the output list.
    for line in output_list:
        print(line)


def add_population_score(df_reduced: pd.DataFrame) -> pd.DataFrame:
    """Add a population score to the scores dataframe.

    The population score is defined under the following premises:
    NOTE: The following values are defined below inside the function, double check in case they change in the future.
    * The score should be around 0.1 for a population <= 1M.
    * The score should be close to 1 for a population of >=8B.
    * The score should be 0.5 for regions that are not included in our population dataset (e.g. "Middle East").

    For reference, the result assigns the following scores (with population calculated on year 2023):
    NOTE: The following lines can be recalculated using debug_population_score_examples.
    * Fiji (population ~924.1k): ~0.1
    * Gambia (population ~2.7M): ~0.2
    * Turkmenistan (population ~7.4M): ~0.3
    * Kazakhstan (population ~20.3M): ~0.4
    * Myanmar (population ~54.1M): ~0.5
    * Russia (population ~145.4M): ~0.6
    * Northern America (UN) (population ~382.9M): ~0.7
    * Americas (UN) (population ~1.0B): ~0.8
    * Upper-middle-income countries (population ~2.8B): ~0.9
    * World (population ~8.1B): ~1.0

    Other references:
    * Africa (population ~1.5B): ~0.83
    * Asia (population ~4.8B): ~0.95
    * Europe (population ~746.9M): ~0.76
    * North America (population ~608.8M): ~0.74
    * South America (population ~433.0M): ~0.71
    * Oceania (population ~45.6M): ~0.48
    * Brazil (population ~211.1M): ~0.64
    * China (population ~1.4B): ~0.83
    * India (population ~1.4B): ~0.83
    * Russia (population ~145.4M): ~0.60
    * United States (population ~343.5M): ~0.68

    Parameters
    ----------
    df_reduced : pd.DataFrame
        Scores data (with a column "entity_name", "year", and different "score*" columns).

    Returns
    -------
    df_reduced : pd.DataFrame
        Scores data after including a "score_population" column.

    """
    # Main parameters:
    # Minimum score assigned to any country.
    min_population_score = 0.1
    # Minimum population to assign a score (any population below this value will be assigned the minimum score).
    min_population = 1e6
    # Maximum score assigned to any region.
    max_population_score = 1.0
    # Maximum population to assign a score (any population above this value will be assigned the maximum score).
    max_population = 8e9
    # Score to assign to all entities for which we don't have population data.
    missing_population_score = 0.5

    # Load the latest population data.
    df_population = load_latest_population()

    # Convert to strings
    df_population = df_population.astype({"entity_name": str, "year": int}).sort_values(["year", "entity_name"])
    df_reduced = df_reduced.astype({"entity_name": str, "year": int}).sort_values(["year", "entity_name"])

    # First, get the unique combinations of country-years in the scores dataframe, and add population to it.
    df_score_population = pd.merge_asof(
        df_reduced[["entity_name", "year"]].drop_duplicates(),
        df_population,
        on="year",
        by="entity_name",
        direction="nearest",
    )

    # FOR DEBUGGING: Construct the population score using all countries (not just the ones in df_reduced).
    # df_score_population = df_population.rename(columns={"country": "entity_name"}).copy()

    # Normalize the population score to the range 0, 1.
    df_score_population["score_population"] = renormalize_score(
        score=df_score_population["population"],
        min_value=min_population,
        min_score=min_population_score,
        max_value=max_population,
        max_score=max_population_score,
    )
    # FOR DEBUGGING: Uncomment to print examples for different scores and for reference countries.
    # debug_population_score_examples(df_score_population=df_score_population)

    # Add the population score to the scores dataframe.
    df_reduced = df_reduced.merge(df_score_population, on=["entity_name", "year"], how="left")

    # Variables that do not have population data will have a population score nan. Fill them with a low value.
    df_reduced["score_population"] = df_reduced["score_population"].fillna(missing_population_score)

    return df_reduced


def debug_views_score_examples(df_score_analytics: pd.DataFrame) -> None:
    # Prepare an empty list to store the output.
    output_list = []

    # Create a copy of the DataFrame to avoid modifying the original data.
    _df = df_score_analytics.copy()

    # For each interval, calculate the number of unique variable_ids and maximum views within that interval.
    for lower_bound, upper_bound in [(0.0, 0.1)] + [(round(0.1 * i, 1), round(0.1 * (i + 1), 1)) for i in range(1, 10)]:
        if lower_bound == 0.0:
            # First interval includes scores less than or equal to 0.1
            filtered_df = _df[(_df["score_analytics"] <= upper_bound)]
            interval_str = f"less than or equal to {upper_bound}"
        else:
            # Other intervals include scores greater than lower_bound up to upper_bound inclusive
            filtered_df = _df[(_df["score_analytics"] > lower_bound) & (_df["score_analytics"] <= upper_bound)]
            interval_str = f"between {lower_bound} and {upper_bound}"

        # Count the number of unique variable_ids.
        unique_variables_count = filtered_df["variable_id"].nunique()

        # Find the maximum views among these variables.
        if not filtered_df.empty:
            max_views = filtered_df["views"].max()
        else:
            max_views = 0

        # Append the formatted string to the output list.
        output_list.append(
            f"* {pretty_print_number(unique_variables_count)} variables with maximum views {pretty_print_number(max_views)} have a score {interval_str}"
        )

    # Print the output list.
    for line in output_list:
        print(line)


def add_analytics_score(df_reduced: pd.DataFrame) -> pd.DataFrame:
    """Add an analytics score to the scores dataframe.

    The analytics score is defined under the following premises:
    NOTE: The following values are defined below inside the function, double check in case they change in the future.
    * We rely on the number of views in the last 14 days.
    * The number of views of a given variable is the sum of the views of all charts where the variable is used.
    * The score should be around 0.1 for a number of chart views <= 14.
    * The score should be close to 1 for a number of chart views >=1e4.
    * The score should be 0.5 for variables that are not used in any charts.
    NOTE: One could argue that we should rather use 0.1 for such variables. But variables that are not used in charts may be used in explorers, and we currently have no way to properly quantify those views.

    For reference, the result assigns the following scores:
    NOTE: The following lines can be recalculated using debug_views_score_examples.
    * ~1.8k variables with maximum views ~14 have a score less than or equal to 0.1
    * ~1.3k variables with maximum views ~31 have a score between 0.1 and 0.2
    * ~1.8k variables with maximum views ~70 have a score between 0.2 and 0.3
    * ~1.7k variables with maximum views ~157 have a score between 0.3 and 0.4
    * ~1.0k variables with maximum views ~353 have a score between 0.4 and 0.5
    * ~631 variables with maximum views ~790 have a score between 0.5 and 0.6
    * ~372 variables with maximum views ~1.8k have a score between 0.6 and 0.7
    * ~242 variables with maximum views ~3.9k have a score between 0.7 and 0.8
    * ~63 variables with maximum views ~8.5k have a score between 0.8 and 0.9
    * ~54 variables with maximum views ~118.7k have a score between 0.9 and 1.0

    Parameters
    ----------
    df_reduced : pd.DataFrame
        Scores data (with a column "entity_name", "year", and different "score*" columns).

    Returns
    -------
    df_reduced : pd.DataFrame
        Scores data after including a "score_analytics" column.

    """
    # Main parameters:
    # Focus on the following specific analytics column.
    analytics_column = "views_14d"
    # Minimum score assigned to any indicator.
    min_views_score = 0.1
    # Minimum number of views per chart to assign a score (any number of views below this value will be assigned the minimum score).
    min_views = 14
    # Maximum score assigned to any indicator.
    max_views_score = 1.0
    # Maximum number of views per chart to assign a score (any number of views above this value will be assigned the maximum score).
    # NOTE: In the last 14 days, our most viewed chart had 60k views.
    #  The variables with the largest number of views (summed over all charts) are countries-continents, with ~120k views, and population, with 108k.
    #  After those, the next variable is from GBD, and has over 60k views.
    #  After the first 23 variables (with over 50k views) it drops to <20k.
    #  So, it makes sense to ignore the views of countries-continents and population, and therefore set the maximum number of views on 60k.
    #  But we could look into it more deeply.
    max_views = 2e4
    # Value to use to fill missing values in the analytics score (e.g. for variables that are not used in charts).
    fillna_value = 0.1

    # Get number of views in charts for each variable id.
    df_views = get_variables_views_in_charts(variable_ids=list(df_reduced["variable_id"].unique()))

    # FOR DEBUGGING: Load views of all variables in charts.
    # df_views = get_variables_views_in_charts(variable_ids=None)

    # Get the sum of the number of views in charts for each variable id in the last 14 days.
    # So, if an indicator is used in multiple charts, their views are summed.
    # This rewards indicators that are used multiple times, and on popular charts.
    # NOTE: The analytics table often contains nans. For now, for convenience, fill them with 1.1 views (to avoid zeros when calculating the log).
    df_score_analytics = (
        df_views.groupby("variable_id")
        .agg({analytics_column: "sum"})
        .reset_index()
        .rename(columns={analytics_column: "views"})
    )
    # To have more convenient numbers, take the natural logarithm of the views.
    df_score_analytics["score_analytics"] = renormalize_score(
        score=df_score_analytics["views"],
        min_value=min_views,
        min_score=min_views_score,
        max_value=max_views,
        max_score=max_views_score,
    )

    # FOR DEBUGGING: Uncomment to print examples for different scores and for reference countries.
    # debug_views_score_examples(df_score_analytics=df_score_analytics)

    # Add the analytics score to the scores dataframe.
    df_reduced = df_reduced.merge(df_score_analytics, on=["variable_id"], how="left")

    # Variables that do not have charts will have an analytics score nan.
    # Fill them with a low value (e.g. 0.1) to avoid zeros when calculating the final score.
    df_reduced["score_analytics"] = df_reduced["score_analytics"].fillna(fillna_value)

    # Fill missing views
    df_reduced["views"] = df_reduced["views"].fillna(0)

    return df_reduced


def add_weighted_score(df: pd.DataFrame) -> pd.DataFrame:
    """Add a weighted combined score."""
    w_score = 1
    w_pop = 1
    w_views = 1
    w_scale = 1
    df["score_weighted"] = (
        w_score * df["score"]
        + w_pop * df["score_population"]
        + w_views * df["score_analytics"]
        + w_scale * df["score_scale"]
    ) / (w_score + w_pop + w_views + w_scale)

    return df


def add_auxiliary_scores(df: pd.DataFrame) -> pd.DataFrame:
    # Add a population score.
    df = add_population_score(df_reduced=df)

    # Add an analytics score.
    df = add_analytics_score(df_reduced=df)

    # Rename columns for convenience.
    df = df.rename(columns={"variable_id": "indicator_id", "anomaly_score": "score"}, errors="raise")

    # Create a weighted combined score.
    df = add_weighted_score(df)

    return df


def anomaly_detection(
    anomaly_types: Optional[Tuple[str, ...]] = None,
    variable_mapping: Optional[dict[int, int]] = None,
    variable_ids: Optional[list[int]] = None,
    dry_run: bool = False,
    force: bool = False,
    reset_db: bool = False,
    sample_n: Optional[int] = None,
) -> None:
    """Detect anomalies."""
    engine = get_engine()

    # Ensure the 'anomalies' table exists. Optionally reset it if reset_db is True.
    gm.Anomaly.create_table(engine, if_exists="replace" if reset_db else "skip")

    # If no anomaly types are provided, default to all available types
    if not anomaly_types:
        anomaly_types = get_args(ANOMALY_TYPE)

    # Parse the variable_mapping if any provided.
    if not variable_mapping:
        variable_mapping = dict()

    if variable_ids is None:
        variable_ids = []

    # Load metadata for:
    # * All variables in variable_ids.
    # * All variables in variable_mapping (both old and new).
    variable_ids_mapping = (
        (set(variable_mapping.keys()) | set(variable_mapping.values())) if variable_mapping else set()
    )
    variable_ids_all = list(variable_ids_mapping | set(variable_ids or []))
    # Dictionary variable_id: Variable object, for all variables (old and new).
    variables = {
        variable.id: variable for variable in _load_variables_meta(engine=engine, variable_ids=variable_ids_all)
    }

    # Create a dictionary of all variable_ids for each dataset_id (only for new variables).
    dataset_variable_ids = {}
    for variable_id in variable_ids:
        variable = variables[variable_id]
        if variable.datasetId not in dataset_variable_ids:
            dataset_variable_ids[variable.datasetId] = []
        dataset_variable_ids[variable.datasetId].append(variable)

    for dataset_id, variables_in_dataset in dataset_variable_ids.items():
        # Limit the number of variables.
        if sample_n and len(variables_in_dataset) > sample_n:
            variables_in_dataset = _sample_variables(variables_in_dataset, sample_n)

        # Get dataset's checksum
        with Session(engine) as session:
            dataset = gm.Dataset.load_dataset(session, dataset_id)

        log.info("loading_data.start", variables=len(variables_in_dataset))
        variables_old = [
            variables[variable_id_old]
            for variable_id_old in variable_mapping.keys()
            if variable_mapping[variable_id_old] in [variable.id for variable in variables_in_dataset]
        ]
        variables_old_and_new = variables_in_dataset + variables_old
        t = time.time()
        try:
            df = load_data_for_variables(engine=engine, variables=variables_old_and_new)
        except FileNotFoundError as e:
            # This happens when a dataset is in DB, but not in a local catalog.
            log.error("loading_data.error", error=str(e))
            continue

        log.info("loading_data.end", t=time.time() - t)

        if df.empty:
            continue

        for anomaly_type in anomaly_types:
            # Instantiate the anomaly detector.
            if anomaly_type not in ANOMALY_DETECTORS:
                raise ValueError(f"Unsupported anomaly type: {anomaly_type}")

            if not force:
                if not needs_update(engine, dataset, anomaly_type):
                    log.info(f"Anomaly type {anomaly_type} for dataset {dataset_id} already exists in the database.")
                    continue

            log.info(f"Detecting anomaly type {anomaly_type} for dataset {dataset_id}")

            # Instantiate the anomaly detector.
            detector = ANOMALY_DETECTORS[anomaly_type]()

            # Select the variable ids that are included in the current dataset.
            variable_ids_for_current_dataset = [variable.id for variable in variables_in_dataset]
            # Select the subset of the mapping that is relevant for the current dataset.
            variable_mapping_for_current_dataset = {
                variable_old: variable_new
                for variable_old, variable_new in variable_mapping.items()
                if variable_new in variable_ids_for_current_dataset
            }

            # Get the anomaly score dataframe for the current dataset and anomaly type.
            df_score = detector.get_score_df(
                df=df,
                variable_ids=variable_ids_for_current_dataset,
                variable_mapping=variable_mapping_for_current_dataset,
            )

            if df_score.empty:
                log.info("No anomalies detected.`")
                continue

            # Get the anomaly scale dataframe for the current dataset and anomaly type.
            df_scale = detector.get_scale_df(
                df=df,
                variable_ids=variable_ids_for_current_dataset,
                variable_mapping=variable_mapping_for_current_dataset,
            )

            # Create a long format score dataframe.
            df_score_long = get_long_format_score_df(df_score=df_score, df_scale=df_scale)

            # TODO: validate format of the output dataframe
            anomaly = gm.Anomaly(
                datasetId=dataset_id,
                datasetSourceChecksum=dataset.sourceChecksum,
                anomalyType=anomaly_type,
            )
            # We could store the full dataframe in the database, but it ends up making the load quite slow.
            # Since we are not using it for now, we will store only the reduced dataframe.
            # anomaly.dfScore = df_score_long

            # Reduce dataframe
            df_score_long_reduced = (
                df_score_long.sort_values("anomaly_score", ascending=False)
                .drop_duplicates(subset=["entity_name", "variable_id"], keep="first")
                .reset_index(drop=True)
            )
            anomaly.dfReduced = df_score_long_reduced

            ##################################################################
            # TODO: Use this as an alternative to storing binary files in the DB
            # anomaly = gm.Anomaly(
            #     datasetId=dataset_id,
            #     anomalyType=detector.anomaly_type,
            # )
            # anomaly.dfScore = None

            # # Export anomaly file
            # anomaly.path_file = export_anomalies_file(df_score, dataset_id, detector.anomaly_type)
            ##################################################################

            if not dry_run:
                with Session(engine) as session:
                    # log.info("Deleting existing anomalies")
                    session.query(gm.Anomaly).filter(
                        gm.Anomaly.datasetId == dataset_id,
                        gm.Anomaly.anomalyType == anomaly_type,
                    ).delete(synchronize_session=False)
                    session.commit()

                    # Don't save anomalies if there are none
                    if df_score_long.empty:
                        log.info(f"No anomalies found for anomaly type {anomaly_type} in dataset {dataset_id}")
                    else:
                        # Insert new anomalies
                        log.info("Writing anomaly to database")
                        session.add(anomaly)
                        session.commit()


def needs_update(engine: Engine, dataset: gm.Dataset, anomaly_type: str) -> bool:
    """If there's an anomaly with the dataset checksum in DB, it doesn't need
    to be updated."""
    with Session(engine) as session:
        try:
            anomaly = gm.Anomaly.load(
                session,
                dataset_id=dataset.id,
                anomaly_type=anomaly_type,
            )
        except NoResultFound:
            return True

        return anomaly.datasetSourceChecksum != dataset.sourceChecksum


def export_anomalies_file(df: pd.DataFrame, dataset_id: int, anomaly_type: str) -> str:
    """Export anomaly df to local file (and upload to staging server if applicable)."""
    filename = f"{dataset_id}_{anomaly_type}.feather"
    path = Path(f".anomalies/{filename}")
    path_str = str(path)
    if OWID_ENV.env_local == "staging":
        create_folder(path.parent)
        df.to_feather(path_str)
    elif OWID_ENV.env_local == "dev":
        # tmp_filename = Path("tmp.feather")
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_file_path = Path(tmp_dir) / filename
            df.to_feather(tmp_file_path)
            upload_file_to_server(tmp_file_path, f"owid@{OWID_ENV.name}:/home/owid/etl/{WIZARD_ANOMALIES_RELATIVE}")
    else:
        raise ValueError(
            f"Unsupported environment: {OWID_ENV.env_local}. Did you try production? That's not supported!"
        )
    return path_str


def load_data_for_variables(engine: Engine, variables: list[gm.Variable]) -> pd.DataFrame:
    # Load data from local catalog.
    # NOTE: Data is returned as a Table, which raises some warnings about units.
    # For now, we do not need the metadata, so, convert to dataframe.
    df = pd.DataFrame(variable_data_table_from_catalog(engine, variables=variables))
    df = df.rename(columns={"country": "entity_name"})

    if "year" not in df.columns and "date" in df.columns:
        log.warning("Anomalist does not work for datasets with `date` column yet.")
        return pd.DataFrame()

    # Define the list of columns that are not index columns.
    data_columns = [v.id for v in variables]

    # Reorder dataframe so that data columns are in the same order as the list of variables.
    # NOTE: I'm not sure if this is necessary.
    df = df[INDEX_COLUMNS + data_columns]

    # Set non-numeric values to NaN.
    # Sometimes the dtypes include, e.g. UInt32, which can cause issues for the detectors.
    # Convert all data columns to float.
    df[data_columns] = df[data_columns].apply(pd.to_numeric, errors="coerce").astype(float)

    # Sort data (which may be needed for some detectors).
    # NOTE: Here, we first convert the entity_name to string, because otherwise the sorting will be based on categorical order (which can be arbitrary).
    df = df.astype({"entity_name": "string[pyarrow]"}).sort_values(INDEX_COLUMNS).reset_index(drop=True)

    return df


# @memory.cache
def _load_variables_meta(engine: Engine, variable_ids: list[int]) -> list[gm.Variable]:
    if len(variable_ids) == 0:
        # If no variable ids are given, return an empty list of variables.
        return []

    q = """
    select id from variables
    where id in %(variable_ids)s
    """
    df = read_sql(q, engine, params={"variable_ids": variable_ids})

    # select all variables using SQLAlchemy
    with Session(engine) as session:
        return gm.Variable.load_variables(session, list(df["id"]))


def combine_and_reduce_scores_df(anomalies: List[gm.Anomaly]) -> pd.DataFrame:
    """Get the combined dataframe with scores for all anomalies, and reduce it to include only the largest anomaly for each contry-indicator."""
    # Combine the reduced dataframes for all anomalies into a single dataframe.
    dfs = []
    for anomaly in anomalies:
        df = anomaly.dfReduced
        if df is None:
            log.warning(f"Anomaly {anomaly} has no reduced dataframe.")
            continue
        df["type"] = anomaly.anomalyType
        dfs.append(df)

    df_reduced = cast(pd.DataFrame, pd.concat(dfs, ignore_index=True))
    # Dtypes
    # df = df.astype({"year": int})

    return df_reduced


def _sample_variables(variables: List[gm.Variable], n: int) -> List[gm.Variable]:
    """Sample n variables. Prioritize variables that are used in charts, then fill the rest
    with random variables."""
    if len(variables) <= n:
        return variables

    # Include all variables that are used in charts.
    # NOTE: if we run this before indicator upgrader, none of the charts will be in charts yet. So the
    #  first round of anomalies with random sampling won't be very useful. Next runs should be useful
    #  though
    df_views = get_variables_views_in_charts(variable_ids=[v.id for v in variables])
    sample_ids = set(df_views.sort_values("views_365d", ascending=False).head(n)["variable_id"])

    # Fill the rest with random variables.
    unused_ids = list(set(v.id for v in variables) - sample_ids)
    random.seed(1)
    if len(sample_ids) < n:
        sample_ids |= set(np.random.choice(unused_ids, n - len(sample_ids), replace=False))

    log.info(
        "sampling_variables",
        original_n=len(variables),
        new_n=len(sample_ids),
    )
    return [v for v in variables if v.id in sample_ids]
