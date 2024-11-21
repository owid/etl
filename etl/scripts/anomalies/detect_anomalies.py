"""Detect anomalies in a given grapher dataset.

"""
from typing import Dict, List, Optional, Tuple

import click
import pandas as pd
import plotly.express as px
from owid.datautils.dataframes import map_series
from rich_click.rich_command import RichCommand
from sqlalchemy.orm import Session
from structlog import get_logger

import etl.grapher_io as io
import etl.grapher_model as gm
from apps.anomalist import detectors
from apps.anomalist.anomalist_api import add_auxiliary_scores
from apps.anomalist.gp_detector import AnomalyGaussianProcessOutlier
from etl.db import get_engine

detectors_classes = {
    "upgrade_missing": detectors.AnomalyUpgradeMissing,
    "upgrade_change": detectors.AnomalyUpgradeChange,
    "time_change": detectors.AnomalyTimeChange,
    "one_class_svm": detectors.AnomalyOneClassSVM,
    "isolation_forest": detectors.AnomalyIsolationForest,
    "gp_outlier": AnomalyGaussianProcessOutlier,
}

# Initialize logger.
log = get_logger()

# Initialize database engine.
engine = get_engine()


def get_dataset_id_of_previous_version(dataset_id: int) -> Optional[int]:
    """Find the previous version of a dataset, if it exists.

    NOTE: Archived datasets are also considered.
    """
    with Session(engine) as session:
        ds = gm.Dataset.load_dataset(session=session, dataset_id=dataset_id)

    # Check if there is any other grapher dataset with the same namespace and short name, but a previous version.
    with Session(engine) as session:
        ds_other = (
            session.query(gm.Dataset)
            .filter(
                gm.Dataset.shortName == ds.shortName,
                gm.Dataset.namespace == ds.namespace,
            )
            .all()
        )
    ds_other = pd.DataFrame(ds_other).sort_values("version", ascending=False)
    ds_previous = ds_other[ds_other["version"] < ds.version]
    if ds_previous.empty:
        # No previous dataset was found.
        dataset_id_previous = None
    else:
        # Load the previous dataset.
        dataset_id_previous = ds_previous.iloc[0].id

    return dataset_id_previous


def load_data_for_dataset_id(dataset_id: int) -> Tuple[pd.DataFrame, List[gm.Variable]]:
    # Get variable ids and names for the current dataset.
    # NOTE: This is necessary to fetch data from S3, but also to be able to map local data column names to variable ids.
    with Session(engine) as session:
        ds_variables = gm.Variable.load_variables_in_datasets(session=session, dataset_ids=[dataset_id])
    ds_variable_ids = {variable.shortName: variable.id for variable in ds_variables}

    # NOTE: In principle, it would be a good idea to attempt to load data from ETL files first, and only from S3 if this fails. However, I've noticed differences between them (e.g. in format types). So we would need to look into this more carefully. For now, simply load from S3 directly.
    df = pd.DataFrame()
    # try:
    #     # Get the dataset info from the database.
    #     with Session(engine) as session:
    #         ds = gm.Dataset.load_dataset(session=session, dataset_id=dataset_id)
    # except gm.NoResultFound:
    #     raise ValueError(f"Dataset with id {dataset_id} not found.")
    # # First attempt to load data from a local ETL grapher step.
    # from etl.paths import DATA_DIR
    # from owid import catalog
    # etl_file = DATA_DIR / f"grapher/{ds.namespace}/{ds.version}/{ds.shortName}"
    # if etl_file.exists():
    #     log.info(f"Loading data from local ETL file: {etl_file}")
    #     ds_etl = catalog.Dataset(etl_file)
    #     if ds_etl.table_names == [ds.shortName]:
    #         df = pd.DataFrame(ds_etl.read(ds.shortName))  # type: ignore
    #     # Change column names to variable ids.
    #     df = df.rename(columns={column: ds_variable_ids[column] for column in df.columns if column in ds_variable_ids}, errors="raise").rename(columns={"country": "entity_name"}, errors="raise")

    if df.empty:
        # If the dataset could not be loaded from a local file, get it from S3.
        # First, find the list of variables in the dataset.
        log.info(f"Loading data for dataset id {dataset_id} from S3.")

        # Load data for all variables from S3.
        df_long = io.variable_data_df_from_s3(
            engine=engine,
            variable_ids=ds_variable_ids.values(),  # type: ignore
            workers=None,
            value_as_str=False,  # type: ignore
        )  # type: ignore

        # Switch from long to wide format dataframe.
        df = (
            df_long.rename(columns={"variableId": "variable_id"})
            .pivot(index=["entityName", "year"], columns="variable_id", values="value")
            .reset_index()
            .rename(columns={"entityName": "entity_name"}, errors="raise")
        )

        # Sort rows (which may be necessary for some anomaly detectors).
        df = df.sort_values(["entity_name", "year"]).reset_index(drop=True)

    return df, ds_variables


def get_all_necessary_data(dataset_id: int):
    # Dataset ids of the latest and previous versions of the electricity_mix dataset.
    df_new, variables_new = load_data_for_dataset_id(dataset_id=dataset_id)

    # Check if there is a previous version of the dataset.
    dataset_id_old = get_dataset_id_of_previous_version(dataset_id=dataset_id)

    if dataset_id_old is not None:
        # If there is a previous version of the dataset, load its data.
        df_old, variables_old = load_data_for_dataset_id(dataset_id=dataset_id_old)
        # Create a mapping from old ids to new variable ids for variables whose shortNames are identical in the old and new versions.
        _variables = {variable.shortName: variable.id for variable in variables_new}
        variable_mapping = {
            old_variable.id: _variables[old_variable.shortName]
            for old_variable in variables_old
            if old_variable.shortName in _variables
        }
    else:
        # Otherwise, create an empty dataframe.
        df_old = pd.DataFrame(columns=["entity_name", "year"])
        variables_old = []
        variable_mapping = {}

    # Create a dataframe with all data (old and new).
    df = df_new.merge(df_old, on=["entity_name", "year"], how="outer")

    # Create a list with all variables (old and new) metadata.
    metadata = {variable.id: variable for variable in variables_old + variables_new}

    # Create a list with all new variable ids.
    variable_ids = [variable.id for variable in variables_new]

    return df, metadata, variable_ids, variable_mapping


def inspect_anomalies(
    df: pd.DataFrame,
    df_data: pd.DataFrame,
    metadata: Dict[int, gm.Variable],
    variable_mapping: Dict[int, int],
    n_anomalies_max: int = 50,
) -> None:
    # Create a temporary dataframe to display.
    df_show = df.iloc[0:n_anomalies_max].copy()

    # Reverse variable mapping.
    variable_id_new_to_old = {v: k for k, v in variable_mapping.items()}
    # Create a column for the corresponding old variable id for each new variable id (if there is a mapping).
    df_show["indicator_id_old"] = map_series(
        df_show["indicator_id"],
        variable_id_new_to_old,
        warn_on_missing_mappings=False,
    )
    for _, row in df_show.iterrows():
        variable_id = row["indicator_id"]
        variable_name = f"{metadata[variable_id].shortName} ({variable_id})"
        variable_title = metadata[variable_id].titlePublic
        country = row["entity_name"]
        anomaly_type = row["anomaly_type"]
        year = row["year"]
        title = f"{variable_title}<br>{variable_name}<br>{country}-{year}<br>"
        title += f'{anomaly_type} - weighted score: {row["score_weighted"]:.0%} | anomaly: {row["score"]:.0%} | scale: {row["score_scale"]:.0%} | population: {row["score_population"]:.0%} | views: {row["score_analytics"]:.0%}<br>'
        new = df_data[df_data["entity_name"] == row["entity_name"]][["entity_name", "year", variable_id]]
        new = new.rename(columns={row["indicator_id"]: variable_name}, errors="raise")
        if anomaly_type in ["upgrade_change", "upgrade_missing"]:
            variable_id_old = row["indicator_id_old"]
            old = df_data[df_data["entity_name"] == row["entity_name"]][["entity_name", "year", variable_id_old]]
            old = old.rename(columns={row["indicator_id_old"]: variable_name}, errors="raise")
            compare = pd.concat([old.assign(**{"source": "old"}), new.assign(**{"source": "new"})], ignore_index=True)
            fig = px.line(
                compare,
                x="year",
                y=variable_name,
                color="source",
                title=title,
                markers=True,
                color_discrete_map={"old": "rgba(256,0,0,0.5)", "new": "rgba(0,256,0,0.5)"},
            )
        else:
            fig = px.line(
                new,
                x="year",
                y=variable_name,
                title=title,
                markers=True,
                color_discrete_map={"new": "rgba(0,256,0,0.5)"},
            )
        # Improve the layout.
        fig.update_layout(yaxis_title="", title=dict(font=dict(size=14)))
        # Add a vertical line on the year of the anomaly.
        # Add a vertical line at a specific year (e.g., 2020)
        fig.add_shape(
            type="line",
            x0=year,
            x1=year,
            y0=0,  # Start y-position (bottom of the plot)
            y1=1,
            xref="x",  # Reference to the x-axis
            yref="paper",  # Reference to the entire plot's height
            line=dict(color="red", width=2, dash="dash"),  # Customize the line style
        )
        # Display the plot.
        fig.show()


@click.command(name="test_anomaly_detectors", cls=RichCommand, help=__doc__)
@click.option(
    "--dataset-id",
    type=int,
    # multiple=True,
    # default=None,
    help="Generate anomalies for the variables of a specific dataset ID.",
)
@click.option(
    "--anomaly-type",
    type=click.Choice(list(detectors_classes)),
    # multiple=True,
    default="upgrade_change",
    help="Type of anomaly detection algorithm to use.",
)
@click.option(
    "--n-anomalies",
    type=int,
    default=10,
    help="Number of anomalies to plot.",
)
def main(dataset_id: int, anomaly_type: str, n_anomalies: int = 10) -> None:
    # Load all necessary data.
    df_data, metadata, variable_ids, variable_mapping = get_all_necessary_data(dataset_id=dataset_id)

    # Initialize the anomaly detector object.
    detector = detectors_classes[anomaly_type]()
    df_score = detector.get_score_df(df=df_data, variable_ids=variable_ids, variable_mapping=variable_mapping)
    df_scale = detector.get_scale_df(df=df_data, variable_ids=variable_ids, variable_mapping=variable_mapping)
    df = (
        detectors.get_long_format_score_df(df_score=df_score, df_scale=df_scale)
        .sort_values(["anomaly_score", "year"], ascending=False)
        .drop_duplicates(subset=["entity_name", "variable_id"], keep="first")
        .reset_index(drop=True)
    )

    # Add population and analytics scores.
    df = add_auxiliary_scores(df=df)

    # Add anomaly type.
    df["anomaly_type"] = detector.anomaly_type

    SCORE_ANOMALY_THRESHOLD = 0.3
    SCORE_POPULATION_THRESHOLD = 0.3
    SCORE_ANALYTICS_THRESHOLD = 0.3
    SCORE_SCALE_THRESHOLD = 0.3
    SCORE_WEIGHTED_THRESHOLD = 0.3
    # Inspect anomalies.
    df_candidates = df[
        (df["score"] > SCORE_ANOMALY_THRESHOLD)
        & (df["score_population"] > SCORE_POPULATION_THRESHOLD)
        & (df["score_analytics"] > SCORE_ANALYTICS_THRESHOLD)
        & (df["score_scale"] > SCORE_SCALE_THRESHOLD)
        & (df["score_weighted"] > SCORE_WEIGHTED_THRESHOLD)
    ].sort_values("score_weighted", ascending=False)
    inspect_anomalies(
        df=df_candidates,
        df_data=df_data,
        metadata=metadata,
        variable_mapping=variable_mapping,
        n_anomalies_max=n_anomalies,
    )

    # # Useful for debugging, to compare old and new series for different countries.
    # i = 0
    # old = list(variable_mapping)[i]
    # new = variable_mapping[old]
    # d = df_data[["entity_name", "year", old, new]].rename(columns={old: "old", new: "new"}).dropna().melt(id_vars=["entity_name", "year"])
    # for c in d["entity_name"].unique():
    #     px.line(d[d["entity_name"]==c],x="year", y="value", color="variable_id", markers=True, title=f"{c}", color_discrete_map={"old": "rgba(256,0,0,0.5)", "new": "rgba(0,256,0,0.5)"}).show()


if __name__ == "__main__":
    main()
