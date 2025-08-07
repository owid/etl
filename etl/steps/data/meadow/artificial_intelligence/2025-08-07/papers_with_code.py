"""Load a snapshot and create a meadow dataset."""

import gzip
import json

import pandas as pd
from owid.catalog.tables import Table, _add_table_and_variables_metadata_to_table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("papers_with_code.gz")

    #
    # Process data.
    #
    # Load data from snapshot.
    with gzip.open(snap.path) as _file:
        data = json.loads(_file.read())

    df_code = extract_task_dataset(data, "Code Generation", "APPS")
    df_code = df_code[["model_name", "paper_date", "Competition Pass@1", "Interview Pass@1"]]
    df_code = df_code.rename(
        columns={
            "Competition Pass@1": "competitions_pass_1",
            "Interview Pass@1": "interviews_pass_1",
        }
    )

    df_image = extract_task_dataset(data, "Image Classification", "ImageNet")
    df_image = df_image[["model_name", "paper_date", "Top 1 Accuracy", "Top 5 Accuracy"]]
    df_image = df_image.groupby(["model_name", "paper_date"])[["Top 1 Accuracy", "Top 5 Accuracy"]].max().reset_index()

    df_language = extract_task_dataset(data, "Multi-task Language Understanding", "MML")
    df_language = df_language[["model_name", "paper_date", "Average (%)"]]
    df_language = df_language.rename(columns={"Average (%)": "average_mmlu_accuracy"})
    df_language = df_language.groupby(["model_name", "paper_date"])["average_mmlu_accuracy"].max().reset_index()

    df_math = extract_task_dataset(data, "Math Word Problem Solving", "MATH")
    df_math = df_math[["model_name", "paper_date", "Accuracy"]]
    df_math = df_math.rename(columns={"Accuracy": "math_accuracy"})
    df_math = df_math.groupby(["model_name", "paper_date"])["math_accuracy"].max().reset_index()

    # Merge all dataframes
    merged_df = df_code.copy()
    for current_df in [df_image, df_language, df_math]:
        merged_df = merged_df.merge(current_df, on=["model_name", "paper_date"], how="outer")
    merged_df = merged_df.replace("%", "", regex=True)

    # Convert to Table (df -> tb)
    tb = _add_table_and_variables_metadata_to_table(
        table=Table(merged_df, underscore=False),
        metadata=snap.to_table_metadata(),
        origin=snap.metadata.origin,
    )
    # Improve tables format.
    tables = [tb.format(["model_name", "paper_date"])]

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables, default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()


def extract_task_dataset(json_data, target_task, target_dataset):
    """
    Extract specific task and dataset from nested JSON structure

    Args:
        json_data: List of task dictionaries
        target_task (str): Task name to search for (e.g., "Code Generation")
        target_dataset (str): Dataset name to search for (e.g., "APPS")

    Returns:
        pandas.DataFrame: Extracted model performance data
    """

    def search_nested_structure(data, target_task, target_dataset):
        """Recursively search through nested task structure"""
        results = []

        if isinstance(data, list):
            for item in data:
                results.extend(search_nested_structure(item, target_task, target_dataset))
        elif isinstance(data, dict):
            # First recursively search subtasks (prioritize deeper nested structures)
            if "subtasks" in data:
                results.extend(search_nested_structure(data["subtasks"], target_task, target_dataset))

            # Then check if current item is the target task
            if data.get("task") == target_task:
                # Search datasets within this task
                datasets = data.get("datasets", [])
                for dataset in datasets:
                    if dataset.get("dataset") == target_dataset:
                        results.append(dataset)

        return results

    # Find matching datasets
    matching_datasets = search_nested_structure(json_data, target_task, target_dataset)

    if not matching_datasets:
        print(f"No data found for task '{target_task}' with dataset '{target_dataset}'")
        return pd.DataFrame()

    # Extract all model results
    all_results = []
    for dataset_entry in matching_datasets:
        sota_data = dataset_entry.get("sota", {})
        rows = sota_data.get("rows", [])

        for row in rows:
            record = {
                "task": target_task,
                "dataset": target_dataset,
                "model_name": row.get("model_name", ""),
                "paper_title": row.get("paper_title", ""),
                "paper_date": row.get("paper_date", ""),
                "paper_url": row.get("paper_url", ""),
                "uses_additional_data": row.get("uses_additional_data", False),
            }

            # Extract metrics
            metrics = row.get("metrics", {})
            for metric_name, metric_value in metrics.items():
                # Handle numeric values
                try:
                    if metric_value and metric_value != "-":
                        record[metric_name] = float(str(metric_value).split()[0])
                    else:
                        record[metric_name] = None
                except (ValueError, IndexError):
                    record[metric_name] = metric_value

            # Extract code links
            code_links = row.get("code_links", [])
            record["code_urls"] = "; ".join([link["url"] for link in code_links])
            record["code_titles"] = "; ".join([link["title"] for link in code_links])

            all_results.append(record)

    df = pd.DataFrame(all_results)
    return df
