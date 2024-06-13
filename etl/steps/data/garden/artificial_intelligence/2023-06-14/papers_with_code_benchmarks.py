"""Load a meadow dataset and create a garden dataset."""

from typing import cast

import pandas as pd
import shared
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    """
    Combine all Paperswithcode benchmarks in meadow into one garden dataset.

    """

    log.info("papers_with_code_benchmarks.start")

    #
    # Load inputs - Atari
    #
    # Load meadow dataset for Atari.
    ds_meadow_atari = cast(Dataset, paths.load_dependency("papers_with_code_atari"))
    tb_atari = ds_meadow_atari["papers_with_code_atari"]
    df_atari = pd.DataFrame(tb_atari)

    # Calculate 'days_since' column.
    df_atari["days_since"] = (
        pd.to_datetime(df_atari["date"].astype(str), format="%Y-%m-%d") - pd.to_datetime("2019-01-01")
    ).dt.days

    df_atari = df_atari.drop("date", axis=1)
    df_atari["performance_atari"] = df_atari["performance_atari"] * 100

    # Process and check improvement for Atari dataframe.
    df_atari = shared.process_df(df_atari, "performance_atari", "performance_atari")
    new_df_atari = shared.check_improvement(df_atari, "performance_atari")

    #
    # Load inputs - ImageNet
    #
    # Load meadow dataset for ImageNet.
    ds_meadow_imagenet = cast(Dataset, paths.load_dependency("papers_with_code_imagenet"))
    tb_imagenet = ds_meadow_imagenet["papers_with_code_imagenet"]
    df_imagenet = pd.DataFrame(tb_imagenet)

    # Calculate 'days_since' column.
    df_imagenet["days_since"] = (
        pd.to_datetime(df_imagenet["date"].astype(str), format="%Y-%m-%d") - pd.to_datetime("2019-01-01")
    ).dt.days

    df_imagenet = df_imagenet.drop("date", axis=1)

    # Group the DataFrame by 'days_since' and 'training_data' columns and find the maximum accuracy for each group
    columns_imagenet = ["papers_with_code_imagenet_top1", "papers_with_code_imagenet_top5"]
    merged_df_imagenet = shared.merge_dfs(df_imagenet, columns_imagenet)
    merged_df_imagenet.sort_values("days_since", inplace=True)
    merged_df_imagenet.reset_index(drop=True, inplace=True)

    for col in columns_imagenet:
        merged_df_imagenet = shared.check_improvement(merged_df_imagenet, col)

    #
    # Load inputs - Math Code Language
    #
    # Load meadow dataset for Math Code Language.
    ds_meadow_math_code_lang = cast(Dataset, paths.load_dependency("papers_with_code_math_code_language"))
    tb_math_code_lang = ds_meadow_math_code_lang["papers_with_code_math_code_language"]
    df_math_code_lang = pd.DataFrame(tb_math_code_lang).reset_index()

    # Calculate 'days_since' column.
    df_math_code_lang["days_since"] = (
        pd.to_datetime(df_math_code_lang["date"].astype(str), format="%Y-%m-%d") - pd.to_datetime("2019-01-01")
    ).dt.days

    df_math_code_lang = df_math_code_lang.drop("date", axis=1)

    columns_math_code_lang = [
        "performance_code_any_competition",
        "performance_code_any_interview",
        "performance_humanities",
        "performance_language_average",
        "performance_math",
        "performance_other",
        "performance_social_sciences",
        "performance_stem",
    ]

    # Group the DataFrame by 'days_since' and 'name' columns and find the maximum performance for each group
    merged_df_math_code_lang = shared.merge_dfs(df_math_code_lang, columns_math_code_lang)
    merged_df_math_code_lang.sort_values("days_since", inplace=True)
    merged_df_math_code_lang.reset_index(drop=True, inplace=True)

    for col in columns_math_code_lang:
        merged_df_math_code_lang = shared.check_improvement(merged_df_math_code_lang, col)

    # Merge all benchmark dataframes
    all_benchmarks = pd.merge(merged_df_math_code_lang, merged_df_imagenet, on=["name", "days_since"], how="outer")
    all_benchmarks = pd.merge(all_benchmarks, new_df_atari, on=["name", "days_since"], how="outer")

    columns_to_replace = [col for col in all_benchmarks.columns if "_improved" in col]
    all_benchmarks[columns_to_replace] = all_benchmarks[columns_to_replace].fillna("Not applicable")
    all_benchmarks[columns_to_replace] = all_benchmarks[columns_to_replace].astype(str)

    all_benchmarks.set_index(["days_since", "name"], inplace=True)
    tb = Table(all_benchmarks, short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=ds_meadow_imagenet.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("papers_with_code_benchmarks.end")
