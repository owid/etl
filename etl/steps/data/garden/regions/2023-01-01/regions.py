"""Region definitions, aliases, members, historical transitions and codes.

Find more details in the README in `docs/data/regions.md`.

"""

import json

import pandas as pd
import yaml
from owid.catalog import Table
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

# Initialize logger.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Path to input file of region definitions.
REGION_DEFINITIONS_FILE = paths.directory / "regions.yml"
# Path to input file of region codes.
REGION_CODES_FILE = paths.directory / "regions.codes.csv"

# Expected region types.
REGION_TYPES = ["country", "continent", "aggregate", "other"]

# Define list of expected duplicated aliases.
# This could happen, for example, if a historical region changes definition on a particular year.
# We could have different codes for, e.g. OWID_USS_1990 and OWID_USS_1991, but with the same alias, "USSR".
# If an alias that is not in this list is found multiple times in the data, an error is raised.
DUPLICATED_ALIASES = []


def parse_raw_definitions(df: pd.DataFrame) -> pd.DataFrame:
    # If "short_name" is not given, fill with "name".
    df["short_name"] = df["short_name"].fillna(df["name"])

    # If "region_type" is not given, fill with "country" (the most common region type).
    df["region_type"] = df["region_type"].fillna("country")

    # If "defined_by" is not given, fill with "owid".
    if "defined_by" not in df.columns:
        df["defined_by"] = pd.Series(dtype=str)
    df["defined_by"] = df["defined_by"].fillna("owid")

    # If "is_historical" is not given, fill with False.
    df["is_historical"] = df["is_historical"].fillna(False)

    return df


def run_sanity_checks(df: pd.DataFrame) -> None:
    # Check that all regions have a name.
    assert df[df["name"].isnull()].empty, f"Some regions do not have a name: {set(df[df['name'].isnull()]['code'])}"

    # Check that there are no repeated codes.
    duplicated_codes = df[df["code"].duplicated()]["code"].tolist()
    assert len(duplicated_codes) == 0, f"Duplicated codes found: {duplicated_codes}"

    # Check that all regions defined in lists exist as properly defined regions.
    for column_of_lists in ["members", "successors", "related"]:
        all_region_codes_in_lists = sum(df[column_of_lists].dropna().tolist(), [])
        unknown_codes_in_list = set(all_region_codes_in_lists) - set(df["code"])
        assert unknown_codes_in_list == set(), f"Unknown codes found in {column_of_lists}: {unknown_codes_in_list}"

    # Check that the members of different continents do not overlap.
    continents = df[df["region_type"] == "continent"][["name", "members"]].set_index("name").to_dict()["members"]
    remaining_continents = continents.copy()
    for continent_i in continents:
        del remaining_continents[continent_i]
        for continent_j in remaining_continents:
            error = f"Overlap found between the members of {continent_i} and {continent_j}."
            assert set(continents[continent_i]).isdisjoint(remaining_continents[continent_j]), error

    # Check that data contains only expected region types.
    unknown_region_types = set(df["region_type"]) - set(REGION_TYPES)
    assert unknown_region_types == set(), f"Unexpected region types: {unknown_region_types}"

    # Ensure that there are no unknown duplicated aliases.
    # Create a dataframe where each row corresponds to an individual alias.
    aliases_all = df[["aliases"]].dropna().explode("aliases")
    # Create a list of unknown duplicated aliases.
    aliases_duplicated = aliases_all[
        aliases_all["aliases"].duplicated() & (~aliases_all["aliases"].isin(DUPLICATED_ALIASES))
    ]["aliases"].tolist()
    error = (
        f"Unknown duplicated aliases: {aliases_duplicated}. "
        "If this duplicate was intended, add it to DUPLICATED_ALIASES at the beginning of the regions.py garden step."
    )
    assert len(aliases_duplicated) == 0, error


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load main regions data from yaml file.
    with open(REGION_DEFINITIONS_FILE) as _file:
        df = pd.DataFrame.from_dict(yaml.safe_load(_file))

    # Load file of region codes.
    # NOTE: Namibia has iso_code "NA" which would be interpreted as NaN without extra arguments.
    df_codes = pd.read_csv(REGION_CODES_FILE, keep_default_na=False, na_values=[""])

    #
    # Process data.
    #
    # Parse raw definitions (from the adjacent yaml file).
    df = parse_raw_definitions(df=df)

    # Run sanity checks on input data.
    run_sanity_checks(df=df)

    # Create a table of legacy codes (ensuring all numeric codes are integer).
    df_codes = df_codes.astype(
        {code: pd.Int64Dtype() for code in ["cow_code", "imf_code", "legacy_country_id", "legacy_entity_id"]}
    )

    # Combine data with legacy codes.
    tb_regions = Table(pd.merge(df, df_codes, on="code", how="left"), short_name="regions")

    # Convert columns that are list of strings into jsons.
    for column in ["aliases", "members", "related", "successors"]:
        tb_regions[column] = tb_regions.groupby("code")[column].transform(
            lambda x: json.dumps(sum(list(x), [])) if pd.notna(x.values) else x
        )

    # Ensure "is_historical" is boolean.
    tb_regions = tb_regions.astype({"is_historical": bool})

    # Set an appropriate index and sort conveniently.
    tb_regions = tb_regions.set_index("code", verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = create_dataset(
        dest_dir=dest_dir,
        tables=[tb_regions],
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
