"""Load the adjacent files of region definitions and codes, and create a garden dataset with different tables.

"""

import yaml
import pandas as pd
from owid.catalog import Dataset, Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Path to input file of region definitions.
REGION_DEFINITIONS_FILE = paths.directory / "regions.yml"
# Path to input file of region codes.
REGION_CODES_FILE = paths.directory / "regions.codes.csv"


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    #
    # Load main regions data from yaml file.
    with open(REGION_DEFINITIONS_FILE) as _file:
        df = pd.DataFrame.from_dict(yaml.safe_load(_file))

    # Load file of region codes.
    df_codes = pd.read_csv(REGION_CODES_FILE)

    #
    # Process data.
    #
    # Create an appropriate index for main dataframe and sort conveniently.
    df = df.set_index(["code"], verify_integrity=True).sort_index()

    # Create an appropriate index for codes dataframe and sort conveniently.
    df_codes = df_codes.set_index(["code"], verify_integrity=True).sort_index()

    # Create table for region definitions.
    tb_definitions = Table(df[["name", "short_name", "region_type", "is_historical", "defined_by"]], short_name="definitions")

    # Create table for aliases.
    tb_aliases = Table(df.rename(columns={"aliases": "alias"})[["alias"]].explode("alias").dropna(how="all"), short_name="aliases")

    # Create table for members.
    tb_members = Table(df.rename(columns={"members": "member"}).explode("member")[["member"]].dropna(how="all"), short_name="members")

    # Create table of historical transitions.
    tb_transitions = Table(df[["end_year", "successors"]].rename(columns={"successors": "successor"}).\
        explode("successor").dropna(how="all").astype({"end_year": int}), short_name="transitions")

    # Create a table of legacy codes (ensuring all numeric codes are integer).
    tb_legacy_codes = Table(df_codes.astype({code: pd.Int64Dtype()
                                       for code in ["cow_code", "imf_code", "legacy_country_id", "legacy_entity_id"]}), short_name="legacy_codes")

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = create_dataset(dest_dir=dest_dir, tables=[tb_definitions, tb_aliases, tb_members, tb_transitions, tb_legacy_codes])

    # Save changes in the new garden dataset.
    ds_garden.save()
