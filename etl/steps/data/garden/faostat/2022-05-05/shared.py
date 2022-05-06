"""FAO Land Use (2021).

"""

from copy import deepcopy
from pathlib import Path

import numpy as np
import pandas as pd
from owid import catalog
from owid.datautils import geo

from etl.paths import DATA_DIR, STEP_DIR


REGIONS_TO_ADD = [
    "North America",
    "South America",
    "Europe",
    "European Union (27)",
    "Africa",
    "Asia",
    "Oceania",
    "Low-income countries",
    "Upper-middle-income countries",
    "Lower-middle-income countries",
    "High-income countries",
]

# +
# We have created a manual ranking of FAOSTAT flags.
# These flags are only used when there is ambiguity in the data.
# This means that a certain country-year has more than one data point with different flags.
# NOTES:
# * We check that the definitions in our manual ranking agree with the ones provided by FAOSTAT.
# * We do not include all flags: We include only the ones that solve an ambiguity in a particular case,
#   and add more flags as we see need.
# * We have found at least one flag that appeard in a dataset, but was not included in the additional metadata
#   (namely flag "R", found in qcl dataset).
#   This flag was added manually, using the definition in List / Flags in:
#   https://www.fao.org/faostat/en/#definitions
# -

# Rank flags by priority (where lowest index is highest priority).
# TODO: Discuss this flag ranking with others (they are quite arbitrary at the moment).
FLAGS_RANKING = (
    pd.DataFrame.from_records(
        columns=["flag", "description"],
        data=[
            (np.nan, "Official data"),
            ("F", "FAO estimate"),
            (
                "A",
                "Aggregate, may include official, semi-official, estimated or calculated data",
            ),
            ("Fc", "Calculated data"),
            (
                "I",
                "Country data reported by International Organizations where the country is a member (Semi-official) - WTO, EU, UNSD, etc.",
            ),
            (
                "W",
                "Data reported on country official publications or web sites (Official) or trade country files",
            ),
            ("Fm", "Manual Estimation"),
            ("Q", "Official data reported on FAO Questionnaires from countries"),
            ("*", "Unofficial figure"),
            ("Im", "FAO data based on imputation methodology"),
            ("M", "Data not available"),
            ("R", "Estimated data using trading partners database"),
            ("SD", "Statistical Discrepancy"),
            ("S", "Standardized data"),
        ],
    )
    .reset_index()
    .rename(columns={"index": "ranking"})
)


def run(dest_dir: str) -> None:
    # Assume dest_dir is a path to the step that needs to be run, e.g. "faostat_qcl", and fetch namespace and dataset
    # short name from that path.
    dataset_short_name = Path(dest_dir).name
    namespace = dataset_short_name.split("_")[0]
    dataset_code = dataset_short_name.split("_")[1]
    # Path to latest dataset in meadow.
    meadow_version_dir = sorted(
        (DATA_DIR / "meadow" / namespace).glob(f"*/{dataset_short_name}")
    )[-1].parent
    assert meadow_version_dir.is_dir()
    # Version of meadow dataset, which will also be the version of the analogous garden dataset.
    version = meadow_version_dir.name
    # Path to latest code in meadow
    meadow_code_dir = STEP_DIR / "data" / "meadow" / namespace / version
    assert meadow_code_dir.is_dir()
    # Get dataset version from folder name with latest date.
    meadow_data_dir = meadow_version_dir / dataset_short_name
    assert meadow_data_dir.is_dir()
    garden_code_dir = STEP_DIR / "data" / "garden" / namespace / version
    assert garden_code_dir.is_dir()
    countries_file = garden_code_dir / f"{namespace}.countries.json"
    assert countries_file.is_file()
    additional_metadata_dir = meadow_version_dir / f"{namespace}_metadata"
    assert additional_metadata_dir.is_dir()

    # Additional metadata of all FAO datasets.
    additional_metadata = catalog.Dataset(additional_metadata_dir)

    ######################################################
    # Check that the flag definitions in all datasets agree with the ones in the manual ranking.
    for table_name in additional_metadata.table_names:
        if "flag" in table_name:
            flag_df = additional_metadata[table_name].reset_index()
            comparison = pd.merge(FLAGS_RANKING, flag_df, on="flag", how="inner")
            error_message = (
                f"Flag definitions in file {table_name} are different to those in our flag ranking. "
                f"Redefine flag ranking."
            )
            assert (
                comparison["description"] == comparison["flags"]
            ).all(), error_message
    ######################################################

    # Load meadow dataset and keep its metadata.
    dataset_meadow = catalog.Dataset(meadow_data_dir)

    # Load main table from dataset.
    data_table_meadow = dataset_meadow[dataset_short_name]

    data = pd.DataFrame(data_table_meadow).reset_index()

    if not data[
        data.duplicated(
            subset=["area_code", "year", "item_code", "element_code", "unit"]
        )
    ].empty:
        print(
            f"INFO: In {dataset_short_name}, multiple values of area_code-item_code-element_code-unit were found for "
            f"a certain area_code-year (before harmonization of country names). Flags may solve the ambiguity."
        )

    # Check that there are as many codes for area, element and unit and actual areas, elements and units.
    for entity in ["area", "element", "item"]:
        if len(data[f"{entity}_code"].unique()) != len(data[f"{entity}"].unique()):
            print(
                f"WARNING: In {dataset_short_name}, the number of unique {entity} codes is different to the number "
                f"of unique {entity}s. Consider ignoring '{entity}' column, and instead mapping '{entity}_code' "
                f"using the {entity} from metadata."
            )
    # Check problematic rows:
    # cols_to_check = ['area_code', 'country', 'area']
    # df_to_check = data[data['country'] != data['area']].drop_duplicates(subset=cols_to_check)[cols_to_check]
    # Create a dictionary that maps FAO area codes to FAO country names:
    # areas_mapping = metadata[f"meta_{DATASET_CODE}_area"][['country']].to_dict("dict")['country']

    # Add flag ranking to data.
    data = pd.merge(
        data,
        FLAGS_RANKING[["flag", "ranking"]].rename(columns={"ranking": "flag_ranking"}),
        on="flag",
        how="left",
    )

    # Harmonize country names.
    data = geo.harmonize_countries(
        df=data,
        countries_file=countries_file,
        country_col="area",
        warn_on_unused_countries=False,
    ).rename(columns={"area": "country"})
    # If countries are missing in countries file, execute etl.harmonize again on the original list of data_raw['area'].

    # After harmonizing, there are some country-year with more than one item-element.
    # This happens for example because there is different data for "Micronesia" and "Micronesia (Federated States of)",
    # which are both mapped to the same country, "Micronesia (country)".
    # The same happens with "China", and "China, mainland".
    # TODO: Solve possible issue of duplicated regions in China
    # (https://github.com/owid/owid-issues/issues/130#issuecomment-1114859105).

    # In cases where a country-year has more than one item-element, try to remove duplicates by looking at the flags.
    # If flags do not remove the duplicates, raise an error.

    if not data[
        data.duplicated(subset=["country", "year", "item", "element", "unit"])
    ].empty:
        print(
            f"INFO: In {dataset_short_name}, multiple values of item-element-unit were found for a certain "
            f"country-year (after harmonization of country names). Flags may solve the ambiguity."
        )

    # Check that we are not missing any potentially useful flag in our ranking.
    if not data[
        data.duplicated(subset=["country", "year", "item", "element", "unit"])
        & data["flag_ranking"].isnull()
    ].empty:
        print(
            f"In dataset {dataset_short_name}, there is ambiguity in data points and we don't have flags defined to "
            f"break the ambiguity. Add those flags to ranking."
        )
        # Take the definition either from
        missing_flags = list(
            set(
                data[data.duplicated(subset=["country", "year", "item", "element"])][
                    "flag"
                ]
            )
            - set(FLAGS_RANKING["flag"])
        )
        flags_data = pd.DataFrame(
            additional_metadata[f"meta_{dataset_code}_flag"]
        ).reset_index()
        if set(missing_flags) < set(flags_data["flag"]):
            print(
                "Manually copy the following lines to FLAGS_RANKING (and put them in the right order):"
            )
            for i, j in (
                pd.DataFrame(additional_metadata[f"meta_{dataset_code}_flag"])
                .loc[missing_flags]
                .iterrows()
            ):
                print(f"{(i, j['flags'])},")
        else:
            print(
                f"Not all flags ({missing_flags}) are defined in additional metadata. Get their definition from "
                f"https://www.fao.org/faostat/en/#definitions"
            )

    # Check that flags are able to fully remove ambiguities.
    mask_ambiguous = data.duplicated(
        subset=["country", "year", "item", "element", "unit", "flag"]
    )
    if not data[mask_ambiguous].empty:
        countries_with_ambiguities = data[mask_ambiguous]["country"].unique()
        print(
            f"WARNING: In {dataset_short_name}, flags do not remove all ambiguities in data. "
            f"We will arbitrarily remove rows where country-year has two or more values for the same "
            f"item-element-unit-flag. Harmonized country names affected:"
        )
        print("".join([f"* {country}\n" for country in countries_with_ambiguities]))
        # TODO: Check whether values are identical. If not, estimate the maximum absolute fractional error.

    # Remove duplicated data points keeping the one with lowest ranking (i.e. highest priority).
    data = (
        data.sort_values(["country", "year", "flag_ranking"], ascending=True)
        .drop_duplicates(
            subset=["country", "year", "item", "element", "unit"], keep="first"
        )
        .reset_index(drop=True)
    )

    # Check that all ambiguities have been removed (this should always be fulfilled).
    error_message = "Unexpected ambiguities in data."
    assert data[
        data.duplicated(subset=["country", "year", "item", "element", "unit"])
    ].empty, error_message

    # We can now remove entity codes and flags.
    data = data.drop(
        columns=["area_code", "element_code", "item_code", "flag", "flag_ranking"]
    )

    # Combine item, element and unit into one column.
    data["variable"] = (
        data["item"].astype(str)
        + " - "
        + data["element"].astype(str)
        + " ("
        + data["unit"].astype(str)
        + ")"
    )

    data = data.drop(columns=["item", "element", "unit"])

    # Column 'value' was stored as integer, therefore nans are of a special kind.
    # Transform column to object type, and convert nans to normal ones.
    data["value"] = data["value"].astype(object)
    data.loc[data["value"].isnull(), "value"] = np.nan

    data = data.pivot(index=["country", "year"], columns=["variable"], values="value")

    # TODO: Run more sanity checks on the new dataset.

    # Initialize new garden dataset.
    dataset_garden = catalog.Dataset.create_empty(dest_dir)
    # Keep original dataset's metadata from meadow.
    dataset_garden.metadata = deepcopy(dataset_meadow.metadata)

    # Create new table for garden dataset.
    data_table_garden = catalog.Table(data)

    # Make all column names snake_case.
    data_table_garden = catalog.utils.underscore_table(data_table_garden)

    # Use the same table metadata as from original meadow table, but update index.
    data_table_garden.metadata = deepcopy(data_table_meadow.metadata)
    data_table_garden.metadata.primary_key = ["country", "year"]
    # TODO: Check why in food_explorer, _fields are also added to the table.
    # data_table_garden._fields = fields
    # Create new dataset in garden.
    dataset_garden.save()
    # Add table to dataset.
    dataset_garden.add(data_table_garden)
