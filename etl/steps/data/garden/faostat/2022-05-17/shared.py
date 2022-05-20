"""Common processing of FAOSTAT datasets.

We have created a manual ranking of FAOSTAT flags. These flags are only used when there is ambiguity in the data,
namely, when there is more than one data value for a certain country-year-item-element-unit.
NOTES:
* We check that the definitions in our manual ranking agree with the ones provided by FAOSTAT.
* We do not include all flags: We include only the ones that solve an ambiguity in a particular case,
  and add more flags as we see need.
* We have found flags that appeared in a dataset, but were not included in the additional metadata
  (namely flag "R", found in qcl dataset, and "W" in rt dataset). These flags were added manually, using the definition
  in List / Flags in:
  https://www.fao.org/faostat/en/#definitions
* Other flags (namel "B", in rl dataset and "w" in rt dataset) were not found either in the additional metadata or in
  the website definitions. They have been assigned the description "Unknown flag".
* Unfortunately, flags do not remove all ambiguities: remaining duplicates are dropped without any meaningful criterion.

"""

from copy import deepcopy
from pathlib import Path

import numpy as np
import pandas as pd
from owid import catalog
from owid.datautils import geo

from etl.paths import DATA_DIR, STEP_DIR


NAMESPACE = Path(__file__).parent.parent.name
VERSION = Path(__file__).parent.name

# Regions to add to the data.
# TODO: Add region aggregates to relevant columns.
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
            (
                "Qm",
                "Official data from questionnaires and/or national sources and/or COMTRADE (reporters)",
            ),
            ("Fk", "Calculated data on the basis of official figures"),
            ("Fb", "Data obtained as a balance"),
            ("E", "Expert sources from FAO (including other divisions)"),
            ("X", "International reliable sources"),
            ("Bk", "Break in series"),
            ("NV", "Data not available"),
            ("FC", "Calculated data"),
            (
                "Z",
                "When the Fertilizer Utilization Account (FUA) does not balance due to utilization from stockpiles, apparent consumption has been set to zero",
            ),
            ("P", "Provisional official data"),
            (
                "W",
                "Data reported on country official publications or web sites (Official) or trade country files",
            ),
            ("B", "Unknown flag"),
            ("w", "Unknown flag"),
            ("NR", "Not reported"),
            ("_P", "Provisional value"),
            ("_O", "Missing value"),
            ("_M", "Unknown flag"),
            ("_U", "Unknown flag"),
            ("_I", "Imputed value (CCSA definition)"),
            ("_V", "Unvalidated value"),
            ("_L", "Unknown flag"),
            ("_A", "Normal value"),
            ("_E", "Estimated value"),
            ("Cv", "Calculated through value"),
            # The definition of flag "_" exists, but it's empty.
            ("_", ""),
        ],
    )
    .reset_index()
    .rename(columns={"index": "ranking"})
)


def check_that_flag_definitions_in_dataset_agree_with_those_in_flags_ranking(
    additional_metadata: catalog.Dataset,
) -> None:
    """TODO"""
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


def check_that_all_flags_in_dataset_are_in_ranking(
    data: pd.DataFrame,
    additional_metadata_for_flags: catalog.Table,
    country_col: str = "area",
) -> None:
    """TODO"""
    if not set(data["flag"]) < set(FLAGS_RANKING["flag"]):
        missing_flags = set(data["flag"]) - set(FLAGS_RANKING["flag"])
        # missing_flags = list(
        #     set(
        #         data[data.duplicated(subset=[country_col, "year", "item", "element"])][
        #             "flag"
        #         ]
        #     )
        #     - set(FLAGS_RANKING["flag"])
        # )
        flags_data = pd.DataFrame(additional_metadata_for_flags).reset_index()
        if set(missing_flags) < set(flags_data["flag"]):
            print(
                "Manually copy the following lines to FLAGS_RANKING (and put them in the right order):"
            )
            for i, j in (
                pd.DataFrame(additional_metadata_for_flags)
                .loc[list(missing_flags)]
                .iterrows()
            ):
                print(f"{(i, j['flags'])},")
        else:
            print(
                f"Not all flags ({missing_flags}) are defined in additional metadata. Get their definition from "
                f"https://www.fao.org/faostat/en/#definitions"
            )
        raise AssertionError(
            "Flags in dataset not found in FLAGS_RANKING. Manually add those flags."
        )


def check_that_there_are_as_many_entity_codes_as_entities(data: pd.DataFrame) -> None:
    """TODO"""
    # Check that there are as many codes for area, element and unit and actual areas, elements and units.
    entities = list({"area", "element", "item"} & set(data.columns))
    for entity in entities:
        if len(data[f"{entity}_code"].unique()) != len(data[f"{entity}"].unique()):
            print(
                f"WARNING: The number of unique {entity} codes is different to the number "
                f"of unique {entity}s. Consider ignoring '{entity}' column, and instead mapping '{entity}_code' "
                f"using the {entity} from metadata."
            )


def remove_rows_with_nan_value(
    data: pd.DataFrame, verbose: bool = True
) -> pd.DataFrame:
    """TODO"""
    data = data.copy()
    # Number of rows with a nan in column "value".
    # We could also remove rows with any nan, however, before doing that, we would need to assign a value to nan flags.
    n_rows_with_nan_value = len(data[data["value"].isnull()])
    if n_rows_with_nan_value > 0:
        if verbose:
            print(
                f"Removing {n_rows_with_nan_value} rows ({n_rows_with_nan_value / len(data): .2%}) "
                f"with nan in column 'value'."
            )
        data = data.dropna(subset="value").reset_index(drop=True)

    return data


def remove_duplicates(data: pd.DataFrame) -> pd.DataFrame:
    """TODO"""
    data = data.copy()

    # Add flag ranking to dataset.
    data = pd.merge(
        data,
        FLAGS_RANKING[["flag", "ranking"]].rename(columns={"ranking": "flag_ranking"}),
        on="flag",
        how="left",
    )

    # Select columns that should be used as indexes.
    index_columns = [
        column
        for column in ["country", "year", "item", "element", "unit"]
        if column in data.columns
    ]

    # Number of ambiguous indices (those that have multiple data values).
    n_ambiguous_indices = len(data[data.duplicated(subset=index_columns, keep="first")])

    if n_ambiguous_indices > 0:
        # Number of ambiguous indices that cannot be solved using flags.
        n_ambiguous_indices_unsolvable = len(
            data[data.duplicated(subset=index_columns + ["flag_ranking"], keep="first")]
        )
        # Remove ambiguous indices (those that have multiple data values).
        # When possible, use flags to prioritise among duplicates.
        data = data.sort_values(index_columns + ["flag_ranking"]).drop_duplicates(
            subset=index_columns, keep="first"
        )
        frac_ambiguous = n_ambiguous_indices / len(data)
        frac_ambiguous_solved_by_flags = 1 - (
            n_ambiguous_indices_unsolvable / n_ambiguous_indices
        )
        print(
            f"Removing {n_ambiguous_indices} ambiguous indices ({frac_ambiguous: .2%})."
        )
        print(
            f"{frac_ambiguous_solved_by_flags: .2%} of ambiguities were solved with flags."
        )

    data = data.drop(columns=["flag_ranking"])

    return data


def clean_year_column(year_column: pd.Series) -> pd.Series:
    """Clean year column.

    Year is given almost always as an integer value. But sometimes (e.g. in the faostat_fs dataset) it is a range of
    years (that differ by exactly 2 years, e.g. "2010-2012"). This function returns a series of integer years, which, in
    the cases where the original year was a range, corresponds to the mean of the range.

    Parameters
    ----------
    year_column : pd.Series
        Original column of year values (which may be integer, or ranges of values).

    Returns
    -------
    year_clean_series : pd.Series
        Clean column of years, as integer values.

    """
    year_clean = []
    for year in year_column:
        if "-" in str(year):
            year_range = year.split("-")
            year_min = int(year_range[0])
            year_max = int(year_range[1])
            assert year_max - year_min == 2
            year_clean.append(year_min + 1)
        else:
            year_clean.append(int(year))

    # Prepare series of integer year values.
    year_clean_series = pd.Series(year_clean)
    year_clean_series.name = "year"

    return year_clean_series


def clean_data(data: pd.DataFrame, countries_file: Path) -> pd.DataFrame:
    """TODO"""
    data = data.copy()

    # Ensure column of values is numeric (transform any possible value like "<1" into a nan).
    data["value"] = pd.to_numeric(data["value"], errors="coerce")

    # Some datasets (at least faostat_fa) use "recipient_country" instead of "area". For consistency, change this.
    if "recipient_country" in data.columns:
        data = data.rename(
            columns={"recipient_country": "area", "recipient_country_code": "area_code"}
        )

    # Remove rows with nan value.
    data = remove_rows_with_nan_value(data)

    # Sanity checks.
    check_that_there_are_as_many_entity_codes_as_entities(data)

    # Harmonize country names.
    assert countries_file.is_file(), "countries file not found."
    data = geo.harmonize_countries(
        df=data,
        countries_file=str(countries_file),
        country_col="area",
        warn_on_unused_countries=False,
    ).rename(columns={"area": "country"})
    # If countries are missing in countries file, execute etl.harmonize again and update countries file.

    # After harmonizing, there are some country-year with more than one item-element.
    # This happens for example because there is different data for "Micronesia" and "Micronesia (Federated States of)",
    # which are both mapped to the same country, "Micronesia (country)".
    # The same happens with "China", and "China, mainland".
    # TODO: Solve possible issue of duplicated regions in China
    # (https://github.com/owid/owid-issues/issues/130#issuecomment-1114859105).
    # In cases where a country-year has more than one item-element, try to remove duplicates by looking at the flags.
    # If flags do not remove the duplicates, raise an error.

    # Ensure year column is integer (sometimes it is given as a range of years, e.g. 2013-2015).
    data["year"] = clean_year_column(data["year"])

    # Remove duplicated data points keeping the one with lowest ranking (i.e. highest priority).
    data = remove_duplicates(data)

    # We can now remove entity codes and flags.
    columns_to_drop = list(
        {"area_code", "element_code", "item_code", "flag"} & set(data.columns)
    )
    data = data.drop(columns=columns_to_drop)

    # Set appropriate indexes.
    data = data.set_index(["country", "year", "item", "element", "unit"]).sort_index()

    return data


def run(dest_dir: str) -> None:
    ####################################################################################################################
    # Common definitions.
    ####################################################################################################################

    # Assume dest_dir is a path to the step that needs to be run, e.g. "faostat_qcl", and fetch namespace and dataset
    # short name from that path.
    dataset_short_name = Path(dest_dir).name
    namespace = dataset_short_name.split("_")[0]
    dataset_code = dataset_short_name.split("_")[1]
    # Path to latest dataset in meadow.
    meadow_version_dir = sorted(
        (DATA_DIR / "meadow" / namespace).glob(f"*/{dataset_short_name}")
    )[-1].parent
    # Version of meadow dataset, which will also be the version of the analogous garden dataset.
    version = meadow_version_dir.name
    # Get dataset version from folder name with latest date.
    meadow_data_dir = meadow_version_dir / dataset_short_name
    garden_code_dir = STEP_DIR / "data" / "garden" / namespace / version
    countries_file = garden_code_dir / f"{namespace}.countries.json"
    additional_metadata_dir = meadow_version_dir / f"{namespace}_metadata"

    ####################################################################################################################
    # Load data.
    ####################################################################################################################

    # Additional metadata of all FAO datasets (used for sanity checks).
    additional_metadata = catalog.Dataset(additional_metadata_dir)

    # Sanity checks.
    check_that_flag_definitions_in_dataset_agree_with_those_in_flags_ranking(
        additional_metadata
    )

    # Load meadow dataset and keep its metadata.
    dataset_meadow = catalog.Dataset(meadow_data_dir)

    # Load main table from dataset.
    data_table_meadow = dataset_meadow[dataset_short_name]
    data = pd.DataFrame(data_table_meadow).reset_index()

    # Sanity checks.
    check_that_all_flags_in_dataset_are_in_ranking(
        data, additional_metadata[f"meta_{dataset_code}_flag"]
    )

    ####################################################################################################################
    # Process data.
    ####################################################################################################################

    data = clean_data(data=data, countries_file=countries_file)

    # Move to grapher step.
    #     # Create new table for garden dataset (use metadata from original meadow table).
    #     data_table_garden = create_wide_table_with_metadata_from_long_dataframe(
    #         data_long=data, table_metadata=data_table_meadow.metadata
    #     )

    # TODO: Run more sanity checks on the new wide data.

    ####################################################################################################################
    # Save outputs.
    ####################################################################################################################

    # Initialize new garden dataset.
    dataset_garden = catalog.Dataset.create_empty(dest_dir)
    # Keep original dataset's metadata from meadow.
    dataset_garden.metadata = deepcopy(dataset_meadow.metadata)
    # Create new dataset in garden.
    dataset_garden.save()
    # Create new table for garden dataset.
    data_table = catalog.Table(data).copy()
    # Table metadatata will be identical to the meadow table except for the index.
    data_table.metadata = deepcopy(data_table_meadow.metadata)
    data_table.metadata.primary_key = list(data.index.names)
    # Add table to dataset.
    dataset_garden.add(data_table)
