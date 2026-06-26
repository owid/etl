"""Build the combined PIP + WID key-indicators (keyvars) file used by inequality_comparison."""

import owid.catalog.processing as pr
from owid.catalog.core import Table, warnings
from shared import build_pip_unsmoothed, build_wid_main

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define PPP year for PIP and WID
# NOTE: Change this in case of new PPP versions in the future
# TODO: Change to 2021 prices
PPP_YEAR_PIP = 2021
PPP_YEAR_WID = 2023


def run() -> None:
    #
    # Load inputs.
    #
    # Load dimensional PIP and WID datasets (the legacy datasets are kept only for the CSV
    # explorers — see shared.py for how the legacy-shaped key-indicator tables are reconstructed).
    ds_pip = paths.load_dataset("world_bank_pip")
    ds_wid = paths.load_dataset("world_inequality_database")

    # Reconstruct the legacy-shaped key-indicator tables from the dimensional datasets.
    tb_pip = build_pip_unsmoothed(ds_pip)
    tb_wid = build_wid_main(ds_wid)

    #
    # Process data.
    #
    tb_pip_keyvars = create_keyvars_file_pip(tb_pip)
    tb_wid_keyvars = create_keyvars_file_wid(tb_wid, extrapolated=False)
    tb_wid_keyvars_extrapolated = create_keyvars_file_wid(tb_wid, extrapolated=True)

    # Concatenate the key-indicator tables.
    tb = pr.concat(
        [tb_pip_keyvars, tb_wid_keyvars, tb_wid_keyvars_extrapolated],
        ignore_index=True,
        short_name="keyvars",
    )

    # Drop rows with null values in value column
    tb = tb.dropna(subset=["value"])

    # Define region "suffixes" in the country column
    region_suffixes_list = ["\\(PIP\\)", "\\(LIS\\)", "\\(WID\\)"]

    # Remove countries that include the text in region_suffixes_list in the country column
    tb = tb[~tb["country"].str.contains("|".join(region_suffixes_list))].reset_index(drop=True)

    # Remove "World" from country column
    tb = tb[tb["country"] != "World"].reset_index(drop=True)

    # Remove urban and rural entities from country column (only WID)
    tb = tb[~tb["country"].isin(["China (urban)", "China (rural)"])].reset_index(drop=True)

    # Replace values of country containing - urban or - rural with the country name only (only PIP)
    tb["country"] = tb["country"].str.replace(" (urban)", "", regex=False).str.replace(" (rural)", "", regex=False)

    # Set index
    tb = tb.set_index(
        [
            "country",
            "year",
            "series_code",
            "indicator_name",
            "source",
            "welfare",
            "resource_sharing",
            "pipreportinglevel",
            "pipwelfare",
            "prices",
            "unit",
        ],
        verify_integrity=True,
    )

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = paths.create_dataset(
        tables=[tb],
        default_metadata=ds_pip.metadata,
        repack=False,
    )
    ds_garden.save()


def create_keyvars_file_pip(tb: Table) -> Table:
    """
    Process the main table from PIP, to adapt it to a concatenated file with LIS and WID
    """

    tb = tb.copy()

    # Set the list of indicators to use
    # TODO: Add averages and shares at the top of the distribution
    indicators_list = [
        "gini",
        "mean",
        "median",
        "decile10_share",
        "palma_ratio",
        "headcount_ratio_50_median",
    ]

    # Select the columns to keep
    tb = tb[["country", "year", "reporting_level", "welfare_type"] + indicators_list]

    with warnings.ignore_warnings([warnings.DifferentValuesWarning]):
        # Make pip table longer
        tb = tb.melt(
            id_vars=["country", "year", "reporting_level", "welfare_type"],
            var_name="indicator_name",
            value_name="value",
        )

    # Rename welfare_type and reporting_level
    tb = tb.rename(columns={"welfare_type": "pipwelfare", "reporting_level": "pipreportinglevel"})

    # Rename welfare and equivalization columns
    tb["indicator_name"] = tb["indicator_name"].replace(
        {
            "palma_ratio": "palmaRatio",
            "headcount_ratio_50_median": "headcountRatio50Median",
            "decile10_share": "p90p100Share",
        }
    )

    # Add descriptive columns
    tb["welfare"] = "disposable"
    tb["resource_sharing"] = "perCapita"
    tb["source"] = "pip"
    tb["prices"] = ""
    tb["prices"] = tb["prices"].where(
        (tb["indicator_name"] != "mean") & (tb["indicator_name"] != "median"),
        f"{PPP_YEAR_PIP}ppp{PPP_YEAR_PIP}",
    )
    tb["prices"] = tb["prices"].astype(str)

    # Add the column series_code, which is the concatenation of welfare, equivalization and indicator_name
    tb["series_code"] = (
        tb["indicator_name"].astype(str)
        + "_"
        + tb["source"].astype(str)
        + "_"
        + tb["welfare"].astype(str)
        + "_"
        + tb["resource_sharing"].astype(str)
        + "_"
        + tb["prices"].astype(str)
    )

    # Remove trailing "_" from series_code
    tb["series_code"] = tb["series_code"].str.rstrip("_")

    # Replace names for descriptive columns
    tb["source"] = tb["source"].replace({"pip": "PIP"})
    tb["prices"] = tb["prices"].replace(
        {f"{PPP_YEAR_PIP}ppp{PPP_YEAR_PIP}": f"{PPP_YEAR_PIP} PPPs, at {PPP_YEAR_PIP} prices"}
    )
    tb["welfare"] = tb["welfare"].replace({"disposable": "Disposable income or consumption"})
    tb["resource_sharing"] = tb["resource_sharing"].replace({"perCapita": "Per capita"})

    # Add unit column
    tb["unit"] = ""
    tb["unit"] = tb["unit"].where(
        (tb["indicator_name"] != "mean") & (tb["indicator_name"] != "median"),
        "dollars",
    )
    tb["unit"] = tb["unit"].where(tb["indicator_name"] != "p90p100Share", "%")
    tb["unit"] = tb["unit"].astype(str)

    return tb


def create_keyvars_file_wid(tb: Table, extrapolated: bool) -> Table:
    """
    Process the main table from WID, to adapt it to a concatenated file with LIS and PIP
    """
    tb = tb.copy()

    # Set the list of indicators to use
    indicators_list = [
        "p0p100_gini_pretax",
        "p0p100_gini_posttax_nat",
        "p0p100_avg_pretax",
        "p0p100_avg_posttax_nat",
        "median_pretax",
        "median_posttax_nat",
        "p99p100_share_pretax",
        "p99p100_share_posttax_nat",
        "p99p100_avg_pretax",
        "p99p100_avg_posttax_nat",
        "p90p100_share_pretax",
        "p90p100_share_posttax_nat",
        "palma_ratio_pretax",
        "palma_ratio_posttax_nat",
        "headcount_ratio_50_median_pretax",
        "headcount_ratio_50_median_posttax_nat",
    ]

    # Add _extrapolated to each member of indicators_list
    if extrapolated:
        indicators_list = [indicator + "_extrapolated" for indicator in indicators_list]

    # Select the columns to keep
    tb = tb[["country", "year"] + indicators_list]

    with warnings.ignore_warnings([warnings.DifferentValuesWarning]):
        # Make wid table longer
        tb = tb.melt(id_vars=["country", "year"], var_name="indicator_welfare", value_name="value")

    # Replace the name posttax_nat with posttax
    tb["indicator_welfare"] = tb["indicator_welfare"].str.replace("posttax_nat", "posttax")

    if extrapolated:
        tb["indicator_welfare"] = tb["indicator_welfare"].str.replace("_extrapolated", "")

    # Split indicator_welfare column into two columns, using the last "_" as separator
    tb[["indicator_name", "welfare"]] = tb["indicator_welfare"].str.rsplit("_", n=1, expand=True)

    # Drop indicator_welfare column
    tb = tb.drop(columns=["indicator_welfare"])

    # Rename welfare column
    tb["welfare"] = tb["welfare"].replace({"pretax": "pretaxNational", "posttax": "posttaxNational"})
    tb["indicator_name"] = tb["indicator_name"].replace(
        {
            "p0p100_gini": "gini",
            "p0p100_avg": "mean",
            "p99p100_share": "p99p100Share",
            "p99p100_avg": "p99p100Average",
            "p90p100_share": "p90p100Share",
            "palma_ratio": "palmaRatio",
            "headcount_ratio_50_median": "headcountRatio50Median",
        }
    )

    # Add descriptive columns
    if extrapolated:
        tb["source"] = "widExtrapolated"
    else:
        tb["source"] = "wid"

    tb["prices"] = ""
    tb["prices"] = tb["prices"].where(
        (tb["indicator_name"] != "mean") & (tb["indicator_name"] != "median"),
        f"2011ppp{PPP_YEAR_WID}",
    )
    tb["prices"] = tb["prices"].astype(str)
    tb["resource_sharing"] = "perAdult"

    # Add the column series_code, which is the concatenation of welfare, equivalization and indicator_name
    tb["series_code"] = (
        tb["indicator_name"].astype(str)
        + "_"
        + tb["source"].astype(str)
        + "_"
        + tb["welfare"].astype(str)
        + "_"
        + tb["resource_sharing"].astype(str)
        + "_"
        + tb["prices"].astype(str)
    )

    # Remove trailing "_" from series_code
    tb["series_code"] = tb["series_code"].str.rstrip("_")

    # Replace names for descriptive columns
    if extrapolated:
        tb["source"] = tb["source"].replace({"widExtrapolated": "WID (including extrapolated datapoints)"})
    else:
        tb["source"] = tb["source"].replace({"wid": "WID"})

    tb["prices"] = tb["prices"].replace({f"2011ppp{PPP_YEAR_WID}": f"2011 PPPs, at {PPP_YEAR_WID} prices"})
    tb["welfare"] = tb["welfare"].replace(
        {"pretaxNational": "Pretax national income", "posttaxNational": "Post-tax national income"}
    )
    tb["resource_sharing"] = tb["resource_sharing"].replace({"perAdult": "Per adult"})

    # Add unit column
    tb["unit"] = ""
    tb["unit"] = tb["unit"].where(
        (tb["indicator_name"] != "mean") & (tb["indicator_name"] != "median"),
        "dollars",
    )
    tb["unit"] = tb["unit"].where(tb["indicator_name"] != "p99p100Share", "%")
    tb["unit"] = tb["unit"].where(tb["indicator_name"] != "p90p100Share", "%")
    tb["unit"] = tb["unit"].astype(str)

    return tb
