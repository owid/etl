"""Load a garden dataset and create an explorers dataset."""

import owid.catalog.processing as pr
from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden datasets
    ds_pip = paths.load_dataset("world_bank_pip")
    ds_pip_1000 = paths.load_dataset("thousand_bins_distribution")
    ds_lis = paths.load_dataset("luxembourg_income_study")
    ds_wid = paths.load_dataset("world_inequality_database")
    ds_wdi = paths.load_dataset("wdi")

    # Read tables from garden datasets.
    tb_pip = ds_pip["income_consumption_2017"].reset_index()
    tb_lis = ds_lis["luxembourg_income_study"].reset_index()
    tb_wid = ds_wid["world_inequality_database"].reset_index()
    tb_lis_adults = ds_lis["luxembourg_income_study_adults"].reset_index()

    tb_pip_percentiles = ds_pip["percentiles_income_consumption_2017"].reset_index()
    tb_pip_percentiles_1000 = ds_pip_1000["thousand_bins_distribution"].reset_index()
    tb_lis_percentiles = ds_lis["lis_percentiles"].reset_index()
    tb_wid_percentiles = ds_wid["world_inequality_database_distribution"].reset_index()
    tb_lis_percentiles_adults = ds_lis["lis_percentiles_adults"].reset_index()
    tb_wdi = ds_wdi["wdi"].reset_index()

    # Process tables

    tb_pip_keyvars = create_keyvars_file_pip(tb_pip)
    tb_lis_keyvars = create_keyvars_file_lis(tb_lis, adults=False)
    tb_lis_keyvars_adults = create_keyvars_file_lis(tb_lis_adults, adults=True)
    tb_wid_keyvars = create_keyvars_file_wid(tb_wid, extrapolated=False)
    tb_wid_keyvars_extrapolated = create_keyvars_file_wid(tb_wid, extrapolated=True)

    tb_pip_percentiles = create_percentiles_file_pip(tb_pip_percentiles)
    tb_lis_percentiles = create_percentiles_file_lis(tb_lis_percentiles, adults=False)
    tb_lis_percentiles_adults = create_percentiles_file_lis(tb_lis_percentiles_adults, adults=True)
    tb_wid_percentiles, tb_wid_percentiles_extrapolated = create_percentiles_file_wid(tb_wid_percentiles)

    tb_wdi = extract_gdp_from_wdi(tb_wdi)

    # Concatenate all the tables
    tb = pr.concat(
        [tb_pip_keyvars, tb_lis_keyvars, tb_lis_keyvars_adults, tb_wid_keyvars, tb_wid_keyvars_extrapolated],
        short_name="keyvars",
    )

    tb_percentiles = pr.concat(
        [
            tb_pip_percentiles,
            tb_pip_percentiles_1000,
            tb_lis_percentiles,
            tb_lis_percentiles_adults,
            tb_wid_percentiles,
            tb_wid_percentiles_extrapolated,
        ],
        short_name="percentiles",
    )

    # Drop rows with null values in value column
    tb = tb.dropna(subset=["value"])

    # When all the values in the column value for country, year, series_code are null, drop
    tb_percentiles = tb_percentiles.groupby(["country", "year", "series_code"]).filter(
        lambda x: x["value"].notnull().any()
    )

    # If only one value is not null for country, year, series_code, drop
    tb_percentiles = tb_percentiles.groupby(["country", "year", "series_code"]).filter(
        lambda x: x["value"].notnull().count() > 1
    )

    # Define region "suffixes" in the country column
    region_suffixes_list = ["\\(PIP\\)", "\\(LIS\\)", "\\(WID\\)"]

    # Remove countries that include the text in region_suffixes_list in the country column
    tb = tb[~tb["country"].str.contains("|".join(region_suffixes_list))].reset_index(drop=True)
    tb_percentiles = tb_percentiles[
        ~tb_percentiles["country"].str.contains("|".join(region_suffixes_list), regex=False)
    ].reset_index(drop=True)

    # Remove "World" from country column
    tb = tb[tb["country"] != "World"].reset_index(drop=True)
    tb_percentiles = tb_percentiles[tb_percentiles["country"] != "World"].reset_index(drop=True)

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
            "pipReportingLevel",
            "pipwelfare",
            "prices",
            "unit",
        ],
        verify_integrity=True,
    )
    tb_percentiles = tb_percentiles.set_index(
        [
            "country",
            "year",
            "series_code",
            "percentile",
            "indicator_name",
            "source",
            "welfare",
            "resource_sharing",
            "pipReportingLevel",
            "pipwelfare",
            "prices",
            "unit",
        ],
        verify_integrity=True,
    )

    # Create explorer dataset, with garden table and metadata in csv format
    ds_explorer = create_dataset(
        dest_dir,
        tables=[tb, tb_percentiles, tb_wdi],
        formats=["csv"],
        default_metadata=ds_lis.metadata,
    )
    ds_explorer.save()


def create_keyvars_file_pip(tb_pip: Table) -> Table:
    """
    Process the main table from PIP, to adapt it to a concatenated file with LIS and WID
    """

    tb_pip = tb_pip.copy()

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
    tb_pip = tb_pip[["country", "year", "reporting_level", "welfare_type"] + indicators_list]

    # Make pip table longer
    tb_pip = tb_pip.melt(
        id_vars=["country", "year", "reporting_level", "welfare_type"],
        var_name="indicator_name",
        value_name="value",
    )

    # Rename welfare_type and reporting_level
    tb_pip = tb_pip.rename(columns={"welfare_type": "pipwelfare", "reporting_level": "pipReportingLevel"})

    # Rename welfare and equivalization columns
    tb_pip["indicator_name"] = tb_pip["indicator_name"].replace(
        {
            "palma_ratio": "palmaRatio",
            "headcount_ratio_50_median": "headcountRatio50Median",
            "decile10_share": "p90p100Share",
        }
    )

    # Add descriptive columns
    tb_pip["welfare"] = "disposable"
    tb_pip["resource_sharing"] = "perCapita"
    tb_pip["source"] = "pip"
    tb_pip["prices"] = ""
    tb_pip["prices"] = tb_pip["prices"].where(
        (tb_pip["indicator_name"] != "mean") & (tb_pip["indicator_name"] != "median"),
        "2017ppp2017",
    )
    tb_pip["prices"] = tb_pip["prices"].astype(str)

    # Add the column series_code, which is the concatenation of welfare, equivalization and indicator_name
    tb_pip["series_code"] = (
        tb_pip["indicator_name"].astype(str)
        + "_"
        + tb_pip["source"].astype(str)
        + "_"
        + tb_pip["welfare"].astype(str)
        + "_"
        + tb_pip["resource_sharing"].astype(str)
        + "_"
        + tb_pip["prices"].astype(str)
    )

    # Remove trailing "_" from series_code
    tb_pip["series_code"] = tb_pip["series_code"].str.rstrip("_")

    # Replace names for descriptive columns
    tb_pip["source"] = tb_pip["source"].replace({"pip": "PIP"})
    tb_pip["prices"] = tb_pip["prices"].replace({"2017ppp2017": "2017 PPPs, at 2017 prices"})
    tb_pip["welfare"] = tb_pip["welfare"].replace({"disposable": "Disposable income or consumption"})
    tb_pip["resource_sharing"] = tb_pip["resource_sharing"].replace({"perCapita": "Per capita"})

    # Add unit column
    tb_pip["unit"] = ""
    tb_pip["unit"] = tb_pip["unit"].where(
        (tb_pip["indicator_name"] != "mean") & (tb_pip["indicator_name"] != "median"),
        "dollars",
    )
    tb_pip["unit"] = tb_pip["unit"].where(tb_pip["indicator_name"] != "p90p100Share", "%")
    tb_pip["unit"] = tb_pip["unit"].astype(str)

    return tb_pip


def create_keyvars_file_lis(tb_lis: Table, adults: bool) -> Table:
    """
    Process the main table from LIS, to adapt it to a concatenated file with WID and PIP
    """

    # Define different names for per capita notation, depending on whether the table is for adults or not
    if adults:
        pc_notation = "perAdult"
        pc_notation_human_readable = "Per adult"
    else:
        pc_notation = "perCapita"
        pc_notation_human_readable = "Per capita"

    tb_lis = tb_lis.copy()

    # Set the list of indicators to use
    # TODO: Add averages and shares at the top of the distribution
    indicators_list = [
        "gini_dhi_eq",
        "gini_dhi_pc",
        "gini_mi_eq",
        "gini_mi_pc",
        "mean_dhi_eq",
        "mean_dhi_pc",
        "mean_mi_eq",
        "mean_mi_pc",
        "median_dhi_eq",
        "median_dhi_pc",
        "median_mi_eq",
        "median_mi_pc",
        "share_p100_dhi_eq",
        "share_p100_dhi_pc",
        "share_p100_mi_eq",
        "share_p100_mi_pc",
        "palma_ratio_dhi_eq",
        "palma_ratio_dhi_pc",
        "palma_ratio_mi_eq",
        "palma_ratio_mi_pc",
        "headcount_ratio_50_median_dhi_eq",
        "headcount_ratio_50_median_dhi_pc",
        "headcount_ratio_50_median_mi_eq",
        "headcount_ratio_50_median_mi_pc",
    ]

    # Select the columns to keep
    tb_lis = tb_lis[["country", "year"] + indicators_list]

    # Make lis table longer
    tb_lis = tb_lis.melt(id_vars=["country", "year"], var_name="indicator_welfare_equivalization", value_name="value")

    # Split indicator_welfare_equivalization column into three columns, using the last two "_" as separators
    tb_lis[["indicator_name", "welfare", "equivalization"]] = tb_lis["indicator_welfare_equivalization"].str.rsplit(
        "_", n=2, expand=True
    )

    # Drop indicator_welfare_equivalization column
    tb_lis = tb_lis.drop(columns=["indicator_welfare_equivalization"])

    # Rename welfare and equivalization columns
    tb_lis["welfare"] = tb_lis["welfare"].replace({"mi": "market", "dhi": "disposable"})
    tb_lis["equivalization"] = tb_lis["equivalization"].replace({"eq": "equivalized", "pc": pc_notation})
    tb_lis["indicator_name"] = tb_lis["indicator_name"].replace(
        {
            "palma_ratio": "palmaRatio",
            "headcount_ratio_50_median": "headcountRatio50Median",
            "share_p100": "p90p100Share",
        }
    )

    # Add descriptive columns
    tb_lis["source"] = "lis"
    tb_lis["prices"] = ""
    tb_lis["prices"] = tb_lis["prices"].where(
        (tb_lis["indicator_name"] != "mean") & (tb_lis["indicator_name"] != "median"),
        "2017ppp2017",
    )
    tb_lis["prices"] = tb_lis["prices"].astype(str)

    # Add the column series_code, which is the concatenation of welfare, equivalization and indicator_name
    tb_lis["series_code"] = (
        tb_lis["indicator_name"].astype(str)
        + "_"
        + tb_lis["source"].astype(str)
        + "_"
        + tb_lis["welfare"].astype(str)
        + "_"
        + tb_lis["equivalization"].astype(str)
        + "_"
        + tb_lis["prices"].astype(str)
    )

    # Remove trailing "_" from series_code
    tb_lis["series_code"] = tb_lis["series_code"].str.rstrip("_")

    # Replace names for descriptive columns
    tb_lis["source"] = tb_lis["source"].replace({"lis": "LIS"})
    tb_lis["prices"] = tb_lis["prices"].replace({"2017ppp2017": "2017 PPPs, at 2017 prices"})
    tb_lis["welfare"] = tb_lis["welfare"].replace({"market": "Market income", "disposable": "Disposable income"})
    tb_lis["equivalization"] = tb_lis["equivalization"].replace(
        {"equivalized": "Equivalized", "perCapita": pc_notation_human_readable}
    )

    # Add unit column
    tb_lis["unit"] = ""
    tb_lis["unit"] = tb_lis["unit"].where(
        (tb_lis["indicator_name"] != "mean") & (tb_lis["indicator_name"] != "median"),
        "dollars",
    )
    tb_lis["unit"] = tb_lis["unit"].where(tb_lis["indicator_name"] != "p90p100Share", "%")
    tb_lis["unit"] = tb_lis["unit"].astype(str)

    # Rename column equivalization to resource_sharing
    tb_lis = tb_lis.rename(columns={"equivalization": "resource_sharing"})

    # Remove equivalized data for adults
    if adults:
        tb_lis = tb_lis[tb_lis["resource_sharing"] != "Equivalized"].reset_index(drop=True)

    return tb_lis


def create_keyvars_file_wid(tb_wid: Table, extrapolated: bool) -> Table:
    """
    Process the main table from WID, to adapt it to a concatenated file with LIS and PIP
    """
    tb_wid = tb_wid.copy()

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
    tb_wid = tb_wid[["country", "year"] + indicators_list]

    # Make wid table longer
    tb_wid = tb_wid.melt(id_vars=["country", "year"], var_name="indicator_welfare", value_name="value")

    # Replace the name posttax_nat with posttax
    tb_wid["indicator_welfare"] = tb_wid["indicator_welfare"].str.replace("posttax_nat", "posttax")

    if extrapolated:
        tb_wid["indicator_welfare"] = tb_wid["indicator_welfare"].str.replace("_extrapolated", "")

    # Split indicator_welfare column into two columns, using the last "_" as separator
    tb_wid[["indicator_name", "welfare"]] = tb_wid["indicator_welfare"].str.rsplit("_", n=1, expand=True)

    # Drop indicator_welfare column
    tb_wid = tb_wid.drop(columns=["indicator_welfare"])

    # Rename welfare column
    tb_wid["welfare"] = tb_wid["welfare"].replace({"pretax": "pretaxNational", "posttax": "posttaxNational"})
    tb_wid["indicator_name"] = tb_wid["indicator_name"].replace(
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
        tb_wid["source"] = "widExtrapolated"
    else:
        tb_wid["source"] = "wid"

    tb_wid["prices"] = ""
    tb_wid["prices"] = tb_wid["prices"].where(
        (tb_wid["indicator_name"] != "mean") & (tb_wid["indicator_name"] != "median"),
        "2011ppp2022",
    )
    tb_wid["prices"] = tb_wid["prices"].astype(str)
    tb_wid["resource_sharing"] = "perAdult"

    # Add the column series_code, which is the concatenation of welfare, equivalization and indicator_name
    tb_wid["series_code"] = (
        tb_wid["indicator_name"].astype(str)
        + "_"
        + tb_wid["source"].astype(str)
        + "_"
        + tb_wid["welfare"].astype(str)
        + "_"
        + tb_wid["resource_sharing"].astype(str)
        + "_"
        + tb_wid["prices"].astype(str)
    )

    # Remove trailing "_" from series_code
    tb_wid["series_code"] = tb_wid["series_code"].str.rstrip("_")

    # Replace names for descriptive columns
    if extrapolated:
        tb_wid["source"] = tb_wid["source"].replace({"widExtrapolated": "WID (including extrapolated datapoints)"})
    else:
        tb_wid["source"] = tb_wid["source"].replace({"wid": "WID"})

    tb_wid["prices"] = tb_wid["prices"].replace({"2011ppp2022": "2011 PPPs, at 2022 prices"})
    tb_wid["welfare"] = tb_wid["welfare"].replace(
        {"pretaxNational": "Pretax national income", "posttaxNational": "Post-tax national income"}
    )
    tb_wid["resource_sharing"] = tb_wid["resource_sharing"].replace({"perAdult": "Per adult"})

    # Add unit column
    tb_wid["unit"] = ""
    tb_wid["unit"] = tb_wid["unit"].where(
        (tb_wid["indicator_name"] != "mean") & (tb_wid["indicator_name"] != "median"),
        "dollars",
    )
    tb_wid["unit"] = tb_wid["unit"].where(tb_wid["indicator_name"] != "p99p100Share", "%")
    tb_wid["unit"] = tb_wid["unit"].where(tb_wid["indicator_name"] != "p90p100Share", "%")
    tb_wid["unit"] = tb_wid["unit"].astype(str)

    return tb_wid


def create_percentiles_file_pip(tb_pip_percentiles: Table) -> Table:
    """
    Process the percentiles table from PIP, to adapt it to a concatenated file with LIS and WID
    """

    # Make pip table longer
    tb_pip_percentiles = tb_pip_percentiles.melt(
        id_vars=["country", "year", "reporting_level", "welfare_type", "percentile"],
        value_vars=["thr", "avg", "share"],
        var_name="indicator_name",
        value_name="value",
    )

    # Reduce percentile column by 1 when variable is share or average (when it's different from thr)
    tb_pip_percentiles["percentile"] = tb_pip_percentiles["percentile"].where(
        tb_pip_percentiles["indicator_name"] == "thr", tb_pip_percentiles["percentile"] - 1
    )

    # Replace percentile 100 with 0 (it's always null and only for thr)
    tb_pip_percentiles["percentile"] = tb_pip_percentiles["percentile"].replace(100, 0)

    # Sort by country, year, reporting_level, welfare_type, percentile and variable
    tb_pip_percentiles = tb_pip_percentiles.sort_values(
        ["country", "year", "reporting_level", "welfare_type", "indicator_name", "percentile"]
    )

    # Create WID nomenclature for percentiles
    tb_pip_percentiles["percentile"] = (
        "p" + tb_pip_percentiles["percentile"].astype(str) + "p" + (tb_pip_percentiles["percentile"] + 1).astype(str)
    )

    # Rename welfare_type and reporting_level columns
    tb_pip_percentiles = tb_pip_percentiles.rename(
        columns={"welfare_type": "pipwelfare", "reporting_level": "pipReportingLevel"}
    )

    # Rename indicator_name column
    tb_pip_percentiles["indicator_name"] = tb_pip_percentiles["indicator_name"].replace(
        {"thr": "threshold", "avg": "average"}
    )

    # Add column prices, and assign it the value 2017 PPPs, at 2017 prices only for indicator names different from share
    tb_pip_percentiles["prices"] = ""
    tb_pip_percentiles["prices"] = tb_pip_percentiles["prices"].where(
        tb_pip_percentiles["indicator_name"] == "share", "2017ppp2017"
    )
    tb_pip_percentiles["prices"] = tb_pip_percentiles["prices"].astype(str)

    # Add descriptive columns
    tb_pip_percentiles["source"] = "pip"
    tb_pip_percentiles["welfare"] = "disposable"
    tb_pip_percentiles["resource_sharing"] = "perCapita"

    # Add the column series_code, which is the concatenation of welfare, equivalization and indicator_name
    tb_pip_percentiles["series_code"] = (
        tb_pip_percentiles["indicator_name"].astype(str)
        + "_"
        + tb_pip_percentiles["source"].astype(str)
        + "_"
        + tb_pip_percentiles["welfare"].astype(str)
        + "_"
        + tb_pip_percentiles["resource_sharing"].astype(str)
        + "_"
        + tb_pip_percentiles["prices"].astype(str)
    )

    # Remove trailing "_" from series_code
    tb_pip_percentiles["series_code"] = tb_pip_percentiles["series_code"].str.rstrip("_")

    # Replace names for descriptive columns
    tb_pip_percentiles["source"] = tb_pip_percentiles["source"].replace({"pip": "PIP"})
    tb_pip_percentiles["prices"] = tb_pip_percentiles["prices"].replace({"2017ppp2017": "2017 PPPs, at 2017 prices"})
    tb_pip_percentiles["welfare"] = tb_pip_percentiles["welfare"].replace(
        {"disposable": "Disposable income or consumption"}
    )
    tb_pip_percentiles["resource_sharing"] = tb_pip_percentiles["resource_sharing"].replace({"perCapita": "Per capita"})

    # Add unit column
    tb_pip_percentiles["unit"] = ""
    tb_pip_percentiles["unit"] = tb_pip_percentiles["unit"].where(tb_pip_percentiles["indicator_name"] != "share", "%")
    tb_pip_percentiles["unit"] = tb_pip_percentiles["unit"].where(
        (tb_pip_percentiles["indicator_name"] != "average") & (tb_pip_percentiles["indicator_name"] != "threshold"),
        "dollars",
    )
    tb_pip_percentiles["unit"] = tb_pip_percentiles["unit"].astype(str)

    return tb_pip_percentiles


def create_percentiles_file_pip_1000(tb_pip_percentiles_1000: Table) -> Table:
    """
    Process the percentiles table from PIP (1000 bins), to adapt it to a concatenated file with LIS and WID
    """

    # Make pip table longer
    tb_pip_percentiles_1000 = tb_pip_percentiles_1000.melt(
        id_vars=["country", "year", "quantile"],
        value_vars=["avg"],
        var_name="indicator_name",
        value_name="value",
    )

    # Rename quantile column
    tb_pip_percentiles_1000 = tb_pip_percentiles_1000.rename(columns={"quantile": "percentile"})

    # Sort by country, year, reporting_level, welfare_type, percentile and variable
    tb_pip_percentiles_1000 = tb_pip_percentiles_1000.sort_values(["country", "year", "indicator_name", "percentile"])

    # Reduce percentile column by 1 when variable is share or average (when it's different from thr)
    tb_pip_percentiles_1000["percentile"] = tb_pip_percentiles_1000["percentile"].where(
        tb_pip_percentiles_1000["indicator_name"] == "thr", tb_pip_percentiles_1000["percentile"] - 1
    )

    # Replace percentile 1000 with 0 (it's always null and only for thr)
    tb_pip_percentiles_1000["percentile"] = tb_pip_percentiles_1000["percentile"].replace(1000, 0)

    # Create WID nomenclature for percentiles (for now I call them t1t2, ..., t999t1000 to dfferentiate them from the other percentiles)
    tb_pip_percentiles_1000["percentile"] = (
        "t"
        + tb_pip_percentiles_1000["percentile"].astype(str)
        + "t"
        + (tb_pip_percentiles_1000["percentile"] + 1).astype(str)
    )

    # Rename indicator_name column
    tb_pip_percentiles_1000["indicator_name"] = tb_pip_percentiles_1000["indicator_name"].replace(
        {"thr": "threshold", "avg": "average"}
    )

    # Add column prices, and assign it the value 2017 PPPs, at 2017 prices only for indicator names different from share
    tb_pip_percentiles_1000["prices"] = ""
    tb_pip_percentiles_1000["prices"] = tb_pip_percentiles_1000["prices"].where(
        tb_pip_percentiles_1000["indicator_name"] == "share", "2017ppp2017"
    )
    tb_pip_percentiles_1000["prices"] = tb_pip_percentiles_1000["prices"].astype(str)

    # Add descriptive columns
    tb_pip_percentiles_1000["source"] = "pipThousandBins"
    tb_pip_percentiles_1000["welfare"] = "disposable"
    tb_pip_percentiles_1000["resource_sharing"] = "perCapita"

    # Add the column series_code, which is the concatenation of welfare, equivalization and indicator_name
    tb_pip_percentiles_1000["series_code"] = (
        tb_pip_percentiles_1000["indicator_name"].astype(str)
        + "_"
        + tb_pip_percentiles_1000["source"].astype(str)
        + "_"
        + tb_pip_percentiles_1000["welfare"].astype(str)
        + "_"
        + tb_pip_percentiles_1000["resource_sharing"].astype(str)
        + "_"
        + tb_pip_percentiles_1000["prices"].astype(str)
    )

    # Remove trailing "_" from series_code
    tb_pip_percentiles_1000["series_code"] = tb_pip_percentiles_1000["series_code"].str.rstrip("_")

    # Replace names for descriptive columns
    tb_pip_percentiles_1000["source"] = tb_pip_percentiles_1000["source"].replace(
        {"pipThousandBins": "PIP (thousand bins)"}
    )
    tb_pip_percentiles_1000["prices"] = tb_pip_percentiles_1000["prices"].replace(
        {"2017ppp2017": "2017 PPPs, at 2017 prices"}
    )
    tb_pip_percentiles_1000["welfare"] = tb_pip_percentiles_1000["welfare"].replace(
        {"disposable": "Disposable income or consumption"}
    )
    tb_pip_percentiles_1000["resource_sharing"] = tb_pip_percentiles_1000["resource_sharing"].replace(
        {"perCapita": "Per capita"}
    )

    # Add unit column
    tb_pip_percentiles_1000["unit"] = ""
    tb_pip_percentiles_1000["unit"] = tb_pip_percentiles_1000["unit"].where(
        tb_pip_percentiles_1000["indicator_name"] != "share", "%"
    )
    tb_pip_percentiles_1000["unit"] = tb_pip_percentiles_1000["unit"].where(
        (tb_pip_percentiles_1000["indicator_name"] != "average")
        & (tb_pip_percentiles_1000["indicator_name"] != "threshold"),
        "dollars",
    )
    tb_pip_percentiles_1000["unit"] = tb_pip_percentiles_1000["unit"].astype(str)

    return tb_pip_percentiles_1000


def create_percentiles_file_lis(tb_lis_percentiles: Table, adults: bool) -> Table:
    """
    Process the percentiles table from LIS, to adapt it to a concatenated file with WID and PIP
    """
    # Define different names for per capita notation, depending on whether the table is for adults or not
    if adults:
        pc_notation = "perAdult"
        pc_notation_human_readable = "Per adult"
    else:
        pc_notation = "perCapita"
        pc_notation_human_readable = "Per capita"

    # Make lis table longer
    tb_lis_percentiles = tb_lis_percentiles.melt(
        id_vars=["country", "year", "welfare", "equivalization", "percentile"],
        value_vars=["thr", "avg", "share"],
        var_name="indicator_name",
        value_name="value",
    )

    # Reduce percentile column by 1 when variable is share or average (when it's different from thr)
    tb_lis_percentiles["percentile"] = tb_lis_percentiles["percentile"].where(
        tb_lis_percentiles["indicator_name"] == "thr", tb_lis_percentiles["percentile"] - 1
    )

    # Replace percentile 100 with 0 (it's always null and only for thr)
    tb_lis_percentiles["percentile"] = tb_lis_percentiles["percentile"].replace(100, 0)

    # Sort by country, year, welfare, equivalization, percentile and variable
    tb_lis_percentiles = tb_lis_percentiles.sort_values(
        ["country", "year", "welfare", "equivalization", "indicator_name", "percentile"]
    )

    # Create WID nomenclature for percentiles
    tb_lis_percentiles["percentile"] = (
        "p" + tb_lis_percentiles["percentile"].astype(str) + "p" + (tb_lis_percentiles["percentile"] + 1).astype(str)
    )

    # Filter out welfare dhci
    tb_lis_percentiles = tb_lis_percentiles[tb_lis_percentiles["welfare"] != "dhci"].reset_index(drop=True)

    # Rename welfare, equivalization and indicator_name columns
    tb_lis_percentiles["welfare"] = tb_lis_percentiles["welfare"].replace({"mi": "market", "dhi": "disposable"})
    tb_lis_percentiles["equivalization"] = tb_lis_percentiles["equivalization"].replace(
        {"eq": "equivalized", "pc": pc_notation}
    )
    tb_lis_percentiles["indicator_name"] = tb_lis_percentiles["indicator_name"].replace(
        {"thr": "threshold", "avg": "average"}
    )

    # Add column prices, and assign it the value 2017 PPPs, at 2017 prices only for indicator names different from share
    tb_lis_percentiles["prices"] = ""
    tb_lis_percentiles["prices"] = tb_lis_percentiles["prices"].where(
        tb_lis_percentiles["indicator_name"] == "share", "2017ppp2017"
    )
    tb_lis_percentiles["prices"] = tb_lis_percentiles["prices"].astype(str)

    # Add descriptive columns
    tb_lis_percentiles["source"] = "lis"

    # Add the column series_code, which is the concatenation of welfare, equivalization and indicator_name
    tb_lis_percentiles["series_code"] = (
        tb_lis_percentiles["indicator_name"].astype(str)
        + "_"
        + tb_lis_percentiles["source"].astype(str)
        + "_"
        + tb_lis_percentiles["welfare"].astype(str)
        + "_"
        + tb_lis_percentiles["equivalization"].astype(str)
        + "_"
        + tb_lis_percentiles["prices"].astype(str)
    )

    # Remove trailing "_" from series_code
    tb_lis_percentiles["series_code"] = tb_lis_percentiles["series_code"].str.rstrip("_")

    # Replace names for descriptive columns
    tb_lis_percentiles["source"] = tb_lis_percentiles["source"].replace({"lis": "LIS"})

    tb_lis_percentiles["prices"] = tb_lis_percentiles["prices"].replace({"2017ppp2017": "2017 PPPs, at 2017 prices"})

    tb_lis_percentiles["welfare"] = tb_lis_percentiles["welfare"].replace(
        {"market": "Market income", "disposable": "Disposable income"}
    )
    tb_lis_percentiles["equivalization"] = tb_lis_percentiles["equivalization"].replace(
        {"equivalized": "Equivalized", "perCapita": pc_notation_human_readable}
    )

    # Rename column equivalization to resource_sharing
    tb_lis_percentiles = tb_lis_percentiles.rename(columns={"equivalization": "resource_sharing"})

    # Add unit column
    tb_lis_percentiles["unit"] = ""
    tb_lis_percentiles["unit"] = tb_lis_percentiles["unit"].where(tb_lis_percentiles["indicator_name"] != "share", "%")
    tb_lis_percentiles["unit"] = tb_lis_percentiles["unit"].where(
        (tb_lis_percentiles["indicator_name"] != "average") & (tb_lis_percentiles["indicator_name"] != "threshold"),
        "dollars",
    )
    tb_lis_percentiles["unit"] = tb_lis_percentiles["unit"].astype(str)

    # Remove equivalized data for adults
    if adults:
        tb_lis_percentiles = tb_lis_percentiles[tb_lis_percentiles["resource_sharing"] != "Equivalized"].reset_index(
            drop=True
        )

    return tb_lis_percentiles


def create_percentiles_file_wid(tb_wid_percentiles) -> Table:
    """
    Process the percentiles table from WID, to adapt it to a concatenated file with LIS and PIP. It generates two tables, one with extrapolated datapoints and one without them
    """
    # WID PERCENTILES

    # Make wid table longer
    tb_wid_percentiles = tb_wid_percentiles.melt(
        id_vars=["country", "year", "welfare", "p", "percentile"],
        value_vars=["thr", "avg", "share", "thr_extrapolated", "avg_extrapolated", "share_extrapolated"],
        var_name="indicator_name",
        value_name="value",
    )

    # Sort by country, year, welfare, equivalization, percentile and variable
    tb_wid_percentiles = tb_wid_percentiles.sort_values(["country", "year", "welfare", "indicator_name", "p"])

    # Select only welfare types needed
    tb_wid_percentiles = tb_wid_percentiles[tb_wid_percentiles["welfare"].isin(["pretax", "posttax_nat"])].reset_index(
        drop=True
    )

    # Rename welfare and indicator_name columns
    tb_wid_percentiles["welfare"] = tb_wid_percentiles["welfare"].replace(
        {"pretax": "pretaxNational", "posttax_nat": "posttaxNational"}
    )

    # In indicator_name, replace values that contain avg with average and thr with threshold
    tb_wid_percentiles["indicator_name"] = tb_wid_percentiles["indicator_name"].replace(
        {
            "avg": "average",
            "thr": "threshold",
            "avg_extrapolated": "average_extrapolated",
            "thr_extrapolated": "threshold_extrapolated",
        }
    )

    # Drop percentile values containing "."
    tb_wid_percentiles = tb_wid_percentiles[~tb_wid_percentiles["percentile"].str.contains("\\.")].reset_index(
        drop=True
    )

    # Add descriptive columns
    tb_wid_percentiles["source"] = "wid"

    # If indicator_name contains extrapolated, add the word extrapolated to the source column
    tb_wid_percentiles["source"] = tb_wid_percentiles["source"].where(
        ~tb_wid_percentiles["indicator_name"].str.contains("extrapolated"),
        tb_wid_percentiles["source"] + "Extrapolated",
    )

    # Remove substring _extrapolated from indicator_name
    tb_wid_percentiles["indicator_name"] = tb_wid_percentiles["indicator_name"].str.replace("_extrapolated", "")

    # Define id columns
    tb_wid_percentiles["resource_sharing"] = "perAdult"
    tb_wid_percentiles["prices"] = ""
    tb_wid_percentiles["prices"] = tb_wid_percentiles["prices"].where(
        tb_wid_percentiles["indicator_name"] == "share", "2011ppp2022"
    )
    tb_wid_percentiles["prices"] = tb_wid_percentiles["prices"].astype(str)

    # Add the column series_code, which is the concatenation of welfare, equivalization and indicator_name
    tb_wid_percentiles["series_code"] = (
        tb_wid_percentiles["indicator_name"].astype(str)
        + "_"
        + tb_wid_percentiles["source"].astype(str)
        + "_"
        + tb_wid_percentiles["welfare"].astype(str)
        + "_"
        + tb_wid_percentiles["resource_sharing"].astype(str)
        + "_"
        + tb_wid_percentiles["prices"].astype(str)
    )

    # Remove trailing "_" from series_code
    tb_wid_percentiles["series_code"] = tb_wid_percentiles["series_code"].str.rstrip("_")

    # Replace names for descriptive columns

    tb_wid_percentiles["prices"] = tb_wid_percentiles["prices"].replace({"2011ppp2022": "2011 PPPs, at 2022 prices"})
    tb_wid_percentiles["welfare"] = tb_wid_percentiles["welfare"].replace(
        {"pretaxNational": "Pretax national income", "posttax_nat": "Post-tax national income"}
    )
    tb_wid_percentiles["resource_sharing"] = tb_wid_percentiles["resource_sharing"].replace({"perAdult": "Per adult"})

    # Add unit column
    tb_wid_percentiles["unit"] = ""
    tb_wid_percentiles["unit"] = tb_wid_percentiles["unit"].where(tb_wid_percentiles["indicator_name"] != "share", "%")
    tb_wid_percentiles["unit"] = tb_wid_percentiles["unit"].where(
        (tb_wid_percentiles["indicator_name"] != "average") & (tb_wid_percentiles["indicator_name"] != "threshold"),
        "dollars",
    )
    tb_wid_percentiles["unit"] = tb_wid_percentiles["unit"].astype(str)

    # Remove columns welfare and percentile
    tb_wid_percentiles = tb_wid_percentiles.drop(columns=["p"])

    # Create two different tables, one for extrapolated
    tb_wid_percentiles_extrapolated = tb_wid_percentiles[tb_wid_percentiles["source"] == "widExtrapolated"].reset_index(
        drop=True
    )
    tb_wid_percentiles = tb_wid_percentiles[tb_wid_percentiles["source"] == "wid"].reset_index(drop=True)

    # Replace source for a more descriptive name
    tb_wid_percentiles["source"] = tb_wid_percentiles["source"].replace({"wid": "WID"})
    tb_wid_percentiles_extrapolated["source"] = tb_wid_percentiles_extrapolated["source"].replace(
        {"widExtrapolated": "WID (including extrapolated datapoints)"}
    )

    return tb_wid_percentiles, tb_wid_percentiles_extrapolated


def extract_gdp_from_wdi(tb_wdi: Table) -> Table:
    """
    Load the table from WDI, to extract different GDP indicators
    """

    # Define list of GDP indicators
    gdp_list = [
        "ny_gdp_mktp_pp_kd",  # constant 2017 international $
        "ny_gdp_mktp_kd",  # constant 2015 US$
    ]

    # Select the columns to keep
    tb_wdi = tb_wdi[["country", "year"] + gdp_list]

    return tb_wdi
