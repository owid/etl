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
    tb_pip_percentiles_1000 = create_percentiles_file_pip_1000(tb_pip_percentiles_1000)
    tb_lis_percentiles = create_percentiles_file_lis(tb_lis_percentiles, adults=False)
    tb_lis_percentiles_adults = create_percentiles_file_lis(tb_lis_percentiles_adults, adults=True)
    tb_wid_percentiles, tb_wid_percentiles_extrapolated = create_percentiles_file_wid(tb_wid_percentiles)

    tb_wdi = extract_gdp_from_wdi(tb_wdi)

    # Concatenate all the tables
    tb = pr.concat(
        [tb_pip_keyvars, tb_lis_keyvars, tb_lis_keyvars_adults, tb_wid_keyvars, tb_wid_keyvars_extrapolated],
        ignore_index=True,
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
        ignore_index=True,
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
            "pipreportinglevel",
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
            "pipreportinglevel",
            "pipwelfare",
            "prices",
            "unit",
        ],
        verify_integrity=True,
    )

    tb_wdi = tb_wdi.set_index(["country", "year"], verify_integrity=True)

    # Create explorer dataset
    ds_explorer = create_dataset(
        dest_dir,
        tables=[tb, tb_percentiles, tb_wdi],
        default_metadata=ds_lis.metadata,
    )
    ds_explorer.save()


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
        "2017ppp2017",
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
    tb["prices"] = tb["prices"].replace({"2017ppp2017": "2017 PPPs, at 2017 prices"})
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


def create_keyvars_file_lis(tb: Table, adults: bool) -> Table:
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

    tb = tb.copy()

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
    tb = tb[["country", "year"] + indicators_list]

    # Make lis table longer
    tb = tb.melt(id_vars=["country", "year"], var_name="indicator_welfare_equivalization", value_name="value")

    # Split indicator_welfare_equivalization column into three columns, using the last two "_" as separators
    tb[["indicator_name", "welfare", "equivalization"]] = tb["indicator_welfare_equivalization"].str.rsplit(
        "_", n=2, expand=True
    )

    # Drop indicator_welfare_equivalization column
    tb = tb.drop(columns=["indicator_welfare_equivalization"])

    # Rename welfare and equivalization columns
    tb["welfare"] = tb["welfare"].replace({"mi": "market", "dhi": "disposable"})
    tb["equivalization"] = tb["equivalization"].replace({"eq": "equivalized", "pc": pc_notation})
    tb["indicator_name"] = tb["indicator_name"].replace(
        {
            "palma_ratio": "palmaRatio",
            "headcount_ratio_50_median": "headcountRatio50Median",
            "share_p100": "p90p100Share",
        }
    )

    # Add descriptive columns
    tb["source"] = "lis"
    tb["prices"] = ""
    tb["prices"] = tb["prices"].where(
        (tb["indicator_name"] != "mean") & (tb["indicator_name"] != "median"),
        "2017ppp2017",
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
        + tb["equivalization"].astype(str)
        + "_"
        + tb["prices"].astype(str)
    )

    # Remove trailing "_" from series_code
    tb["series_code"] = tb["series_code"].str.rstrip("_")

    # Replace names for descriptive columns
    tb["source"] = tb["source"].replace({"lis": "LIS"})
    tb["prices"] = tb["prices"].replace({"2017ppp2017": "2017 PPPs, at 2017 prices"})
    tb["welfare"] = tb["welfare"].replace({"market": "Market income", "disposable": "Disposable income"})
    tb["equivalization"] = tb["equivalization"].replace(
        {"equivalized": "Equivalized", "perCapita": pc_notation_human_readable}
    )

    # Add unit column
    tb["unit"] = ""
    tb["unit"] = tb["unit"].where(
        (tb["indicator_name"] != "mean") & (tb["indicator_name"] != "median"),
        "dollars",
    )
    tb["unit"] = tb["unit"].where(tb["indicator_name"] != "p90p100Share", "%")
    tb["unit"] = tb["unit"].astype(str)

    # Rename column equivalization to resource_sharing
    tb = tb.rename(columns={"equivalization": "resource_sharing"})

    # Remove equivalized data for adults
    if adults:
        tb = tb[tb["resource_sharing"] != "Equivalized"].reset_index(drop=True)

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
        "2011ppp2022",
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

    tb["prices"] = tb["prices"].replace({"2011ppp2022": "2011 PPPs, at 2022 prices"})
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


def create_percentiles_file_pip(tb: Table) -> Table:
    """
    Process the percentiles table from PIP, to adapt it to a concatenated file with LIS and WID
    """

    # Make pip table longer
    tb = tb.melt(
        id_vars=["country", "year", "reporting_level", "welfare_type", "percentile"],
        value_vars=["thr", "avg", "share"],
        var_name="indicator_name",
        value_name="value",
    )

    # Reduce percentile column by 1 when variable is share or average (when it's different from thr)
    tb["percentile"] = tb["percentile"].where(tb["indicator_name"] == "thr", tb["percentile"] - 1)

    # Replace percentile 100 with 0 (it's always null and only for thr)
    tb["percentile"] = tb["percentile"].replace(100, 0)

    # Sort by country, year, reporting_level, welfare_type, percentile and variable
    tb = tb.sort_values(["country", "year", "reporting_level", "welfare_type", "indicator_name", "percentile"])

    # Create WID nomenclature for percentiles
    tb["percentile"] = "p" + tb["percentile"].astype(str) + "p" + (tb["percentile"] + 1).astype(str)

    # Rename welfare_type and reporting_level columns
    tb = tb.rename(columns={"welfare_type": "pipwelfare", "reporting_level": "pipreportinglevel"})

    # Rename indicator_name column
    tb["indicator_name"] = tb["indicator_name"].replace({"thr": "threshold", "avg": "average"})

    # Add column prices, and assign it the value 2017 PPPs, at 2017 prices only for indicator names different from share
    tb["prices"] = ""
    tb["prices"] = tb["prices"].where(tb["indicator_name"] == "share", "2017ppp2017")
    tb["prices"] = tb["prices"].astype(str)

    # Add descriptive columns
    tb["source"] = "pip"
    tb["welfare"] = "disposable"
    tb["resource_sharing"] = "perCapita"

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
    tb["prices"] = tb["prices"].replace({"2017ppp2017": "2017 PPPs, at 2017 prices"})
    tb["welfare"] = tb["welfare"].replace({"disposable": "Disposable income or consumption"})
    tb["resource_sharing"] = tb["resource_sharing"].replace({"perCapita": "Per capita"})

    # Add unit column
    tb["unit"] = ""
    tb["unit"] = tb["unit"].where(tb["indicator_name"] != "share", "%")
    tb["unit"] = tb["unit"].where(
        (tb["indicator_name"] != "average") & (tb["indicator_name"] != "threshold"),
        "dollars",
    )
    tb["unit"] = tb["unit"].astype(str)

    return tb


def create_percentiles_file_pip_1000(tb: Table) -> Table:
    """
    Process the percentiles table from PIP (1000 bins), to adapt it to a concatenated file with LIS and WID
    """

    # Make pip table longer
    tb = tb.melt(
        id_vars=["country", "year", "quantile"],
        value_vars=["avg"],
        var_name="indicator_name",
        value_name="value",
    )

    # Rename quantile column
    tb = tb.rename(columns={"quantile": "percentile"})

    # Sort by country, year, reporting_level, welfare_type, percentile and variable
    tb = tb.sort_values(["country", "year", "indicator_name", "percentile"])

    # Reduce percentile column by 1 when variable is share or average (when it's different from thr)
    tb["percentile"] = tb["percentile"].where(tb["indicator_name"] == "thr", tb["percentile"] - 1)

    # Replace percentile 1000 with 0 (it's always null and only for thr)
    tb["percentile"] = tb["percentile"].replace(1000, 0)

    # Create WID nomenclature for percentiles (for now I call them t1t2, ..., t999t1000 to dfferentiate them from the other percentiles)
    tb["percentile"] = "t" + tb["percentile"].astype(str) + "t" + (tb["percentile"] + 1).astype(str)

    # Rename indicator_name column
    tb["indicator_name"] = tb["indicator_name"].replace({"thr": "threshold", "avg": "average"})

    # Add column prices, and assign it the value 2017 PPPs, at 2017 prices only for indicator names different from share
    tb["prices"] = ""
    tb["prices"] = tb["prices"].where(tb["indicator_name"] == "share", "2017ppp2017")
    tb["prices"] = tb["prices"].astype(str)

    # Add descriptive columns
    tb["source"] = "pipThousandBins"
    tb["welfare"] = "disposable"
    tb["resource_sharing"] = "perCapita"

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
    tb["source"] = tb["source"].replace({"pipThousandBins": "PIP (thousand bins)"})
    tb["prices"] = tb["prices"].replace({"2017ppp2017": "2017 PPPs, at 2017 prices"})
    tb["welfare"] = tb["welfare"].replace({"disposable": "Disposable income or consumption"})
    tb["resource_sharing"] = tb["resource_sharing"].replace({"perCapita": "Per capita"})

    # Add unit column
    tb["unit"] = ""
    tb["unit"] = tb["unit"].where(tb["indicator_name"] != "share", "%")
    tb["unit"] = tb["unit"].where(
        (tb["indicator_name"] != "average") & (tb["indicator_name"] != "threshold"),
        "dollars",
    )
    tb["unit"] = tb["unit"].astype(str)

    return tb


def create_percentiles_file_lis(tb: Table, adults: bool) -> Table:
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
    tb = tb.melt(
        id_vars=["country", "year", "welfare", "equivalization", "percentile"],
        value_vars=["thr", "avg", "share"],
        var_name="indicator_name",
        value_name="value",
    )

    # Reduce percentile column by 1 when variable is share or average (when it's different from thr)
    tb["percentile"] = tb["percentile"].where(tb["indicator_name"] == "thr", tb["percentile"] - 1)

    # Replace percentile 100 with 0 (it's always null and only for thr)
    tb["percentile"] = tb["percentile"].replace(100, 0)

    # Sort by country, year, welfare, equivalization, percentile and variable
    tb = tb.sort_values(["country", "year", "welfare", "equivalization", "indicator_name", "percentile"])

    # Create WID nomenclature for percentiles
    tb["percentile"] = "p" + tb["percentile"].astype(str) + "p" + (tb["percentile"] + 1).astype(str)

    # Filter out welfare dhci
    tb = tb[tb["welfare"] != "dhci"].reset_index(drop=True)

    # Rename welfare, equivalization and indicator_name columns
    tb["welfare"] = tb["welfare"].replace({"mi": "market", "dhi": "disposable"})
    tb["equivalization"] = tb["equivalization"].replace({"eq": "equivalized", "pc": pc_notation})
    tb["indicator_name"] = tb["indicator_name"].replace({"thr": "threshold", "avg": "average"})

    # Add column prices, and assign it the value 2017 PPPs, at 2017 prices only for indicator names different from share
    tb["prices"] = ""
    tb["prices"] = tb["prices"].where(tb["indicator_name"] == "share", "2017ppp2017")
    tb["prices"] = tb["prices"].astype(str)

    # Add descriptive columns
    tb["source"] = "lis"

    # Add the column series_code, which is the concatenation of welfare, equivalization and indicator_name
    tb["series_code"] = (
        tb["indicator_name"].astype(str)
        + "_"
        + tb["source"].astype(str)
        + "_"
        + tb["welfare"].astype(str)
        + "_"
        + tb["equivalization"].astype(str)
        + "_"
        + tb["prices"].astype(str)
    )

    # Remove trailing "_" from series_code
    tb["series_code"] = tb["series_code"].str.rstrip("_")

    # Replace names for descriptive columns
    tb["source"] = tb["source"].replace({"lis": "LIS"})

    tb["prices"] = tb["prices"].replace({"2017ppp2017": "2017 PPPs, at 2017 prices"})

    tb["welfare"] = tb["welfare"].replace({"market": "Market income", "disposable": "Disposable income"})
    tb["equivalization"] = tb["equivalization"].replace(
        {"equivalized": "Equivalized", "perCapita": pc_notation_human_readable}
    )

    # Rename column equivalization to resource_sharing
    tb = tb.rename(columns={"equivalization": "resource_sharing"})

    # Add unit column
    tb["unit"] = ""
    tb["unit"] = tb["unit"].where(tb["indicator_name"] != "share", "%")
    tb["unit"] = tb["unit"].where(
        (tb["indicator_name"] != "average") & (tb["indicator_name"] != "threshold"),
        "dollars",
    )
    tb["unit"] = tb["unit"].astype(str)

    # Remove equivalized data for adults
    if adults:
        tb = tb[tb["resource_sharing"] != "Equivalized"].reset_index(drop=True)

    return tb


def create_percentiles_file_wid(tb) -> Table:
    """
    Process the percentiles table from WID, to adapt it to a concatenated file with LIS and PIP. It generates two tables, one with extrapolated datapoints and one without them
    """
    # WID PERCENTILES

    # Make wid table longer
    tb = tb.melt(
        id_vars=["country", "year", "welfare", "p", "percentile"],
        value_vars=["thr", "avg", "share", "thr_extrapolated", "avg_extrapolated", "share_extrapolated"],
        var_name="indicator_name",
        value_name="value",
    )

    # Sort by country, year, welfare, equivalization, percentile and variable
    tb = tb.sort_values(["country", "year", "welfare", "indicator_name", "p"])

    # Select only welfare types needed
    tb = tb[tb["welfare"].isin(["pretax", "posttax_nat"])].reset_index(drop=True)

    # Rename welfare and indicator_name columns
    tb["welfare"] = tb["welfare"].replace({"pretax": "pretaxNational", "posttax_nat": "posttaxNational"})

    # In indicator_name, replace values that contain avg with average and thr with threshold
    tb["indicator_name"] = tb["indicator_name"].replace(
        {
            "avg": "average",
            "thr": "threshold",
            "avg_extrapolated": "average_extrapolated",
            "thr_extrapolated": "threshold_extrapolated",
        }
    )

    # Drop percentile values containing "."
    tb = tb[~tb["percentile"].str.contains("\\.")].reset_index(drop=True)

    # Add descriptive columns
    tb["source"] = "wid"

    # If indicator_name contains extrapolated, add the word extrapolated to the source column
    tb["source"] = tb["source"].where(
        ~tb["indicator_name"].str.contains("extrapolated"),
        tb["source"] + "Extrapolated",
    )

    # Remove substring _extrapolated from indicator_name
    tb["indicator_name"] = tb["indicator_name"].str.replace("_extrapolated", "")

    # Define id columns
    tb["resource_sharing"] = "perAdult"
    tb["prices"] = ""
    tb["prices"] = tb["prices"].where(tb["indicator_name"] == "share", "2011ppp2022")
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

    tb["prices"] = tb["prices"].replace({"2011ppp2022": "2011 PPPs, at 2022 prices"})
    tb["welfare"] = tb["welfare"].replace(
        {"pretaxNational": "Pretax national income", "posttax_nat": "Post-tax national income"}
    )
    tb["resource_sharing"] = tb["resource_sharing"].replace({"perAdult": "Per adult"})

    # Add unit column
    tb["unit"] = ""
    tb["unit"] = tb["unit"].where(tb["indicator_name"] != "share", "%")
    tb["unit"] = tb["unit"].where(
        (tb["indicator_name"] != "average") & (tb["indicator_name"] != "threshold"),
        "dollars",
    )
    tb["unit"] = tb["unit"].astype(str)

    # Remove columns welfare and percentile
    tb = tb.drop(columns=["p"])

    # Create two different tables, one for extrapolated
    tb_extrapolated = tb[tb["source"] == "widExtrapolated"].reset_index(drop=True)
    tb = tb[tb["source"] == "wid"].reset_index(drop=True)

    # Replace source for a more descriptive name
    tb["source"] = tb["source"].replace({"wid": "WID"})
    tb_extrapolated["source"] = tb_extrapolated["source"].replace(
        {"widExtrapolated": "WID (including extrapolated datapoints)"}
    )

    return tb, tb_extrapolated


def extract_gdp_from_wdi(tb: Table) -> Table:
    """
    Load the table from WDI, to extract different GDP indicators
    """

    # Define list of GDP indicators
    gdp_list = [
        "ny_gdp_mktp_pp_kd",  # constant 2017 international $
        "ny_gdp_mktp_kd",  # constant 2015 US$
    ]

    # Select the columns to keep
    tb = tb[["country", "year"] + gdp_list]

    return tb
