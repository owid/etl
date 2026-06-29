# TODO: Extract data in arrow files, for faster processing.
"""
DATA EXTRACTION FOR THE WORLD BANK POVERTY AND INEQUALITY PLATFORM (PIP) API

This code generates key indicators and percentiles from the World Bank PIP API.
This is done by combining the results of several queries to the API:
    - A set of poverty lines (8) to obtain key indicators per PPP year (PPP_VERSIONS) and for countries and regions.
    - Thousands of poverty lines to construct percentiles for the countries missing from the published percentile file.
    - Thousands of poverty lines to construct estimates of relative poverty for countries.

Regions are NOT queried with poverty lines any more. Regional percentiles, regional medians and
regional relative poverty are built directly from the World Bank's "1000 Binned Global Distribution"
file (2021 PPP, see GLOBAL_DIST_1000BINS_URL), by pooling each region's member-country bins. This
removes the ~5,400 `pip-grp` percentile queries plus the regional relative-poverty queries that the
previous versions ran. As a result, regional data is produced for 2021 PPP only (the bins file is
2021 PPP); country data still covers every PPP version.

Country percentiles are partially constructed because the data officially published by the World Bank
is missing some countries.

To run this code from scratch,
    - Connect to the staging server of this pull request:
        - Hit Cmd + Shift + P and select Remote-SSH: Connect to Host
        - Type in owid@staging-site-{branch_name}
    - Delete the files in the local cache folder:
        rm -rf .cache/*
    - (If needed) Delete the files in R2:
        rclone delete r2:owid-private/cache/pip_api --fast-list --transfers 32 --checkers 32 --verbose
    - Update GLOBAL_DIST_1000BINS_URL to the bins file matching the new PIP release (see its NOTE).
    - Check if you need to update the poverty lines in the function `poverty_lines_countries`.
        - Check the list of countries without percentile data. It will show up as a list in the output (_These countries are available in a common query but not in the percentile file:_)
        - Open
            https://api.worldbank.org/pip/v1/pip?country=LCA&year=all&povline=150&fill_gaps=false&welfare_type=all&reporting_level=all&additional_ind=false&ppp_version=2021&identity=PROD&format=csv
        - And see if any of the `headcount` values is lower than 0.99. If so, you need to add more poverty lines to the function.
    - Run the code. It extracts the data AND creates both snapshots in one go (no separate upload
      scripts). You have two options to see the output, in the terminal or in the background:
        etls wb/{version}/world_bank_pip
        nohup uv run etls wb/{version}/world_bank_pip > output.log 2>&1 &
      Add --skip-upload to build the snapshots locally without uploading them to S3.
    - You can kill the process with:
        pkill -f world_bank_pip

The script writes two CSVs to the cache folder and ingests them as snapshots:
    - world_bank_pip.csv: key indicators (8 poverty lines for countries and regions), plus
      additional indicators (thresholds, relative poverty).
    - world_bank_pip_percentiles.csv: country percentiles (from the WB Databank and constructed for
      missing countries) and regional percentiles (built from the 1000-bin global distribution).

"""

import io
import time
from multiprocessing.pool import ThreadPool
from pathlib import Path

import click
import numpy as np
import pandas as pd
import requests
from botocore.exceptions import ClientError
from joblib import Memory
from owid.catalog.s3_utils import connect_r2_cached
from structlog import get_logger
from tenacity import retry
from tenacity.stop import stop_after_attempt
from tenacity.wait import wait_random_exponential

from etl.files import checksum_str
from etl.paths import CACHE_DIR
from etl.snapshot import Snapshot

# Namespace and version of this snapshot folder (used to create the snapshots directly from here).
SNAPSHOT_NAMESPACE = Path(__file__).parent.parent.name
SNAPSHOT_VERSION = Path(__file__).parent.name

# Initialize logger.
log = get_logger()

memory = Memory(CACHE_DIR, verbose=0)

# Basic parameters to use in the functions
MAX_REPEATS = 15
TIMEOUT = 500
FILL_GAPS = "false"
# NOTE: Although the number of workers is set to MAX_WORKERS, the actual number of workers for regional queries is half of that, because the API (`pip-grp`) is less able to handle concurrent requests.
MAX_WORKERS = 2
TOLERANCE_PERCENTILES = 1

# Set to False to skip percentile extraction (e.g. when percentile source links haven't been updated yet).
# When False, the script will still generate key indicators, relative poverty, and filled data,
# but will skip: percentile construction, median patching, and decile threshold merging.
EXTRACT_COUNTRY_PERCENTILES = True

# Select live (1) or internal (0) API
LIVE_API = 1

# URL of the World Bank's "1000 Binned Global Distribution" file (2021 PPP), used to build
# regional percentiles and regional relative poverty directly, instead of running thousands of
# `pip-grp` API queries. Each row is a (country, year, bin) with the bin's average welfare (`welf`,
# 2021 PPP/day) and population (`pop`, millions).
# NOTE: Update this link on every new PIP release. Find the latest file at
# https://datacatalog.worldbank.org/search/dataset/0064304 — the file name embeds the release
# vintage (e.g. `20260324_2021`), which must match the PIP API release used for the rest of the data.
GLOBAL_DIST_1000BINS_URL = (
    "https://datacatalogfiles.worldbank.org/ddh-published/0064304/DR0094424/"
    "GlobalDist1000bins_1990_2026_20260324_2021_01_02_PROD.dta"
)
# The bins file is published for 2021 PPP only, so regional percentiles/relative poverty are
# produced for 2021 PPP. (Country data still covers every PPP version in POVLINES_DICT.)
BINS_PPP_VERSION = 2021


# Constants
def poverty_lines_countries():
    """
    These poverty lines are used to calculate percentiles for countries that are not in the percentile file.
    # NOTE: In future updates, check if these poverty lines are enough for the extraction
    """
    # Define poverty lines and their increase

    under_2_dollars = list(range(1, 200, 1))
    between_2_and_5_dollars = list(range(200, 500, 2))
    between_5_and_10_dollars = list(range(500, 1000, 5))
    between_10_and_20_dollars = list(range(1000, 2000, 10))
    between_20_and_30_dollars = list(range(2000, 3000, 10))
    between_30_and_55_dollars = list(range(3000, 5500, 10))
    between_55_and_80_dollars = list(range(5500, 8000, 10))
    between_80_and_100_dollars = list(range(8000, 10000, 10))
    between_100_and_150_dollars = list(range(10000, 15000, 10))

    # povlines is all these lists together
    povlines = (
        under_2_dollars
        + between_2_and_5_dollars
        + between_5_and_10_dollars
        + between_10_and_20_dollars
        + between_20_and_30_dollars
        + between_30_and_55_dollars
        + between_55_and_80_dollars
        + between_80_and_100_dollars
        + between_100_and_150_dollars
    )

    return povlines


# Define poverty lines for key indicators, depending on the PPP version.
# It includes the international poverty line, lower and upper-middle income lines, and some other lines.
# NOTE: Define this dictionary to show the most recent PPP prices second
POVLINES_DICT = {
    2017: [100, 215, 365, 500, 685, 700, 1000, 2000, 3000, 4000],
    2021: [100, 300, 420, 500, 700, 830, 1000, 2000, 3000, 4000],
}

# Define international poverty lines as the second value in each list in POVLINES_DICT
INTERNATIONAL_POVERTY_LINES = {ppp_year: poverty_lines[1] for ppp_year, poverty_lines in POVLINES_DICT.items()}

# Define PPP versions from POVLINES_DICT
PPP_VERSIONS = list(POVLINES_DICT.keys())

# Define current International Poverty Line (IPL) in the latest prices
INTERNATIONAL_POVERTY_LINE_CURRENT = INTERNATIONAL_POVERTY_LINES[PPP_VERSIONS[1]] / 100

# Define poverty lines to construct percentiles for countries missing from the published file.
# Regional percentiles no longer use poverty-line queries — they come from the 1000-bin distribution.
POV_LINES_COUNTRIES = poverty_lines_countries()

# Define old PIP regions, which will be phased out in future versions of the API
OLD_REGIONS = [
    "EAP",
    "ECA",
    "LAC",
    "MNA",
    "OHI",
    "SAR",
    "SSA",
]

# # DEBUGGING
# PPP_VERSIONS = [2021]
# POV_LINES_COUNTRIES = [1, 1000, 25000, 50000]


@click.command()
@click.option(
    "--live-api/--internal-api",
    default=True,
    type=bool,
    help="Select live (1) or internal (0) API",
)
@click.option("--upload/--skip-upload", default=True, type=bool, help="Create and upload the snapshots to S3")
def run(live_api: bool, upload: bool) -> None:
    if live_api:
        wb_api = WB_API("https://api.worldbank.org/pip/v1")
    else:
        wb_api = WB_API("https://apiv2qa.worldbank.org/pip/v1")

    ##########################################################################################
    # Run a regional query for the World to check if the headcount ratio coincides with the newly published data

    versions = pip_versions(wb_api)

    df_world = pip_query_region(
        wb_api=wb_api,
        popshare_or_povline="povline",
        value=INTERNATIONAL_POVERTY_LINE_CURRENT,
        versions=versions,
        country_code="all",
        year="all",
        welfare_type="all",
        reporting_level="all",
        ppp_version=PPP_VERSIONS[1],
        download="true",
    )

    log.warning(
        "This is the headcount ratio series for `World`. Please check if it coincides with the newly published data."
    )
    print(df_world[(df_world["country"] == "World") & (df_world["year"] >= 2020)][["year", "headcount"]])

    ##########################################################################################

    # Generate percentiles by extracting the raw files and processing them afterward
    df_percentiles = generate_consolidated_percentiles(generate_percentiles_raw(wb_api), wb_api)

    # Generate relative poverty indicators file
    df_relative = generate_relative_poverty(wb_api)

    # Generate key indicators file and patch medians
    df = generate_key_indicators(wb_api)
    df = median_patch(df, country_or_region="country")

    # Add relative poverty indicators and decile thresholds to the key indicators file
    df = add_relative_poverty_and_decile_thresholds(df, df_relative, df_percentiles, wb_api)

    df = add_filled_data(df, wb_api)

    # Ingest the generated files as snapshots directly from this script (no separate upload scripts).
    create_snapshots(upload=upload)


def create_snapshots(upload: bool = True) -> None:
    """
    Create the world_bank_pip and world_bank_pip_percentiles snapshots from the CSV files this
    script just wrote to the cache folder. This replaces the previous setup where a separate
    pip_api.py extracted the data and two upload scripts ingested it: now one
    `etls .../world_bank_pip` run extracts and ingests both snapshots.
    """
    for short_name in ["world_bank_pip", "world_bank_pip_percentiles"]:
        snap = Snapshot(f"{SNAPSHOT_NAMESPACE}/{SNAPSHOT_VERSION}/{short_name}.csv")
        snap.create_snapshot(filename=f"{CACHE_DIR}/{short_name}.csv", upload=upload)
        log.info(f"Snapshot created: {snap.uri}")


class WB_API:
    def __init__(self, api_address, check_health=False):
        self.api_address = api_address
        self.check_health = check_health

    def health_check(self):
        return pd.read_json(f"{self.api_address}/health-check")[0][0]

    def api_health(self):
        """
        Check if the API is running.
        """
        if not self.check_health:
            return

        # Initialize repeat counter
        repeat = 0

        # health comes from a json containing the status
        health = self.health_check()

        # If the status is different to "PIP API is running", repeat the request until MAX_REPEATS
        while health != "PIP API is running" and repeat < MAX_REPEATS:
            repeat += 1

        if repeat >= MAX_REPEATS:
            # If the status is different to "PIP API is running" after MAX_REPEATS, log fatal error
            raise AssertionError(f"Health check: {health} (repeated {repeat} times)")

    def versions(self):
        return memory.cache(pd.read_csv)(f"{self.api_address}/versions?format=csv")

    def get_table(self, table):
        return pd.read_csv(f"{self.api_address}/aux?table={table}&long_format=false&format=csv")

    def fetch_csv(self, url):
        return _fetch_csv(f"{self.api_address}{url}")


@retry(wait=wait_random_exponential(multiplier=1), stop=stop_after_attempt(MAX_REPEATS))
def _get_request(url: str) -> requests.Response:
    response = requests.get(url, timeout=TIMEOUT)
    if response.status_code != 200:
        log.info("fetch_csv.retry", url=url)
        raise Exception("API timed out")

    if b"Server Error" in response.content:
        raise Exception("API returned server error")

    return response


@memory.cache
def _fetch_csv(url: str) -> pd.DataFrame:
    r2 = connect_r2_cached()
    r2_bucket = "owid-private"
    r2_key = "cache/pip_api/" + checksum_str(url)

    # try to get it from cache
    try:
        obj = r2.get_object(Bucket=r2_bucket, Key=r2_key)  # type: ignore[reportAttributeAccessIssue]
        s = obj["Body"].read().decode("utf-8")
        # we might have cached invalid responses, in that case fetch it again
        if "Server Error" not in s:
            df = pd.read_csv(io.StringIO(s))
            log.info("fetch_csv.cache_hit", url=url)
            return df
        else:
            log.info("fetch_csv.cache_with_error", url=url)
    except ClientError:
        pass

    log.info("fetch_csv.start", url=url)
    response = _get_request(url)
    log.info("fetch_csv.success", url=url, t=response.elapsed.total_seconds())

    # save the result to R2 cache
    r2.put_object(  # type: ignore[reportAttributeAccessIssue]
        Body=response.content,
        Bucket=r2_bucket,
        Key=r2_key,
    )

    df = pd.read_csv(io.StringIO(response.content.decode("utf-8")))
    return df


@memory.cache
def _fetch_percentiles(version: int) -> pd.DataFrame:
    # These URLs were copied from https://datacatalog.worldbank.org/search/dataset/0063646/_poverty_and_inequality_platform_pip_percentiles
    # NOTE: Check if these links are still valid for the new PIP release. If not, update them to the new links.
    if version == PPP_VERSIONS[0]:
        url = "https://datacatalogfiles.worldbank.org/ddh-published/0063646/DR0090251/world_100bin_revised.csv"
    elif version == PPP_VERSIONS[1]:
        url = "https://datacatalogfiles.worldbank.org/ddh-published/0063646/DR0090357/world_100bin_revised.csv"
    else:
        raise ValueError(f"Version {version} is not supported")

    _df_percentiles = pd.read_csv(url)

    # # Drop  Unnamed: 0 column (it sometimes appears in the files)
    # _df_percentiles = _df_percentiles.drop(columns=["Unnamed: 0"])

    return _df_percentiles


# Module-level cache so the (expensive) bins pooling + computation runs only once per process.
_REGIONAL_BINS_CACHE: dict = {}


def fetch_bins() -> pd.DataFrame:
    """
    Download the World Bank 1000-binned global distribution (2021 PPP) and return a tidy frame.

    Each row is a (country, year, bin): `welf` is the bin's average daily per-capita welfare in
    2021 PPP USD and `pop` is the bin's population in millions. The file is ~1.1 GB, so it is cached
    on disk and only downloaded once.
    """
    # Key the cache filename to the source URL (its name embeds the PIP release vintage), so that
    # bumping GLOBAL_DIST_1000BINS_URL for a later release downloads fresh instead of silently
    # reusing a stale .dta left in .cache from a previous run.
    local_path = Path(CACHE_DIR) / GLOBAL_DIST_1000BINS_URL.rsplit("/", 1)[-1]
    if not local_path.is_file():
        log.info("Downloading the 1000-bin global distribution file (~1.1 GB). This runs once and is cached.")
        local_path.parent.mkdir(parents=True, exist_ok=True)
        with requests.get(GLOBAL_DIST_1000BINS_URL, stream=True, timeout=TIMEOUT) as r:
            r.raise_for_status()
            with open(local_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    f.write(chunk)
        log.info("1000-bin global distribution file downloaded.")

    df_bins = pd.read_stata(local_path, columns=["year", "code", "quantile", "welf", "pop"])
    df_bins["year"] = df_bins["year"].astype(int)
    df_bins["quantile"] = df_bins["quantile"].astype(int)
    return df_bins


def build_regional_bins(wb_api: WB_API) -> pd.DataFrame:
    """
    Pool the per-country 1000-bin distributions into PIP regional distributions (2021 PPP).

    Region membership is taken from the PIP `country_list` auxiliary table (the authoritative source
    for PIP groupings), not from the bins file's own region columns. Each country's bins are assigned
    to its standard region (EAS, ECS, ...), to its africa-split region (AFE/AFW, only for Sub-Saharan
    countries), and to the World aggregate (WLD).

    Returns a long frame with columns: country_code (region code), country (region name), year, welf,
    pop. Region names/codes match those the PIP `pip-grp` API returns, so they harmonize identically
    downstream.
    """
    df_bins = fetch_bins()

    # Region membership from the PIP country_list aux table.
    country_list = pip_aux_tables(wb_api, table="country_list")["country_list"]
    membership = country_list[["country_code", "region", "region_code", "africa_split", "africa_split_code"]].copy()

    # Drop the redundant country_code coming from the merge (it equals the bins `code`).
    df = df_bins.merge(membership, left_on="code", right_on="country_code", how="left").drop(columns=["country_code"])

    # Every bins country must map to a PIP region.
    unmapped = sorted(df.loc[df["region_code"].isna(), "code"].unique())
    assert not unmapped, f"These bins countries are not in the PIP country_list: {unmapped}"

    base_cols = ["country_code", "country", "year", "welf", "pop"]

    # Standard regions (EAS, ECS, LCN, MEA, NAC, SAS, SSF).
    df_standard = df.rename(columns={"region_code": "country_code", "region": "country"})[base_cols]

    # Africa split (AFE, AFW): only Sub-Saharan countries have a non-null africa_split.
    df_africa = df[df["africa_split_code"].notna()].rename(
        columns={"africa_split_code": "country_code", "africa_split": "country"}
    )[base_cols]

    # World (WLD): every country.
    df_world = df.assign(country_code="WLD", country="World")[base_cols]

    return pd.concat([df_standard, df_africa, df_world], ignore_index=True)


def compute_regional_distributions_from_bins(wb_api: WB_API) -> dict:
    """
    Build regional percentiles and regional relative poverty from the 1000-bin global distribution,
    replacing the thousands of `pip-grp` API queries the previous versions used.

    For each region and year, the member countries' bins are pooled and sorted by welfare, and:
        - percentiles 1-99: `thr` (welfare at the p-th cumulative-population point), `avg` (mean
          welfare of the 1%-of-population band) and `share` (welfare share of the band, as a fraction
          like the country data);
        - relative poverty below 40/50/60% of the regional median: headcount ratio, poverty gap index
          (FGT1), poverty severity (FGT2) and the Watts index (all as fractions, like the API output).

    Returns {"percentiles": DataFrame, "relative": DataFrame}. Cached per process.
    """
    if "result" in _REGIONAL_BINS_CACHE:
        return _REGIONAL_BINS_CACHE["result"]

    regional_bins = build_regional_bins(wb_api)

    percentiles = np.arange(1, 100)
    upper_frac = percentiles / 100.0
    lower_frac = (percentiles - 1) / 100.0

    perc_rows = []
    rel_rows = []

    for (country_code, country, year), g in regional_bins.groupby(["country_code", "country", "year"], sort=False):
        g = g.sort_values("welf")
        welf = g["welf"].to_numpy()
        pop = g["pop"].to_numpy()

        cum_pop = np.cumsum(pop)
        total_pop = cum_pop[-1]
        # Cumulative population share (%) at the midpoint of each bin, used to read thresholds.
        cum_share_mid = (cum_pop - pop / 2) / total_pop * 100

        # Threshold at percentile p: welfare level at the p-th cumulative-population point.
        thr = np.interp(percentiles, cum_share_mid, welf)

        # Cumulative welfare vs cumulative population (exact: within a bin, welfare accrues linearly
        # with population at the bin's welfare). Prepend 0 to integrate from the bottom.
        cum_pop0 = np.concatenate([[0.0], cum_pop])
        cum_welf0 = np.concatenate([[0.0], np.cumsum(welf * pop)])
        total_welf = cum_welf0[-1]

        # Welfare held by each 1%-of-population band ((p-1)% , p%].
        band_welf = np.interp(upper_frac * total_pop, cum_pop0, cum_welf0) - np.interp(
            lower_frac * total_pop, cum_pop0, cum_welf0
        )
        avg = band_welf / (total_pop / 100.0)  # mean welfare within the band
        share = band_welf / total_welf  # welfare share of the band (fraction)

        perc_rows.append(
            pd.DataFrame(
                {
                    "ppp_version": BINS_PPP_VERSION,
                    "country": country,
                    "country_code": country_code,
                    "year": year,
                    "reporting_level": np.nan,
                    "welfare_type": np.nan,
                    "target_percentile": percentiles,
                    "thr": thr,
                    "avg": avg,
                    "share": share,
                }
            )
        )

        # Relative poverty: the median is the 50th-percentile threshold.
        median = float(np.interp(50, cum_share_mid, welf))
        rel = {"country": country, "country_code": country_code, "year": year, "median": median}
        for pct in [40, 50, 60]:
            line = median * pct / 100.0
            below = welf < line
            pop_below = pop[below]
            welf_below = welf[below]
            gap = (line - welf_below) / line
            rel[f"headcount_ratio_{pct}_median"] = pop_below.sum() / total_pop
            rel[f"poverty_gap_index_{pct}_median"] = (gap * pop_below).sum() / total_pop
            rel[f"poverty_severity_{pct}_median"] = (gap**2 * pop_below).sum() / total_pop
            rel[f"watts_{pct}_median"] = (np.log(line / welf_below) * pop_below).sum() / total_pop
        rel_rows.append(rel)

    result = {
        "percentiles": pd.concat(perc_rows, ignore_index=True),
        "relative": pd.DataFrame(rel_rows),
    }
    _REGIONAL_BINS_CACHE["result"] = result
    log.info("Regional percentiles and relative poverty computed from the 1000-bin distribution.")
    return result


############################################################################################################
# FUNCTIONS


def pip_aux_tables(wb_api: WB_API, table="all"):
    """
    Download aux tables if the API is running.
    """

    wb_api.api_health()

    if table == "all":
        aux_tables_list = [
            "aux_versions",
            "countries",
            "country_coverage",
            "country_list",
            "cpi",
            "decomposition",
            "dictionary",
            "framework",
            "gdp",
            "incgrp_coverage",
            "indicators",
            "interpolated_means",
            "missing_data",
            "national_poverty_lines",
            "pce",
            "pop",
            "pop_region",
            "poverty_lines",
            "ppp",
            "region_coverage",
            "regions",
            "spl",
            "survey_means",
        ]
        # Create a list of dataframes
        df_dict = {}

        # Download each table and append it to the list
        for tab in aux_tables_list:
            df = wb_api.get_table(tab)

            # Add table to df_dict
            df_dict[tab] = df

    else:
        df = wb_api.get_table(table)

        # Add table to df_dict
        df_dict = {table: df}

    log.info(f'Auxiliary tables downloaded ("{table}")')

    return df_dict


def pip_versions(wb_api) -> dict:
    """
    Download latest PIP data versions if the API is running.
    """

    wb_api.api_health()

    df = wb_api.versions()
    df = df[["ppp_version", "release_version", "version"]]

    # Obtain the max release_version
    max_release_version = df["release_version"].max()

    # Get the version for both ppp versions
    versions = df[df["release_version"] == max_release_version]

    # Set index and convert to dict
    versions = versions.set_index("ppp_version", verify_integrity=True).sort_index().to_dict(orient="index")

    ppp_version_old = versions[PPP_VERSIONS[0]]["version"]
    ppp_version_current = versions[PPP_VERSIONS[1]]["version"]

    log.info(
        f"PIP dataset versions extracted: {PPP_VERSIONS[0]} = {ppp_version_old}, {PPP_VERSIONS[1]} = {ppp_version_current}"
    )

    return versions


def pip_query_country(
    wb_api: WB_API,
    popshare_or_povline,
    value,
    versions,
    country_code="all",
    year="all",
    fill_gaps="true",
    welfare_type="all",
    reporting_level="all",
    ppp_version=PPP_VERSIONS[1],
    download="false",
) -> pd.DataFrame:
    """
    Query country data from the PIP API.
    """

    # Test health of the API
    wb_api.api_health()

    # Round povline (popshare) to 2 decimals to work with cents as the minimum unit
    value = round(value, 2)

    # Extract version and release_version from versions dict
    version = versions[ppp_version]["version"]
    release_version = versions[ppp_version]["release_version"]

    # NOTE: There is a bug for China: when querying specific poverty lines (as for relative poverty), the API doesn't respond
    # One hacky way to fix it is to call a more generalized query first (with no welfare_type, reporting_level nor release version)
    if country_code == "CHN":
        wb_api.fetch_csv(
            f"/pip?country={country_code}&year={year}&{popshare_or_povline}={value}&ppp_version={ppp_version}&fill_gaps={fill_gaps}&format=csv"
        )

    # Build query
    df = wb_api.fetch_csv(
        f"/pip?{popshare_or_povline}={value}&country={country_code}&year={year}&fill_gaps={fill_gaps}&welfare_type={welfare_type}&reporting_level={reporting_level}&ppp_version={ppp_version}&version={version}&release_version={release_version}&format=csv"
    )

    # Add PPP version as column
    df["ppp_version"] = ppp_version

    # Replace names of columns and drop redundancies
    df = df.rename(columns={"country_name": "country", "reporting_year": "year"})
    df = df.drop(columns=["region_code"])

    # Reorder columns: ppp_version, country, year, povline and the rest
    first_columns = ["ppp_version", "country", "year", "poverty_line"]
    df = df[first_columns + [column for column in df.columns if column not in first_columns]]

    if download == "true":
        # make sure the directory exists. If not, create it
        Path(f"{CACHE_DIR}/pip_country_data").mkdir(parents=True, exist_ok=True)
        # Save to csv
        df.to_csv(
            f"{CACHE_DIR}/pip_country_data/pip_country_{country_code}_year_{year}_{popshare_or_povline}_{int(round(value * 100))}_welfare_{welfare_type}_rep_{reporting_level}_fillgaps_{fill_gaps}_ppp_{ppp_version}.csv",
            index=False,
        )

    if country_code == "all":
        log.info(f"Country data extracted for {popshare_or_povline} = {value} ({ppp_version} PPPs)")
    else:
        log.info(
            f"Country data extracted for {popshare_or_povline} = {value} ({ppp_version} PPPs) in {country_code} (year = {year})"
        )

    return df


def pip_query_region(
    wb_api: WB_API,
    popshare_or_povline,
    value,
    versions,
    country_code="all",
    year="all",
    welfare_type="all",
    reporting_level="all",
    ppp_version=PPP_VERSIONS[1],
    download="false",
) -> pd.DataFrame:
    """
    Query regional data from the PIP API.
    """

    # Test health of the API
    wb_api.api_health()

    # Round povline (popshare) to 2 decimals to work with cents as the minimum unit
    value = round(value, 2)

    # Extract version and release_version from versions dict
    version = versions[ppp_version]["version"]
    release_version = versions[ppp_version]["release_version"]

    # Build query
    df = wb_api.fetch_csv(
        f"/pip-grp?group_by=wb&{popshare_or_povline}={value}&country={country_code}&year={year}&welfare_type={welfare_type}&reporting_level={reporting_level}&ppp_version={ppp_version}&version={version}&release_version={release_version}&format=csv"
    )

    # Add PPP version as column
    df["ppp_version"] = ppp_version

    # Replace names of columns and drop redundancies
    df = df.rename(columns={"region_name": "country", "reporting_year": "year", "region_code": "country_code"})

    # Reorder columns: ppp_version, country, year, povline and the rest
    first_columns = ["ppp_version", "country", "year", "poverty_line"]
    df = df[first_columns + [column for column in df.columns if column not in first_columns]]

    if download == "true":
        # make sure the directory exists. If not, create it
        Path(f"{CACHE_DIR}/pip_region_data").mkdir(parents=True, exist_ok=True)
        # Save to csv
        df.to_csv(
            f"{CACHE_DIR}/pip_region_data/pip_region_{country_code}_year_{year}_{popshare_or_povline}_{int(round(value * 100))}_ppp_{ppp_version}.csv",
            index=False,
        )

    if country_code == "all":
        log.info(f"Regional data extracted for {popshare_or_povline} = {value} ({ppp_version} PPPs)")
    else:
        log.info(
            f"Regional data extracted for {popshare_or_povline} = {value} ({ppp_version} PPPs) in {country_code} (year = {year})"
        )

    return df


# GENERATE PERCENTILES FILES
# This is data not given directly by the query, but we can get it by querying a huge set of poverty lines and assign percentiles according to headcount ratio results.


def generate_percentiles_raw(wb_api: WB_API):
    """
    Generate raw percentile query results for COUNTRIES that are missing from the World Bank's
    published percentile file. Uses concurrency to speed up the process.

    Regions are no longer queried here: regional percentiles are built directly from the 1000-bin
    global distribution (see compute_regional_distributions_from_bins), which removes the thousands
    of `pip-grp` API calls the previous versions needed.
    """
    start_time = time.time()

    def get_percentiles_data(povline, versions, ppp_version, country_code):
        """
        Check if country percentiles data exists. If not, run the query.
        """
        if Path(
            f"{CACHE_DIR}/pip_country_data/pip_country_{country_code}_year_all_povline_{povline}_welfare_all_rep_all_fillgaps_{FILL_GAPS}_ppp_{ppp_version}.csv"
        ).is_file():
            return

        else:
            return pip_query_country(
                wb_api,
                popshare_or_povline="povline",
                value=povline / 100,
                versions=versions,
                country_code=country_code,
                year="all",
                fill_gaps=FILL_GAPS,
                welfare_type="all",
                reporting_level="all",
                ppp_version=ppp_version,
                download="true",
            )

    def concurrent_percentiles_function(country_code):
        """
        Executes get_percentiles_data concurrently.
        """
        # Make sure the directory exists. If not, create it
        Path(f"{CACHE_DIR}/pip_country_data").mkdir(parents=True, exist_ok=True)

        with ThreadPool(MAX_WORKERS) as pool:
            tasks = [
                (povline, versions, ppp_version, country_code)
                for ppp_version in PPP_VERSIONS
                for povline in POV_LINES_COUNTRIES
            ]
            pool.starmap(get_percentiles_data, tasks)

    def get_query_country(povline, ppp_version, country_code):
        """
        Here I check if the country file exists even after the original extraction. If it does, I read it. If not, I start the queries again.
        """

        file_path_country = f"{CACHE_DIR}/pip_country_data/pip_country_{country_code}_year_all_povline_{povline}_welfare_all_rep_all_fillgaps_{FILL_GAPS}_ppp_{ppp_version}.csv"
        if Path(file_path_country).is_file():
            df_query_country = pd.read_csv(file_path_country)
        else:
            # Run the main function to get the data
            log.warning(
                f"We need to come back to the extraction! countries = {country_code}, {povline}, {ppp_version} PPPs)"
            )
            get_percentiles_data(povline, versions, ppp_version, country_code)
            df_query_country = pd.read_csv(file_path_country)

        return df_query_country

    def get_list_of_missing_countries():
        """
        Compare the list of countries in a common query (reference file) and the list of countries in the percentile file.
        It generates missing_countries, which is a string with all the elements of the list, in the format for querying multiple countries in the API.
        And also missing_countries_list, which is a list of the countries.
        """
        # Obtain the percentile files the World Bank publishes in their Databank

        df_percentiles_published_latest_ppp = _fetch_percentiles(PPP_VERSIONS[1])

        # FOR COUNTRIES
        # Get data from the most common query
        df_reference = pip_query_country(
            wb_api,
            popshare_or_povline="povline",
            value=INTERNATIONAL_POVERTY_LINE_CURRENT,
            versions=versions,
            country_code="all",
            year="all",
            fill_gaps=FILL_GAPS,
            welfare_type="all",
            reporting_level="all",
            ppp_version=PPP_VERSIONS[1],
            download="true",
        )

        # Edit percentile file to get the list of different countries
        df_percentiles_pub = df_percentiles_published_latest_ppp.copy()
        df_percentiles_pub = df_percentiles_pub.drop(
            columns=["percentile", "avg_welfare", "pop_share", "welfare_share", "quantile"]
        ).drop_duplicates()

        # Merge the two files
        df_merge = pd.merge(
            df_reference,
            df_percentiles_pub,
            on=["country_code", "year", "reporting_level", "welfare_type"],
            how="outer",
            indicator=True,
        )

        # Obtain the list of countries that are in the reference file but not in the percentile file
        list_missing_countries = df_merge.loc[df_merge["_merge"] == "left_only", "country_code"].unique().tolist()

        # Generate a string with all the elements of the list, in the format for querying multiple countries in the API
        missing_countries = "&country=".join(list_missing_countries)

        return missing_countries, list_missing_countries

    # Obtain latest versions of the PIP dataset
    versions = pip_versions(wb_api)

    # Run the main function
    if EXTRACT_COUNTRY_PERCENTILES:
        missing_countries, list_missing_countries = get_list_of_missing_countries()
        log.info(
            f"These countries are available in a common query but not in the percentile file: {list_missing_countries}"
        )

        # Only run the function if there are missing countries
        if list_missing_countries:
            concurrent_percentiles_function(country_code=missing_countries)
            log.info("Country files downloaded")
        else:
            log.info("All countries are in the percentile file. No need to extract them.")
    else:
        log.warning("Skipping country percentile extraction (EXTRACT_COUNTRY_PERCENTILES is False)")
        list_missing_countries = []
        missing_countries = ""

    # If every country is already in the published percentile file, there is nothing to construct.
    if not list_missing_countries:
        log.info("No missing-country percentiles to construct from the API.")
        return pd.DataFrame()

    log.info("Now we are concatenating the country files")
    with ThreadPool(MAX_WORKERS) as pool:
        tasks = [
            (povline, ppp_version, missing_countries) for ppp_version in PPP_VERSIONS for povline in POV_LINES_COUNTRIES
        ]
        dfs = pool.starmap(get_query_country, tasks)

    df_country = pd.concat(dfs, ignore_index=True)
    log.info("Country files concatenated")

    # Check that all the poverty lines are present.
    df_country["poverty_line_cents"] = round(df_country["poverty_line"] * 100).astype(int)
    assert set(df_country["poverty_line_cents"].unique()) == set(POV_LINES_COUNTRIES), log.fatal(
        "Not all poverty lines are in the country file!"
    )
    df_country = df_country.drop(columns=["poverty_line_cents"])

    # Check that the set of countries matches the ones we needed to extract.
    assert set(df_country["country_code"].unique()) == set(list_missing_countries), log.fatal(
        f"List of countries is different from the one we needed to extract! ({list_missing_countries})"
    )

    end_time = time.time()
    elapsed_time = round(end_time - start_time, 2)
    log.info(f"Construction of raw country percentile data completed. Execution time: {elapsed_time} seconds")

    return df_country


def calculate_percentile(p, df):
    """
    Calculates a single percentile and returns a DataFrame with the results.
    """
    # If reporting_level and welfare_type columns do not exist, add them with NaNs
    # This happens when only regional data is processed (missing countries are empty)
    if "reporting_level" not in df.columns:
        df["reporting_level"] = None
    if "welfare_type" not in df.columns:
        df["welfare_type"] = None

    df["distance_to_p"] = abs(df["headcount"] * 100 - p)
    df_closest = (
        df.sort_values("distance_to_p")
        .groupby(
            ["ppp_version", "country", "country_code", "year", "reporting_level", "welfare_type"],
            as_index=False,
            sort=False,
            dropna=False,  # This is to avoid dropping rows with NaNs (reporting_level and welfare_type for regions)
        )
        .first()
    )
    df_closest["target_percentile"] = p
    df_closest = df_closest[
        [
            "ppp_version",
            "country",
            "country_code",
            "year",
            "reporting_level",
            "welfare_type",
            "target_percentile",
            "poverty_line",
            "headcount",
            "distance_to_p",
        ]
    ]
    log.info(f"Percentile {p}: calculated")
    return df_closest


def format_official_percentiles(year, wb_api: WB_API):
    """
    Download percentiles from the World Bank Databank and format them to the same format as the constructed percentiles
    """
    # Load percentile files from the World Bank Databank
    df_percentiles_published = _fetch_percentiles(year)

    # Obtain country names from the aux table
    aux_dict = pip_aux_tables(wb_api, table="countries")
    df_countries = aux_dict["countries"]

    # Merge the two files to get country names
    df_percentiles_published = pd.merge(
        df_percentiles_published,
        df_countries[["country_code", "country_name"]],
        on="country_code",
        how="left",
    )

    # Rename columns
    df_percentiles_published = df_percentiles_published.rename(
        columns={
            "country_name": "country",
            "percentile": "target_percentile",
            "avg_welfare": "avg",
            "welfare_share": "share",
            "quantile": "thr",
        }
    )

    # Drop pop_share
    df_percentiles_published = df_percentiles_published.drop(columns=["pop_share"])

    # Make thr null if target_percentile is 100
    df_percentiles_published.loc[df_percentiles_published["target_percentile"] == 100, "thr"] = np.nan

    # Add ppp_version column
    df_percentiles_published["ppp_version"] = year

    return df_percentiles_published


def generate_consolidated_percentiles(df, wb_api: WB_API):
    """
    Generates percentiles from the raw data. This is the final file with percentiles.
    """
    start_time = time.time()

    path_file_percentiles = f"{CACHE_DIR}/world_bank_pip_percentiles_before_checks.csv"

    if Path(path_file_percentiles).is_file():
        log.info("Percentiles file already exists. No need to consolidate.")
        df_percentiles = pd.read_csv(path_file_percentiles)

    else:
        log.info("Consolidating percentiles")

        # Define percentiles, from 1 to 99
        percentiles = range(1, 100, 1)

        frames = []

        # Estimate percentiles for the COUNTRIES missing from the published file (df may be empty
        # if every country is already published). Regions are handled separately, from the bins.
        if not df.empty:
            dfs = [calculate_percentile(p, df) for p in percentiles]
            df_constructed = pd.concat(dfs, ignore_index=True)
            # Rename headcount to estimated_percentile and poverty_line to thr
            df_constructed = df_constructed.rename(columns={"headcount": "estimated_percentile", "poverty_line": "thr"})
            frames.append(df_constructed)
            log.info("Country percentiles for missing countries calculated")
        else:
            log.info("No missing-country percentiles to construct.")

        # Add official percentiles from the World Bank Databank
        if EXTRACT_COUNTRY_PERCENTILES:
            frames.append(format_official_percentiles(PPP_VERSIONS[0], wb_api))
            frames.append(format_official_percentiles(PPP_VERSIONS[1], wb_api))
        else:
            log.warning("Skipping official country percentiles (EXTRACT_COUNTRY_PERCENTILES is False)")

        # Add regional percentiles built from the 1000-bin global distribution (2021 PPP only).
        df_regions = compute_regional_distributions_from_bins(wb_api)["percentiles"].copy()
        # estimated_percentile/distance_to_p let these rows flow through sanity_checks like the
        # API-constructed ones (distance_to_p = 0 means they always pass the tolerance check). Both
        # columns are dropped before the final CSV, so the output schema is unchanged.
        df_regions["estimated_percentile"] = df_regions["target_percentile"] / 100
        df_regions["distance_to_p"] = 0.0
        frames.append(df_regions)

        df_percentiles = pd.concat(frames, ignore_index=True)
        log.info("Percentiles calculated and consolidated")

        # Drop duplicates. Keep the second one (the official one)
        df_percentiles = df_percentiles.drop_duplicates(
            subset=[
                "ppp_version",
                "country",
                "country_code",
                "year",
                "reporting_level",
                "welfare_type",
                "target_percentile",
            ],
            keep="last",
        )

        # Sort by ppp_version, country, year, reporting_level, welfare_type and target_percentile
        df_percentiles = df_percentiles.sort_values(
            by=[
                "ppp_version",
                "country",
                "country_code",
                "year",
                "reporting_level",
                "welfare_type",
                "target_percentile",
            ]
        )

        # Save to csv
        df_percentiles.to_csv(f"{CACHE_DIR}/world_bank_pip_percentiles_before_checks.csv", index=False)

    # SANITY CHECKS
    df_percentiles = sanity_checks(df_percentiles)

    # Drop distance_to_p, estimated_percentile
    df_percentiles = df_percentiles.drop(columns=["distance_to_p", "estimated_percentile"])

    # Rename target_percentile to percentile
    df_percentiles = df_percentiles.rename(columns={"target_percentile": "percentile"})

    # Save to csv
    df_percentiles.to_csv(f"{CACHE_DIR}/world_bank_pip_percentiles.csv", index=False)

    end_time = time.time()
    elapsed_time = round(end_time - start_time, 2)
    log.info(f"Percentiles calculated and checked. Execution time: {elapsed_time} seconds")

    return df_percentiles


def sanity_checks(df_percentiles):
    """
    Run different sanity checks to the percentiles file.
    """
    log.info("Starting sanity checks")

    # Count number of rows before checks
    rows_before = len(df_percentiles)

    # Consecutive percentiles (1, 2, 3, etc)
    # Create a column called check that is True if target_percentile is consecutive for each ppp_version, country, year, reporting_level, and welfare_type
    df_percentiles["check"] = (
        df_percentiles.groupby(["ppp_version", "country", "year", "reporting_level", "welfare_type"], dropna=False)[
            "target_percentile"
        ].diff()
        == 1
    )

    # Replace check with True if target_percentile is 1
    df_percentiles.loc[df_percentiles["target_percentile"] == 1, "check"] = True

    # Assign the boolean value to the entire group
    df_percentiles["check"] = df_percentiles.groupby(
        ["ppp_version", "country", "year", "reporting_level", "welfare_type"], dropna=False
    )["check"].transform("all")

    # Define mask
    mask = ~df_percentiles["check"]
    df_error = df_percentiles[mask].reset_index(drop=True).copy()

    if len(df_error) > 0:
        log.warning(
            f"""Percentiles are not consecutive! These distributions will not be used:
                {df_error[["ppp_version", "country", "year", "reporting_level", "welfare_type"]].drop_duplicates()}"""
        )
        # Drop faulty distributions
        df_percentiles = df_percentiles[~mask].reset_index(drop=True)

    ############################################################################################################
    # Distance_to_p is higher than TOLERANCE_PERCENTILES
    df_percentiles["check"] = df_percentiles["distance_to_p"] > TOLERANCE_PERCENTILES

    # Assign the boolean value to the entire group
    df_percentiles["check"] = df_percentiles.groupby(
        ["ppp_version", "country", "year", "reporting_level", "welfare_type"], dropna=False
    )["check"].transform("any")

    # Define mask
    mask = df_percentiles["check"]
    df_error = df_percentiles[mask].reset_index(drop=True).copy()

    if len(df_error) > 0:
        log.warning(
            f"""Percentiles are not accurate! These distributions will not be used:
                {df_error[["ppp_version", "country", "year", "reporting_level", "welfare_type"]].drop_duplicates()}"""
        )
        # Drop faulty distributions
        df_percentiles = df_percentiles[~mask].reset_index(drop=True)

    ############################################################################################################
    # Nulls for thr, avg and share for the entire group of ppp_version, country, year, reporting_level, and welfare_type
    df_percentiles["check_thr"] = df_percentiles.groupby(
        ["ppp_version", "country", "year", "reporting_level", "welfare_type"], dropna=False
    )["thr"].transform(lambda x: x.isnull().all())
    df_percentiles["check_avg"] = df_percentiles.groupby(
        ["ppp_version", "country", "year", "reporting_level", "welfare_type"], dropna=False
    )["avg"].transform(lambda x: x.isnull().all())
    df_percentiles["check_share"] = df_percentiles.groupby(
        ["ppp_version", "country", "year", "reporting_level", "welfare_type"], dropna=False
    )["share"].transform(lambda x: x.isnull().all())

    df_percentiles["check"] = df_percentiles["check_thr"] & df_percentiles["check_avg"] & df_percentiles["check_share"]

    # Define mask
    mask = df_percentiles["check"]
    df_error = df_percentiles[mask].reset_index(drop=True).copy()

    if len(df_error) > 0:
        log.warning(
            f"""There are null values for thr, avg and share! These distributions need to be corrected:
                {df_error[["ppp_version", "country", "year", "reporting_level", "welfare_type"]].drop_duplicates()}"""
        )
        # Drop distributions with null values for thr, avg and share
        df_percentiles = df_percentiles[~mask].reset_index(drop=True)

    ############################################################################################################
    # Find negative values for thr
    df_percentiles["check"] = df_percentiles["thr"] < 0

    # Define mask
    mask = df_percentiles["check"]

    df_error = df_percentiles[mask].reset_index(drop=True).copy()

    if len(df_error) > 0:
        log.warning(
            f"""There are negative values for thr! These distributions need to be corrected:
                {df_error[["ppp_version", "country", "year", "reporting_level", "welfare_type"]].drop_duplicates()}"""
        )
        # Correct cases where thr, avg and share are negative, by assigning 0
        df_percentiles.loc[mask, "thr"] = 0

    ############################################################################################################
    # Find negative values for avg
    df_percentiles["check"] = df_percentiles["avg"] < 0

    # Define mask
    mask = df_percentiles["check"]

    df_error = df_percentiles[mask].reset_index(drop=True).copy()

    if len(df_error) > 0:
        log.warning(
            f"""There are negative values for avg! These distributions need to be corrected:
                {df_error[["ppp_version", "country", "year", "reporting_level", "welfare_type"]].drop_duplicates()}"""
        )
        # Correct cases where thr, avg and share are negative, by assigning 0
        df_percentiles.loc[mask, "avg"] = 0

    ############################################################################################################
    # Find negative values for share
    df_percentiles["check"] = df_percentiles["share"] < 0

    # Define mask
    mask = df_percentiles["check"]

    df_error = df_percentiles[mask].reset_index(drop=True).copy()

    if len(df_error) > 0:
        log.warning(
            f"""There are negative values for share! These distributions need to be corrected:
                {df_error[["ppp_version", "country", "year", "reporting_level", "welfare_type"]].drop_duplicates()}"""
        )
        # Correct cases where thr, avg and share are negative, by assigning 0
        df_percentiles.loc[mask, "share"] = 0

    ############################################################################################################
    # thr is increasing for each ppp_version, country, year, reporting_level, and welfare_type
    df_percentiles["check"] = (
        df_percentiles.groupby(["ppp_version", "country", "year", "reporting_level", "welfare_type"], dropna=False)[
            "thr"
        ]
        .diff()
        .round(2)
        >= 0
    )

    # Replace check with True if thr is NaN
    df_percentiles.loc[df_percentiles["thr"].isna(), "check"] = True

    # Replace check with True if target_percentile is 1
    df_percentiles.loc[(df_percentiles["target_percentile"] == 1), "check"] = True

    # Define mask
    mask = ~df_percentiles["check"]
    df_error = df_percentiles[mask].reset_index(drop=True).copy()

    if len(df_error) > 0:
        log.warning(
            f"""Thresholds are not increasing! These distributions need to be corrected:
                {df_error[["ppp_version", "country", "year", "reporting_level", "welfare_type"]].drop_duplicates()}"""
        )
        # Correct cases where thr is not increasing, by repeating the previous thr
        df_percentiles.loc[mask, "thr"] = df_percentiles.loc[mask, "thr"].shift(1)

    ############################################################################################################
    # avg is increasing for each ppp_version, country, year, reporting_level, and welfare_type
    df_percentiles["check"] = (
        df_percentiles.groupby(["ppp_version", "country", "year", "reporting_level", "welfare_type"], dropna=False)[
            "avg"
        ]
        .diff()
        .round(2)
        >= 0
    )

    # Replace check with True if avg is NaN
    df_percentiles.loc[df_percentiles["avg"].isna(), "check"] = True

    # Replace check with True if target_percentile is 1
    df_percentiles.loc[(df_percentiles["target_percentile"] == 1), "check"] = True

    # Define mask
    mask = ~df_percentiles["check"]
    df_error = df_percentiles[mask].reset_index(drop=True).copy()

    if len(df_error) > 0:
        log.warning(
            f"""Averages are not increasing! These distributions need to be corrected:
                {df_error[["ppp_version", "country", "year", "reporting_level", "welfare_type"]].drop_duplicates()}"""
        )
        # Correct cases where avg is not increasing, by repeating the previous avg
        df_percentiles.loc[mask, "avg"] = df_percentiles.loc[mask, "avg"].shift(1)

    ############################################################################################################
    # Check that avg are between thresholds
    # Create thr_lower, which is the threshold for the previous percentile
    df_percentiles["thr_lower"] = df_percentiles.groupby(
        ["ppp_version", "country", "year", "reporting_level", "welfare_type"], dropna=False
    )["thr"].shift(1)
    df_percentiles["check"] = (round(df_percentiles["avg"] - df_percentiles["thr_lower"], 2) >= 0) & (
        round(df_percentiles["thr"] - df_percentiles["avg"]) >= 0
    )

    # Assign True if target_percentile is 1
    df_percentiles.loc[df_percentiles["target_percentile"] == 1, "check"] = True

    # Assign True if target_percentile is 100 and avg is greater than thr_lower
    df_percentiles.loc[
        (df_percentiles["target_percentile"] == 100)
        & (round(df_percentiles["avg"] - df_percentiles["thr_lower"], 2) >= 0),
        "check",
    ] = True

    # Assign True if avg is null
    df_percentiles.loc[df_percentiles["avg"].isnull(), "check"] = True

    # Assign the boolean value to the entire group
    df_percentiles["check"] = df_percentiles.groupby(
        ["ppp_version", "country", "year", "reporting_level", "welfare_type"], dropna=False
    )["check"].transform("all")

    # Define mask
    mask = ~df_percentiles["check"]
    df_error = df_percentiles[mask].reset_index(drop=True).copy()

    if len(df_error) > 0:
        log.warning(
            f"""Averages are not between thresholds! These distributions need to be corrected:
                {df_error[["ppp_version", "country", "year", "reporting_level", "welfare_type"]].drop_duplicates()}"""
        )
        # Correct cases where avg is not between thresholds, by averaging the two thresholds
        df_percentiles.loc[mask, "avg"] = (df_percentiles.loc[mask, "thr_lower"] + df_percentiles.loc[mask, "thr"]) / 2

    # Drop check columns
    df_percentiles = df_percentiles.drop(columns=["check", "check_thr", "check_avg", "check_share", "thr_lower"])

    # Count number of rows after checks
    rows_after = len(df_percentiles)

    log.info(f"Percentiles file generated. {rows_before - rows_after} rows have been deleted.")

    return df_percentiles


# GENERATE RELATIVE POVERTY INDICATORS FILE
# This is data not given directly by the query, but we can get it by calculating 40, 50, 60% of the median and query
# NOTE: Medians need to be patched first in order to get data for all country-years (there are several missing values)


def generate_relative_poverty(wb_api: WB_API):
    """
    Generates relative poverty indicators from query results. Uses concurrency to speed up the process.
    """
    start_time = time.time()

    def get_relative_data(df_row, pct, versions):
        """
        This function is structured in a way to make it work with concurrency.
        It checks if the country file related to the row exists. If not, it runs the query.
        """
        if ~np.isnan(df_row["median"]):
            if Path(
                f"{CACHE_DIR}/pip_country_data/pip_country_{df_row['country_code']}_year_{df_row['year']}_povline_{int(round(df_row['median'] * pct))}_welfare_{df_row['welfare_type']}_rep_{df_row['reporting_level']}_fillgaps_{FILL_GAPS}_ppp_{PPP_VERSIONS[1]}csv"
            ).is_file():
                return
            else:
                return pip_query_country(
                    wb_api,
                    popshare_or_povline="povline",
                    value=df_row["median"] * pct / 100,
                    versions=versions,
                    country_code=df_row["country_code"],
                    year=df_row["year"],
                    fill_gaps=FILL_GAPS,
                    welfare_type=df_row["welfare_type"],
                    reporting_level=df_row["reporting_level"],
                    ppp_version=PPP_VERSIONS[1],
                    download="true",
                )

    def concurrent_relative_function(df):
        """
        This is the main function to make concurrency work for country data.
        """
        # Make sure the directory exists. If not, create it
        Path(f"{CACHE_DIR}/pip_country_data").mkdir(parents=True, exist_ok=True)
        with ThreadPool(MAX_WORKERS) as pool:
            tasks = [(df.iloc[i], pct, versions) for pct in [40, 50, 60] for i in range(len(df))]
            pool.starmap(get_relative_data, tasks)

    def add_relative_indicators(df):
        """
        Integrates the relative indicators to the country df from the cached query results.
        """
        for pct in [40, 50, 60]:
            # Initialize lists
            headcount_ratio_list = []
            pgi_list = []
            pov_severity_list = []
            watts_list = []
            for i in range(len(df)):
                if ~np.isnan(df["median"].iloc[i]) and df.iloc[i]["country_code"] not in OLD_REGIONS:
                    # Here I check if the file exists even after the original extraction. If it does, I read it. If not, I start the queries again.
                    file_path = f"{CACHE_DIR}/pip_country_data/pip_country_{df.iloc[i]['country_code']}_year_{df.iloc[i]['year']}_povline_{int(round(df.iloc[i]['median'] * pct))}_welfare_{df.iloc[i]['welfare_type']}_rep_{df.iloc[i]['reporting_level']}_fillgaps_{FILL_GAPS}_ppp_{PPP_VERSIONS[1]}.csv"
                    if Path(file_path).is_file():
                        results = pd.read_csv(file_path)
                    else:
                        # Run the main function to get the data
                        get_relative_data(df.iloc[i], pct, versions)
                        results = pd.read_csv(file_path)

                    headcount_ratio_value = results["headcount"].iloc[0]
                    headcount_ratio_list.append(headcount_ratio_value)

                    pgi_value = results["poverty_gap"].iloc[0]
                    pgi_list.append(pgi_value)

                    pov_severity_value = results["poverty_severity"].iloc[0]
                    pov_severity_list.append(pov_severity_value)

                    watts_value = results["watts"].iloc[0]
                    watts_list.append(watts_value)

                else:
                    headcount_ratio_list.append(np.nan)
                    pgi_list.append(np.nan)
                    pov_severity_list.append(np.nan)
                    watts_list.append(np.nan)

            # Add the lists as columns to the df
            df[f"headcount_ratio_{pct}_median"] = headcount_ratio_list
            df[f"poverty_gap_index_{pct}_median"] = pgi_list
            df[f"poverty_severity_{pct}_median"] = pov_severity_list
            df[f"watts_{pct}_median"] = watts_list

        return df

    # Obtain versions
    versions = pip_versions(wb_api)

    # FOR COUNTRIES
    # Get data from the most common query
    df_country = pip_query_country(
        wb_api,
        popshare_or_povline="povline",
        value=INTERNATIONAL_POVERTY_LINE_CURRENT,
        versions=versions,
        country_code="all",
        year="all",
        fill_gaps=FILL_GAPS,
        welfare_type="all",
        reporting_level="all",
        ppp_version=PPP_VERSIONS[1],
        download="true",
    )

    # Patch medians
    df_country = median_patch(df_country, country_or_region="country")

    # Run the main function to get the data
    concurrent_relative_function(df_country)

    # Add relative indicators from the results above
    df_country = add_relative_indicators(df=df_country)

    # FOR REGIONS
    # Get data from the most common query
    df_region = pip_query_region(
        wb_api,
        popshare_or_povline="povline",
        value=INTERNATIONAL_POVERTY_LINE_CURRENT,
        versions=versions,
        country_code="all",
        year="all",
        welfare_type="all",
        reporting_level="all",
        ppp_version=PPP_VERSIONS[1],
    )

    # Patch medians (regional p50 now comes from the bins-based percentiles file)
    df_region = median_patch(df_region, country_or_region="region")

    # Add regional relative poverty indicators computed from the 1000-bin distribution, instead of
    # running thousands of `pip-grp` queries at 40/50/60% of each regional median.
    rel_region = compute_regional_distributions_from_bins(wb_api)["relative"]
    rel_cols = [c for c in rel_region.columns if c.endswith("_median")]
    df_region = df_region.merge(
        rel_region[["country_code", "year"] + rel_cols], on=["country_code", "year"], how="left"
    )

    # Concatenate df_country and df_region
    df = pd.concat([df_country, df_region], ignore_index=True)

    # Save to csv
    df.to_csv(f"{CACHE_DIR}/pip_relative.csv", index=False)

    end_time = time.time()
    elapsed_time = round(end_time - start_time, 2)
    log.info(f"Relative poverty indicators calculated. Execution time: {elapsed_time} seconds")

    return df


# GENERATE MAIN INDICATORS FILE


def generate_key_indicators(wb_api: WB_API):
    """
    Generate the main indicators file, from a set of poverty lines and PPP versions. Uses concurrency to speed up the process.
    """
    start_time = time.time()

    def get_country_data(povline, ppp_version, versions):
        """
        This function is defined inside the main function because it needs to be called by concurrency.
        For country data.
        """
        return pip_query_country(
            wb_api,
            popshare_or_povline="povline",
            value=povline / 100,
            versions=versions,
            country_code="all",
            year="all",
            fill_gaps=FILL_GAPS,
            welfare_type="all",
            reporting_level="all",
            ppp_version=ppp_version,
            download="true",
        )

    def get_region_data(povline, ppp_version, versions):
        """
        This function is defined inside the main function because it needs to be called by concurrency.
        For regional data.
        """
        return pip_query_region(
            wb_api,
            popshare_or_povline="povline",
            value=povline / 100,
            versions=versions,
            country_code="all",
            year="all",
            welfare_type="all",
            reporting_level="all",
            ppp_version=ppp_version,
            download="true",
        )

    def concurrent_function():
        """
        This function makes concurrency work for country data.
        """
        with ThreadPool(MAX_WORKERS) as pool:
            tasks = [
                (povline, ppp_version, versions)
                for ppp_version, povlines in POVLINES_DICT.items()
                for povline in povlines
            ]
            results = pool.starmap(get_country_data, tasks)

        # Concatenate list of dataframes
        results = pd.concat(results, ignore_index=True)

        return results

    def concurrent_region_function():
        """
        This function makes concurrency work for regional data.
        """
        with ThreadPool(int(round(MAX_WORKERS / 2))) as pool:
            tasks = [
                (povline, ppp_version, versions)
                for ppp_version, povlines in POVLINES_DICT.items()
                for povline in povlines
            ]
            results = pool.starmap(get_region_data, tasks)

        # Concatenate list of dataframes
        results = pd.concat(results, ignore_index=True)

        return results

    def get_china_india_data_filled(povline, ppp_version, versions):
        """
        This function extracts filled data for China and India to be used in the key indicators file.
        """
        return pip_query_country(
            wb_api,
            popshare_or_povline="povline",
            value=povline / 100,
            versions=versions,
            country_code="CHN&country=IND",
            year="all",
            fill_gaps="true",
            welfare_type="all",
            reporting_level="national",
            ppp_version=ppp_version,
            download="true",
        )

    def concurrent_function_china_india():
        """
        This function makes concurrency work for China and India data.
        """
        with ThreadPool(MAX_WORKERS) as pool:
            tasks = [
                (povline, ppp_version, versions)
                for ppp_version, povlines in POVLINES_DICT.items()
                for povline in povlines
            ]
            results = pool.starmap(get_china_india_data_filled, tasks)

        # Concatenate list of dataframes
        results = pd.concat(results, ignore_index=True)

        return results

    # Obtain latest versions of the PIP dataset
    versions = pip_versions(wb_api)

    # Run the main function
    results = concurrent_function()
    results_region = concurrent_region_function()

    # Query China and India data
    results_china_india = concurrent_function_china_india()

    # Calculate World (excluding China) and World (excluding India) data
    results_region = calculate_world_excluding_china_and_india(results_region, results_china_india)

    # If country is nan but country_code is TWN, replace country with Taiwan, China
    results.loc[results["country"].isnull() & (results["country_code"] == "TWN"), "country"] = "Taiwan, China"

    # I check if the set of countries is the same in the df and in the aux table (list of countries)
    aux_dict = pip_aux_tables(wb_api, table="countries")
    assert set(results["country"]) == set(aux_dict["countries"]["country_name"]), log.fatal(
        f"List of countries is not the same! Differences: {set(results['country']) - set(aux_dict['countries']['country_name'])}"
    )

    # # I check if the set of regions is the same in the df and in the aux table (list of regions) + World (excluding China) + World (excluding India)
    # aux_dict = pip_aux_tables(wb_api, table="regions")

    # countries_to_check = set(aux_dict["regions"]["region"]) | {"World (excluding China)", "World (excluding India)"}

    # assert set(results_region["country"]) == (countries_to_check), log.fatal(
    #     f"List of regions is not the same! Differences: {set(results_region['country']) - countries_to_check}"
    # )

    # Concatenate df_country and df_region
    df = pd.concat([results, results_region], ignore_index=True)

    # Sort ppp_version, country, year and poverty_line
    df = df.sort_values(by=["ppp_version", "country", "year", "poverty_line"])  # type: ignore

    # Save to csv
    df.to_csv(f"{CACHE_DIR}/pip_raw.csv", index=False)

    end_time = time.time()
    elapsed_time = round(end_time - start_time, 2)
    log.info(f"Key indicators calculated. Execution time: {elapsed_time} seconds")

    return df


def calculate_world_excluding_china_and_india(results_region: pd.DataFrame, results_china_india: pd.DataFrame):
    """
    Calculate World (excluding China) and World (excluding India) data.
    """

    results_region = results_region.copy()
    results_china_india = results_china_india.copy()

    # Filter results to show only World
    results_world = results_region[results_region["country"] == "World"].copy().reset_index(drop=True)

    # Keep country, year, poverty_line and headcount columns
    results_world = results_world[["ppp_version", "country", "year", "poverty_line", "headcount", "reporting_pop"]]
    results_china_india = results_china_india[
        ["ppp_version", "country", "year", "poverty_line", "headcount", "reporting_pop"]
    ]

    # Create headcount_ratio column
    results_world["headcount_number"] = results_world["headcount"] * results_world["reporting_pop"]
    results_china_india["headcount_number"] = results_china_india["headcount"] * results_china_india["reporting_pop"]

    # Make these columns integer
    results_world["headcount_number"] = results_world["headcount_number"].astype(int)
    results_china_india["headcount_number"] = results_china_india["headcount_number"].astype(int)

    # Merge results_world and results_china_india
    results_excluding = pd.merge(
        results_china_india,
        results_world,
        on=["ppp_version", "year", "poverty_line"],
        how="left",
        suffixes=("", "_world"),
    )

    # Calculate headcount_excluding as the difference between headcount_world and headcount
    results_excluding["headcount_number_excluding"] = (
        results_excluding["headcount_number_world"] - results_excluding["headcount_number"]
    )

    # Same with reporting_pop
    results_excluding["reporting_pop_excluding"] = (
        results_excluding["reporting_pop_world"] - results_excluding["reporting_pop"]
    )

    # Estimate headcount_excluding
    results_excluding["headcount_excluding"] = (
        results_excluding["headcount_number_excluding"] / results_excluding["reporting_pop_excluding"]
    )

    # Keep country, year , poverty_line, headcount_excluding and reporting_pop_excluding columns
    results_excluding = results_excluding[
        ["ppp_version", "country", "year", "poverty_line", "headcount_excluding", "reporting_pop_excluding"]
    ]

    # Rename countries to World (excluding China) and World (excluding India)
    results_excluding["country"] = results_excluding["country"].replace(
        {"China": "World (excluding China)", "India": "World (excluding India)"}
    )

    # Rename columns to headcount and reporting_pop
    results_excluding = results_excluding.rename(
        columns={"headcount_excluding": "headcount", "reporting_pop_excluding": "reporting_pop"}
    )

    # Concatenate tables
    results_region = pd.concat([results_region, results_excluding], ignore_index=True)

    return results_region


def median_patch(df, country_or_region):
    """
    Patch missing values in the median column.
    PIP queries do not return all the medians, so they are patched with the results of the percentile file.
    """

    # Read percentile file
    df_percentiles = pd.read_csv(f"{CACHE_DIR}/world_bank_pip_percentiles.csv")

    # In df_percentiles, keep only the rows with percentile = 50
    df_percentiles = df_percentiles[df_percentiles["percentile"] == 50].reset_index()

    # If I want to patch the median for regions, I need to drop reporting_level and welfare_type columns
    if country_or_region == "country":
        # Merge df and df_percentiles
        df = pd.merge(
            df,
            df_percentiles[
                ["ppp_version", "country", "country_code", "year", "reporting_level", "welfare_type", "thr"]
            ],
            on=["ppp_version", "country", "country_code", "year", "reporting_level", "welfare_type"],
            how="left",
        )

        # Replace missing values in median with thr
        df["median"] = df["median"].fillna(df["thr"])

        # Drop thr column
        df = df.drop(columns=["thr"])

    elif country_or_region == "region":
        # Merge df and df_percentiles
        df = pd.merge(
            df,
            df_percentiles[["ppp_version", "country", "country_code", "year", "thr"]],
            on=["ppp_version", "country", "country_code", "year"],
            how="left",
        )

        # Rename thr to median
        df = df.rename(columns={"thr": "median"})

    else:
        raise ValueError("country_or_region must be 'country' or 'region'")

    log.info("Medians patched!")

    return df


def add_relative_poverty_and_decile_thresholds(df, df_relative, df_percentiles, wb_api: WB_API):
    """
    Add relative poverty indicators and decile thresholds to the key indicators file.
    """

    # Add relative poverty indicators
    # They don't change with the PPP version, so we can use the latest PPP version (PPP_VERSIONS[1]) I estimated before.
    df = pd.merge(
        df,
        df_relative[
            [
                "country",
                "country_code",
                "year",
                "reporting_level",
                "welfare_type",
                "headcount_ratio_40_median",
                "poverty_gap_index_40_median",
                "poverty_severity_40_median",
                "watts_40_median",
                "headcount_ratio_50_median",
                "poverty_gap_index_50_median",
                "poverty_severity_50_median",
                "watts_50_median",
                "headcount_ratio_60_median",
                "poverty_gap_index_60_median",
                "poverty_severity_60_median",
                "watts_60_median",
            ]
        ],
        on=["country", "country_code", "year", "reporting_level", "welfare_type"],
        how="left",
    )

    # In df_percentiles, keep only the rows with percentile = 10, 20, 30, ... 90
    df_percentiles = df_percentiles[
        (df_percentiles["percentile"] % 10 == 0) & (df_percentiles["percentile"] != 100)
    ].reset_index()

    # Make tb_percentile wide, with percentile as columns
    df_percentiles = df_percentiles.pivot(
        index=["ppp_version", "country", "country_code", "year", "reporting_level", "welfare_type"],
        columns="percentile",
        values="thr",
    )

    # Rename columns
    df_percentiles.columns = ["decile" + str(int(round(col / 10))) + "_thr" for col in df_percentiles.columns]

    # Reset index
    df_percentiles = df_percentiles.reset_index()

    # Merge df and df_percentiles
    df = pd.merge(
        df,
        df_percentiles,
        on=["ppp_version", "country", "country_code", "year", "reporting_level", "welfare_type"],
        how="left",
    )

    # Add regional definitions
    df = add_regional_definitions(wb_api, df=df)

    # Save key indicators file
    df.to_csv(f"{CACHE_DIR}/world_bank_pip.csv", index=False)

    log.info("Relative poverty indicators and decile thresholds added. Key indicators file done :)")

    return df


def add_regional_definitions(wb_api: WB_API, df: pd.DataFrame) -> pd.DataFrame:
    """
    Extract the complete definitions of regions and their countries from the World Bank API.
    This is a more complete version of the regional definitions that are already in the PIP dataset (Saudi Arabia, for example, is missing).
    """

    # Remove region_name column
    df = df.drop(columns=["region_name"])

    # Get regional definitions
    df_regional_definitions = pip_aux_tables(wb_api, table="country_list")

    # Make it a dataframe
    df_regional_definitions = pd.DataFrame.from_dict(df_regional_definitions["country_list"])

    # Rename country_name to country
    df_regional_definitions = df_regional_definitions.rename(
        columns={"country_name": "country", "region": "region_name"}
    )

    # Keep the relevant columns
    df_regional_definitions = df_regional_definitions[["country", "region_name"]]

    # Define MAX_YEAR as the maximum year in the df
    MAX_YEAR = df["year"].max()

    # Add year = MAX_YEAR to the regional definitions
    df_regional_definitions["year"] = MAX_YEAR

    # Save to csv
    df_regional_definitions.to_csv(f"{CACHE_DIR}/world_bank_pip_regions.csv", index=False)

    log.info("Regional definitions generated from API.")

    return df


def add_filled_data(df: pd.DataFrame, wb_api: WB_API) -> pd.DataFrame:
    """
    Add filled data for China and India to the key indicators file.
    """

    # Obtain latest versions of the PIP dataset
    versions = pip_versions(wb_api)

    # Initialize empty lists to store filled data for countries and regions
    df_country_filled = []
    df_region_filled = []

    # Read filled data for countries
    for ppp_version, povlines in POVLINES_DICT.items():
        for povline in povlines:
            # Query filled data
            df_country_filled_by_povline = pip_query_country(
                wb_api,
                popshare_or_povline="povline",
                value=povline / 100,
                versions=versions,
                country_code="all",
                year="all",
                fill_gaps="true",
                welfare_type="all",
                reporting_level="all",
                ppp_version=ppp_version,
                download="true",
            )

            df_region_filled_by_povline = pip_query_region(
                wb_api,
                popshare_or_povline="povline",
                value=povline / 100,
                versions=versions,
                country_code="all",
                year="all",
                welfare_type="all",
                reporting_level="all",
                ppp_version=ppp_version,
                download="true",
            )

            # Append filled data for the current povline
            df_country_filled.append(df_country_filled_by_povline)
            df_region_filled.append(df_region_filled_by_povline)

    # Concatenate all filled data for countries and regions
    df_country_filled = pd.concat(df_country_filled, ignore_index=True)
    df_region_filled = pd.concat(df_region_filled, ignore_index=True)

    # Concatenate filled data for countries and regions
    df_filled = pd.concat([df_country_filled, df_region_filled], ignore_index=True)

    # Remove region_name column
    df_filled = df_filled.drop(columns=["region_name"])

    # Add the column filled to identify if the data is filled or not
    df["filled"] = False
    df_filled["filled"] = True

    # Concatenate with the original df
    df = pd.concat([df, df_filled], ignore_index=True)

    # Export df_filled to csv
    df.to_csv(f"{CACHE_DIR}/world_bank_pip.csv", index=False)

    log.info("Filled data for countries and regions included. Now we are really done :)")

    return df
