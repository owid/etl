"""Snapshot of the WHO Mortality Database, read from the platform's export API.

WHO publishes its Mortality Database two ways: raw death-registration files on who.int, and a
platform (https://platform.who.int/mortality) where deaths are already grouped into 193 named
cause groups, with age-standardized rates and shares of total deaths. We ingest the platform
version, because those two derived indicators only exist there.

The platform's download button calls an export endpoint, one cause group per request. This
script calls that endpoint for every cause group in CAUSES and stacks the results into one
table. The 24 groups here are the tier of WHO's cause tree that partitions all registered
deaths: 23 groups that tile "All Causes", plus All Causes itself.

Each export opens with five metadata lines and a blank line. The fifth line carries the ICD-10
codes that define the cause group; we keep it as `icd10_codes`, so the definition travels with
the data instead of being typed by hand.

Raw exports are cached under CACHE_DIR, so re-running after a parsing fix does not re-download
roughly 700 MB. Delete that folder to force fresh downloads.
"""

import base64
import io
import json
from multiprocessing.pool import ThreadPool
from pathlib import Path

import pandas as pd
import requests
from owid.repack import repack_frame
from structlog import get_logger
from tenacity import retry, stop_after_attempt, wait_random_exponential

from etl.paths import CACHE_DIR
from etl.snapshot import Snapshot

SNAPSHOT_VERSION = Path(__file__).parent.name
log = get_logger()

API = "https://apps.who.int/data/mortality/api/EN"
# The export endpoint takes a base64-encoded JSON query. An empty query returns the full dataset
# for the indicator; naming filters in it makes the server return a 500.
EXPORT_QUERY = base64.b64encode(json.dumps({"dataFilters": [], "divisionFactors": [], "query": []}).encode()).decode()
# The endpoint answers this browser-like User-Agent; a plain one has not been tested.
HEADERS = {"User-Agent": "Mozilla/5.0 owid-etl"}
CACHE = CACHE_DIR / "who_mortality_database"
N_PARALLEL_DOWNLOADS = 4

# WHO's label for each cause group, and its indicator code on the platform. Labels are kept as WHO
# writes them; garden renames the two we label differently.
CAUSES = {
    "All Causes": "CG0000",
    "Infectious and parasitic diseases": "CG0020",
    "Respiratory infections": "CG0380",
    "Maternal conditions": "CG0420",
    "Perinatal conditions": "CG0490",
    "Nutritional deficiencies": "CG0530",
    "Malignant neoplasms": "CG0600",
    "Other neoplasms": "CG0780",
    "Diabetes mellitus and endocrine disorders": "CG0789",
    "Neuropsychiatric conditions": "CG0810",
    "Sense organ diseases": "CG0980",
    "Cardiovascular diseases": "CG1040",
    "Respiratory diseases": "CG1110",
    "Digestive diseases": "CG1150",
    "Genitourinary diseases": "CG1200",
    "Skin diseases": "CG1240",
    "Musculoskeletal diseases": "CG1250",
    "Congenital anomalies": "CG1310",
    "Oral conditions": "CG1430",
    "Sudden infant death syndrome": "CG1475",
    "Unintentional injuries": "CG1490",
    "Intentional injuries": "CG1560",
    "Ill-defined injuries/accidents": "CG1605",
    "Ill-defined diseases": "CG1610",
}

# Five metadata lines and a blank line precede the header row.
N_HEADER_LINES = 6
EXPECTED_COLUMNS = [
    "Region Code",
    "Region Name",
    "Country Code",
    "Country Name",
    "Year",
    "Sex",
    "Age group code",
    "Age Group",
    "Number",
    "Percentage of cause-specific deaths out of total deaths",
    "Age-standardized death rate per 100 000 standard population",
    "Death rate per 100 000 population",
]
CATEGORICAL_COLUMNS = [
    "Region Code",
    "Region Name",
    "Country Code",
    "Country Name",
    "Sex",
    "Age group code",
    "Age Group",
]


def run(upload: bool = True) -> None:
    snap = Snapshot(f"who/{SNAPSHOT_VERSION}/mortality_database.feather")

    # Step 1: Check our cause list against the platform's current indicator list, so a renamed or
    # renumbered cause group fails here instead of silently fetching the wrong thing.
    codes = fetch_indicator_codes()
    mismatched = {label: (code, codes.get(label)) for label, code in CAUSES.items() if codes.get(label) != code}
    assert not mismatched, f"Cause groups whose label or code changed on the platform: {mismatched}"

    # Step 2: Download one export per cause group (cached), in parallel.
    CACHE.mkdir(parents=True, exist_ok=True)
    with ThreadPool(N_PARALLEL_DOWNLOADS) as pool:
        paths = pool.map(download_export, CAUSES.values())

    # Step 3: Parse each export and stack them, labelling rows with the cause and its ICD-10 codes.
    frames = []
    for label, path in zip(CAUSES, paths):
        df, icd10_codes = read_export(path)
        df["cause"] = label
        df["icd10_codes"] = icd10_codes
        frames.append(df)
    df = pd.concat(frames, ignore_index=True)

    # Step 4: Integrity checks on the stacked table.
    assert df["cause"].nunique() == len(CAUSES), "Some cause groups are missing after stacking"
    assert (df["Number"].dropna() >= 0).all(), "Negative death counts in the export"
    assert df["icd10_codes"].notna().all() and (df["icd10_codes"] != "").all(), "A cause group has no ICD-10 codes"
    assert not df.duplicated(subset=["Country Name", "Year", "Sex", "Age group code", "cause"]).any(), "Duplicate rows"
    log.info(
        "who_mortality_database.stacked",
        rows=len(df),
        causes=df["cause"].nunique(),
        years=(int(df["Year"].min()), int(df["Year"].max())),
    )

    df = repack_frame(df)
    snap.create_snapshot(data=df, upload=upload)


def fetch_indicator_codes() -> dict[str, str]:
    """Return WHO's indicator label -> code mapping for every cause group on the platform."""
    response = requests.get(f"{API}/dimensions/indicators", headers=HEADERS, timeout=60)
    response.raise_for_status()
    return {item["label"]: item["key"] for item in response.json()}


@retry(stop=stop_after_attempt(5), wait=wait_random_exponential(min=5, max=90))
def download_export(code: str) -> Path:
    """Download the full export for one cause group, unless it is already cached."""
    path = CACHE / f"{code}.csv"
    if path.exists():
        return path
    log.info("who_mortality_database.download", code=code)
    response = requests.get(
        f"{API}/facts/data-export",
        params={"queryJson": EXPORT_QUERY, "indicatorCode": code},
        headers=HEADERS,
        timeout=600,
    )
    response.raise_for_status()
    assert response.text.startswith("Indicator Code:"), f"Unexpected export body for {code}: {response.text[:200]!r}"
    # Write to a temporary name first, so an interrupted download never leaves a partial file in the cache.
    tmp = path.with_suffix(".part")
    tmp.write_text(response.text, encoding="utf-8")
    tmp.rename(path)
    return path


def read_export(path: Path) -> tuple[pd.DataFrame, str]:
    """Parse one export: the data table, plus the ICD-10 codes from its metadata header."""
    text = path.read_text(encoding="utf-8")
    header, body = text.split("\n", N_HEADER_LINES)[:N_HEADER_LINES], text.split("\n", N_HEADER_LINES)[N_HEADER_LINES]

    # Line 5 reads: "Indicator metadata: K00-K14" (quoted when it contains a comma).
    metadata_line = header[4].strip().strip('"')
    prefix = "Indicator metadata: "
    assert metadata_line.startswith(prefix), f"Unexpected metadata line in {path.name}: {metadata_line!r}"
    icd10_codes = metadata_line[len(prefix) :].strip()

    # Every data row ends with a trailing comma, one field more than the header. `index_col=False` makes
    # pandas ignore it instead of shifting the columns.
    df = pd.read_csv(io.StringIO(body), index_col=False, dtype={col: "category" for col in CATEGORICAL_COLUMNS})
    assert list(df.columns) == EXPECTED_COLUMNS, f"Unexpected columns in {path.name}: {list(df.columns)}"
    return df, icd10_codes
