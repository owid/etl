from pathlib import Path

from owid.catalog import Table
from owl import Action, Dataset, Snapshot, SnapshotCapture
from owl.catalog import load_snapshot
from owl.grapher import upsert_dataset

from etl.data_helpers import geo

URL_DOWNLOAD = "https://raw.githubusercontent.com/akarlinsky/death_registration/main/death_reg_final.csv"
COUNTRIES_FILE = Path(__file__).with_name("deaths_karlinsky.countries.json")


@Snapshot
def raw_data(snap: SnapshotCapture) -> None:
    snap.download(URL_DOWNLOAD, suffix=".csv")


@Dataset
def deaths_karlinsky(raw_data: Snapshot) -> Table:
    tb = load_snapshot(raw_data, short_name="deaths_karlinsky")

    tb = tb.drop(columns=["continent", "source"])
    tb = tb.rename(columns={"country_name": "country"})

    tb = geo.harmonize_countries(
        df=tb,
        countries_file=COUNTRIES_FILE,
    )

    tb = tb.format(["country", "year"])
    _sanity_checks(tb)
    return tb


@Action(kind="grapher", default=False)
def upsert_to_grapher(deaths_karlinsky: Dataset) -> None:
    upsert_dataset(deaths_karlinsky)


def _sanity_checks(tb: Table) -> None:
    columns_expected = {
        "death_comp",
        "expected_deaths",
        "expected_gbd",
        "expected_ghe",
        "expected_wpp",
        "reg_deaths",
        "expected_confidence_score",
    }
    columns_new = set(tb.columns).difference(columns_expected)
    if columns_new:
        raise ValueError(f"Unexpected columns {columns_new}")

    for col in ["death_comp"]:
        assert all(tb[col] <= 100), f"{col} has values larger than 100%"
        assert all(tb[col] >= 0), f"{col} has values lower than 0%"
