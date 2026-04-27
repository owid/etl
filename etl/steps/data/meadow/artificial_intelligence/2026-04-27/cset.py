"""Load a snapshot and create a meadow dataset."""

import owid.catalog.processing as pr
from owid.catalog import Table

from etl.helpers import PathFinder

paths = PathFinder(__file__)

# Each inner list is a pair of CSVs to merge on [country, year, field].
FILE_GROUPS = [
    ["companies_yearly_disclosed.csv", "companies_yearly_estimated.csv"],
    ["patents_yearly_applications.csv", "patents_yearly_granted.csv"],
    ["publications_yearly_articles.csv", "publications_yearly_citations.csv"],
]


def run() -> None:
    snap = paths.load_snapshot("cset.zip")

    with snap.extracted() as archive:
        field_tables = [read_and_merge(archive, file_ids) for file_ids in FILE_GROUPS]

    tb = pr.multi_merge(field_tables, on=["year", "country", "field"], how="outer")
    tb = tb.format(["country", "year", "field"])

    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)
    ds_meadow.save()


def read_and_merge(archive, file_ids: list[str]) -> Table:
    """Read and merge a pair of CSV files from the archive into a single table."""
    tables = [_read_csv(archive, file_name) for file_name in file_ids]
    return pr.multi_merge(tables, on=["year", "country", "field"], how="outer")


def _read_csv(archive, file_name: str) -> Table:
    """Read one CSV from the archive, filter to complete rows, and normalise field names."""
    tb = archive.read(f"cat/{file_name}")

    if "complete" in tb.columns:
        tb["complete"] = tb["complete"].astype(str)
        if file_name == "companies_yearly_estimated.csv":
            tb = _handle_estimated_investment(tb)
        else:
            tb = tb[tb["complete"] == "True"].drop(columns=["complete"])

    # Normalise field capitalisation: keep first character, lowercase the rest
    # (e.g. "AI" → "Ai", "Natural language processing" stays as-is)
    tb["field"] = tb["field"].str[:1] + tb["field"].str[1:].str.lower()

    if file_name != "companies_yearly_estimated.csv":
        numeric_cols = tb.select_dtypes(include=["number"]).columns.tolist()
        if numeric_cols:
            tb = tb.groupby(["country", "year", "field"], as_index=False)[numeric_cols].sum()

    return tb


def _handle_estimated_investment(tb: Table) -> Table:
    """Split estimated-investment rows into actuals and projections.

    The source marks incomplete (projected) years with complete=False. We keep
    actuals as-is and expose projected values in separate *_projected columns,
    filling any missing projected years with the last known actual value.
    """
    value_cols = [c for c in tb.columns if c not in ["country", "year", "field", "complete"]]

    tb_actual = tb[tb["complete"] == "True"].drop(columns=["complete"])
    tb_actual = tb_actual.dropna(subset=value_cols, how="all")

    # Last actual year per country/field — used as baseline projection.
    last_year = tb_actual.groupby(["country", "field"])["year"].max().rename("last_year")

    # Source rows labelled as projections (complete=False), restricted to years
    # >= the last actual year to avoid leaking incomplete historical rows.
    tb_projected = (
        tb[tb["complete"] == "False"]
        .drop(columns=["complete"])
        .merge(last_year.reset_index(), on=["country", "field"], how="left")
    )
    tb_projected = tb_projected[tb_projected["year"] >= tb_projected["last_year"]].drop(columns=["last_year"])
    tb_projected = tb_projected.rename(columns={c: f"{c}_projected" for c in value_cols})

    # Last actual year per country/field used as a baseline projection
    tb_last_actual = tb_actual.loc[tb_actual.groupby(["country", "field"])["year"].idxmax()][
        ["country", "field", "year"] + value_cols
    ].rename(columns={c: f"{c}_projected" for c in value_cols})

    tb_projections = pr.concat([tb_projected, tb_last_actual], ignore_index=True)
    return pr.merge(tb_actual, tb_projections, on=["country", "year", "field"], how="outer")
