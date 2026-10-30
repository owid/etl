"""Load a snapshot and create a meadow dataset."""

from owid.catalog import processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("aggregate_replacement_ratio.gz")

    # Read the compressed SDMX TSV: the first column joins the dimension codes with commas, the year columns are tab-separated.
    tb = snap.read_csv(sep=r",|\t", engine="python", compression="gzip")

    #
    # Process data.
    #
    # Remove the "\TIME_PERIOD" suffix from the geo column.
    tb = tb.rename(columns={column: column.replace("\\TIME_PERIOD", "") for column in tb.columns})

    # Reshape to long format, with one row per country, sex and year.
    dimension_columns = ["freq", "sex", "unit", "geo"]
    tb = tb.melt(id_vars=dimension_columns, var_name="year", value_name="value")
    tb["year"] = tb["year"].str.strip().astype(int)

    # The data is annual only.
    assert set(tb["freq"]) == {"A"}, f"Unexpected frequencies: {set(tb['freq'])}"
    tb = tb.drop(columns=["freq"], errors="raise")

    # Split each cell (e.g. "0.47 b") into its value and Eurostat's observation flag (b: break in series, p: provisional).
    cells = tb["value"].astype(str).str.strip()
    tb["flag"] = cells.str.extract(r"([a-z]+)$", expand=False).fillna("")
    tb["flag"] = tb["flag"].copy_metadata(tb["value"])
    tb["value"] = pr.to_numeric(cells.str.replace(r"[a-z\s]+$", "", regex=True).replace(":", None)).copy_metadata(
        tb["value"]
    )

    # Drop cells without data.
    tb = tb.dropna(subset=["value"]).reset_index(drop=True)

    # Use categoricals for the low-cardinality columns.
    for column in ["sex", "unit", "geo", "flag"]:
        tb[column] = tb[column].astype("category")

    # Improve table format.
    tb = tb.format(["geo", "sex", "unit", "year"])

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()
