"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("global_hunger_index.xlsx")

    # Load the sheet of scores (named "GHI Scores <year>", in the 2026 report with a trailing space).
    excel = snap.ExcelFile()
    [sheet_name] = [sheet for sheet in excel.sheet_names if sheet.startswith("GHI Scores")]
    tb = excel.parse(sheet_name, header=2, dtype=str)

    # Load the sheet of rankings (named "GHI Ranking <year>"), which also gives a provisional range of scores in the latest
    # year for some countries without a score.
    [ranking_sheet_name] = [sheet for sheet in excel.sheet_names if sheet.startswith("GHI Ranking")]
    tb_ranking = excel.parse(ranking_sheet_name, header=2, dtype=str)

    #
    # Process data.
    #
    # Keep the country column and one column per score year (headers look like "2026\n'21-'26"), and drop the columns of changes.
    tb = tb.rename(columns={tb.columns[0]: "country"})
    year_columns = {column: int(column[:4]) for column in tb.columns if column[:4].isdigit()}
    tb = tb[["country"] + list(year_columns)].rename(columns=year_columns)

    # Drop the empty row and the notes below the table.
    tb = tb.dropna(subset=list(year_columns.values()), how="all")

    # Transpose to one row per country and year, keeping the scores as they appear in the sheet (including "<5" and "—").
    tb = tb.melt(id_vars=["country"], var_name="year", value_name="global_hunger_index")

    # Improve table format.
    tb = tb.format(["country", "year"], short_name=paths.short_name)

    # Keep the countries whose score in the latest year is a provisional range (marked with "*", e.g. "35-49.9*").
    latest_year = max(year_columns.values())
    tb_provisional = tb_ranking.rename(columns={"Country": "country", str(latest_year): "provisional_range"})
    tb_provisional = tb_provisional[tb_provisional["provisional_range"].str.endswith("*", na=False)][
        ["country", "provisional_range"]
    ]

    # Split rows that list several countries (e.g. "Burundi, South Sudan and Yemen") into one row per country.
    tb_provisional["country"] = tb_provisional["country"].str.split(r", | and ", regex=True)
    tb_provisional = tb_provisional.explode("country")
    tb_provisional["year"] = latest_year

    # Improve table format.
    tb_provisional = tb_provisional.format(["country", "year"], short_name=f"{paths.short_name}_provisional")

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=[tb, tb_provisional], default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()
