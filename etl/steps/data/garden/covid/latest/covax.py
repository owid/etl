"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("covax")

    # Read table from meadow dataset.
    tb = ds_meadow.read("covax")

    #
    # Process data.
    #
    # Country: column name, harmonize country names
    tb = tb.rename(columns={"entity": "country"})
    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
    )

    # Split into multiple tables
    # tb_donated = extract_table(tb, "only_donated")
    # tb_delivered = extract_table(tb, "delivered")
    # tb_announced = extract_table(tb, "only_announced")

    # # Format
    # tables = [
    #     tb_donated.format(["country", "year"], short_name="donated"),
    #     tb_delivered.format(["country", "year"], short_name="delivered"),
    #     tb_announced.format(["country", "year"], short_name="announced"),
    # ]

    tables = [tb.format(["country", "year"], short_name="covax")]
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir,
        tables=tables,
        check_variables_metadata=True,
        default_metadata=ds_meadow.metadata,
        formats=["csv", "feather"],
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def extract_table(tb: Table, indicator_name: str) -> Table:
    tb = tb.loc[:, ["country", "year"] + list(tb.filter(regex=f"{indicator_name}.*").columns)]

    # TODO: Can't have multiple columns (across different tables) with the same name!
    # rename = {col: col.replace(f"{indicator_name}_", "").replace(indicator_name, "absolute") for col in tb.columns}

    # tb = tb.rename(columns=rename)
    return tb
