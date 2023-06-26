from owid.catalog import Dataset

from etl.helpers import PathFinder, create_dataset

# naming conventions
N = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Load dataset from meadow.
    ds_meadow = N.meadow_dataset

    # Load tables from dataset.
    tb_1p5celsius = ds_meadow["co2_mitigation_curves_1p5celsius"]
    tb_2celsius = ds_meadow["co2_mitigation_curves_2celsius"]

    #
    # Process data.
    #
    # Rename columns (that will become rows at a later step).
    tb_1p5celsius = tb_1p5celsius.rename(
        columns={
            column: f"Start in {column.replace('_', '')}"
            for column in tb_1p5celsius.columns
            if column not in ["year", "historical"]
        }
    ).rename(columns={"historical": "Historical"})
    # Switch to a long table.
    tb_1p5celsius = tb_1p5celsius.melt(id_vars="year", var_name="origin", value_name="emissions")
    # Set an appropriate index and sort.
    tb_1p5celsius = tb_1p5celsius.set_index(["year", "origin"], verify_integrity=True).sort_index().sort_index(axis=1)

    # Repeat the process for the other table.
    tb_2celsius = tb_2celsius.rename(
        columns={
            column: f"Start in {column.replace('_', '')}"
            for column in tb_2celsius.columns
            if column not in ["year", "historical"]
        }
    ).rename(columns={"historical": "Historical"})
    # Switch to a long table.
    tb_2celsius = tb_2celsius.melt(id_vars="year", var_name="origin", value_name="emissions")
    # Set an appropriate index and sort.
    tb_2celsius = tb_2celsius.set_index(["year", "origin"], verify_integrity=True).sort_index().sort_index(axis=1)

    #
    # Save outputs.
    #
    # Create a new dataset.
    ds_garden = create_dataset(dest_dir=dest_dir, tables=[tb_1p5celsius, tb_2celsius], default_metadata=ds_meadow.metadata)
    ds_garden.save()
