"""Load a snapshot and create a meadow dataset."""

from owid.catalog import Table
from owid.catalog import processing as pr

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("esvac_sales_corrected.zip")

    tables = Table()
    for year in range(2010, 2023):
        # Add the column names
        cols = [
            "country",
            f"sales_for_food_producing_animals_{year-1}",
            "sales_for_food_producing_animals",
            f"pcu_{year-1}",
            "pcu",
            "percentage_change_pcu",
            f"mg_per_pcu_{year-1}",
            "mg_per_pcu",
            "percentage_change_mg_per_pcu",
            "year",
        ]
        # Load data from snapshot.

        tb = snap.read_in_archive(
            filename=f"esvac_corrected/esvac_{year}.xlsx", sheet_name="Population corrected sales by c", skiprows=5
        )
        tb["year"] = year
        assert tb.columns[2] == str(year), f"Year {year} not found in the table"
        # Check the right year is being processed
        assert tb.columns[2] == str(year), f"Year {year} not found in the table"
        assert len(tb.columns) == len(cols)
        tb.columns = cols
        if year >= 2017:
            tb_uk = snap.read_in_archive(
                filename=f"esvac_corrected/uk_esvac_{year}.xlsx",
                sheet_name="Population corrected sales by c",
                skiprows=5,
            )
            assert tb_uk.columns[2] == str(year), f"Year {year} not found in the table"
            # Check the right year is being processed
            assert tb_uk.columns[2] == str(year), f"Year {year} not found in the table"
            assert len(tb.columns) == len(cols)
            tb_uk.columns = cols
            tb = pr.concat([tb, tb_uk])

        # Remove rows with missing values
        tb = tb.dropna(
            subset=[
                "sales_for_food_producing_animals",
                "pcu",
                "mg_per_pcu",
            ]
        )
        # Drop columns that are not needed
        tb = tb.drop(
            columns=[
                f"sales_for_food_producing_animals_{year-1}",
                f"pcu_{year-1}",
                f"mg_per_pcu_{year-1}",
            ]
        )
        # Combine tables
        tables = pr.concat([tables, tb])
    #
    # Process data.
    #
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tables.format(["country", "year"], short_name="esvac_sales_corrected")

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
