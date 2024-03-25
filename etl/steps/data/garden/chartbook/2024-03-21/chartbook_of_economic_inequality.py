"""
Construct the Chartbook of Economic Inequality dataset.

It comprises tables from multiple datasets, constructed as a long format table.
"""

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden datasets.
    ds_altimir = paths.load_dataset("altimir_1986")
    ds_sedlac = paths.load_dataset("sedlac")

    # Load garden tables
    tb_sedlac = ds_sedlac["sedlac_no_spells"].reset_index()
    tb_altimir = ds_altimir["altimir_1986"].reset_index()

    #
    # Process data.
    tb_sedlac = tb_sedlac[tb_sedlac["country"].isin(["Argentina (urban)", "Brazil"])].reset_index(drop=True)

    # Argentina
    # Rename Argentina (urban) to Argentina
    tb_sedlac["country"] = tb_sedlac["country"].replace("Argentina (urban)", "Argentina")

    tb = tb.set_index(["country", "year", "spell", "spell_name"], verify_integrity=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
