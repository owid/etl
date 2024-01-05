"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshots.
    # GDP
    snap_gdp = paths.load_snapshot("fariss_et_al_gdp.rds")
    tb_gdp = snap_gdp.read()

    # GDP per capita
    snap_gdp_pc = paths.load_snapshot("fariss_et_al_gdp_pc.rds")
    tb_gdp_pc = snap_gdp_pc.read()

    # Population
    snap_pop = paths.load_snapshot("fariss_et_al_pop.rds")
    tb_pop = snap_pop.read()

    #
    # Process data.
    #
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb_gdp = tb_gdp.underscore().set_index(["indicator", "gwno", "year"], verify_integrity=True).sort_index()
    tb_gdp_pc = tb_gdp_pc.underscore().set_index(["indicator", "gwno", "year"], verify_integrity=True).sort_index()
    tb_pop = tb_pop.underscore().set_index(["indicator", "gwno", "year"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb_gdp, tb_gdp_pc, tb_pop], check_variables_metadata=True)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
