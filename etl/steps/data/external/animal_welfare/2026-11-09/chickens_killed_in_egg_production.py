"""Create the data used in the technical publication "How many chickens are killed in egg production?".

The publication is the notebook docs/analyses/chickens_killed_in_egg_production/chickens_killed_in_egg_production.ipynb,
which loads all its data from this dataset. Some of these tables (from USDA, Defra and Eurostat) are not used in any
chart; listing their steps as dependencies of this step in the dag shows where they are used, so they are not flagged as
unused.
"""

from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# FAOSTAT QCL items used in the publication: meat of chickens, eggs from hens, and eggs from all birds.
QCL_ITEM_CODES = ["00001058", "00001062", "00001783"]
# FAOSTAT QCL elements used in the publication: laying (number of laying hens), producing animals/slaughtered, yield
# (eggs per hen), yield/carcass weight (kilograms per hen), and production (in tonnes, and in number of eggs).
QCL_ELEMENT_CODES = ["005313", "005321", "005413", "005424", "005510", "005513"]
# Columns of the QCL table used in the publication (besides its index columns).
QCL_COLUMNS = ["country", "flag", "value"]


def run() -> None:
    #
    # Load inputs.
    #
    # Load FAOSTAT QCL dataset and read its main (long) table.
    ds_qcl = paths.load_dataset("faostat_qcl")
    tb_qcl = ds_qcl.read("faostat_qcl", safe_types=False, reset_index=False)

    # Load the other tables used in the publication: our estimates, the figures from USDA, Defra and Eurostat, and the
    # latest income groups.
    tables = [
        paths.load_dataset(dataset_name).read(table_name, reset_index=False)
        for dataset_name, table_name in [
            ("chickens_killed_for_eggs", "chickens_killed_for_eggs"),
            ("us_laying_hens_and_chicks", "us_laying_hens_and_chicks"),
            ("uk_laying_hens_and_chicks", "uk_laying_hens_and_chicks"),
            ("laying_hen_chicks", "laying_hen_chicks"),
            ("income_groups", "income_groups_latest"),
        ]
    ]

    #
    # Process data.
    #
    # Keep only the QCL items, elements and columns used in the publication.
    item_code = tb_qcl.index.get_level_values("item_code")
    element_code = tb_qcl.index.get_level_values("element_code")
    tb_qcl = tb_qcl[item_code.isin(QCL_ITEM_CODES) & element_code.isin(QCL_ELEMENT_CODES)][QCL_COLUMNS]

    # Run sanity checks on outputs.
    sanity_check_outputs(tb_qcl=tb_qcl)

    #
    # Save outputs.
    #
    # Create a new external dataset.
    ds = paths.create_dataset(tables=[tb_qcl] + tables)

    # Save changes in the new external dataset.
    ds.save()


def sanity_check_outputs(tb_qcl: Table) -> None:
    # Every item and element should still be in the QCL data.
    error = "Some QCL items or elements used in the publication are missing."
    assert set(tb_qcl.index.get_level_values("item_code")) == set(QCL_ITEM_CODES), error
    assert set(tb_qcl.index.get_level_values("element_code")) == set(QCL_ELEMENT_CODES), error
