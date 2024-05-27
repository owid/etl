"""TODO: Explain this step.

"""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load Harris et al. (2015) dataset and read its main table.
    ds_harris = paths.load_dataset("harris_et_al_2015")
    tb_harris = ds_harris["harris_et_al_2015"].reset_index()

    # Load Floud et al. (2011) dataset and read its main table.
    ds_floud = paths.load_dataset("floud_et_al_2011")
    tb_floud = ds_floud["floud_et_al_2011"].reset_index()

    # Load Jonsson (1998) dataset and read its main table.
    ds_jonsson = paths.load_dataset("jonsson_1998")
    tb_jonsson = ds_jonsson["jonsson_1998"].reset_index()

    # Load Grigg (1995) dataset and read its main table.
    ds_grigg = paths.load_dataset("grigg_1995")
    tb_grigg = ds_grigg["grigg_1995"].reset_index()

    # Load Fogel (2004) dataset and read its main table.
    ds_fogel = paths.load_dataset("fogel_2004")
    tb_fogel = ds_fogel["fogel_2004"].reset_index()

    # Load FAO (2000) dataset and read its main table.
    ds_fao = paths.load_dataset("fao_2000")
    tb_fao = ds_fao["fao_2000"].reset_index()

    #
    # Process data.
    #
    # TODO: Continue processing.
    tb = tb_grigg.copy()

    # Set an appropriate index and sort conveniently.
    tb = tb.format(short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    ds_garden.save()
