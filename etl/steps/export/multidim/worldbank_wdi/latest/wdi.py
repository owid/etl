"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Default collection config
    config = paths.load_collection_config()

    # # Load grapher dataset.
    # ds = paths.load_dataset("wdi")
    # tb = ds.read("wdi", load_data=False)

    #
    # Create collection object
    #
    c = paths.create_collection(
        config=config,
        short_name="ilostat_national_vs_modeled",
        # tb=tb,
        # indicator_names=[
        #     "sl_uem_totl_ne_zs",
        #     "sl_uem_totl_zs",
        #     "sl_tlf_cact_ne_zs",
        #     "sl_tlf_cact_zs",
        #     "sl_emp_totl_sp_ne_zs",
        #     "sl_emp_totl_sp_zs",
        # ],
    )

    #
    # Save garden dataset.
    #
    c.save()
