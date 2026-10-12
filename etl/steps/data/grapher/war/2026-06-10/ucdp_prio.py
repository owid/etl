"""Load a garden dataset and create a grapher dataset."""

from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# UCDP world regions. In grapher, they carry the suffix " (UCDP)" (as in the regions dataset), so that they are
# recognized as UCDP regions and not as OWID's.
REGIONS_UCDP = ["Africa", "Americas", "Asia and Oceania", "Europe", "Middle East"]


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("ucdp_prio")

    # Read table from garden dataset.
    tb = ds_garden["ucdp_prio"]
    # tb_country = ds_garden["ucdp_prio_country"]

    #
    # Process data.
    #
    tb = tb.rename_index_names({"region": "country"})
    tb = add_suffix_to_regions(tb)

    #
    # Save outputs.
    #
    tables = [
        tb,
        # tb_country,
    ]
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = paths.create_dataset(
        tables=tables,
        check_variables_metadata=True,
        default_metadata=ds_garden.metadata,
    )

    # Save changes in the new grapher dataset.
    ds_grapher.save()


def add_suffix_to_regions(tb: Table) -> Table:
    """Rename UCDP regions, e.g. "Africa" -> "Africa (UCDP)"."""
    index = tb.index.names
    tb = tb.reset_index()
    tb["country"] = tb["country"].astype("string").replace({r: f"{r} (UCDP)" for r in REGIONS_UCDP})
    return tb.set_index(index)
