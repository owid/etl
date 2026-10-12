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
    ds_garden = paths.load_dataset("ucdp_preview")

    # Read table from garden dataset.
    tb = ds_garden.read("ucdp_preview")

    # Process data.
    #
    # Set index (region names already carry the suffix " (UCDP)")
    tb = tb.format(["year", "country", "conflict_type"])

    # Get country-level data
    tb_participants = add_suffix_to_regions(ds_garden["ucdp_preview_country"])
    tb_locations = add_suffix_to_regions(ds_garden["ucdp_preview_locations"])

    tables = [
        tb,
        tb_participants,
        tb_locations,
    ]

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = paths.create_dataset(
        tables=tables,
        check_variables_metadata=True,
        default_metadata=ds_garden.metadata,
    )

    # Remove source description so that it doesn't get appended to the dataset description.
    # ds_grapher.metadata.sources[0].description = ""

    # Save changes in the new grapher dataset.
    ds_grapher.save()


def add_suffix_to_regions(tb: Table) -> Table:
    """Rename UCDP regions, e.g. "Africa" -> "Africa (UCDP)"."""
    index = tb.index.names
    tb = tb.reset_index()
    tb["country"] = tb["country"].astype("string").replace({r: f"{r} (UCDP)" for r in REGIONS_UCDP})
    return tb.set_index(index)
