"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder, create_dataset, grapher_checks

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

VARS_TO_KEEP = [
    "rgdpe_pc",
    "rgdpo_pc",
    "cgdpe_pc",
    "cgdpo_pc",
    "rgdpna_pc",
    "rgdpe",
    "rgdpo",
    "cgdpe",
    "cgdpo",
    "rgdpna",
    "avh",
    "emp",
    "productivity",
    "labsh",
    "csh_c",
    "csh_i",
    "csh_g",
    "csh_x",
    "csh_m",
    "csh_r",
    "ccon",
    "cda",
    "cn",
    "rconna",
    "rdana",
    "rnna",
    "irr",
    "delta",
    "pop",
    "trade_openness",
    "rtfpna",
]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("penn_world_table")

    # Read table from garden dataset.
    tb = ds_garden["penn_world_table"]

    #
    # Process data.
    #
    # Select country, year and only those variables with metadata specified
    # in the metadata sheet.
    tb = tb.loc[:, [col for col in tb.columns if col in ["country", "year"] + VARS_TO_KEEP]]

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(dest_dir, tables=[tb], default_metadata=ds_garden.metadata)

    #
    # Checks.
    #
    grapher_checks(ds_grapher)

    # Save changes in the new grapher dataset.
    ds_grapher.save()
