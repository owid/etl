"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("iucn_number_each_status")

    # Read table from meadow dataset.
    tb = ds_meadow.read("iucn_number_each_status")
    tb = group_classes(tb)
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def group_classes(tb: Table) -> Table:
    """
    Aggregate given groups into broader, more widely known categories.

    """

    fish_groups = {
        "Actinopterygii": "Fishes",
        "Chondrichthyes": "Fishes",
        "Sarcopterygii": "Fishes",
        "Cephalaspidomorphi": "Fishes",
        "Myxini": "Fishes",
        "Petromyzontida": "Fishes",
    }

    molluscs_groups = {
        "Bivalvia": "Molluscs",
        "Cephalopoda": "Molluscs",
        "Gastropoda": "Molluscs",
        "Monoplacophora": "Molluscs",
        "Polyplacophora": "Molluscs",
        "Solengastres": "Molluscs",
    }

    plant_groups = {
        "Andreaeopsida": "Plants",
        "Anthocerotopsida": "Plants",
        "Bryopsida": "Plants",
        "Bryopsidophyceae": "Plants",
        "Charophyceae": "Plants",
        "Chlorophyceae": "Plants",
        "Cycadopsida": "Plants",
        "Florideophyceae": "Plants",
        "Ginkgoopsida": "Plants",
        "Gnetopsida": "Plants",
        "Jungermanniopsida": "Plants",
        "Liliopsida": "Plants",
        "Lycopodiopsida": "Plants",
        "Magnoliopsida": "Plants",
        "Pinopsida": "Plants",
        "Polypodiopsida": "Plants",
        "Polytrichopsida": "Plants",
        "Sphagnopsida": "Plants",
        "Takakiopsida": "Plants",
        "Ulvophyceae": "Plants",
    }
    crustaceans_groups = {
        "Malacostraca": "Crustaceans",
        "Ostracoda": "Crustaceans",
        "Hexanauplia": "Crustaceans",
        "Maxillopoda": "Crustaceans",
        "Brachiopoda": "Crustaceans",
    }

    # Replace the class names with the broader fish categories
    tb["country"] = tb["country"].replace(fish_groups)
    # Replace the class names with the broader molluscs categories
    tb["country"] = tb["country"].replace(molluscs_groups)
    # Replace the class names with the broader plant categories
    tb["country"] = tb["country"].replace(plant_groups)
    # Replace the class names with the broader crustaceans categories
    tb["country"] = tb["country"].replace(crustaceans_groups)

    tb = tb.groupby(["country", "year"]).sum().reset_index()

    return tb
