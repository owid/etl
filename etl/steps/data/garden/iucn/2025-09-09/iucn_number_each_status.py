"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("iucn_number_each_status")

    # Read table from meadow dataset.
    tb = ds_meadow.read("iucn_number_each_status")
    tb = group_classes(tb)
    tb = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata)

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
        "Myxini": "Fishes",
        "Petromyzonti": "Fishes",
    }

    molluscs_groups = {
        "Bivalvia": "Molluscs",
        "Cephalopoda": "Molluscs",
        "Gastropoda": "Molluscs",
        "Monoplacophora": "Molluscs",
        "Polyplacophora": "Molluscs",
        "Solenogastres": "Molluscs",
    }

    plant_groups = {
        "Andreaeopsida": "Plants",
        "Anthocerotopsida": "Plants",
        "Bryopsida": "Plants",
        "Bryopsidophyceae": "Plants",
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
        "Charophyaceae": "Plants",
        "Marchantiopsida": "Plants",
    }
    crustaceans_groups = {
        "Malacostraca": "Crustaceans",
        "Ostracoda": "Crustaceans",
        "Hexanauplia": "Crustaceans",
        "Maxillopoda": "Crustaceans",
    }

    fungi_groups = {
        "Exobasidiomycetes": "Fungi",
        "Geoglossomycetes": "Fungi",
        "Arthoniomycetes": "Fungi",
        "Pezizomycetes": "Fungi",
        "Eurotiomycetes": "Fungi",
        "Leotiomycetes": "Fungi",
        "Dacrymycetes": "Fungi",
        "Ustilaginomycetes": "Fungi",
        "Dothideomycetes": "Fungi",
        "Agaricomycetes": "Fungi",
        "Sordariomycetes": "Fungi",
        "Wallemiomycetes": "Fungi",
        "Lecanoromycetes": "Fungi",
    }

    # Combine all group mappings for validation
    all_groups = {**fish_groups, **molluscs_groups, **plant_groups, **crustaceans_groups, **fungi_groups}

    # Check if any expected class names are missing from the data
    unique_classes = set(tb["country"].unique())
    expected_classes = set(all_groups.keys())
    classes_in_data = expected_classes.intersection(unique_classes)
    missing_classes = expected_classes - unique_classes

    if missing_classes:
        print(f"Warning: Expected classes not found in data: {missing_classes}")

    # Replace the class names with broader categories
    tb["country"] = tb["country"].replace(all_groups)

    tb = tb.groupby(["country", "year"]).sum().reset_index()

    return tb
