"""Load a garden dataset and create a grapher dataset."""

import owid.catalog.processing as pr

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("urban_agglomerations_largest_cities")

    # Read table from garden dataset.
    tb = ds_garden["urban_agglomerations_largest_cities"].reset_index()

    tb_capitals = tb[["country", "year", "population_capital", "rank_order", "urban_agglomeration"]][
        tb["rank_order"] == "Capital"
    ]

    # Exclude irrelevant columns for the grapher
    tb_capitals = tb_capitals.drop(columns=["rank_order"])

    # Set the index to "country" and "year"
    tb_capitals = tb_capitals.set_index(["country", "year"])

    # Define the administrative capitals
    admin_capitals = {
        "Benin": "Cotonou",
        "Bolivia": "La Paz",
        "Channel Islands": "St. Helier",
        "Cote d'Ivoire": "Abidjan",
        "Netherlands": "Amsterdam",
        "South Africa": "Pretoria",
        "Sri Lanka": "Colombo",
    }

    # Identify rows with non-unique index
    duplicated_indices = tb_capitals.index.duplicated(keep=False)

    # Create a mask for rows that are both duplicated and have an admin capital
    mask = duplicated_indices & tb_capitals["urban_agglomeration"].isin(admin_capitals.values())

    # Filter only the duplicated rows to keep only admin capitals
    filtered_duplicated_rows = tb_capitals[mask]
    tb_without_duplicates = tb_capitals[~duplicated_indices].reset_index()
    filtered_duplicated_rows = filtered_duplicated_rows.reset_index()

    # Use pandas.concat to combine non-duplicated and filtered duplicated rows
    tb_capitals = pr.concat([tb_without_duplicates, filtered_duplicated_rows])

    tb_capitals = tb_capitals.set_index(["country", "year"], verify_integrity=True)
    tb_capitals = tb_capitals.drop(columns=["urban_agglomeration"])
    tb_capitals[
        "population_capital"
    ].metadata.description_processing = "The dataset was filtered to retain only the administrative capitals Cotonou (Benin), La Paz (Bolivia), St. Helier (Channel Islands), Abidjan (Cote d'Ivoire), Amsterdam (Netherlands), Pretoria (South Africa), and Colombo (Sri Lanka)."

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(
        dest_dir, tables=[tb_capitals], check_variables_metadata=True, default_metadata=ds_garden.metadata
    )

    ds_grapher.metadata.title = "World Urbanization Prospects Dataset - Population of the capital city"

    # Save changes in the new grapher dataset.
    ds_grapher.save()
