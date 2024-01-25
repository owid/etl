"""Load a meadow dataset and create a garden dataset."""

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("urban_agglomerations_size_class")

    # Read table from meadow dataset.
    tb = ds_meadow["urban_agglomerations_size_class"].reset_index()

    #
    # Process data.
    #
    tb = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )

    # Pivot table to have one column per size class of urban settlement
    tb_pivot = tb.pivot(
        index=["country", "year"], columns=["size_class_of_urban_settlement", "type_of_data"], values="value"
    )
    tb_pivot.columns = ["_".join(col).strip() for col in tb_pivot.columns.values]

    # Convert population columns to thousands
    for col in tb_pivot.columns:
        if "_Population" in col:
            tb_pivot[col] = tb_pivot[col] * 1000

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb_pivot], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
