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
    ds_meadow = paths.load_dataset("hmd")

    # Read table from meadow dataset.
    tb_lt = ds_meadow["life_tables"].reset_index()
    tb_ex = ds_meadow["exposures"].reset_index()

    #
    # Process data.
    #
    # Standardise dimension values
    tb_lt["sex"] = tb_lt["sex"].map(
        {
            "Males": "male",
            "Females": "female",
            "Total": "both",
        }
    )
    tb_ex["sex"] = tb_ex["sex"].map(
        {
            "Male": "male",
            "Female": "female",
            "Total": "both",
        }
    )
    # Sanity checks
    columns_dim = ["format", "type", "sex", "age"]
    for col in columns_dim:
        not_in_ex = set(tb_lt[col]) - set(tb_ex[col])
        not_in_lt = set(tb_ex[col]) - set(tb_lt[col])
        assert not not_in_lt, f"Found values in column {col} in exposures but not in life tables!"
        assert not not_in_ex, f"Found values in column {col} in life tables but not in exposures!"

    # Harmonise countries
    tb_lt = geo.harmonize_countries(
        df=tb_lt, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )
    tb_ex = geo.harmonize_countries(
        df=tb_ex, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )
    # Combine
    columns_primary = ["format", "type", "country", "year", "sex", "age"]
    tb = tb_lt.merge(tb_ex, on=columns_primary, how="outer")
    # Short name
    tb.metadata.short_name = paths.short_name
    # Set index
    tb = tb.set_index(columns_primary, verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
