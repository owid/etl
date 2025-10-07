"""Garden step that combines OECD family database sources into a single dataset."""

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow datasets
    ds_marriage_divorce = paths.load_dataset("marriage_divorce_rates")
    ds_births_outside_marriage = paths.load_dataset("births_outside_marriage")
    ds_children_in_families = paths.load_dataset("children_in_families")

    # Get tables from each dataset
    tb_marriage_divorce = ds_marriage_divorce.read("marriage_divorce_rates")
    tb_births_outside_marriage = ds_births_outside_marriage.read("births_outside_marriage")
    tb_children_in_families = ds_children_in_families.read("children_in_families")

    #
    # Process data.
    #

    # Harmonize country names for all tables
    tb_marriage_divorce = geo.harmonize_countries(
        tb_marriage_divorce, countries_file=paths.country_mapping_path, warn_on_unused_countries=False
    )
    tb_births_outside_marriage = geo.harmonize_countries(
        tb_births_outside_marriage, countries_file=paths.country_mapping_path, warn_on_unused_countries=False
    )
    tb_children_in_families = geo.harmonize_countries(
        tb_children_in_families, countries_file=paths.country_mapping_path, warn_on_unused_countries=False
    )

    #
    # Save outputs.
    #
    # Create a new garden dataset with multiple tables
    tables = [
        tb_marriage_divorce.format(["country", "year", "gender", "indicator"]),
        tb_births_outside_marriage.format(["country", "year"]),
        tb_children_in_families.format(["country", "year", "indicator"]),
    ]
    ds_garden = create_dataset(dest_dir, tables=tables, check_variables_metadata=True)

    # Save the dataset
    ds_garden.save()
