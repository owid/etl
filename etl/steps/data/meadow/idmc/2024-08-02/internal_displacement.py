"""Load a snapshot and create meadow dataset. This is meadow and garden step in one, since data manipulation is minimal."""

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

COLUMNS_TO_DROP = [
    "ISO3",
    "Conflict Stock Displacement (Raw)",
    "Conflict Internal Displacements (Raw)",
    "Disaster Internal Displacements (Raw)",
    "Disaster Stock Displacement (Raw)",
]

COL_RENAMES = {
    "Name": "country",
    "Year": "year",
    "Conflict Stock Displacement": "conflict_stock_displacement",
    "Conflict Internal Displacements": "conflict_internal_displacements",
    "Disaster Internal Displacements": "disaster_internal_displacements",
    "Disaster Stock Displacement": "disaster_stock_displacement",
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("internal_displacement.xlsx")
    ds_pop = paths.load_dataset("population")

    # Load data from snapshot.
    tb = snap.read(safe_types=False)

    # rename and drop columns
    tb = tb.drop(columns=COLUMNS_TO_DROP, errors="raise")
    tb = tb.rename(columns=COL_RENAMES, errors="raise")

    # Harmonize country names.
    tb = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, warn_on_missing_countries=True, warn_on_unused_countries=True
    )

    # calculate population averages
    tb = geo.add_population_to_table(tb, ds_pop)

    tb["total_stock_displacement"] = tb["conflict_stock_displacement"] + tb["disaster_stock_displacement"]
    tb["total_internal_displacements"] = tb["conflict_internal_displacements"] + tb["disaster_internal_displacements"]

    columns_to_calculate = [
        ("share_of_internally_displaced_pop", "total_stock_displacement"),
        ("share_of_conflict_displaced_pop", "conflict_stock_displacement"),
        ("share_of_disaster_displaced_pop", "disaster_stock_displacement"),
        ("displacements_per_1000_people", "total_internal_displacements"),
        ("conflict_displacements_per_1000_people", "conflict_internal_displacements"),
        ("disaster_displacements_per_1000_people", "disaster_internal_displacements"),
    ]

    for new_column, source_column in columns_to_calculate:
        tb[new_column] = (tb[source_column] / tb["population"]) * 1000

    tb = tb.drop(columns=["population"], errors="raise")

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
