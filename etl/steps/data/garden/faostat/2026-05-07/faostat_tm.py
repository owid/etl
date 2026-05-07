"""FAOSTAT garden step for the Detailed Trade Matrix (faostat_tm)."""

from etl.helpers import PathFinder

paths = PathFinder(__file__)

# Map raw meadow column names → cleaner garden names.
COLUMN_RENAMES = {
    "reporter_countries": "reporter_country",
    "partner_countries": "partner_country",
}

# Final bilateral index for the garden table.
INDEX_COLUMNS = [
    "reporter_country",
    "partner_country",
    "item_code",
    "element_code",
    "year",
]

# Flag definitions.
FLAGS = {
    "A": "Official figure",
    "X": "Figure from external organization",
    "E": "Estimated value",
    "I": "Value imputed by a receiving agency",
}


def run() -> None:
    #
    # Load data.
    #
    # Use `safe_types=False` to save time and memory.
    ds_meadow = paths.load_dataset("faostat_tm")
    tb = ds_meadow.read("faostat_tm", safe_types=False)

    #
    # Process data.
    #
    # Rename columns conveniently.
    tb = tb.rename(columns=COLUMN_RENAMES, errors="raise")

    # Harmonize reporter and partner country names.
    # paths.regions.harmonizer(tb=tb, country_col="partner_country", institution="FAO")
    tb = paths.regions.harmonize_names(tb=tb, country_col="reporter_country", warn_on_unused_countries=False)
    tb = paths.regions.harmonize_names(tb=tb, country_col="partner_country", warn_on_unused_countries=False)

    # TODO: Add sanity checks (e.g. that reported imports/exports don't differ dramatically in both directions).
    def sanity_check_inputs(tb):
        missing_flags = set(tb["flag"].cat.categories) - set(FLAGS)
        assert not missing_flags, f"Missing flag definitions: {sorted(missing_flags)}"

    # Sanity check inputs.
    sanity_check_inputs(tb=tb)

    # Map flags.
    tb["flag"] = tb["flag"].cat.rename_categories(FLAGS)
    tb["flag"] = tb["flag"].copy_metadata(tb["value"])

    # Improve table format.
    tb = tb.format(keys=INDEX_COLUMNS)

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=[tb])
    ds_garden.save()
