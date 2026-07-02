"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    # Load meadow dataset and table.
    ds_meadow = paths.load_dataset("postnatal_care")
    tb = ds_meadow["postnatal_care"].reset_index()

    # Harmonize country names (and drop excluded countries).
    tb = paths.regions.harmonize_names(
        tb,
        country_col="country",
        countries_file=paths.country_mapping_path,
        excluded_countries_file=paths.excluded_countries_path,
    )

    # Drop rows without postnatal care coverage data and round values.
    tb = tb.dropna(subset=["postnatal_care_coverage"])
    tb["postnatal_care_coverage"] = tb["postnatal_care_coverage"].astype(float).round(2)

    tb = tb.format(["country", "year"], short_name=paths.short_name)

    # Save garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)
    ds_garden.save()
