"""Load a meadow dataset and create a garden dataset."""

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

REGIONS = [reg for reg in geo.REGIONS.keys() if reg != "European Union (27)"]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("knomad")
    ds_regions = paths.load_dataset("regions")
    ds_income_groups = paths.load_dataset("income_groups")

    # Read table from meadow dataset.
    tb = ds_meadow.read("knomad")

    # Filter only on bilateral remittances
    tb = tb[tb["indicator_id"] == "WB.KNOMAD.BRE"]

    # Rename and drop columns
    tb = tb.drop(columns=["indicator_id", "economy_iso3", "indicator"])
    tb = tb.rename(columns={"value": "remittance_flows"})
    tb["remittance_flows"] = tb["remittance_flows"].astype(float) * 1e6  # in million USD

    # filter on 2021 as that is the year we have data for
    tb = tb[tb["year"] == 2021]

    #
    # Harmonize country names
    #
    tb = geo.harmonize_countries(
        df=tb, country_col="country_origin", countries_file=paths.country_mapping_path, warn_on_unused_countries=False
    )
    tb = geo.harmonize_countries(
        df=tb,
        country_col="country_receiving",
        countries_file=paths.country_mapping_path,
        warn_on_unused_countries=False,
    )

    # aggregate over countries:
    tb = geo.add_regions_to_table(
        tb=tb,
        ds_regions=ds_regions,
        ds_income_groups=ds_income_groups,
        regions=REGIONS,
        frac_allowed_nans_per_year=0.1,
        index_columns=["country_receiving", "country_origin", "year"],
        country_col="country_origin",
    )

    tb = geo.add_regions_to_table(
        tb=tb,
        ds_regions=ds_regions,
        ds_income_groups=ds_income_groups,
        regions=REGIONS,
        frac_allowed_nans_per_year=0.1,
        index_columns=["country_receiving", "country_origin", "year"],
        country_col="country_receiving",
    )

    tb = tb.format(["country_origin", "country_receiving", "year"], short_name="bilateral_remittance")

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
