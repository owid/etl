"""Garden step for UN World Urbanization Prospects (National Definitions)."""

import owid.catalog.processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    ds_meadow = paths.load_dataset("wup_national_definitions")
    tb = ds_meadow.read("wup_urban_rural_population")

    ds_hyde = paths.load_dataset("all_indicators")
    tb_hyde = ds_hyde.read("all_indicators")[["country", "year", "urbc_c_share", "urbc_c", "rurc_c"]]

    #
    # Process data.
    #

    # Harmonize country names.
    tb = paths.regions.harmonize_names(tb=tb)

    # After harmonization, some countries have duplicates (different source names map to same target).
    # Verify that duplicate values are identical before dropping.
    duplicates = tb[tb.duplicated(subset=["country", "year", "area_type"], keep=False)]

    if len(duplicates) > 0:
        value_cols = ["population", "share", "growth_rate", "share_growth_rate"]
        for col in value_cols:
            max_unique = duplicates.groupby(["country", "year", "area_type"])[col].nunique().max()
            if max_unique > 1:
                raise ValueError(f"Duplicate rows have different values in column '{col}'!")

    tb = tb.drop_duplicates(subset=["country", "year", "area_type"], keep="first")
    tb = tb.drop(columns="loc_id")

    # Add data_type dimension (estimates vs projections).
    tb["data_type"] = tb["year"].apply(lambda y: "estimates" if y <= 2025 else "projections")

    # Build combined indicators: HYDE for years before UN national definitions start, UN estimates thereafter (no projections).
    un_estimates = tb[tb["data_type"] == "estimates"]
    min_un_year = int(un_estimates["year"].min())
    tb_hyde_pre = tb_hyde[tb_hyde["year"] < min_un_year].copy()

    # Urban share
    tb_un_share = un_estimates[un_estimates["area_type"] == "urban"][["country", "year", "share"]].copy()
    tb_hyde_share = tb_hyde_pre[["country", "year", "urbc_c_share"]].rename(columns={"urbc_c_share": "share"})
    tb_share = pr.concat([tb_hyde_share, tb_un_share], ignore_index=True)

    # Urban population (UN is in thousands; multiply by 1000 to match HYDE which is in people)
    tb_un_urban = un_estimates[un_estimates["area_type"] == "urban"][["country", "year", "population"]].copy()
    tb_un_urban["population"] = tb_un_urban["population"] * 1000
    tb_hyde_urban = tb_hyde_pre[["country", "year", "urbc_c"]].rename(columns={"urbc_c": "population"})
    tb_urban_pop = pr.concat([tb_hyde_urban, tb_un_urban], ignore_index=True)

    # Rural population
    tb_un_rural = un_estimates[un_estimates["area_type"] == "rural"][["country", "year", "population"]].copy()
    tb_un_rural["population"] = tb_un_rural["population"] * 1000
    tb_hyde_rural = tb_hyde_pre[["country", "year", "rurc_c"]].rename(columns={"rurc_c": "population"})
    tb_rural_pop = pr.concat([tb_hyde_rural, tb_un_rural], ignore_index=True)

    # Merge into a single combined table
    tb_combined = pr.merge(
        tb_share, tb_urban_pop.rename(columns={"population": "urban_population"}), on=["country", "year"], how="outer"
    )
    tb_combined = pr.merge(
        tb_combined,
        tb_rural_pop.rename(columns={"population": "rural_population"}),
        on=["country", "year"],
        how="outer",
    )
    tb_combined = tb_combined.format(["country", "year"], short_name="urban_share_with_hyde")

    tb = tb.format(["country", "year", "area_type", "data_type"])

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=[tb, tb_combined], default_metadata=ds_meadow.metadata)
    ds_garden.save()
