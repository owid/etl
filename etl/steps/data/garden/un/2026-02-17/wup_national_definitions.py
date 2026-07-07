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

    # Convert population from thousands to people (UN source reports in thousands).
    tb["population"] = tb["population"] * 1000

    # Add data_type dimension (estimates vs projections).
    tb["data_type"] = tb["year"].apply(lambda y: "estimates" if y <= 2025 else "projections")

    # Build combined indicators: HYDE for years before UN national definitions start, UN thereafter.
    un_estimates = tb[tb["data_type"] == "estimates"]
    min_un_year = int(un_estimates["year"].min())
    tb_hyde_pre = tb_hyde[tb_hyde["year"] < min_un_year].copy()

    # HYDE components (reused in both combined tables)
    tb_hyde_share = tb_hyde_pre[["country", "year", "urbc_c_share"]].rename(columns={"urbc_c_share": "share"})
    tb_hyde_urban = tb_hyde_pre[["country", "year", "urbc_c"]].rename(columns={"urbc_c": "population"})
    tb_hyde_rural = tb_hyde_pre[["country", "year", "rurc_c"]].rename(columns={"rurc_c": "population"})

    # --- Combined: HYDE + UN estimates only (no projections) ---
    tb_share = pr.concat(
        [tb_hyde_share, un_estimates[un_estimates["area_type"] == "urban"][["country", "year", "share"]].copy()],
        ignore_index=True,
    )
    tb_urban_pop = pr.concat(
        [tb_hyde_urban, un_estimates[un_estimates["area_type"] == "urban"][["country", "year", "population"]].copy()],
        ignore_index=True,
    )
    tb_rural_pop = pr.concat(
        [tb_hyde_rural, un_estimates[un_estimates["area_type"] == "rural"][["country", "year", "population"]].copy()],
        ignore_index=True,
    )

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

    # --- Combined: HYDE + UN estimates + UN projections ---
    tb_share_all = pr.concat(
        [tb_hyde_share, tb[tb["area_type"] == "urban"][["country", "year", "share"]].copy()],
        ignore_index=True,
    )
    tb_urban_pop_all = pr.concat(
        [tb_hyde_urban, tb[tb["area_type"] == "urban"][["country", "year", "population"]].copy()],
        ignore_index=True,
    )
    tb_rural_pop_all = pr.concat(
        [tb_hyde_rural, tb[tb["area_type"] == "rural"][["country", "year", "population"]].copy()],
        ignore_index=True,
    )

    tb_combined_with_proj = pr.merge(
        tb_share_all,
        tb_urban_pop_all.rename(columns={"population": "urban_population"}),
        on=["country", "year"],
        how="outer",
    )
    tb_combined_with_proj = pr.merge(
        tb_combined_with_proj,
        tb_rural_pop_all.rename(columns={"population": "rural_population"}),
        on=["country", "year"],
        how="outer",
    )
    tb_combined_with_proj = tb_combined_with_proj.format(
        ["country", "year"], short_name="urban_share_with_hyde_and_projections"
    )

    tb = tb.format(["country", "year", "area_type", "data_type"])

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(
        tables=[tb, tb_combined, tb_combined_with_proj], default_metadata=ds_meadow.metadata
    )
    ds_garden.save()
