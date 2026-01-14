"""Load a meadow dataset and create a garden dataset."""

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


RENAME_COLUMNS = {
    "MG_INTERNAL_DISP_PERS: Internally displaced persons (IDPs): POP_CONF_VIOLENCE: Share due to conflict and violence (PS: Persons)": "idps_under_18_conflict_violence",
    "MG_INTERNAL_DISP_PERS: Internally displaced persons (IDPs): POP_DISASTER: Share due to disaster (PS: Persons)": "idps_under_18_disaster",
    "MG_INTERNAL_DISP_PERS: Internally displaced persons (IDPs): _T: Total (PS: Persons)": "idps_under_18_total",
    # "MG_INTNL_MG_CNTRY_DEST: International migrants, by country of destination: _T: Total (PS: Persons)": "international_migrants_under_18_dest", # removed in 2025 update by UNICEF, kept for reference
    "MG_NEW_INTERNAL_DISP: New internal displacements: POP_CONF_VIOLENCE: Share due to conflict and violence (NUMBER: Number)": "new_idps_under_18_conflict_violence",
    "MG_NEW_INTERNAL_DISP: New internal displacements: POP_DISASTER: Share due to disaster (NUMBER: Number)": "new_idps_under_18_disaster",
    "MG_NEW_INTERNAL_DISP: New internal displacements: _T: Total (NUMBER: Number)": "new_idps_under_18_total",
    "MG_RFGS_CNTRY_ASYLM: Refugees, by country of asylum: _T: Total (PS: Persons)": "refugees_under_18_asylum",
    "MG_RFGS_CNTRY_ORIGIN: Refugees, by country of origin: _T: Total (PS: Persons)": "refugees_under_18_origin",
    "MG_UNRWA_RFGS_CNTRY_ASYLM: Refugees under UNRWA mandate, by host country: _T: Total (PS: Persons)": "refugees_under_18_unrwa_asylum",
}

ORIGINS = {
    "United Nations High Commissioner for Refugees, Global Trends: Forced Displacement in 2023. UNHCR, 2024.": {
        "attribution": "United Nations High Commissioner for Refugees via UNICEF (2024)",
        "attribution_short": "UNHCR",
        "citation_full": "United Nations High Commissioner for Refugees, Global Trends: Forced Displacement in 2023. UNHCR, 2024. Cited via UNICEF.",
        "date_published": "2024",
    },
    "Internal Displacement Monitoring Centre, Global Internal Displacement Database (GIDD), IDMC, 2024.": {
        "attribution": "Internal Displacement Monitoring Centre via UNICEF (2024)",
        "attribution_short": "IDMC",
        "citation_full": "Internal Displacement Monitoring Centre, Global Internal Displacement Database (GIDD), IDMC, 2024. Cited via UNICEF.",
        "date_published": "2024",
    },
    "The United Nations Relief and Works Agency for Palestine Refugees. UNRWA, 2024": {
        "attribution": "The United Nations Relief and Works Agency for Palestine Refugees via UNICEF (2024)",
        "attribution_short": "UNRWA",
        "citation_full": "The United Nations Relief and Works Agency for Palestine Refugees, UNRWA, 2024. Cited via UNICEF.",
        "date_published": "2024",
    },
}


def run() -> None:
    # Load inputs.
    #
    # Load meadow dataset.

    ds_meadow = paths.load_dataset("child_migration")
    ds_population = paths.load_dataset("population")

    # Read table from meadow dataset.
    tb = ds_meadow.read("child_migration")

    # combine indicator, statistical population and unit columns (to get one indicator per combination)

    tb["indicator"] = (
        tb["indicator"].astype(str) + ": " + tb["stat_pop"].astype(str) + " (" + tb["unit"].astype(str) + ")"
    )

    sources = get_sources_per_indicator(tb)

    # pivot table to get seperate columns per asylum and origin
    tb = tb.pivot(
        index=["country", "year"],
        columns="indicator",
        values="value",
    ).reset_index()

    # filter on relevant columns

    # rename columns
    tb = tb.rename(columns=RENAME_COLUMNS, errors="raise")

    #
    # Process data.
    #
    # Harmonize country names.
    tb["country"] = tb["country"].str.split(":").str[1].str.strip()
    tb = paths.regions.harmonize_names(tb=tb)

    # calculate shares per population
    tb = geo.add_population_to_table(tb, ds_population, warn_on_missing_countries=False)

    tb = calculate_shares(tb)

    # drop population column
    tb = tb.drop(columns=["population"])

    # update origins for metadata (to add in original source)
    tb = overwrite_origins(tb, sources)

    # drop duplicated (aggregated rows show up more than once)
    tb = tb.drop_duplicates()

    # Improve table format.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()


def get_sources_per_indicator(tb):
    sources = {}
    for indicator in tb["indicator"].unique():
        subset = tb[tb["indicator"] == indicator]
        data_sources = subset["data_source"].unique()
        sources[indicator] = data_sources.tolist()
    return sources


def calculate_shares(tb):
    tb["refugees_under_18_asylum_per_1000"] = tb["refugees_under_18_asylum"] / tb["population"] * 1000
    tb["refugees_under_18_origin_per_1000"] = tb["refugees_under_18_origin"] / tb["population"] * 1000

    tb["idps_under_18_total_per_1000"] = tb["idps_under_18_total"] / tb["population"] * 1000
    tb["new_idps_under_18_total_per_1000"] = tb["new_idps_under_18_total"] / tb["population"] * 1000

    tb["idps_under_18_conflict_violence_per_1000"] = tb["idps_under_18_conflict_violence"] / tb["population"] * 1000
    tb["idps_under_18_disaster_per_1000"] = tb["idps_under_18_disaster"] / tb["population"] * 1000

    tb["new_idps_under_18_conflict_violence_per_1000"] = (
        tb["new_idps_under_18_conflict_violence"] / tb["population"] * 1000
    )
    tb["new_idps_under_18_disaster_per_1000"] = tb["new_idps_under_18_disaster"] / tb["population"] * 1000

    return tb


def overwrite_origins(tb, sources):
    indicator_cols = [col for col in tb.columns if col not in ["country", "year"]]
    for col in indicator_cols:
        # remove per 1000 to get original indicator name
        if col.endswith("_per_1000"):
            col = col[: -len("_per_1000")]
        # get source(s) for this indicator
        s_col = sources[[key for key, val in RENAME_COLUMNS.items() if val == col][0]]
        assert len(s_col) == 1, f"Multiple sources found for indicator {col}: {s_col}"
        src_key = s_col[0]
        tb[col].metadata.origins[0].attribution = ORIGINS[src_key]["attribution"]
        tb[col].metadata.origins[0].citation_full = ORIGINS[src_key]["citation_full"]
        tb[col].metadata.origins[0].date_published = ORIGINS[src_key]["date_published"]
        tb[col].metadata.origins[0].attribution_short = ORIGINS[src_key]["attribution_short"]
    return tb
