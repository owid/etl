"""Load a meadow dataset and create a garden dataset."""

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

RENAME_COLUMNS = {
    "MG_INTERNAL_DISP_PERS: Internally displaced persons (IDPs): POP_CONF_VIOLENCE: Share due to conflict and violence (PS: Persons)": "idps_under_18_conflict_violence",
    "MG_INTERNAL_DISP_PERS: Internally displaced persons (IDPs): POP_DISASTER: Share due to disaster (PS: Persons)": "idps_under_18_disaster",
    "MG_INTERNAL_DISP_PERS: Internally displaced persons (IDPs): _T: Total (PS: Persons)": "idps_under_18_total",
    "MG_INTNL_MG_CNTRY_DEST: International migrants, by country of destination: _T: Total (PS: Persons)": "international_migrants_under_18_dest",
    "MG_NEW_INTERNAL_DISP: New internal displacements: POP_CONF_VIOLENCE: Share due to conflict and violence (NUMBER: Number)": "new_idps_under_18_conflict_violence",
    "MG_NEW_INTERNAL_DISP: New internal displacements: POP_DISASTER: Share due to disaster (NUMBER: Number)": "new_idps_under_18_disaster",
    "MG_NEW_INTERNAL_DISP: New internal displacements: _T: Total (NUMBER: Number)": "new_idps_under_18_total",
    "MG_RFGS_CNTRY_ASYLM: Refugees, by country of asylum: _T: Total (PS: Persons)": "refugees_under_18_asylum",
    "MG_RFGS_CNTRY_ORIGIN: Refugees, by country of origin: _T: Total (PS: Persons)": "refugees_under_18_origin",
    "MG_UNRWA_RFGS_CNTRY_ASYLM: Refugees under UNRWA mandate, by host country: _T: Total (PS: Persons)": "refugees_under_18_unrwa_asylum",
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("child_migration")
    ds_population = paths.load_dataset("population")

    # Read table from meadow dataset.
    tb = ds_meadow.read_table("child_migration")

    # combine indicator, statistical population and unit columns (to get one indicator per combination)

    tb["indicator"] = (
        tb["indicator"].astype(str) + ": " + tb["stat_pop"].astype(str) + " (" + tb["unit"].astype(str) + ")"
    )

    # pivot table to get seperate columns per asylum and origin
    tb = tb.pivot(
        index=["country", "year"],
        columns="indicator",
        values="value",
    ).reset_index()

    # rename columns
    tb = tb.rename(columns=RENAME_COLUMNS, errors="raise")
    # harmonize countries (Exclude aggregates as there are minimal data points available for aggregates and UNICEF regions are often duplicates)
    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
        excluded_countries_file=paths.excluded_countries_path,
        warn_on_missing_countries=True,
    )

    # calculate shares per population
    tb = geo.add_population_to_table(tb, ds_population)

    tb["refugees_under_18_asylum_per_1000"] = tb["refugees_under_18_asylum"] / tb["population"] * 1000
    tb["refugees_under_18_origin_per_1000"] = tb["refugees_under_18_origin"] / tb["population"] * 1000

    tb["migrants_under_18_dest_per_1000"] = tb["international_migrants_under_18_dest"] / tb["population"] * 1000

    tb["idps_under_18_total_per_1000"] = tb["idps_under_18_total"] / tb["population"] * 1000
    tb["new_idps_under_18_total_per_1000"] = tb["new_idps_under_18_total"] / tb["population"] * 1000

    tb["idps_under_18_conflict_violence_per_1000"] = tb["idps_under_18_conflict_violence"] / tb["population"] * 1000
    tb["idps_under_18_disaster_per_1000"] = tb["idps_under_18_disaster"] / tb["population"] * 1000

    tb["new_idps_under_18_conflict_violence_per_1000"] = (
        tb["new_idps_under_18_conflict_violence"] / tb["population"] * 1000
    )
    tb["new_idps_under_18_disaster_per_1000"] = tb["new_idps_under_18_disaster"] / tb["population"] * 1000

    # drop population column
    tb = tb.drop(columns=["population"])

    # drop duplicated (aggregated rows show up more than once)
    tb = tb.drop_duplicates()

    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
