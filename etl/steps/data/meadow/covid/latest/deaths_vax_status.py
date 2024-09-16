"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve tables from snapshots
    tb_en = paths.read_snap_table("deaths_vax_status_england.csv")
    tb_us = paths.read_snap_table("deaths_vax_status_us.csv")
    tb_swi = paths.read_snap_table("deaths_vax_status_switzerland.csv")
    tb_cl = paths.read_snap_table("deaths_vax_status_chile.csv")

    #
    # Process data.
    #
    # US
    rename_cols = {
        "Entity": "entity",
        "Day": "date",
        "unvaccinated": "us_unvaccinated",
        "vaccinated_without": "us_vaccinated_no_biv_booster",
        "vaccinated_with": "us_vaccinated_with_biv_booster",
    }
    tb_us = tb_us.rename(columns=rename_cols)[rename_cols.values()]
    tb_us = tb_us.format(["entity", "date"], short_name="us")

    # England
    rename_cols = {
        "Entity": "entity",
        "Day": "date",
        "Unvaccinated": "england_unvaccinated",
        "Fully vaccinated": "england_fully_vaccinated",
    }
    tb_en = tb_en.rename(columns=rename_cols)[rename_cols.values()]
    tb_en = tb_en.format(["entity", "date"], short_name="england")

    # Switzerland
    rename_cols = {
        "Entity": "entity",
        "Day": "date",
        "Unvaccinated": "swi_unvaccinated",
        "Fully vaccinated, no booster": "swi_vaccinated_no_booster",
        "Fully vaccinated + booster": "swi_vaccinated_with_booster",
    }
    tb_swi = tb_swi.rename(columns=rename_cols)[rename_cols.values()]
    tb_swi = tb_swi.format(["entity", "date"], short_name="switzerland")

    # Chile
    rename_cols = {
        "Entity": "entity",
        "Day": "date",
        "0 or 1 dose": "chile_0_1_dose",
        "2 doses": "chile_2_doses",
        "3 doses": "chile_3_doses",
        "4 doses": "chile_4_doses",
    }
    tb_cl = tb_cl.rename(columns=rename_cols)[rename_cols.values()]
    tb_cl = tb_cl.format(["entity", "date"], short_name="chile")

    # Table list
    tables = [
        tb_us,
        tb_en,
        tb_cl,
        tb_swi,
    ]

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(
        dest_dir,
        tables=tables,
        check_variables_metadata=True,
    )

    # Save changes in the new meadow dataset.
    ds_meadow.save()
