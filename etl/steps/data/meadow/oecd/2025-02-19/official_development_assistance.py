"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define the list of OECD datasets to upload, the file format, the columns to keep and the index columns.
DATASETS = {
    "dac1": {
        "file_name": "Table1_Data.csv",
        "columns": ["Donor", "PART", "Aid type", "Fund flows", "AMOUNTTYPE", "Year", "Value", "Flags"],
        "index": ["donor", "part", "aid_type", "fund_flows", "amounttype", "year"],
    },
    "dac2a": {
        "file_name": "Table2a_Data.csv",
        "columns": ["Recipient", "Donor", "PART", "Aid type", "AMOUNTTYPE", "Year", "Value", "Flags"],
        "index": ["recipient", "donor", "part", "aid_type", "amounttype", "year"],
    },
    "dac5": {
        "file_name": "Table5_Data.csv",
        "columns": ["Donor", "Sector", "Aid type", "AMOUNTTYPE", "Year", "Value", "Flags"],
        "index": ["donor", "sector", "aid_type", "amounttype", "year"],
    },
    # NOTE: We are still deciding how to handle the CRS dataset. We will decide after Stockholm.
    # "crs": {
    #     "file_format": "zip",
    #     "file_name": "CRS.parquet",
    #     "columns": [
    #         "year",
    #         "donor_name",
    #         "agency_name",
    #         "crs_id",
    #         "project_number",
    #         "initial_report",
    #         "recipient_name",
    #         "region_name",
    #         "incomegroup_name",
    #         "flow_name",
    #         "bi_multi",
    #         "category",
    #         "finance_t",
    #         "aid_t",
    #         "usd_commitment",
    #         "usd_disbursement",
    #         "usd_received",
    #         "usd_commitment_defl",
    #         "usd_disbursement_defl",
    #         "usd_received_defl",
    #         "usd_adjustment",
    #         "usd_adjustment_defl",
    #         "usd_amount_untied",
    #         "usd_amount_partial_tied",
    #         "usd_amount_tied",
    #         "usd_amount_untied_defl",
    #         "usd_amount_partial_tied_defl",
    #         "usd_amounttied_defl",
    #         "usd_irtc",
    #         "usd_expert_commitment",
    #         "usd_expert_extended",
    #         "usd_export_credit",
    #         "currency_code",
    #         "commitment_national",
    #         "disbursement_national",
    #         "grant_equiv",
    #         "usd_grant_equiv",
    #         "short_description",
    #         "project_title",
    #         "purpose_name",
    #         "sector_name",
    #         "channel_name",
    #         "channel_reported_name",
    #         "geography",
    #         "ld_cflag",
    #         "ld_cflag_name",
    #         "expected_start_date",
    #         "completion_date",
    #         "long_description",
    #         "sd_gfocus",
    #         "keywords",
    #         "gender",
    #         "environment",
    #         "dig",
    #         "trade",
    #         "rmnch",
    #         "drr",
    #         "nutrition",
    #         "disability",
    #         "ftc",
    #         "pba",
    #         "investment_project",
    #         "assoc_finance",
    #         "biodiversity",
    #         "climate_mitigation",
    #         "climate_adaptation",
    #         "desertification",
    #         "commitment_date",
    #         "type_repayment",
    #         "number_repayment",
    #         "interest1",
    #         "interest2",
    #         "repaydate1",
    #         "repaydate2",
    #         "usd_interest",
    #         "usd_outstanding",
    #         "usd_arrears_principal",
    #         "usd_arrears_interest",
    #         "capital_expend",
    #         "ps_iflag",
    #         "psi_add_type",
    #         "psi_add_assess",
    #         "psi_add_dev_obj",
    #     ],
    #     "index": [
    #         "year",
    #         "donor_name",
    #         "agency_name",
    #         "crs_id",
    #         "project_number",
    #         "initial_report",
    #         "recipient_name",
    #         "region_name",
    #         "incomegroup_name",
    #         "flow_name",
    #         "bi_multi",
    #         "category",
    #         "finance_t",
    #         "aid_t",
    #         "currency_code",
    #         "short_description",
    #         "project_title",
    #         "purpose_name",
    #         "sector_name",
    #         "channel_name",
    #         "channel_reported_name",
    #         "investment_project",
    #     ],
    # },
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    tables = []
    for dataset, config in DATASETS.items():
        # Retrieve snapshot.
        snap = paths.load_snapshot(f"official_development_assistance_{dataset}.zip")

        # Load data from snapshot.
        tb = snap.read_in_archive(f"{config['file_name']}")

        # Rename DATATYPE column to AMOUNTTYPE.
        if "DATATYPE" in tb.columns:
            tb = tb.rename(columns={"DATATYPE": "AMOUNTTYPE"})

        # # Export duplicates to a CSV file.
        # if dataset == "crs":
        #     tb[tb.duplicated(subset=config["index"], keep=False)].to_csv(f"duplicates_{dataset}.csv", index=False)

        # Process data.
        tb = tb[config["columns"]].format(config["index"], short_name=dataset)

        # Add table to list.
        tables.append(tb)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=tables, check_variables_metadata=True, default_metadata=snap.metadata)  # type: ignore

    # Save changes in the new meadow dataset.
    ds_meadow.save()
