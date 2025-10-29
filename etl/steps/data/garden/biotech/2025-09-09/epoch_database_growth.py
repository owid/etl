from owid.catalog import processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    ds_meadow = paths.load_dataset("epoch_database_growth")

    # Read table.
    tb_uni_prot = ds_meadow["uniprot"]
    tb_mgnify = ds_meadow["mgnify"]
    tb_pdb = ds_meadow["pdb"]
    tb_alpha_fold = ds_meadow["alpha_fold"]
    tb_esm_atlas = ds_meadow["esm_atlas"]
    tb_ena = ds_meadow["ena"]
    tb_gb_all = ds_meadow["gb_all"]
    tb_gb_traditional = ds_meadow["gb_traditional"]
    tb_ddbj = ds_meadow["ddbj"]
    tb_refseq = ds_meadow["refseq"]

    # data formats

    tb_uni_prot["year"] = tb_uni_prot["date"].dt.year
    tb_mgnify["year"] = tb_mgnify["release"].dt.year
    tb_pdb["year"] = tb_pdb["year"]  # YYYY
    tb_alpha_fold["year"] = tb_alpha_fold["release_time"].dt.year
    tb_esm_atlas["year"] = tb_esm_atlas["release_time"].dt.year
    tb_ena["year"] = tb_ena["month"].dt.year
    tb_gb_all["year"] = tb_gb_all["date"].dt.year
    tb_gb_traditional["year"] = tb_gb_traditional["date"].dt.year
    tb_ddbj["year"] = tb_ddbj["date"].dt.year
    tb_refseq["year"] = tb_refseq["date"].dt.year

    tables = [
        {
            "name": "uniprot",
            "table": tb_uni_prot,
            "value_cols": [
                "uniprotkb",
                "uniprotkb_trembl",
                "uniprotkb_swiss_prot",
                "uniref100",
                "uniref90",
                "uniref50",
                "uniparc",
            ],
        },
        {
            "name": "mgnify",
            "table": tb_mgnify,
            "value_cols": [
                "total_seq",
                "total_clust",
                "full_seq",
                "full_clust",
                "partial_seq",
                "partial_clust",
                "swiss_prot_seq",
                "swiss_prot_clust",
                "trembl_seq",
                "trembl_clust",
            ],
        },
        {
            "name": "pdb",
            "table": tb_pdb,
            "value_cols": [
                "total_number_of_entries_available",
                "number_of_structures_released_annually",
                "growth_rate__pct_per_year",
            ],
        },
        {
            "name": "alpha_fold",
            "table": tb_alpha_fold,
            "value_cols": [
                "number_of_predicted_structures",
            ],
        },
        {
            "name": "esm_atlas",
            "table": tb_esm_atlas,
            "value_cols": [
                "number_of_predicted_structures",
            ],
        },
        {
            "name": "ena",
            "table": tb_ena,
            "value_cols": [
                "entries",
                "nucleotides",
            ],
        },
        {
            "name": "gb_all",
            "table": tb_gb_all,
            "value_cols": [
                "reported_sequences",
                "bases",
            ],
        },
        {
            "name": "gb_traditional",
            "table": tb_gb_traditional,
            "value_cols": [
                "reported_sequences",
                "bases",
            ],
        },
        {
            "name": "ddbj",
            "table": tb_ddbj,
            "value_cols": [
                "entries",
                "bases",
            ],
        },
        {
            "name": "refseq",
            "table": tb_refseq,
            "value_cols": [
                "taxons",
                "nucleotides",
                "amino_acids",
                "records",
            ],
        },
    ]

    # get maximum per year
    harmonized_tables = []
    for table_dict in tables:
        # give value_cols prefix with table name
        tb = table_dict["table"]
        tb = tb.rename(columns={col: f"{table_dict['name']}_{col}" for col in table_dict["value_cols"]})
        table_dict["value_cols"] = [f"{table_dict['name']}_{col}" for col in table_dict["value_cols"]]

        harm_tb = tb.groupby("year")[table_dict["value_cols"]].max().reset_index()
        harmonized_tables.append(harm_tb)

    tb_databases = pr.multi_merge(
        harmonized_tables,
        on="year",
        how="outer",
    ).sort_values("year")

    # filter on most important columns for first version
    important_cols = [
        "year",
        "gb_all_reported_sequences",
        "pdb_total_number_of_entries_available",
        "uniprot_uniprotkb_swiss_prot",
        "alpha_fold_number_of_predicted_structures",
        "esm_atlas_number_of_predicted_structures",
        "refseq_records",
    ]
    tb_databases = tb_databases[important_cols]

    tb_databases = tb_databases.format(["year"], short_name="epoch_database_growth")

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(
        tables=[tb_databases],
        check_variables_metadata=True,
        default_metadata=ds_meadow.metadata,
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
