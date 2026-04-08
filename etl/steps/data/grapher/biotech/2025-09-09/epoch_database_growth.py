from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    ds_garden = paths.load_dataset("epoch_database_growth")

    # Read table.
    tb_databases = ds_garden.read("epoch_database_growth")

    # from wide to long
    tb_databases = tb_databases.melt(
        id_vars=["year"],
        var_name="database",
        value_name="entries",
    )

    tb_databases["entries"].m.unit = "entries"
    tb_databases["entries"].m.short_unit = ""
    tb_databases["entries"].m.title = "Number of entries in biological databases"

    tb_databases = tb_databases.rename(columns={"database": "country"})

    # rename slugs for databases to readable names
    tb_databases = tb_databases.replace(
        {
            "gb_all_reported_sequences": "GenBank",
            "pdb_total_number_of_entries_available": "Protein Data Bank (PDB)",
            "uniprot_uniprotkb_swiss_prot": "UniProtKB (Swiss-Prot)",
            "alpha_fold_number_of_predicted_structures": "AlphaFoldDB",
            "esm_atlas_number_of_predicted_structures": "ESMAtlas",
            "refseq_records": "RefSeq",
        }
    )

    tb_databases = tb_databases.format(["year", "country"])

    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_grapher = paths.create_dataset(
        tables=[tb_databases],
        default_metadata=ds_garden.metadata,
    )

    # Save changes in the new garden dataset.
    ds_grapher.save()
