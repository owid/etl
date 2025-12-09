from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

SHEET_NAMES = [
    "Overview",
    "UniProt_2004-2023",
    "UniProt_2004-2014",
    "UniProt_2014_2023",
    "MGnify protein sequence",
    "MetaClust",
    "PDB_1976-2023",
    "AlphaFoldDB_2021-2022",
    "ESMAtlas",
    "ENA_1982-2020",
    "GenBank-all records",
    "GenBank-traditional",
    "DDBJ",
    "RefSeq",
]


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("epoch_database_growth.xlsx")

    # Load data from snapshot.
    tb_uni_prot = snap.read_excel(sheet_name="UniProt_2004-2023", usecols="A:I")
    tb_uni_prot.m.short_name = "uniprot"

    tb_mgnify = snap.read_excel(sheet_name="MGnify protein sequence", usecols="A:K")
    tb_mgnify.m.short_name = "mgnify"

    tb_pdb = snap.read_excel(sheet_name="PDB_1976-2023", usecols="A:D")
    tb_pdb.m.short_name = "pdb"

    tb_alpha_fold = snap.read_excel(sheet_name="AlphaFoldDB_2021-2022", usecols="A:B")
    tb_alpha_fold.m.short_name = "alpha_fold"

    tb_esm_atlas = snap.read_excel(sheet_name="ESMAtlas", usecols="A:C")
    tb_esm_atlas.m.short_name = "esm_atlas"

    tb_ena = snap.read_excel(sheet_name="ENA_1982-2020", usecols="A:D")
    tb_ena.m.short_name = "ena"

    tb_gb_all = snap.read_excel(sheet_name="GenBank-all records", usecols="A:D")
    tb_gb_all.m.short_name = "gb_all"

    tb_gb_traditional = snap.read_excel(sheet_name="GenBank-traditional", usecols="A:D")
    tb_gb_traditional.m.short_name = "gb_traditional"

    tb_ddbj = snap.read_excel(sheet_name="DDBJ", usecols="A:E")
    tb_ddbj.m.short_name = "ddbj"

    tb_refseq = snap.read_excel(sheet_name="RefSeq", usecols="A:F")
    tb_refseq.m.short_name = "refseq"

    tables = [
        tb_uni_prot,
        tb_mgnify,
        tb_pdb,
        tb_alpha_fold,
        tb_esm_atlas,
        tb_ena,
        tb_gb_all,
        tb_gb_traditional,
        tb_ddbj,
        tb_refseq,
    ]
    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = paths.create_dataset(
        tables=tables,
        check_variables_metadata=True,
        default_metadata=snap.metadata,
    )

    # Save changes in the new meadow dataset.
    ds_meadow.save()
