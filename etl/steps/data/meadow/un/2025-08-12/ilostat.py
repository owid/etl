"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("ilostat.parquet")
    snap_toc_country = paths.load_snapshot("ilostat_table_of_contents_country.parquet")
    snap_dic_classif1 = paths.load_snapshot("ilostat_dictionary_classif1.parquet")
    snap_dic_classif2 = paths.load_snapshot("ilostat_dictionary_classif2.parquet")
    snap_dic_indicator = paths.load_snapshot("ilostat_dictionary_indicator.parquet")
    snap_dic_note_classif = paths.load_snapshot("ilostat_dictionary_note_classif.parquet")
    snap_dic_note_indicator = paths.load_snapshot("ilostat_dictionary_note_indicator.parquet")
    snap_dic_note_source = paths.load_snapshot("ilostat_dictionary_note_source.parquet")
    snap_dic_obs_status = paths.load_snapshot("ilostat_dictionary_obs_status.parquet")
    snap_dic_ref_area = paths.load_snapshot("ilostat_dictionary_ref_area.parquet")
    snap_dic_sex = paths.load_snapshot("ilostat_dictionary_sex.parquet")
    snap_dic_source = paths.load_snapshot("ilostat_dictionary_source.parquet")

    # Load data from snapshot.
    tb = snap.read(safe_types=False)
    tb_toc_country = snap_toc_country.read()
    tb_dic_classif1 = snap_dic_classif1.read()
    tb_dic_classif2 = snap_dic_classif2.read()
    tb_dic_indicator = snap_dic_indicator.read()
    tb_dic_note_classif = snap_dic_note_classif.read()
    tb_dic_note_indicator = snap_dic_note_indicator.read()
    tb_dic_note_source = snap_dic_note_source.read()
    tb_dic_obs_status = snap_dic_obs_status.read()
    tb_dic_ref_area = snap_dic_ref_area.read()
    tb_dic_sex = snap_dic_sex.read()
    tb_dic_source = snap_dic_source.read()

    #
    # Process data.
    #
    # Improve tables format.
    tables = [
        tb.format(["ref_area", "source", "indicator", "sex", "classif1", "classif2", "time"]),
        tb_toc_country.format(["id"]),
        tb_dic_classif1.format(["classif1"]),
        tb_dic_classif2.format(["classif2"]),
        tb_dic_indicator.format(["indicator"]),
        tb_dic_note_classif.format(["note_classif"]),
        tb_dic_note_indicator.format(["note_indicator"]),
        tb_dic_note_source.format(["note_source"]),
        tb_dic_obs_status.format(["obs_status"]),
        tb_dic_ref_area.format(["ref_area"]),
        tb_dic_sex.format(["sex"]),
        tb_dic_source.format(["source"]),
    ]

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables, default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()
