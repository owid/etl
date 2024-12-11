"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
from shared import add_dim_some_education, get_index_columns, make_table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
# Not all columns are present in historical and projections datasets. This dictionary contains the expected differences.
TABLE_COLUMN_DIFFERENCES = {
    "by_edu": {
        "missing_in_hist": {"macb", "net"},
    },
    "by_sex_age": {
        "missing_in_proj": {"net"},
    },
    "main": {
        "missing_in_hist": {"emi", "imm"},
        "missing_in_proj": {"macb"},
    },
}
DTYPES = {
    "sex": "category",
    "age": "category",
    "education": "category",
    "country": "category",
    "year": "UInt16",
    "scenario": "UInt8",
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_proj = paths.load_dataset("wittgenstein_human_capital_proj")
    ds_hist = paths.load_dataset("wittgenstein_human_capital_historical")

    # Read table from meadow dataset.
    paths.log.info("reading tables...")
    tbs_proj = {t.m.short_name: t.reset_index() for t in ds_proj}
    tbs_hist = {t.m.short_name: t.reset_index() for t in ds_hist}

    #
    # Processing
    #
    assert tbs_proj.keys() == tbs_hist.keys(), "Mismatch in tables between historical and projection datasets"

    tables = []
    for key in tbs_proj.keys():
        paths.log.info(f"Building {key}")

        # Get tables
        tb_proj = tbs_proj[key]
        tb_hist = tbs_hist[key]

        # Dtypes
        tb_proj = tb_proj.astype({k: v for k, v in DTYPES.items() if k in tb_proj.columns})
        tb_hist = tb_hist.astype({k: v for k, v in DTYPES.items() if k in tb_hist.columns})

        # Check
        sanity_checks(tb_proj, tb_hist)

        # Keep only the columns that are present in both datasets
        columns_common = tb_proj.columns.intersection(tb_hist.columns)

        # Concatenate
        tb = pr.concat([tb_proj[columns_common], tb_hist[columns_common]], ignore_index=True)

        # Remove duplicates
        index = get_index_columns(tb)
        tb = tb.drop_duplicates(subset=index, keep="first")

        # Format
        tb = tb.format(index, short_name=key)

        # Reduce origins
        for col in tb.columns:
            tb[col].metadata.origins = [tb[col].metadata.origins[0]]

        # Add to list
        tables.append(tb)
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir,
        tables=tables,
        check_variables_metadata=True,
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def sanity_checks(tb_proj, tb_hist):
    # Short name sanity check
    assert (
        tb_proj.m.short_name == tb_hist.m.short_name
    ), f"Mismatch in short_name of historical ({tb_hist.m.short_name}) and projection ({tb_proj.m.short_name})"
    key = tb_proj.m.short_name

    # Look for differences
    missing_in_hist = set(tb_proj.columns) - set(tb_hist.columns)
    missing_in_proj = set(tb_hist.columns) - set(tb_proj.columns)

    # Check with expected differences
    if key in TABLE_COLUMN_DIFFERENCES:
        missing_in_hist_expected = TABLE_COLUMN_DIFFERENCES[key].get("missing_in_hist", set())
        missing_in_proj_expected = TABLE_COLUMN_DIFFERENCES[key].get("missing_in_proj", set())
        assert missing_in_hist == missing_in_hist_expected, (
            f"Table {key}: Missing columns in historical dataset. "
            f"Expected: {missing_in_hist_expected}, Found: {missing_in_hist}"
        )
        assert missing_in_proj == missing_in_proj_expected, (
            f"Table {key}: Missing columns in projection dataset. "
            f"Expected: {missing_in_proj_expected}, Found: {missing_in_proj}"
        )
    else:
        assert set(tb_proj.columns) == set(tb_hist.columns), (
            f"Table {key}: Mismatch in columns between historical and projection. "
            f"Projection columns: {tb_proj.columns.tolist()}, Historical columns: {tb_hist.columns.tolist()}"
        )
