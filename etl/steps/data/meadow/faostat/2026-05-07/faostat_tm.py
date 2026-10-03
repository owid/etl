"""FAOSTAT meadow step for the Detailed Trade Matrix (faostat_tm).

The bulk archive expands to roughly 8.5 GB of CSV, so reading it with standard pandas would take too much memory.
Instead, we stream the CSV through pyarrow in ~64 MB batches, and write a slim parquet file to disk.

"""

import tempfile
import zipfile
from pathlib import Path

import owid.catalog.processing as pr
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq
import structlog
from owid.catalog import Table

from etl.helpers import PathFinder
from etl.snapshot import Snapshot

log = structlog.get_logger()
paths = PathFinder(__file__)

# Columns to keep from the raw CSV. The bulk file also contains the M49 code
# variants, the FAOSTAT numeric reporter / partner country codes, "Year Code",
# "Item Code (CPC)" and a "Note" column; none of those are needed downstream
# and dropping them keeps the parquet small. The numeric reporter / partner
# country codes are dropped because the categorical name columns already carry
# the same information and country names get harmonized to OWID-standard names
# in garden anyway. We do keep "Item Code" and "Element Code" though, since
# they are the canonical FAOSTAT identifiers and enable joining against other
# FAOSTAT datasets (e.g. QCL production) for context indicators downstream.
COLUMNS_TO_KEEP = [
    "Reporter Countries",
    "Partner Countries",
    "Item Code",
    "Item",
    "Element Code",
    "Element",
    "Year",
    "Unit",
    "Value",
    "Flag",
]

# String columns whose cardinality is small enough that storing them as
# dictionaries (categoricals) rather than plain strings dramatically reduces
# parquet size and final pandas memory usage.
DICTIONARY_COLUMNS = [
    "Reporter Countries",
    "Partner Countries",
    "Item",
    "Element",
    "Unit",
    "Flag",
]

# Explicit dtypes for numeric columns. Year and code columns easily fit into
# 16- or 32-bit integers, which halves their memory footprint.
COLUMN_TYPES = {
    "Item Code": pa.int32(),
    "Element Code": pa.int32(),
    "Year": pa.int16(),
    "Value": pa.float64(),
}

# Final, snake-cased index for the meadow table. Reporter / partner are keyed
# by the (categorical) country name columns rather than numeric codes — see
# the note on COLUMNS_TO_KEEP above.
INDEX_COLUMNS = [
    "reporter_countries",
    "partner_countries",
    "item_code",
    "element_code",
    "year",
]


def _stream_csv_to_parquet(csv_path: Path, parquet_path: Path) -> None:
    """Stream-read the CSV in batches, dictionary-encode string columns, and
    write a parquet file. Memory usage is bounded by the batch block size
    (64 MB), regardless of the total file size."""
    read_options = pacsv.ReadOptions(block_size=64 << 20)
    convert_options = pacsv.ConvertOptions(
        include_columns=COLUMNS_TO_KEEP,
        column_types=COLUMN_TYPES,
        strings_can_be_null=True,
    )
    reader = pacsv.open_csv(csv_path, read_options=read_options, convert_options=convert_options)
    writer = None
    try:
        for batch in reader:
            arrays = [
                batch.column(name).dictionary_encode() if name in DICTIONARY_COLUMNS else batch.column(name)
                for name in batch.schema.names
            ]
            out_batch = pa.RecordBatch.from_arrays(arrays, names=batch.schema.names)
            if writer is None:
                writer = pq.ParquetWriter(parquet_path, out_batch.schema, compression="zstd")
            writer.write_batch(out_batch)
    finally:
        if writer is not None:
            writer.close()


def read_snapshot_data(snap: Snapshot) -> Table:
    """Load the FAOSTAT detailed trade matrix snapshot as a Table.

    The bulk zip is extracted to a temporary directory, the inner CSV is
    streamed through pyarrow (column projection + dictionary encoding) into a
    slim parquet file, and that parquet is then read back as a Table with the
    snapshot's origin metadata attached. Both the extracted CSV and the
    intermediate parquet live in the temp dir and are cleaned up on exit."""
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)

        # The FAOSTAT bulk archive contains a single "(Normalized).csv" file.
        with zipfile.ZipFile(snap.path) as zf:
            csv_name = next(n for n in zf.namelist() if n.endswith("(Normalized).csv"))
            log.info("faostat_tm.extract", member=csv_name)
            zf.extract(csv_name, tmp_path)
        csv_path = tmp_path / csv_name

        parquet_path = tmp_path / "faostat_tm.parquet"
        log.info("faostat_tm.csv_to_parquet", csv_path=str(csv_path))
        _stream_csv_to_parquet(csv_path, parquet_path)

        # Free disk before loading the slim parquet back into memory.
        csv_path.unlink()

        log.info("faostat_tm.read_parquet", parquet_path=str(parquet_path))
        tb = pr.read_parquet(
            parquet_path,
            origin=snap.metadata.origin,
            metadata=snap.to_table_metadata(),
        )

    return tb


def run() -> None:
    #
    # Load inputs.
    #
    # Load snapshot.
    snap = paths.load_snapshot()

    # Read snapshot data in a memory-efficient way.
    tb = read_snapshot_data(snap)

    #
    # Process data.
    #
    # Improve table format.
    tb = tb.format(keys=INDEX_COLUMNS)

    #
    # Save outputs.
    #
    ds = paths.create_dataset(tables=[tb])
    ds.save()
