"""Load FrontierMath benchmark data from Epoch AI zip archive into meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Which cut of FrontierMath we chart. Epoch reissued the benchmark on 2026-06-12 after finding errors in
# 42% of the problems, and stopped evaluating new models on the original set; the v2 files are the live ones.
BENCHMARK_FILE = "frontiermath_tiers_1_3_v2.csv"


def run() -> None:
    """Extract and load FrontierMath CSV from benchmark_data.zip."""
    #
    # Load inputs.
    #
    snap = paths.load_snapshot("epoch_benchmark_data.zip")

    #
    # Process data.
    #
    # Extract the benchmark table from the zip file.
    with snap.extracted() as archive:
        tb = archive.read(BENCHMARK_FILE, safe_types=False)
        # Epoch's model registry ships in the same archive and maps every model version string used across
        # its benchmark files to a curated human-readable name.
        tb_registry = archive.read("epoch_capabilities_index.csv", safe_types=False)
        # The archive also indexes its own benchmarks, marking retired ones in "superseded_by". Reading a
        # superseded file is how this chart silently stopped gaining models for three months, so refuse to.
        tb_benchmarks = archive.read("benchmark_metadata.csv", safe_types=False)

    superseded = tb_benchmarks.loc[tb_benchmarks["source_file"] == BENCHMARK_FILE, "superseded_by"].dropna()
    assert superseded.empty, (
        f"Epoch marks {BENCHMARK_FILE} as superseded by {superseded.iloc[0]!r}. Point this step at the "
        "replacement file and rewrite the metadata: a reissued benchmark is a new series, not a refresh."
    )

    tb = tb.reset_index(drop=True)

    columns = ["Model version", "mean_score", "Release date"]
    tb = tb[columns]

    tb = tb.format(["release_date", "model_version"])

    # Keep only the name; the registry's "Display name" column is populated for about half its rows, so it
    # cannot be relied on to name every model.
    tb_registry = tb_registry.reset_index(drop=True)
    tb_registry = tb_registry[["Model version", "Model name"]]
    # Epoch repeats a registry row outright now and then (deepseek-r1-0528-qwen3-8b, from 2026-09-01), which
    # would break the index below. Deduplicating after narrowing to the two columns we read means only rows
    # indistinguishable in the data we use get collapsed: one version carrying two different names still
    # reaches format() twice and fails there, as it should.
    tb_registry = tb_registry.drop_duplicates()
    # Every table read from an archive is named after the snapshot, so the second one needs an explicit
    # short name to avoid clashing with the benchmark table.
    tb_registry = tb_registry.format(["model_version"], short_name="model_registry")

    #
    # Save outputs.
    #
    ds_meadow = paths.create_dataset(
        tables=[tb, tb_registry], check_variables_metadata=True, default_metadata=snap.metadata
    )
    ds_meadow.save()
