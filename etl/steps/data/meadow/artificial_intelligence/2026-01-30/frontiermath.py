"""Load FrontierMath benchmark data from Epoch AI zip archive into meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    """Extract and load FrontierMath CSV from benchmark_data.zip."""
    #
    # Load inputs.
    #
    snap = paths.load_snapshot("epoch_benchmark_data.zip")

    #
    # Process data.
    #
    # Extract frontiermath.csv from the zip file.
    with snap.extracted() as archive:
        tb = archive.read("frontiermath.csv", safe_types=False)
        # Epoch's model registry ships in the same archive and maps every model version string used across
        # its benchmark files to a curated human-readable name.
        tb_registry = archive.read("epoch_capabilities_index.csv", safe_types=False)

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
