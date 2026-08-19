"""Process FrontierMath benchmark data for garden step.

Entities are individual model evaluations, named from Epoch's own model registry rather than parsed out of
its version strings. The registry (`epoch_capabilities_index.csv`) travels in the same snapshot archive as
the benchmark itself and covers every model version Epoch publishes a benchmark score for.
"""

import re
from datetime import datetime

import pandas as pd
from owid.catalog import Table
from owid.catalog import processing as pr
from structlog import get_logger

from etl.files import ruamel_load
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

log = get_logger()

# Some model version strings embed the build date ("claude-opus-4-5-20251101"). It is occasionally the only
# thing separating two evaluations that Epoch gives the same name — both GPT-4o builds are called "GPT-4o" —
# so it has to survive into the entity name.
DATE_IN_VERSION = re.compile(r"(\d{4})-?(\d{2})-?(\d{2})")

# Epoch is inconsistent about putting the date in its own names: "Claude 3.5 Sonnet (October 2024)" for one
# build, plain "Claude 3.5 Sonnet" for another. Don't append a second date to the ones that already have one.
DATE_IN_NAME = re.compile(r"\((?:[A-Z][a-z]{2,9}\s+)?\d{4}\)\s*$")


def date_suffix(model_version: str) -> str:
    """Render the build date embedded in a model version string, e.g. " (Nov 2025)"."""
    match = DATE_IN_VERSION.search(model_version)
    if match is None:
        return ""
    year, month, day = match.groups()
    try:
        return datetime(int(year), int(month), int(day)).strftime(" (%b %Y)")
    except ValueError:
        return ""


def setting_suffix(model_version: str) -> str:
    """Render the context window or reasoning effort Epoch appends after an underscore, e.g. ", max".

    Everything after the first underscore is kept verbatim. The vocabulary is open-ended (max, xhigh, none,
    unknown, minimal, 32K, ...) and dropping a token we don't recognize would merge two evaluations of the
    same model into a single entity.
    """
    _, _, setting = model_version.partition("_")
    return f", {setting}" if setting else ""


def build_model_name(model_version: str, epoch_name: str | None, overrides: dict[str, str]) -> str:
    """Compose the entity name for one evaluation: model name, build date if needed, then the setting."""
    if model_version in overrides:
        return f"{overrides[model_version]}{setting_suffix(model_version)}"

    if epoch_name is None or pd.isna(epoch_name):
        # Epoch curates no name for a handful of older open-weight checkpoints. Show its version string
        # rather than guessing at one: unlovely, but unambiguous and conspicuous enough to get fixed.
        return model_version

    date = "" if DATE_IN_NAME.search(epoch_name) else date_suffix(model_version)
    return f"{epoch_name}{date}{setting_suffix(model_version)}"


def sanity_check_inputs(tb: Table, tb_registry: Table) -> None:
    """Check the benchmark table and the model registry before joining them."""
    assert not tb[["model_version", "release_date"]].isna().any().any(), "Benchmark rows are missing a key."
    assert not tb_registry.duplicated("model_version").any(), "Model registry lists a version twice."
    # Scores are published as fractions. If Epoch ever switched to percentages, multiplying by 100 below
    # would silently put every model at a hundred times its real score.
    assert tb["mean_score"].between(0, 1).all(), "Scores outside 0-1: are they still published as fractions?"


def sanity_check_outputs(tb: Table) -> None:
    """Check that no two models were given the same name, and that the scores survived processing."""
    models = tb[["model_version", "model_name"]].drop_duplicates()
    collisions = models[models.duplicated("model_name", keep=False)].sort_values("model_name")
    # Two models sharing a name become one entity holding several observations, which grapher renders as a
    # single connected series instead of separate points. Pin one of them in the overrides file.
    assert collisions.empty, f"Distinct models share a name:\n{collisions.to_string(index=False)}"
    assert tb["mean_score"].between(0, 100).all(), "Scores outside 0-100%."
    assert tb.columns[tb.isna().all()].empty, "Output has a fully empty column."


def run() -> None:
    """Process FrontierMath benchmark data."""
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("frontiermath")

    # Read tables from meadow dataset.
    tb = ds_meadow.read("epoch_benchmark_data")
    tb_registry = ds_meadow.read("model_registry")

    # Load the names we set ourselves, where the registry cannot or should not decide.
    with paths.side_file(f"{paths.short_name}.model_names.yml").open() as f:
        overrides = ruamel_load(f)["names"] or {}

    sanity_check_inputs(tb, tb_registry)

    #
    # Process data.
    #
    tb["mean_score"] = tb["mean_score"] * 100

    # Name each evaluation from Epoch's registry.
    evaluations = len(tb)
    tb = pr.merge(tb, tb_registry, on="model_version", how="left")
    assert len(tb) == evaluations, "The registry join changed the number of evaluations."

    unnamed = sorted(set(tb.loc[tb["model_name"].isna(), "model_version"]))
    if unnamed:
        log.warning(f"Not in Epoch's model registry, falling back to the version string: {unnamed}")

    tb["model_name"] = [
        build_model_name(version, name, overrides) for version, name in zip(tb["model_version"], tb["model_name"])
    ]

    sanity_check_outputs(tb)

    tb["model_version"] = tb["model_name"]
    tb = tb.drop(columns=["model_name"])

    tb = tb.format(["release_date", "model_version"])
    #
    # Save outputs.
    #
    # Create garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata)

    # Save changes.
    ds_garden.save()
