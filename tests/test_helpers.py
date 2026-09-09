from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest
from owid import catalog

from etl import paths
from etl.helpers import (
    PathFinder,
    create_dataset,
    end_with_punctuation,
)


def test_PathFinder_paths():
    def _assert(pf):
        assert pf.channel == "meadow"
        assert pf.namespace == "papers"
        assert pf.version == "2022-11-03"
        assert pf.short_name == "zijdeman_et_al_2015"

    # saved as short_name/__init__.py
    pf = PathFinder(str(paths.STEP_DIR / "data/meadow/papers/2022-11-03/zijdeman_et_al_2015/__init__.py"))
    _assert(pf)
    assert pf.directory == paths.STEP_DIR / "data/meadow/papers/2022-11-03/zijdeman_et_al_2015"

    # saved as short_name/anymodule.py
    pf = PathFinder(str(paths.STEP_DIR / "data/meadow/papers/2022-11-03/zijdeman_et_al_2015/anymodule.py"))
    _assert(pf)
    assert pf.directory == paths.STEP_DIR / "data/meadow/papers/2022-11-03/zijdeman_et_al_2015"

    # saved as short_name.py
    pf = PathFinder(str(paths.STEP_DIR / "data/meadow/papers/2022-11-03/zijdeman_et_al_2015.py"))
    _assert(pf)
    assert pf.directory == paths.STEP_DIR / "data/meadow/papers/2022-11-03"


def test_create_dataset(tmp_path):
    meta = catalog.DatasetMeta(title="Test title")

    dest_dir = tmp_path / "data/garden/flowers/2020-01-01/rose"
    dest_dir.parent.mkdir(parents=True)

    # create metadata YAML file
    step_dir = tmp_path / "etl/steps"
    meta_yml = step_dir / "data/garden/flowers/2020-01-01/rose.meta.yml"
    meta_yml.parent.mkdir(parents=True)
    meta_yml.write_text(
        """
dataset:
    description: Test description
tables: {}""".strip()
    )

    # create dataset
    with patch("etl.paths.STEP_DIR", step_dir):
        ds = create_dataset(dest_dir, tables=[], default_metadata=meta)

    # check metadata
    assert ds.metadata.channel == "garden"
    assert ds.metadata.namespace == "flowers"
    assert ds.metadata.version == "2020-01-01"
    assert ds.metadata.short_name == "rose"
    assert ds.metadata.description == "Test description"
    assert ds.metadata.title == "Test title"


def test_PathFinder_with_private_steps():
    pf = PathFinder(str(paths.STEP_DIR / "data/garden/namespace/2023/name/__init__.py"))

    pf._dag = {
        "data://garden/namespace/2023/name": {
            "snapshot://namespace/2023/snapshot_a",
            "snapshot-private://namespace/2023/snapshot_b",
            # There could be two steps with the same name, one public and one private (odd case).
            "snapshot-private://namespace/2023/snapshot_a",
        }
    }
    assert pf.step == "data://garden/namespace/2023/name"
    assert pf.get_dependency_step_name("snapshot_a") == "snapshot://namespace/2023/snapshot_a"
    assert pf.get_dependency_step_name("snapshot_b") == "snapshot-private://namespace/2023/snapshot_b"
    # In the odd case that two dependencies have the same name, but one is public and the other is private,
    # assume it's public, unless explicitly stated otherwise.
    assert pf.get_dependency_step_name("snapshot_a", is_private=True) == "snapshot-private://namespace/2023/snapshot_a"

    pf._dag = {
        "data-private://garden/namespace/2023/name": {
            "snapshot-private://namespace/2023/name",
        }
    }
    assert pf.step == "data-private://garden/namespace/2023/name"
    assert pf.get_dependency_step_name("name") == "snapshot-private://namespace/2023/name"


@pytest.mark.parametrize(
    "text,expected",
    [
        ("Share of adults with an account", "Share of adults with an account."),
        ("Share of adults with an account.", "Share of adults with an account."),
        ("Which of these do you agree with?", "Which of these do you agree with?"),
        ('... do you agree with?"', '... do you agree with?"'),
        ("Deaths (per 100,000 people).", "Deaths (per 100,000 people)."),
        ("Trailing space is kept as is ", "Trailing space is kept as is."),
        ("", ""),
        ("   ", "   "),
        (None, None),
    ],
)
def test_end_with_punctuation(text, expected):
    assert end_with_punctuation(text) == expected


@pytest.mark.parametrize("missing", [pd.NA, np.nan, pd.NaT])
def test_end_with_punctuation_missing_values(missing):
    # producer text read out of a table column is a pandas missing value, not None
    assert end_with_punctuation(missing) is missing


def test_PathFinder_viz_step_names():
    """A viz recipe under etl/steps/viz/<channel>/... maps to a `viz://<channel>/...` step name, and back."""
    pf = PathFinder(str(paths.STEP_DIR / "viz/chart/animal_welfare/latest/banning_of_chick_culling.py"))
    assert pf.step_type == "viz"
    assert pf.channel == "chart"
    assert pf._create_current_step_name() == "viz://chart/animal_welfare/latest/banning_of_chick_culling"
    assert pf.dest_dir == paths.VIZ_DIR / "chart/animal_welfare/latest/banning_of_chick_culling"
    assert pf.config_path == paths.STEP_DIR / "viz/chart/animal_welfare/latest/banning_of_chick_culling.config.yml"

    pf = PathFinder(str(paths.STEP_DIR / "viz/static/population/2026-01-26/world_population_growth.py"))
    assert pf._create_current_step_name() == "viz://static/population/2026-01-26/world_population_growth"

    pf = PathFinder(str(paths.STEP_DIR / "export/github/co2_data/latest/owid_co2.py"))
    assert pf._create_current_step_name() == "export://github/co2_data/latest/owid_co2"
    assert pf.dest_dir == paths.EXPORT_DIR / "github/co2_data/latest/owid_co2"

    assert PathFinder._get_attributes_from_step_name("viz://explorer/who/latest/influenza") == {
        "channel": "explorer",
        "namespace": "who",
        "version": "latest",
        "short_name": "influenza",
        "is_private": False,
    }
    # A dependency step name for a collection is built as a `viz://` step.
    assert (
        PathFinder.create_step_name(
            short_name="conflict_data_source", channel="explorer", namespace="war", version="latest", step_type="viz"
        )
        == "viz://explorer/war/latest/conflict_data_source"
    )
