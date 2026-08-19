import importlib.util
from pathlib import Path

import pandas as pd
import pytest
from owid.catalog import Table

STEP_PATH = Path(__file__).parents[1] / "etl/steps/data/garden/artificial_intelligence/2026-01-30/frontiermath.py"


def load_step_module():
    spec = importlib.util.spec_from_file_location("frontiermath", STEP_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_minor_version_survives():
    step = load_step_module()
    # Two builds Epoch names apart must not end up as one entity.
    assert step.build_model_name("claude-opus-4-6_max", "Claude Opus 4.6", {}) == "Claude Opus 4.6, max"
    assert step.build_model_name("claude-opus-4-8_max", "Claude Opus 4.8", {}) == "Claude Opus 4.8, max"


def test_build_date_is_appended_only_when_the_name_lacks_one():
    step = load_step_module()
    # Epoch reuses one name for both GPT-4o builds, so the date in the version string does the separating.
    assert step.build_model_name("gpt-4o-2024-08-06", "GPT-4o", {}) == "GPT-4o (Aug 2024)"
    assert step.build_model_name("gpt-4o-2024-11-20", "GPT-4o", {}) == "GPT-4o (Nov 2024)"
    # ...but Epoch sometimes dates the name itself, and then a second date would be appended on top.
    name = "Claude 3.5 Sonnet (October 2024)"
    assert step.build_model_name("claude-3-5-sonnet-20241022", name, {}) == name


def test_unrecognized_setting_is_kept():
    step = load_step_module()
    # "none" is outside the usual high/medium/low vocabulary; dropping it would merge this evaluation with
    # a bare "gpt-5.1-2025-11-13" run.
    assert step.build_model_name("gpt-5.1-2025-11-13_none", "GPT-5.1", {}) == "GPT-5.1 (Nov 2025), none"


def test_override_replaces_the_name_but_keeps_the_setting():
    step = load_step_module()
    overrides = {"gemini-2.5-pro-preview-06-05": "Gemini 2.5 Pro (preview)"}
    assert step.build_model_name("gemini-2.5-pro-preview-06-05", "Gemini 2.5 Pro (Jun 2025)", overrides) == (
        "Gemini 2.5 Pro (preview)"
    )
    assert step.build_model_name("claude-opus-4-6_max", "Claude Opus 4.6", {"claude-opus-4-6_max": "Opus"}) == (
        "Opus, max"
    )


def test_model_missing_from_the_registry_falls_back_to_its_version_string():
    step = load_step_module()
    assert step.build_model_name("Baichuan2-13B-Chat", None, {}) == "Baichuan2-13B-Chat"
    assert step.build_model_name("Baichuan2-13B-Chat", pd.NA, {}) == "Baichuan2-13B-Chat"


def test_sanity_check_rejects_two_models_with_one_name():
    step = load_step_module()

    def output(names):
        return Table(
            pd.DataFrame(
                {
                    "model_version": ["claude-opus-4-6_max", "claude-opus-4-8_max"],
                    "model_name": names,
                    "mean_score": [40.7, 47.2],
                }
            )
        )

    with pytest.raises(AssertionError, match="share a name"):
        step.sanity_check_outputs(output(["Claude Opus 4, max", "Claude Opus 4, max"]))

    step.sanity_check_outputs(output(["Claude Opus 4.6, max", "Claude Opus 4.8, max"]))


def test_sanity_check_rejects_scores_that_are_no_longer_fractions():
    step = load_step_module()
    inputs = Table(
        pd.DataFrame(
            {
                "model_version": ["claude-opus-4-8_max"],
                "release_date": ["2026-05-28"],
                "mean_score": [47.24],
            }
        )
    )
    registry = Table(pd.DataFrame({"model_version": ["claude-opus-4-8_max"], "model_name": ["Claude Opus 4.8"]}))
    with pytest.raises(AssertionError, match="still published as fractions"):
        step.sanity_check_inputs(inputs, registry)

    inputs["mean_score"] = [0.4724]
    step.sanity_check_inputs(inputs, registry)
