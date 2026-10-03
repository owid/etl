import pandas as pd

from apps.wizard.utils import as_valid_json, cache_all


def test_as_valid_json():
    s = "[{'display': {'name': \"Prevalence of Alzheimer's disease and dementia\"}}]"
    assert as_valid_json(s) == [{"display": {"name": "Prevalence of Alzheimer's disease and dementia"}}]

    s = '{\n    "time": true\n}'
    assert as_valid_json(s) == {"time": True}


def test_cache_all_passes_original_arguments():
    """The cache key is a canonical (hashable) form of the arguments, but the wrapped function
    must still receive the originals - not the canonical form."""

    @cache_all
    def summarize(df, labels):
        return type(df).__name__, type(labels).__name__, len(df)

    df = pd.DataFrame({"producer": ["a", "a", "b"], "views": [1, 2, 3]})

    assert summarize(df, ["x"]) == ("DataFrame", "list", 3)


def test_cache_all_hits_cache_for_equal_dataframes():
    calls = []

    @cache_all
    def count(df):
        calls.append(df)
        return len(df)

    df = pd.DataFrame({"producer": ["a", "b"], "views": [1, 2]})

    assert count(df) == 2
    assert count(df.copy()) == 2
    assert len(calls) == 1

    # A different frame must not reuse the cached result.
    assert count(pd.DataFrame({"producer": ["a"], "views": [7]})) == 1
    assert len(calls) == 2
