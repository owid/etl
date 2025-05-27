from apps.wizard.utils import as_valid_json


def test_as_valid_json():
    s = "[{'display': {'name': \"Prevalence of Alzheimer's disease and dementia\"}}]"
    assert as_valid_json(s) == [{"display": {"name": "Prevalence of Alzheimer's disease and dementia"}}]

    s = '{\n    "time": true\n}'
    assert as_valid_json(s) == {"time": True}
