import pytest
import yaml

from etl.harmonize import _add_alias_to_regions


def test_add_alias_to_regions():
    yaml_content = """
- code: "CYP"
  name: "Cyprus"
  related:
    - "OWID_CYN"

- code: "CZE"
  name: "Czechia"
  aliases:
    - "Czech Republic"
"""
    y = yaml.safe_load(_add_alias_to_regions(yaml_content, "Cyprus", "Alias"))
    assert y[0]["aliases"] == ["Alias"]

    y = yaml.safe_load(_add_alias_to_regions(yaml_content, "Czechia", "Alias"))
    assert y[1]["aliases"] == ["Czech Republic", "Alias"]

    with pytest.raises(ValueError):
        yaml.safe_load(_add_alias_to_regions(yaml_content, "Unknown", "Alias"))
