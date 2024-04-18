import pandas as pd
from owid.catalog import Dataset, DatasetMeta, Table

from etl.datadiff import DatasetDiff


def _create_datasets(tmp_path):
    (tmp_path / "catalog_a").mkdir()
    (tmp_path / "catalog_b").mkdir()

    ds_meta_a = DatasetMeta(namespace="n", version="v", short_name="ds", source_checksum="1")
    ds_a = Dataset.create_empty(tmp_path / "catalog_a" / "ds", ds_meta_a)
    ds_a.metadata.channel = "garden"  # type: ignore

    ds_meta_b = DatasetMeta(namespace="n", version="v", short_name="ds", source_checksum="2")
    ds_b = Dataset.create_empty(tmp_path / "catalog_b" / "ds", ds_meta_b)
    ds_b.metadata.channel = "garden"  # type: ignore

    return ds_a, ds_b


def test_DatasetDiff_summary(tmp_path):
    ds_a, ds_b = _create_datasets(tmp_path)

    tab_a = Table(pd.DataFrame({"a": [1, 2]}), short_name="tab")
    tab_a.metadata.description = "tab"

    tab_b = Table(pd.DataFrame({"a": [1, 3], "b": ["a", "b"]}), short_name="tab")
    tab_b["a"].metadata.description = "col a"

    ds_a.add(tab_a)
    ds_b.add(tab_b)

    out = []
    differ = DatasetDiff(ds_a, ds_b, print=lambda x: out.append(x))
    differ.summary()

    assert out == [
        "[white]= Dataset [b]garden/n/v/ds[/b]",
        "\t[yellow]~ Table [b]tab[/b] (changed [u]metadata[/u])",
        "\t\t[yellow]~ Column [b]a[/b] (changed [u]metadata[/u], changed [u]data[/u])",
        "\t\t[green]+ Column [b]b[/b]",
    ]


def test_new_data(tmp_path):
    ds_a, ds_b = _create_datasets(tmp_path)

    tab_a = Table({"country": ["UK", "US"], "a": [1, 3]}, short_name="tab")
    tab_b = Table({"country": ["UK", "US", "FR"], "a": [1, 2, 3]}, short_name="tab")

    ds_a.add(tab_a)
    ds_b.add(tab_b)

    out = []
    differ = DatasetDiff(ds_a, ds_b, print=lambda x: out.append(x), verbose=True)
    differ.summary()

    assert out == [
        "[white]= Dataset [b]garden/n/v/ds[/b]",
        "\t[white]= Table [b]tab[/b]",
        "\t\t[yellow]~ Dim [b]country[/b]",
        "\t\t\t\t[violet]+ New values: 1 / 3 (33.33%)\n\t\t\t\t[violet]  country\n\t\t\t\t[violet]       FR",
        "\t\t[yellow]~ Column [b]a[/b] (new [u]data[/u], changed [u]data[/u])",
        "\t\t\t\t[violet]+ New values: 1 / 3 (33.33%)\n\t\t\t\t[violet]  country  a\n\t\t\t\t[violet]       FR  3\n\t\t\t\t[violet]~ Changed values: 1 / 3 (33.33%)\n\t\t\t\t[violet]  country  a -  a +\n\t\t\t\t[violet]       US  3.0    2",
    ]
