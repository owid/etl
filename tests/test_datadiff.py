import pandas as pd
from owid.catalog import Dataset, DatasetMeta, Table

from etl.datadiff import DatasetDiff, _data_diff


def test_DatasetDiff_summary(tmp_path):
    (tmp_path / "catalog_a").mkdir()
    (tmp_path / "catalog_b").mkdir()

    ds_meta_a = DatasetMeta(namespace="n", version="v", short_name="ds", source_checksum="1")
    ds_a = Dataset.create_empty(tmp_path / "catalog_a" / "ds", ds_meta_a)
    ds_a.metadata.channel = "garden"  # type: ignore

    ds_meta_b = DatasetMeta(namespace="n", version="v", short_name="ds", source_checksum="2")
    ds_b = Dataset.create_empty(tmp_path / "catalog_b" / "ds", ds_meta_b)
    ds_b.metadata.channel = "garden"  # type: ignore

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
        "\t\t[yellow]~ Column [b]a[/b] (changed [u]data & metadata[/u])",
        "\t\t[green]+ Column [b]b[/b]",
    ]


def test_data_diff():
    table_a = Table({"country": ["UK", "US"], "a": [1, 2]})
    table_b = Table({"country": ["UK", "US"], "a": [1, 3]})
    out = _data_diff(table_a, table_b, col="a", dims=["country"], tabs=0)
    print(out)
    assert (
        out
        == """
[violet]- Changed values: 1 / 2 (50.00%)
[violet]- country: US
[violet]- Avg. change: 1.00 (40%)
    """.strip()
    )
