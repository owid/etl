"""WB Gender Garden step."""
from pathlib import Path

from owid import catalog
from owid.datautils import geo

from etl.paths import BASE_DIR as base_path


def load_meadow_ds() -> catalog.Dataset:
    meadow_path = base_path / "data/meadow/wb/2022/wb_gender"
    ds = catalog.Dataset(meadow_path)
    return ds


def clean_data(ds: catalog.Dataset) -> catalog.Table:
    df = ds["data"].reset_index()
    # Harmonize country names
    df = geo.harmonize_countries(
        df=df,
        countries_file=Path(__file__) / "wb_gender.country_std.csv",
        country_col="location",
        make_missing_countries_nan=True,
        show_full_warning=False,
    )
    # Drop countries without hamonized name
    df = df.dropna(subset=["location"])
    # Set index
    column_idx = ["location", "variable", "year"]
    df.sort_values(column_idx).set_index(column_idx)
    return df
