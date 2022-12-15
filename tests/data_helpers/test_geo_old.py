import pandas as pd

from etl import data_helpers as dh


def test_calculate_region_sums():
    # make sure we're not including European Union in World
    df = pd.DataFrame(
        {
            "year": [2000, 2000, 2000, 2000],
            "country": ["France", "Germany", "China", "European Union"],
            "population": [1, 2, 5, 3],
        }
    )
    out = dh.calculate_region_sums(df)
    assert out.to_dict(orient="records") == [
        {"year": 2000, "country": "France", "population": 1},
        {"year": 2000, "country": "Germany", "population": 2},
        {"year": 2000, "country": "China", "population": 5},
        {"year": 2000, "country": "European Union", "population": 3},
        {"year": 2000, "country": "Asia", "population": 5},
        {"year": 2000, "country": "Europe", "population": 3},
        {"year": 2000, "country": "European Union (27)", "population": 3},
        {"year": 2000, "country": "World", "population": 8},
    ]
