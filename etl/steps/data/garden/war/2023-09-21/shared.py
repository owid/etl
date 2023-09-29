from typing import List, Type

from owid.catalog import Table
from typing_extensions import Self


class Normaliser:
    """Normalise indicators."""
    country_column: str

    def code_to_region(self: Self) -> None:
        """Convert code to region name."""
        raise NotImplementedError("Subclasses must implement this method")

    @classmethod
    def get_num_countries_per_year(cls: Type[Self], tb: Table) -> Table:
        """Get number of countries (and country-pairs) per region per year.

        `tb` is expected to be the table cow_ssm_system from the cow_ssm dataset.
        """
        tb["region"] = tb["ccode"].apply(cls.code_to_region)

        # Get number of countries per region per year
        tb = (
            tb
            .groupby(["region", "year"], as_index=False)
            .agg({cls.country_column: "nunique"})
            .rename(columns={cls.country_column: "num_countries"})
        )
        # Get number of country-pairs per region per year
        tb["num_country_pairs"] = (tb["num_countries"] * (tb["num_countries"] - 1) / 2).astype(int)

        return tb

    @classmethod
    def add_country_normalised_indicators_cow(cls: Type[Self], tb: Table, tb_codes: Table, columns_to_scale: List[str]) -> Table:
        """Scale columns `columns_to_scale` based on the number of countries (and country-pairs) in each region and year.

        For each indicator listed in `columns_to_scale`, two new columns are added to the table:
        - `{indicator}_per_country`: the indicator value divided by the number of countries in the region and year.
        - `{indicator}_per_country_pair`: the indicator value divided by the number of country-pairs in the region and year.
        """
        # From raw cow_ssm_system table get number of countryes (and country-pairs) per region per year
        tb_codes = cls.get_num_countries_per_year(tb_codes)
        # Merge with main table
        tb = tb.merge(tb_codes, on=["year", "region"], how="left")

        for col in columns_to_scale:
            tb[f"{col}_per_country"] = tb[col] / tb["num_countries"]
            tb[f"{col}_per_country_pair"] = tb[col] / tb["num_country_pairs"]
        return tb


class COWNormaliser(Normaliser):
    """Normalise COW data based on the number of countries (and country-pairs) in each region and year."""
    country_column: str = "statenme"

    @classmethod
    def code_to_region(cls: Type[Self], cow_code: int) -> str:
        """Convert code to region name."""
        match cow_code:
            case c if 2 <= c <= 165:
                return "Americas"
            case c if 200 <= c <= 399:
                return "Europe"
            case c if 402 <= c <= 626:
                return "Africa"
            case c if 630 <= c <= 698:
                return "Middle East"
            case c if 700 <= c <= 999:
                return "Asia and Oceania"
            case _:
                raise ValueError(f"Invalid COW code: {cow_code}")
