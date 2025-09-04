"""This module contains a class for each type of data anomaly detected.

If after a data update an anomaly is no longer in the data, remove the corresponding class from this module.

See documentation of class DataAnomaly below for more details on how anomaly classes are structured.

"""

import abc
import os
from typing import Tuple

import plotly.express as px
from owid.catalog import Table
from structlog import get_logger

log = get_logger()

# Sentence to add before describing data anomalies (if there is any).
ANOMALY_DESCRIPTION_INTRODUCTION = "\n\nProcessing of possible data anomalies by Our World in Data:"

# If environment variable INSPECT_ANOMALIES is set to True, run the step in interactive mode.
INSPECT_ANOMALIES = bool(os.getenv("INSPECT_ANOMALIES", False))


class DataAnomaly(abc.ABC):
    """Abstract class for a certain type of data anomaly."""

    affected_item_codes = []
    affected_element_codes = []
    affected_years = []
    affected_countries = []

    def __init__(self) -> None:
        pass

    @property
    @abc.abstractmethod
    def description(self) -> str:
        """A human-readable text that describes the anomaly.

        NOTE: The description will be added to the dataset metadata description, and hence will be shown in grapher.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def check(self, tb: Table) -> None:
        """A method that ensures the anomaly exists in the data.

        This is useful to detect if an anomaly has been corrected after a data update.

        Parameters
        ----------
        tb : Table
            Data containing anomalies.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def fix(self, tb: Table) -> Table:
        """A method that removes the anomaly.

        Parameters
        ----------
        tb : Table
            Data that contains anomalies to be removed.

        Returns
        -------
        tb_fixed : Table
            Data after removing anomalies.
        """
        raise NotImplementedError

    def inspect(self, tb: Table) -> None:
        """An optional method that plots (in the browser) a visualization that shows the anomaly.

        It can be used before and after removing the anomalies.

        Parameters
        ----------
        tb : Table
            Data to be inspected (before or after having anomalies removed).
        """
        raise NotImplementedError

    def handle_anomalies(self, tb: Table, inspect_anomalies: bool = INSPECT_ANOMALIES) -> Table:
        """A helper method that uses all the previous methods in the usual order.

        Parameters
        ----------
        tb : Table
            Data with anomalies.
        inspect_anomalies : bool, optional
            True to open charts in the browser to visualize the data before and after removing the anomalies.

        Returns
        -------
        tb_fixed : Table
            Data after removing anomalies.
        """
        log.info(f"Handling anomaly: {self.description}")
        log.info("Checking that known data anomalies are present in the data")
        self.check(tb=tb)

        if inspect_anomalies:
            log.info("Inspect anomaly before fixing.")
            self.inspect(tb=tb)

        log.info("Fixing anomalies.")
        tb_fixed = self.fix(tb=tb)

        # Add processing description.
        mask_affected_rows = (tb_fixed["item_code"].isin(self.affected_item_codes)) & (
            tb_fixed["element_code"].isin(self.affected_element_codes)
        )
        tb_fixed.loc[mask_affected_rows, "description_processing"] = tb_fixed[mask_affected_rows][
            "description_processing"
        ].fillna("")
        tb_fixed.loc[mask_affected_rows, "description_processing"] += self.description

        if inspect_anomalies:
            original_description = self.description
            self.description = self.description + " *FIXED*"
            log.info("Inspect anomaly after fixing.")
            self.inspect(tb=tb_fixed)
            self.description = original_description

        return tb_fixed


def _split_long_title(text: str) -> str:
    """Split a given text to have at most 100 characters per line, where line breaks are noted by the HTML tag <br>."""
    # I couldn't find an easy way to show a figure below a long text, so I split the title into lines of a fixed width.
    line_length = 100
    html_text = "<br>".join([text[i : i + line_length] for i in range(0, len(text), line_length)])

    return html_text


class SpinachAreaHarvestedAnomaly(DataAnomaly):
    description = "* The area harvested of spinach for China (which refers to mainland) in 1984 was zero. This caused that other aggregate regions that included China mainland had a spurious reduction in area harvested of spinach in that year, and a spurious increase in yield. Therefore, we removed those spurious aggregate values.\n"

    affected_item_codes = [
        "00000373",
    ]
    affected_element_codes = [
        "005312",
        "5312pc",
        "005412",
    ]
    affected_years = [
        1984,
    ]
    affected_countries = [
        "China",
        "China (FAO)",
        "Asia",
        "Asia (FAO)",
        "Upper-middle-income countries",
        "Eastern Asia (FAO)",
        "World",
    ]

    def check(self, tb):
        # Check that the data point is indeed zero.
        assert (
            tb[
                (tb["country"] == "China")
                & (tb["item_code"].isin(self.affected_item_codes))
                & (tb["element_code"].isin(self.affected_element_codes))
                & (tb["year"].isin(self.affected_years))
            ]["value"]
            == 0
        ).all()
        # For consistency, check that other years do have non-zero data for the same item and element.
        assert (
            tb[
                (tb["country"] == "China")
                & (tb["item_code"].isin(self.affected_item_codes))
                & (tb["element_code"].isin(self.affected_element_codes))
                & ~(tb["year"].isin(self.affected_years))
            ]["value"]
            > 0
        ).all()

    def inspect(self, tb):
        log.info(
            "The anomaly causes: "
            "\n* A dip in area harvested of spinach in that year (element code 005312). "
            "\n* In terms of per capita area (element code 005312pc), the dip is not as visible. "
            "\n* A big increase in yield (element code 005412) for that year."
        )
        for element_code in self.affected_element_codes:
            selection = (
                (tb["country"].isin(self.affected_countries))
                & (tb["item_code"].isin(self.affected_item_codes))
                & (tb["element_code"] == element_code)
            )
            tb_affected = tb[selection].astype({"country": str})
            title = _split_long_title(self.description + f"Element code {element_code}")
            fig = px.line(tb_affected, x="year", y="value", color="country", title=title)
            fig.show()

    def fix(self, tb):
        indexes_to_drop = tb[
            (
                (tb["country"].isin(self.affected_countries))
                & (tb["item_code"].isin(self.affected_item_codes))
                & (tb["element_code"].isin(self.affected_element_codes))
                & (tb["year"].isin(self.affected_years))
            )
        ].index
        tb_fixed = tb.drop(indexes_to_drop).reset_index(drop=True)

        return tb_fixed


class EggYieldNorthernEuropeAnomaly(DataAnomaly):
    description = "* The amount of eggs produced per bird for Northern Europe (FAO) was unreasonably high before 1973, with values between 857 and 1961, while from 1973 on values were more reasonable, below 241. Therefore, we removed all values for Northern Europe (FAO) between 1961 and 1972.\n"

    affected_item_codes = [
        "00001062",
    ]
    affected_element_codes = [
        "005413",
    ]
    affected_years = [
        1961,
        1962,
        1963,
        1964,
        1965,
        1966,
        1967,
        1968,
        1969,
        1970,
        1971,
        1972,
    ]
    affected_countries = [
        "Northern Europe (FAO)",
    ]

    def check(self, tb):
        # Check that the data prior to 1973 is indeed higher than expected, and significantly lower from then on.
        assert (
            tb[
                (tb["country"].isin(self.affected_countries))
                & (tb["item_code"].isin(self.affected_item_codes))
                & (tb["element_code"].isin(self.affected_element_codes))
                & (tb["year"].isin(self.affected_years))
            ]["value"]
            > 850
        ).all()
        assert (
            tb[
                (tb["country"].isin(self.affected_countries))
                & (tb["item_code"].isin(self.affected_item_codes))
                & (tb["element_code"].isin(self.affected_element_codes))
                & ~(tb["year"].isin(self.affected_years))
            ]["value"]
            < 365
        ).all()

    def inspect(self, tb):
        log.info(
            "The anomaly causes: "
            "\n* The egg yield of Northern Europe (FAO) before 1973 much higher than any other year."
        )
        for element_code in self.affected_element_codes:
            selection = (tb["item_code"].isin(self.affected_item_codes)) & (tb["element_code"] == element_code)
            tb_affected = tb[selection].astype({"country": str}).sort_values(["country", "year"])
            title = _split_long_title(self.description + f"Element code {element_code}")
            fig = px.line(tb_affected, x="year", y="value", color="country", title=title)
            fig.show()

    def fix(self, tb):
        indexes_to_drop = tb[
            (
                (tb["country"].isin(self.affected_countries))
                & (tb["item_code"].isin(self.affected_item_codes))
                & (tb["element_code"].isin(self.affected_element_codes))
                & (tb["year"].isin(self.affected_years))
            )
        ].index
        tb_fixed = tb.drop(indexes_to_drop).reset_index(drop=True)

        return tb_fixed


class EggYieldLesothoAnomaly(DataAnomaly):
    description = "* The amount of eggs produced per bird for Lesotho was unreasonably high in 2018, with a value of 2094 eggs per hen. Therefore, we removed this value for Lesotho in 2018.\n"

    affected_item_codes = [
        "00001062",
    ]
    affected_element_codes = [
        "005413",
    ]
    affected_years = [
        2018,
    ]
    affected_countries = [
        "Lesotho",
    ]

    def check(self, tb):
        # Check that the data on that year is indeed higher than expected, and significantly lower on other years.
        assert (
            tb[
                (tb["country"].isin(self.affected_countries))
                & (tb["item_code"].isin(self.affected_item_codes))
                & (tb["element_code"].isin(self.affected_element_codes))
                & (tb["year"].isin(self.affected_years))
            ]["value"]
            > 2000
        ).all()
        assert (
            tb[
                (tb["country"].isin(self.affected_countries))
                & (tb["item_code"].isin(self.affected_item_codes))
                & (tb["element_code"].isin(self.affected_element_codes))
                & ~(tb["year"].isin(self.affected_years))
            ]["value"]
            < 365
        ).all()

    def inspect(self, tb):
        log.info(
            "The anomaly causes: "
            "\n* The egg yield of Northern Europe (FAO) before 1973 much higher than any other year."
        )
        for element_code in self.affected_element_codes:
            selection = (tb["item_code"].isin(self.affected_item_codes)) & (tb["element_code"] == element_code)
            tb_affected = tb[selection].astype({"country": str}).sort_values(["country", "year"])
            title = _split_long_title(self.description + f"Element code {element_code}")
            fig = px.line(tb_affected, x="year", y="value", color="country", title=title)
            fig.show()

    def fix(self, tb):
        indexes_to_drop = tb[
            (
                (tb["country"].isin(self.affected_countries))
                & (tb["item_code"].isin(self.affected_item_codes))
                & (tb["element_code"].isin(self.affected_element_codes))
                & (tb["year"].isin(self.affected_years))
            )
        ].index
        tb_fixed = tb.drop(indexes_to_drop).reset_index(drop=True)

        return tb_fixed


class TeaProductionAnomaly(DataAnomaly):
    description = "* Tea production data in FAOSTAT increases abruptly from 1990 to 1991 for many different countries (including some of the main producers, like China and India). However, data from 1991 is flagged as 'Estimated value' (while data prior to 1991 is flagged as 'Official figure'). Therefore, we decided to remove all tea production data (as well as per-capita production and yield).\n"

    affected_item_codes = [
        "00000667",
    ]
    affected_element_codes = [
        "005510",  # Production.
        "5510pc",  # Per capita production.
        "005412",  # Yield.
    ]
    # Countries affected by the anomaly.
    # NOTE: All countries will be removed (since some of them are the main contributors to tea production), but these
    # ones will be used to check for the anomaly.
    affected_countries = [
        "Africa",
        "Africa (FAO)",
        "Americas (FAO)",
        "Argentina",
        "Asia",
        "Asia (FAO)",
        "Bangladesh",
        "China",
        "China (FAO)",
        "Eastern Africa (FAO)",
        "Eastern Asia (FAO)",
        "India",
        "Indonesia",
        "Iran",
        "Kenya",
        "Land Locked Developing Countries (FAO)",
        "Least Developed Countries (FAO)",
        "Low-income countries",
        "Low Income Food Deficit Countries (FAO)",
        "Lower-middle-income countries",
        "Malawi",
        "Net Food Importing Developing Countries (FAO)",
        "Rwanda",
        "South America",
        "South America (FAO)",
        "South-eastern Asia (FAO)",
        "Southern Asia (FAO)",
        "Sri Lanka",
        "Tanzania",
        "Turkey",
        "Uganda",
        "Upper-middle-income countries",
        "Vietnam",
        "Western Asia (FAO)",
        "World",
        "Zimbabwe",
    ]

    def check(self, tb):
        # Check that the data on 1990 has the flag "A" (Official figure) for each of the affected countries.
        flagged_official = tb[
            (tb["country"].isin(self.affected_countries))
            & (tb["item_code"].isin(self.affected_item_codes))
            & (tb["element_code"].isin(self.affected_element_codes))
            & (tb["year"] == 1990)
        ]
        flagged_estimate = tb[
            (tb["country"].isin(self.affected_countries))
            & (tb["item_code"].isin(self.affected_item_codes))
            & (tb["element_code"].isin(self.affected_element_codes))
            & (tb["year"] > 1990)
        ]
        # Check that all affected countries have official data on 1990, and estimated on 1991.
        for country in self.affected_countries:
            # Assert it for each individual country.
            # Check that tea production increases by at least a factor of 3.
            high_value = flagged_estimate[
                (flagged_estimate["country"] == country) & (flagged_estimate["element_code"] == "005510")
            ]["value"].iloc[0]
            low_value = flagged_official[
                (flagged_official["country"] == country) & (flagged_official["element_code"] == "005510")
            ]["value"].iloc[0]
            assert high_value / low_value > 3

    def inspect(self, tb):
        log.info("The anomaly causes: " "\n* The production of tea to increase dramatically from 1990 to 1991.")
        for element_code in self.affected_element_codes:
            selection = (tb["item_code"].isin(self.affected_item_codes)) & (tb["element_code"] == element_code)
            tb_affected = tb[selection].astype({"country": str}).sort_values(["country", "year"])
            title = _split_long_title(self.description + f"Element code {element_code}")
            fig = px.line(tb_affected, x="year", y="value", color="country", title=title)
            fig.show()

    def fix(self, tb):
        indexes_to_drop = tb[
            ((tb["item_code"].isin(self.affected_item_codes)) & (tb["element_code"].isin(self.affected_element_codes)))
        ].index
        tb_fixed = tb.drop(indexes_to_drop).reset_index(drop=True)

        return tb_fixed


class HighYieldAnomaly(DataAnomaly):
    description = ()  # type: ignore

    affected_item_codes = []
    affected_element_codes = []
    affected_years = []
    affected_countries = []

    def check(self, tb):
        # Check that the data in the affected years is higher than expected, and significantly lower from then on.
        assert (
            tb[
                (tb["country"].isin(self.affected_countries))
                & (tb["item_code"].isin(self.affected_item_codes))
                & (tb["element_code"].isin(self.affected_element_codes))
                & (tb["year"].isin(self.affected_years))
            ]["value"]
            > 100
        ).all()
        assert (
            tb[
                (tb["country"].isin(self.affected_countries))
                & (tb["item_code"].isin(self.affected_item_codes))
                & (tb["element_code"].isin(self.affected_element_codes))
                & ~(tb["year"].isin(self.affected_years))
            ]["value"]
            < 100
        ).all()

    def inspect(self, tb):
        log.info("The anomaly causes: " "\n* The yield of certain items, countries and years to be unreasonably high.")
        for element_code in self.affected_element_codes:
            selection = (tb["item_code"].isin(self.affected_item_codes)) & (tb["element_code"] == element_code)
            tb_affected = tb[selection].astype({"country": str}).sort_values(["country", "year"])
            title = _split_long_title(self.description + f"Element code {element_code}")
            fig = px.line(tb_affected, x="year", y="value", color="country", title=title)
            fig.show()

    def fix(self, tb):
        indexes_to_drop = tb[
            (
                (tb["country"].isin(self.affected_countries))
                & (tb["item_code"].isin(self.affected_item_codes))
                & (tb["element_code"].isin(self.affected_element_codes))
                & (tb["year"].isin(self.affected_years))
            )
        ].index
        tb_fixed = tb.drop(indexes_to_drop).reset_index(drop=True)

        return tb_fixed


class FruitYieldAnomaly(HighYieldAnomaly):
    description = "* Yields were unreasonably high (possibly by a factor of 1000) for some items, countries and years. For example, the yield of item 'Fruit Primary' in Denmark prior to 1985 was larger than 6000 tonnes/ha. Similar issues happened to Antigua and Barbuda and Burkina Faso. Therefore, we removed these possibly spurious values.\n"

    affected_item_codes = [
        # Item code for "Fruit Primary".
        "00001738",
    ]
    affected_element_codes = [
        # Element code for "Yield".
        "005412",
    ]
    affected_years = [
        1961,
        1962,
        1963,
        1964,
        1965,
        1966,
        1967,
        1968,
        1969,
        1970,
        1971,
        1972,
        1973,
        1974,
        1975,
        1976,
        1977,
        1978,
        1979,
        1980,
        1981,
        1982,
        1983,
        1984,
    ]
    affected_countries = [
        "Antigua and Barbuda",
        "Burkina Faso",
        "Denmark",
    ]


class FruitYieldNetherlandsAnomaly(HighYieldAnomaly):
    description = "* Yields were unreasonably high (possibly by a factor of 1000) for some items, countries and years. This happened to item 'Fruit Primary' in Netherlands prior to 1984. Therefore, we removed these possibly spurious values.\n"

    affected_item_codes = [
        # Item code for "Fruit Primary".
        "00001738",
    ]
    affected_element_codes = [
        # Element code for "Yield".
        "005412",
    ]
    affected_years = [
        1961,
        1962,
        1963,
        1964,
        1965,
        1966,
        1967,
        1968,
        1969,
        1970,
        1971,
        1972,
        1973,
        1974,
        1975,
        1976,
        1977,
        1978,
        1979,
        1980,
        1981,
        1982,
        1983,
    ]
    affected_countries = [
        "Netherlands",
    ]


class LocustBeansYieldAnomaly(HighYieldAnomaly):
    description = "* Yields were unreasonably high (possibly by a factor of 1000) for some items, countries and years. This happened to item 'Locust beans (carobs)' for region 'Net Food Importing Developing Countries (FAO)' and 'Lower-middle-income countries'. Therefore, we removed these possibly spurious values.\n"

    affected_item_codes = [
        # Item code for "Locust beans (carobs)".
        "00000461",
    ]
    affected_element_codes = [
        # Element code for "Yield".
        "005412",
    ]
    affected_years = [
        1961,
        1962,
        1963,
        1964,
        1965,
        1966,
        1967,
        1968,
        1969,
        1970,
        1971,
        1972,
        1973,
        1974,
        1975,
        1976,
        1977,
        1978,
        1979,
        1980,
        1981,
        1982,
        1983,
        1984,
    ]
    affected_countries = [
        "Net Food Importing Developing Countries (FAO)",
        "Lower-middle-income countries",
    ]


class WalnutsYieldAnomaly(HighYieldAnomaly):
    description = "* Yields were unreasonably high (possibly by a factor of 1000) for some items, countries and years. This happened to item 'Walnuts, in shell' for region 'Eastern Asia (FAO)'. Therefore, we removed these possibly spurious values.\n"

    affected_item_codes = [
        # Item code for "Walnuts, in shell".
        "00000222",
    ]
    affected_element_codes = [
        # Element code for "Yield".
        "005412",
    ]
    affected_years = [
        1961,
        1962,
        1963,
        1964,
        1965,
        1966,
        1967,
        1968,
        1969,
        1970,
        1971,
        1972,
        1973,
        1974,
        1975,
        1976,
        1977,
        1978,
        1979,
        1980,
        1981,
        1982,
        1983,
        1984,
    ]
    affected_countries = [
        "Eastern Asia (FAO)",
    ]


class OtherTropicalFruitYieldNorthernAfricaAnomaly(HighYieldAnomaly):
    description = "* Yields were unreasonably high (possibly by a factor of 1000) for some items, countries and years. This happened to item 'Other tropical fruits, n.e.c.' for region 'Northern Africa (FAO)'. Therefore, we removed these possibly spurious values.\n"

    affected_item_codes = [
        # Item code for "Other tropical fruits, n.e.c.".
        "00000603",
    ]
    affected_element_codes = [
        # Element code for "Yield".
        "005412",
    ]
    affected_years = [
        1961,
        1962,
        1963,
        1964,
        1965,
        1966,
        1967,
        1968,
        1969,
        1970,
        1971,
        1972,
        1973,
        1974,
        1975,
        1976,
    ]
    affected_countries = [
        "Northern Africa (FAO)",
    ]


class OtherTropicalFruitYieldSouthAmericaAnomaly(HighYieldAnomaly):
    description = "* Yields were unreasonably high (possibly by a factor of 1000) for some items, countries and years. This happened to item 'Other tropical fruits, n.e.c.' for South America. Therefore, we removed these possibly spurious values.\n"

    affected_item_codes = [
        # Item code for "Other tropical fruits, n.e.c.".
        "00000603",
    ]
    affected_element_codes = [
        # Element code for "Yield".
        "005412",
    ]
    affected_years = [
        1961,
        1962,
        1963,
        1964,
        1965,
        1966,
        1967,
        1968,
        1969,
    ]
    affected_countries = [
        "South America (FAO)",
        "South America",
    ]


class UnstableNumberOfPoultryBirdsInEurope(DataAnomaly):
    description = "* The number of poultry birds in Europe, the EU, and High-income countries, had spurious jumps on recent years. The reason was that some European countries (at least Germany, Italy and Spain) lacked data on those years. However, the original FAOSTAT data for Europe and the EU did have data for those years (even though the member countries did not). So, we replaced the affected years of Europe and the EU by the original FAOSTAT data. On the other hand, the affected years for High-income countries were removed (since this aggregate was not included in the original FAOSTAT data).\n"

    affected_item_codes = [
        "00001057",  # Chickens.
        "00002029",  # Poultry.
        "00001079",  # Turkeys.
    ]
    affected_element_codes = [
        "005112",  # Stocks.
        "5112pc",  # Stocks per capita.
    ]
    affected_years = [
        2018,
        2019,
        # NOTE: 2020 is not affected by the issue.
        # 2020,
        2021,
        2022,
        2023,
    ]
    # Countries affected by the anomaly.
    affected_countries = [
        "Europe",
        "European Union (27)",
        "High-income countries",
    ]

    def check(self, tb):
        # Check that the data on the affected years is missing for some European countries.
        assert tb[
            (tb["country"].isin(["Germany", "Spain", "Italy"]))
            & (tb["item_code"].isin(self.affected_item_codes))
            & (tb["element_code"].isin(self.affected_element_codes))
            & (tb["year"].isin([2018, 2019, 2021, 2022]))
        ].empty
        assert tb[
            # NOTE: In 2023, Germany is informed, but not Spain and Italy, which also causes a big dip.
            (tb["country"].isin(["Spain", "Italy"]))
            & (tb["item_code"].isin(self.affected_item_codes))
            & (tb["element_code"].isin(self.affected_element_codes))
            & (tb["year"].isin([2023]))
        ].empty

    def inspect(self, tb):
        log.info(
            "The missing data on the number of poultry birds in European countries causes: "
            f"\n* The number of poultry birds in Europe, EU27 and HEC to jump down on {', '.join(map(str, self.affected_years))}."
        )
        for item_code in self.affected_item_codes:
            for element_code in self.affected_element_codes:
                selection = (
                    (tb["item_code"] == item_code)
                    & (tb["element_code"] == element_code)
                    & (tb["country"].isin(self.affected_countries))
                )
                tb_affected = tb[selection].astype({"country": str}).sort_values(["country", "year"])
                title = _split_long_title(self.description + f" ITEM/ELEMENT: {item_code}/{element_code}")
                fig = px.line(tb_affected, x="year", y="value", color="country", title=title)
                fig.show()

    def fix(self, tb):
        tb_fixed = tb.copy()
        # We will first remove all those values (for all affected items, elements, countries and years).
        # Then, where possible, we will add the original values from FAO.
        # This is only possible for the total number of animals for EU and Europe.
        # For High-income countries, we don't have data from FAO.
        # And we don't keep the original per-capita values from FAO.
        tb_fixed.loc[
            (
                (tb_fixed["item_code"].isin(self.affected_item_codes))
                & (tb_fixed["element_code"].isin(self.affected_element_codes))
                & (tb_fixed["year"].isin(self.affected_years))
                & (tb_fixed["country"].isin(self.affected_countries))
            ),
            "value",
        ] = None

        for country in ["Europe", "European Union (27)"]:
            for item_code in self.affected_item_codes:
                for element_code in ["005112"]:
                    for year in self.affected_years:
                        # First identify the value from FAO.
                        country_fao = f"{country} (FAO)"
                        _value_from_fao = tb[
                            (tb["item_code"] == item_code)
                            & (tb["element_code"] == element_code)
                            & (tb["country"] == country_fao)
                            & (tb["year"] == year)
                        ]["value"]
                        assert len(_value_from_fao) == 1, "Unexpected error in 'UnstableNumberOfPoultryBirdsInEurope'."
                        value_from_fao = _value_from_fao.item()

                        # Now replace the current (non-FAO) value, and replace it with the FAO one.
                        tb_fixed.loc[
                            (
                                (tb_fixed["item_code"] == item_code)
                                & (tb_fixed["element_code"] == element_code)
                                & (tb_fixed["country"] == country)
                                & (tb_fixed["year"] == year)
                            ),
                            "value",
                        ] = value_from_fao

        # Drop rows with no values, if any.
        tb_fixed = tb_fixed.dropna(subset="value").reset_index(drop=True)

        return tb_fixed


class ConstantInsecticidesForAgricultureInAustraliaAnomaly(DataAnomaly):
    description = "* The use of insecticides for agriculture in Australia became a constant from 2018 onwards. When inspecting the data in the FAOSTAT page (https://www.fao.org/faostat/en/#data/RP) we can see that those values are flagged as 'Imputed value' from 2018 until 2022 (whereas 2017 are flagged as 'Official figure'). So, we removed the imputed values from 2018 onwards.\n"

    affected_item_codes = [
        "00001309",  # Insecticides.
    ]
    affected_element_codes = [
        "005157",  # Agricultural use.
    ]
    affected_years = [
        2018,
        2019,
        2020,
        2021,
        2022,
    ]
    # Countries affected by the anomaly.
    affected_countries = ["Australia"]

    def check(self, tb):
        # Check that the data has only one value from 2018 onwards.
        assert set(
            tb[
                (tb["country"].isin(["Australia"]))
                & (tb["item_code"].isin(self.affected_item_codes))
                & (tb["element_code"].isin(self.affected_element_codes))
                & (tb["year"].isin(self.affected_years))
            ]["value"].astype(int)
        ) == {11059}

    def inspect(self, tb):
        log.info(
            "The insecticide use for agriculture in Australia causes: "
            f"\n* The value remains constant for years {', '.join(map(str, self.affected_years))}."
        )
        for element_code in self.affected_element_codes:
            selection = (
                (tb["item_code"].isin(self.affected_item_codes))
                & (tb["element_code"] == element_code)
                & (tb["country"].isin(self.affected_countries))
            )
            tb_affected = tb[selection].astype({"country": str}).sort_values(["country", "year"])
            title = _split_long_title(self.description)
            fig = px.line(tb_affected, x="year", y="value", color="country", title=title)
            fig.show()

    def fix(self, tb):
        indexes_to_drop = tb[
            (
                (tb["item_code"].isin(self.affected_item_codes))
                & (tb["element_code"].isin(self.affected_element_codes))
                & (tb["year"].isin(self.affected_years))
                & (tb["country"].isin(self.affected_countries))
            )
        ].index
        tb_fixed = tb.drop(indexes_to_drop).reset_index(drop=True)

        return tb_fixed


detected_anomalies = {
    "faostat_qcl": [
        SpinachAreaHarvestedAnomaly,
        EggYieldNorthernEuropeAnomaly,
        EggYieldLesothoAnomaly,
        TeaProductionAnomaly,
        FruitYieldAnomaly,
        FruitYieldNetherlandsAnomaly,
        LocustBeansYieldAnomaly,
        WalnutsYieldAnomaly,
        OtherTropicalFruitYieldNorthernAfricaAnomaly,
        OtherTropicalFruitYieldSouthAmericaAnomaly,
        UnstableNumberOfPoultryBirdsInEurope,
    ],
    "faostat_fbsc": [],
    "faostat_rp": [
        ConstantInsecticidesForAgricultureInAustraliaAnomaly,
    ],
}


def handle_anomalies(dataset_short_name: str, tb: Table) -> Tuple[Table, str]:
    if dataset_short_name not in detected_anomalies:
        # If there is no anomaly class for a given dataset, return the same data and an empty anomaly description.
        return tb, ""
    else:
        # If there are anomalies, fix them, and return the fixed data and a text describing all anomalies.
        tb_fixed = tb.copy()
        anomaly_descriptions = ANOMALY_DESCRIPTION_INTRODUCTION

        for anomaly_class in detected_anomalies[dataset_short_name]:
            anomaly = anomaly_class()
            anomaly_descriptions += "\n\n+" + anomaly.description
            tb_fixed = anomaly.handle_anomalies(tb=tb_fixed)

        return tb_fixed, anomaly_descriptions
