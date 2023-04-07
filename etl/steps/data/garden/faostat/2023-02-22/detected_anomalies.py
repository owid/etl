"""This module contains a class for each type of data anomaly detected.

If after a data update an anomaly is no longer in the data, remove the corresponding class from this module.

See documentation of class DataAnomaly below for more details on how anomaly classes are structured.

"""
import abc
import os
from typing import Tuple

import pandas as pd
import plotly.express as px
from structlog import get_logger

log = get_logger()

# Sentence to add before describing data anomalies (if there is any).
ANOMALY_DESCRIPTION_INTRODUCTION = "\n\nProcessing of possible data anomalies by Our World in Data:"

# If environment variable INSPECT_ANOMALIES is set to True, run the step in interactive mode.
INSPECT_ANOMALIES = bool(os.getenv("INSPECT_ANOMALIES", False))


class DataAnomaly(abc.ABC):
    """Abstract class for a certain type of data anomaly."""

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
    def check(self, df: pd.DataFrame) -> None:
        """A method that ensures the anomaly exists in the data.

        This is useful to detect if an anomaly has been corrected after a data update.

        Parameters
        ----------
        df : pd.DataFrame
            Data containing anomalies.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def fix(self, df: pd.DataFrame) -> pd.DataFrame:
        """A method that removes the anomaly.

        Parameters
        ----------
        df : pd.DataFrame
            Data that contains anomalies to be removed.

        Returns
        -------
        df_fixed : pd.DataFrame
            Data after removing anomalies.
        """
        raise NotImplementedError

    def inspect(self, df: pd.DataFrame) -> None:
        """An optional method that plots (in the browser) a visualization that shows the anomaly.

        It can be used before and after removing the anomalies.

        Parameters
        ----------
        df : pd.DataFrame
            Data to be inspected (before or after having anomalies removed).
        """
        raise NotImplementedError

    def handle_anomalies(self, df: pd.DataFrame, inspect_anomalies: bool = INSPECT_ANOMALIES) -> pd.DataFrame:
        """A helper method that uses all the previous methods in the usual order.

        Parameters
        ----------
        df : pd.DataFrame
            Data with anomalies.
        inspect_anomalies : bool, optional
            True to open charts in the browser to visualize the data before and after removing the anomalies.

        Returns
        -------
        df_fixed : pd.DataFrame
            Data after removing anomalies.
        """
        log.info(f"Handling anomaly: {self.description}")
        log.info("Checking that known data anomalies are present in the data")
        self.check(df=df)

        if inspect_anomalies:
            log.info("Inspect anomaly before fixing.")
            self.inspect(df=df)

        log.info("Fixing anomalies.")
        df_fixed = self.fix(df=df)

        if inspect_anomalies:
            log.info("Inspect anomaly after fixing.")
            self.inspect(df=df_fixed)

        return df_fixed


def _split_long_title(text: str) -> str:
    """Split a given text to have at most 100 characters per line, where line breaks are noted by the HTML tag <br>."""
    # I couldn't find an easy way to show a figure below a long text, so I split the title into lines of a fixed width.
    line_length = 100
    html_text = "<br>".join([text[i : i + line_length] for i in range(0, len(text), line_length)])

    return html_text


class SpinachAreaHarvestedAnomaly(DataAnomaly):
    description = (  # type: ignore
        "The area harvested of spinach for China (which refers to mainland) in 1984 is missing. "
        "This causes that other regions that are aggregates which include China mainland have a spurious reduction in "
        "area harvested of spinach in that year, and a spurious increase in yield. "
        "Therefore, we remove those spurious aggregate values."
    )

    affected_item_codes = [
        "00000373",
    ]
    affected_element_codes = [
        "005312",
        "5312pc",
        "005419",
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

    def check(self, df):
        # Check that the data point is indeed missing.
        assert df[
            (df["country"] == "China")
            & (df["item_code"].isin(self.affected_item_codes))
            & (df["element_code"].isin(self.affected_element_codes))
            & (df["year"].isin(self.affected_years))
        ].empty
        # For consistency, check that other years do have data for the same item and element.
        assert not df[
            (df["country"] == "China")
            & (df["item_code"].isin(self.affected_item_codes))
            & (df["element_code"].isin(self.affected_element_codes))
            & ~(df["year"].isin(self.affected_years))
        ].empty

    def inspect(self, df):
        log.info(
            "The anomaly causes: "
            "\n* A dip in area harvested of spinach in that year (element code 005312). "
            "\n* In terms of per capita area (element code 005312pc), the dip is not as visible. "
            "\n* A big increase in yield (element code 005419) for that year."
        )
        for element_code in self.affected_element_codes:
            selection = (
                (df["country"].isin(self.affected_countries))
                & (df["item_code"].isin(self.affected_item_codes))
                & (df["element_code"] == element_code)
            )
            df_affected = df[selection].astype({"country": str})
            title = _split_long_title(self.description + f"Element code {element_code}")
            fig = px.line(df_affected, x="year", y="value", color="country", title=title)
            fig.show()

    def fix(self, df):
        indexes_to_drop = df[
            (
                (df["country"].isin(self.affected_countries))
                & (df["item_code"].isin(self.affected_item_codes))
                & (df["element_code"].isin(self.affected_element_codes))
                & (df["year"].isin(self.affected_years))
            )
        ].index
        df_fixed = df.drop(indexes_to_drop).reset_index(drop=True)

        return df_fixed


class CocoaBeansFoodAvailableAnomaly(DataAnomaly):
    description = (  # type: ignore
        "Food available for consumption for cocoa beans from 2010 onwards presents many zeros for different countries. "
        "These zeros are likely to correspond to missing data. "
        "This issue may be caused by a change in FAO methodology precisely on 2010. "
        "Therefore, to be conservative, we eliminate those zeros and treat them as missing values. "
        "For aggregate regions (like continents), data from 2010 onwards is not zero, but a small number (resulting "
        "from summing many spurious zeros). "
        "Therefore, we also remove data for region aggregates from 2010 onwards."
    )

    affected_item_codes = [
        "00002633",
    ]
    affected_element_codes = [
        "000645",
        "0645pc",
        "005142",
        "5142pc",
    ]
    # List of countries with value of exactly zero for all years after 2010.
    # This list does not need to include all countries with that problem (it's used just to check they are still zero).
    expected_countries_with_all_zero = [
        "United States",
        "China",
        "Norway",
    ]

    def check(self, df):
        assert (
            df[
                (
                    (df["item_code"].isin(self.affected_item_codes))
                    & (df["element_code"].isin(self.affected_element_codes))
                    & (df["year"] >= 2010)
                    & (df["country"].isin(self.expected_countries_with_all_zero))
                )
            ]["value"]
            == 0
        ).all()
        # Check that, for the same countries, there is at least one value prior to 2010 where value is not zero.
        assert (
            df[
                (
                    (df["item_code"].isin(self.affected_item_codes))
                    & (df["element_code"].isin(self.affected_element_codes))
                    & (df["year"] < 2010)
                    & (df["country"].isin(self.expected_countries_with_all_zero))
                )
            ]["value"]
            > 0
        ).any()

    def inspect(self, df):
        log.info(
            "The anomaly causes: "
            "\n* Zeros from 2010 onwards. "
            "\n* I's usually zero all years, but some countries also have single non-zero values (e.g. Afghanistan)."
        )
        for element_code in self.affected_element_codes:
            selection = (df["item_code"].isin(self.affected_item_codes)) & (df["element_code"] == element_code)
            df_affected = df[selection].astype({"country": str}).sort_values(["country", "year"])
            title = _split_long_title(self.description + f"Element code {element_code}")
            fig = px.line(df_affected, x="year", y="value", color="country", title=title, markers=True)
            fig.show()

    def fix(self, df):
        # Remove all possibly spurious zeros from 2010 onwards in all countries.
        indexes_to_drop = df[
            (
                (df["year"] > 2010)
                & (df["item_code"].isin(self.affected_item_codes))
                & (df["element_code"].isin(self.affected_element_codes))
                & (df["value"] == 0)
            )
        ].index.tolist()
        # Additionally, remove all data for region aggregates from 2010 onwards.
        # List of possibly affected region aggregates, including all original FAO region aggregates.
        aggregates = [
            "North America",
            "South America",
            "Europe",
            "European Union (27)",
            "Africa",
            "Asia",
            "Oceania",
            "Low-income countries",
            "Upper-middle-income countries",
            "Lower-middle-income countries",
            "High-income countries",
            "World",
        ] + sorted(set(df[df["country"].str.contains("FAO")]["country"]))
        indexes_to_drop.extend(
            df[
                (df["country"].isin(aggregates))
                & (df["year"] >= 2010)
                & (df["item_code"].isin(self.affected_item_codes))
                & (df["element_code"].isin(self.affected_element_codes))
            ].index.tolist()
        )

        df_fixed = df.drop(indexes_to_drop).reset_index(drop=True)

        return df_fixed


class EggYieldNorthernEuropeAnomaly(DataAnomaly):
    description = (  # type: ignore
        "The amount of eggs produced per bird for Northern Europe (FAO) is unreasonably high before 1973, with values "
        "between 50kg and 115kg, while from 1973 on it has more reasonable values, below 20kg. "
        "Therefore, we remove all values for Northern Europe (FAO) between 1961 and 1972."
    )

    affected_item_codes = [
        "00001783",
    ]
    affected_element_codes = [
        "005410",
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

    def check(self, df):
        # Check that the data prior to 1973 is indeed higher than expected, and significantly lower from then on.
        assert (
            df[
                (df["country"].isin(self.affected_countries))
                & (df["item_code"].isin(self.affected_item_codes))
                & (df["element_code"].isin(self.affected_element_codes))
                & (df["year"].isin(self.affected_years))
            ]["value"]
            > 40
        ).all()
        assert (
            df[
                (df["country"].isin(self.affected_countries))
                & (df["item_code"].isin(self.affected_item_codes))
                & (df["element_code"].isin(self.affected_element_codes))
                & ~(df["year"].isin(self.affected_years))
            ]["value"]
            < 40
        ).all()

    def inspect(self, df):
        log.info(
            "The anomaly causes: "
            "\n* The egg yield of Northern Europe (FAO) before 1973 much higher than any other year."
        )
        for element_code in self.affected_element_codes:
            selection = (df["item_code"].isin(self.affected_item_codes)) & (df["element_code"] == element_code)
            df_affected = df[selection].astype({"country": str}).sort_values(["country", "year"])
            title = _split_long_title(self.description + f"Element code {element_code}")
            fig = px.line(df_affected, x="year", y="value", color="country", title=title)
            fig.show()

    def fix(self, df):
        indexes_to_drop = df[
            (
                (df["country"].isin(self.affected_countries))
                & (df["item_code"].isin(self.affected_item_codes))
                & (df["element_code"].isin(self.affected_element_codes))
                & (df["year"].isin(self.affected_years))
            )
        ].index
        df_fixed = df.drop(indexes_to_drop).reset_index(drop=True)

        return df_fixed


class TeaProductionAnomaly(DataAnomaly):
    description = (  # type: ignore
        "Tea production in FAO data increased dramatically from 1990 to 1991 for many different countries (including "
        "some of the main producers, like China and India). However, data from 1991 was flagged as 'Estimated value' "
        "(while data prior to 1991 is flagged as 'Official figure'). This potentially anomalous increase was not "
        "present in the previous version of the data. Therefore, we removed tea production data (as well as "
        "per-capita production and yield) from 1991 onwards."
    )

    affected_item_codes = [
        "00000667",
    ]
    affected_element_codes = [
        "005510",  # Production.
        "5510pc",  # Per capita production.
        "005419",  # Yield.
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

    def check(self, df):
        # Check that the data on 1990 has the flag "A" (Official figure) for each of the affected countries.
        flagged_official = df[
            (df["country"].isin(self.affected_countries))
            & (df["item_code"].isin(self.affected_item_codes))
            & (df["element_code"].isin(self.affected_element_codes))
            & (df["year"] == 1990)
        ]
        flagged_estimate = df[
            (df["country"].isin(self.affected_countries))
            & (df["item_code"].isin(self.affected_item_codes))
            & (df["element_code"].isin(self.affected_element_codes))
            & (df["year"] > 1990)
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

    def inspect(self, df):
        log.info("The anomaly causes: " "\n* The production of tea to increase dramatically from 1990 to 1991.")
        for element_code in self.affected_element_codes:
            selection = (df["item_code"].isin(self.affected_item_codes)) & (df["element_code"] == element_code)
            df_affected = df[selection].astype({"country": str}).sort_values(["country", "year"])
            title = _split_long_title(self.description + f"Element code {element_code}")
            fig = px.line(df_affected, x="year", y="value", color="country", title=title)
            fig.show()

    def fix(self, df):
        indexes_to_drop = df[
            (
                (df["item_code"].isin(self.affected_item_codes))
                & (df["element_code"].isin(self.affected_element_codes))
                & (df["year"] > 1990)
            )
        ].index
        df_fixed = df.drop(indexes_to_drop).reset_index(drop=True)

        return df_fixed


detected_anomalies = {
    "faostat_qcl": [
        SpinachAreaHarvestedAnomaly,
        EggYieldNorthernEuropeAnomaly,
        TeaProductionAnomaly,
    ],
    "faostat_fbsc": [
        CocoaBeansFoodAvailableAnomaly,
    ],
}


def handle_anomalies(dataset_short_name: str, data: pd.DataFrame) -> Tuple[pd.DataFrame, str]:
    if dataset_short_name not in detected_anomalies:
        # If there is no anomaly class for a given dataset, return the same data and an empty anomaly description.
        return data, ""
    else:
        # If there are anomalies, fix them, and return the fixed data and a text describing all anomalies.
        data_fixed = data.copy()
        anomaly_descriptions = ANOMALY_DESCRIPTION_INTRODUCTION

        for anomaly_class in detected_anomalies[dataset_short_name]:
            anomaly = anomaly_class()
            anomaly_descriptions += "\n\n+" + anomaly.description
            data_fixed = anomaly.handle_anomalies(df=data_fixed)

        return data_fixed, anomaly_descriptions
