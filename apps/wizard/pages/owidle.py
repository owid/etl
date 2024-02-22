"""Game owidle."""
import datetime as dt
from math import asin, cos, radians, sin, sqrt
from pathlib import Path
from typing import List, Tuple

import geopandas as gpd
import pandas as pd
import plotly.express as px
import streamlit as st
from owid.catalog import Dataset, Table
from st_pages import add_indentation

from etl.paths import DATA_DIR

##########################################
# CONFIG PAGE & SESSION STATE INIT
##########################################
st.set_page_config(page_title="Wizard: owidle", layout="wide", page_icon="🪄")
add_indentation()
st.title(
    body="👾 :rainbow[owidle] ",
)
st.markdown(
    "Guess the country using the population and GDP hints. For each guess, you will get a geographical hint (distance and direction to the country). There is a daily challenge!"
)

## Maximum number of guesses allowed to the user
NUM_GUESSES = 6
default_guess = [
    {
        "guess": "",
        "distance": "",
        "direction": "",
        "score": "",
    }
    for i in range(NUM_GUESSES)
]
# Contains the list of guesses by the user
st.session_state.guesses = st.session_state.get("guesses", default_guess)
# Contains the number of guesses by the user
st.session_state.num_guesses = st.session_state.get("num_guesses", 0)
# Tells whether the user has succeded in guessing the correct country
st.session_state.user_has_succeded = st.session_state.get("user_has_succeded", False)


##########################################
## LOAD DATA
##########################################
COLORS = [
    "#C15065",
    "#2C8465",
    "#286BBB",
    "#6D3E91",
    "#996D39",
    "#BE5915",
]
MAX_DISTANCE_ON_EARTH = 20_000


@st.cache_data
def load_data(placeholder: str) -> Tuple[pd.DataFrame, gpd.GeoDataFrame, str]:
    """Load data for the game."""
    # Load population indicator
    ds = Dataset(DATA_DIR / "garden" / "un" / "2022-07-11" / "un_wpp")
    tb = ds["population"].reset_index()
    tb = tb.loc[
        (tb["metric"] == "population") & (tb["sex"] == "all") & (tb["age"] == "all") & (tb["variant"] == "estimates"),
        ["year", "location", "value"],
    ]
    # df = pd.DataFrame(tb)

    # Load GDP indicator
    ds_gdp = Dataset(DATA_DIR / "garden/worldbank_wdi/2023-05-29/wdi")
    tb_gdp = ds_gdp["wdi"].reset_index()[["year", "country", "ny_gdp_pcap_pp_kd"]]

    # Load geographic info
    ## file from https://gist.github.com/metal3d/5b925077e66194551df949de64e910f6 (later harmonized)
    geo = pd.read_csv(
        Path(__file__).parent / "owidle_countries.csv",
    )
    geo = Table(geo)

    # Combine
    tb = tb.merge(geo, left_on="location", right_on="Country", how="left")
    tb = tb.merge(tb_gdp, left_on=["year", "location"], right_on=["year", "country"], how="left")

    # Remove NaNs
    tb = tb.dropna(subset=["Latitude", "Longitude"])

    # Get indicator df
    tb_indicator = tb[["year", "location", "value", "ny_gdp_pcap_pp_kd"]].rename(
        columns={
            "value": "population",
            "ny_gdp_pcap_pp_kd": "gdp_per_capita",
        }
    )

    # Get geographic df
    df_geo = gpd.GeoDataFrame(
        tb,
        geometry=gpd.points_from_xy(tb.Longitude, y=tb.Latitude),
        crs="EPSG:4326",
    )
    df_geo = df_geo[
        [
            "location",
            "geometry",
        ]
    ].drop_duplicates()
    # df_geo = df_geo.to_crs(3310)

    # Arbitrary daily solution
    seed = (dt.date.today() - dt.date(1993, 7, 13)).days
    solution = tb["location"].sample(random_state=seed).item()

    return tb_indicator, df_geo, solution


DATA, GEO, SOLUTION = load_data("cached")
# st.write(SOLUTION)
OPTIONS = sorted(DATA["location"].unique())


##########################################
# COMPARE GUESS WITH SOLUTION
##########################################
def get_flat_earth_bearing(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Flat-earther bearing

    For globe projected initial or final bearing: https://www.movable-type.co.uk/scripts/latlong.html
    # y = sin(lon2 - lon1) * cos(lat2)
    # x = cos(lat1) * sin(lat2) - sin(lat1) * cos(lat2) * cos(lon2 - lon1)
    # angle = atan2(x, y)
    # bearing = (degrees(angle) + 360) % 360  # in degrees
    """
    from math import atan2, degrees

    relative_lat = lat2 - lat1
    relative_lon = lon2 - lon1
    angle = atan2(relative_lon, relative_lat)

    return degrees(angle)


def haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> int:
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))
    r = 6371  # Radius of earth in kilometers. Use 3956 for miles
    return int(c * r)


def distance_to_solution(country_selected: str) -> Tuple[str, str, str]:
    """Estimate distance (km) from guessed to solution, including direction."""
    # Estimate distance
    # st.write(GEO)
    # GEO_DIST = cast(gpd.GeoDataFrame, GEO.to_crs(3310))
    # GEO = cast(gpd.GeoDataFrame, GEO.to_crs(3310))
    solution = GEO.loc[GEO["location"] == SOLUTION, "geometry"]
    guess = GEO.loc[GEO["location"] == country_selected, "geometry"]

    # Estimate direction
    guess = guess.item()
    solution = solution.item()
    direction = get_flat_earth_bearing(guess.y, guess.x, solution.y, solution.x)
    if (direction > -22.5) & (direction <= 22.5):
        arrow = "⬆️"
    elif (direction > 22.5) & (direction <= 67.5):
        arrow = "↗️"
    elif (direction > 67.5) & (direction <= 112.5):
        arrow = "➡️"
    elif (direction > 112.5) & (direction <= 157.5):
        arrow = "↘️"
    elif (direction > 157.5) | (direction <= -157.5):
        arrow = "⬇️"
    elif (direction > -157.5) & (direction <= -112.5):
        arrow = "↙️"
    elif (direction > -112.5) & (direction <= -67.5):
        arrow = "⬅️"
    else:
        arrow = "↖️"

    distance = haversine(guess.y, guess.x, solution.y, solution.x)

    if country_selected == SOLUTION:
        st.session_state.user_has_succeded = True
        return "0", "🎉", "100"

    # Estimate score
    score = int(round(100 - (distance / MAX_DISTANCE_ON_EARTH) * 100, 0))

    # Ensure string types
    score = str(score)
    distance = str(distance)

    return distance, arrow, score


# Actions once user clicks on "GUESS" button
def guess() -> None:
    """Actions performed once the user clicks on 'GUESS' button."""
    # If guess is None, don't do anything
    if st.session_state.guess_last is None:
        pass
    else:
        # Check if guess was already submitted
        if st.session_state.guess_last in [guess["guess"] for guess in st.session_state.guesses]:
            st.toast("⚠️ You have already guessed this country!")
        else:
            # Estimate distance from correct answer
            distance, direction, score = distance_to_solution(st.session_state.guess_last)
            # Add to session state
            st.session_state.guesses[st.session_state.num_guesses] = {
                "guess": st.session_state.guess_last,
                "distance": distance,
                "direction": direction,
                "score": score,
            }
            # Increment number of guesses
            st.session_state.num_guesses += 1


##########################################
# PLOT CHART
##########################################


def _plot_chart(
    countries_guessed: List[str],
    column_indicator: str,
    title: str,
    column_country: str,
    indicator_name: str | None = None,
):
    # Filter out solution countri if given within guessed countries
    countries_guessed = [c for c in countries_guessed if c != SOLUTION]
    countries_to_plot = [SOLUTION] + countries_guessed

    # Filter only data for countries to plot
    df = DATA[DATA["location"].isin(countries_to_plot)].reset_index(drop=True)

    # Sort
    priority = {c: i for i, c in enumerate(countries_to_plot)}
    df["priority"] = df[column_country].map(priority)
    df = df.sort_values(["priority", "year"])

    # Map locations to lcolor
    color_map = dict(zip(countries_to_plot, COLORS))
    color_map["?"] = color_map[SOLUTION]

    # Map locations to line dash
    line_dash_map = {
        **{c: "dashdot" for c in countries_guessed},
        SOLUTION: "solid",
        "?": "solid",
    }

    # Hide country name if user has not succeded yet.
    if not st.session_state.user_has_succeded:
        df[column_country] = df[column_country].replace({SOLUTION: "?"})

    # Change location column name to "Country"
    df["Country"] = df[column_country]

    # Drop NaN
    df = df.dropna(subset=[column_indicator])

    # Create plotly figure & plot
    fig = px.line(
        df,
        x="year",
        y=column_indicator,
        title=title,
        color="Country",
        color_discrete_map=color_map,
        line_dash="Country",
        line_dash_map=line_dash_map,
        # labels={
        #     "year": "Year",
        #     column_indicator: indicator_name if indicator_name else column_indicator,
        # },
        # markers=True,
        line_shape="spline",
    )

    # Remove axis labels
    fig.update_layout(xaxis_title=None, yaxis_title=None)
    # Legends
    fig.update_layout(
        legend=dict(
            orientation="h",
            # yanchor="bottom",
            # y=1.02,  # Places the legend above the chart
            # xanchor="center",
            # x=0.5,
        )
    )

    st.plotly_chart(
        fig,
        theme="streamlit",
        use_container_width=True,
    )


@st.cache_data
def plot_chart_population(countries_guessed: List[str]):
    """Plot timeseries."""
    _plot_chart(
        countries_guessed,
        column_indicator="population",
        title="Population",
        column_country="location",
        indicator_name="Population",
    )


@st.cache_data
def plot_chart_gdp_pc(countries_guessed: List[str]):
    """Plot timeseries."""
    _plot_chart(
        countries_guessed,
        column_indicator="gdp_per_capita",
        title="GDP per capita (constant 2017 intl-$)",
        column_country="location",
        indicator_name="GDP per capita (constant 2017 intl-$)",
    )


def display_metadata(metadata):
    if not isinstance(metadata, dict):
        metadata = metadata.to_dict()
    ds = pd.Series(metadata).astype(str)
    ds.name = "value"
    st.table(data=ds)


##########################################
# PLOT CHARTS
##########################################
with st.container(border=True):
    countries_guessed = [guess["guess"] for guess in st.session_state.guesses if guess["guess"] != ""]
    col1, col2 = st.columns(2)
    with col1:
        plot_chart_population(countries_guessed)
        with st.expander("Sources"):
            try:
                metadata = DATA["population"].metadata.to_dict()
                origins = metadata.pop("origins", None)
                _ = metadata.pop("display", None)
                # display_metadata(metadata)
                if origins:
                    for origin in origins:
                        display_metadata(origin)
            except Exception:
                st.info("Metadata couldn't be accssed")
    with col2:
        plot_chart_gdp_pc(countries_guessed)
        with st.expander("Sources"):
            try:
                metadata = DATA["gdp_per_capita"].metadata.to_dict()
                origins = metadata.pop("origins", None)
                _ = metadata.pop("display", None)
                display_metadata(metadata)
                if origins:
                    for origin in origins:
                        display_metadata(origin)
                # else:
                #     st.info("")
            except Exception:
                st.info("Metadata couldn't be accssed")

##########################################
# INPUT FROM USER
##########################################
with st.form("form_guess", border=False, clear_on_submit=True):
    col_guess_1, col_guess_2 = st.columns([4, 1])
    # with col_guess_1:
    value = st.selectbox(
        label="Guess a country",
        placeholder="Choose a country... ",
        options=OPTIONS,
        label_visibility="collapsed",
        index=None,
        key="guess_last",
    )
    # Disable button if user has finished their guesses or has succeeded
    disabled = (st.session_state.num_guesses >= NUM_GUESSES) or st.session_state.user_has_succeded
    # Label for button
    if st.session_state.user_has_succeded:
        label = "YOU GUESSED IT!"
    elif st.session_state.num_guesses >= NUM_GUESSES:
        label = "MAYBE NEXT TIME!"
    else:
        label = f"GUESS {st.session_state.num_guesses + 1}/6"

    # Show button
    btn = st.form_submit_button(
        label=label,
        type="primary",
        use_container_width=True,
        on_click=lambda: guess(),
        disabled=disabled,
    )

    st.session_state["placeholder_warning_repear"] = st.container()
##########################################
# SHOW GUESSES (this far)
##########################################
guesses_display = []
num_guesses_bound = min(st.session_state.num_guesses, NUM_GUESSES)
for i in range(num_guesses_bound):
    with st.container(border=True):
        col1, col2, col3, col4 = st.columns([30, 10, 7, 7])
        with col1:
            # st.text(st.session_state.guesses[i]["guess"])
            st.markdown(f"**{st.session_state.guesses[i]['guess']}**")
        with col2:
            st.markdown(f"{st.session_state.guesses[i]['distance']}km")
        with col3:
            st.markdown(st.session_state.guesses[i]["direction"])
        with col4:
            st.markdown(f"{st.session_state.guesses[i]['score']}%")


##########################################
# SHOW REMAINING GUESSES BOXES
##########################################
for i in range(num_guesses_bound, NUM_GUESSES):
    with st.container(border=True):
        st.empty()

##########################################
# FINAL MESSAGE
##########################################
## Successful
if st.session_state.user_has_succeded:
    st.balloons()
    st.success("🎉 You have guessed the correct country! 🎉")
    col, _ = st.columns(2)
    with col:
        s = []
        for count, i in enumerate(range(num_guesses_bound)):
            _s = f"{st.session_state.guesses[i]['distance']}km"
            s.append(_s)

        r = "round" if num_guesses_bound == 1 else "rounds"
        s = (
            f"I did the daily owidle challenge in {num_guesses_bound} {r}!\n\n"
            + " → ".join(s)
            + "\n\nVisit http://etl.owid.io/wizard/owidle"
        )
        st.subheader("Share it")
        st.code(s)
    st.stop()
## Unsuccessful
elif st.session_state.num_guesses >= NUM_GUESSES:
    st.error(f"The correct answer was **{SOLUTION}**. Better luck next time!")
    st.stop()
