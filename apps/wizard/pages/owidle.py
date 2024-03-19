"""Game owidle."""
import datetime as dt
import math
from itertools import product
from pathlib import Path
from typing import List, Tuple

import geopandas as gpd
import pandas as pd
import plotly.express as px
import pyproj
import streamlit as st
from geographiclib.geodesic import Geodesic
from owid.catalog import Dataset, Table
from st_pages import add_indentation

from etl.paths import DATA_DIR

##########################################
#
# NEW UPDATE MESSAGES
#
# Add here updates to the app (backend).
# This will be shown in the sidebar if less than 5 days old.
#
##########################################
UPDATES = {
    "2024-03-04": [
        "🐛 Fixed bug in distance estimation.",
        "✨ Bearing angle is now estimated using flat-earth approximation.",
    ],
    "2024-03-05": [
        "🐛 Fixed bug in bearing estimation.",
    ],
    "2024-03-07": [
        "✨ Add score mosaic at the end.",
    ],
    "2024-03-15": [
        "✨ Charts start with 0 in the y-axis",
        "✨ Hard mode: Improved readability of year hint emojis.",
        "🐛 Hard mode: Fixed score mosaic 100%-rounding for years.",
    ],
    "2024-03-18": [
        "✨ Hard mode: If user guesses country, leave it as default selection.",
        "✨ Hard mode: Year emoji hint help message now shows in latest.",
        "🐛 Hard mode: Year score in mosaic was pointing to geographic score.",
    ],
}
DAYS_TO_SHOW_UPDATES = 3
OWID_NUM = (dt.datetime.now(dt.timezone.utc).date() - dt.date(2024, 2, 20)).days
##########################################
#
# CONFIG PAGE & SESSION STATE INIT
#
# Configuration of the session, and page layout.
#
##########################################
st.set_page_config(page_title="Wizard: owidle", layout="wide", page_icon="🪄")
add_indentation()

# Contains the number of guesses by the user
st.session_state.num_guesses = st.session_state.get("num_guesses", 0)
# Tells whether the user has succeded in guessing the correct country
st.session_state.user_has_succeded = st.session_state.get("user_has_succeded", False)
st.session_state.user_has_succeded_country = st.session_state.get("user_has_succeded_country", False)
st.session_state.user_has_succeded_year = st.session_state.get("user_has_succeded_year", False)
# Wether we are playing easy mode
st.session_state.owidle_difficulty = st.session_state.get("owidle_difficulty_", 1)
# Wether we are playing easy mode
st.session_state.guess_last = st.session_state.get("guess_last", None)

## Maximum number of guesses allowed to the user
NUM_GUESSES = 6
if st.session_state.owidle_difficulty == 2:
    NUM_GUESSES += 0

default_guess = [
    {
        "name": "",
        "distance": "",
        "direction": "",
        "score": "",
        "year": "",
        "distance_year": "",
        "direction_year": "",
        "score_year": "",
    }
    for i in range(NUM_GUESSES)
]
# Difficulty levels
DIF_LVLS = {
    0: "Easy",
    1: "Standard",
    2: "Hard",
}
# Contains the list of guesses by the user
st.session_state.guesses = st.session_state.get("guesses", default_guess)
# Number of the minimum number of countries shown if set to EASY mode
NUM_COUNTRIES_EASY_MODE = 6

# Colors for the plot
COLORS = [
    "#C15065",
    "#2C8465",
    "#286BBB",
    "#6D3E91",
    "#996D39",
    "#BE5915",
]

# EXTRA: have multiple rounds
# seed_idx = st.selectbox(
#     label="round",
#     options=[r + 1 for r in range(20)],
#     index=0,
#     format_func=lambda x: f"round {x}",
#     key="seed_idx",
# )

# st.write(seed_idx)
# seeds = []

# for i in range(20):
#     seed = (dt.datetime.now(dt.timezone.utc).date() - dt.date(1999, 7, i + 1)).days
#     seeds.append(seed)
# # st.session_state["seed_idx"] = st.session_state.get("seed_idx_", 0)
# seed_idx = 0
# SEED = seeds[int(seed_idx)]

##########################################
#
# SIDEBAR
#
# Show updates, if any, in the sidebar
#
##########################################

# Side bar if need to display updates
UPDATES = {
    dt_str: updates
    for dt_str, updates in UPDATES.items()
    if dt.datetime.strptime(dt_str, "%Y-%m-%d") >= (dt.datetime.now() - dt.timedelta(days=DAYS_TO_SHOW_UPDATES))
}
if UPDATES:
    with st.sidebar:
        st.markdown("### 🆕 Updates")
        for date, updates in reversed(UPDATES.items()):
            st.markdown(f"#### {date}")
            for update in updates:
                st.info(update)


EQUATOR_IN_KM = 40_075
MAX_DISTANCE_ON_EARTH = EQUATOR_IN_KM / 2
# Projection to use to estimate bearing angles
USE_WGS84 = False


##########################################
#
# TITLE
#
# Title, description and difficulty level
#
##########################################

# TITLE
if st.session_state.owidle_difficulty == 2:
    title = ":red[O W I D L E]"
    st.title(title)
    st.markdown(":red[You already know how this goes, don't you‽]")

    DIF_LVLS_HARD = {
        0: "👶",
        1: "alright",
        2: "🐐",
    }
    st.radio(
        label=":red[Difficulty]",
        options=DIF_LVLS.keys(),
        format_func=lambda x: f":red[{DIF_LVLS_HARD[x]}]",
        key="owidle_difficulty_",
        index=2,
        help="Choose dificulty level. Hard: Year & country must be guessed. Standard: Country must be guessed. Easy: Country must be guessed, but the dropdown will only show countries within the radius of your most recent guess (or the nearest 6 countries).",
        disabled=st.session_state.num_guesses > 0,
    )
else:
    if st.session_state.owidle_difficulty == 0:
        title = "👶 :gray[(beginner)] :rainbow[owidle]"
    else:
        title = "👾 :rainbow[owidle]"
    st.title(title)

    st.markdown(
        "Guess the country using the population and GDP hints. For each guess, you will get a geographical hint (distance and direction to the country). There is a daily challenge!"
    )

    st.radio(
        label="Difficulty",
        options=DIF_LVLS.keys(),
        format_func=lambda x: DIF_LVLS[x],
        key="owidle_difficulty_",
        index=1,
        help="Choose dificulty level. Hard: Year & country must be guessed. Standard: Country must be guessed. Easy: Country must be guessed, but the dropdown will only show countries within the radius of your most recent guess (or the nearest 6 countries).",
        disabled=st.session_state.num_guesses > 0,
    )

##########################################
#
## LOAD DATA
#
# Load data for the game, including indicators, but also geographic data.
#
##########################################


@st.cache_data
def load_data(placeholder: str) -> Tuple[pd.DataFrame, gpd.GeoDataFrame]:
    """Load data for the game."""
    # Load population indicator
    ds = Dataset(DATA_DIR / "garden" / "un" / "2022-07-11" / "un_wpp")
    tb = ds["population"].reset_index()
    tb = tb.loc[
        (tb["metric"] == "population") & (tb["sex"] == "all") & (tb["age"] == "all") & (tb["variant"] == "estimates"),
        ["year", "location", "value"],
    ]
    # df = pd.DataFrame(tb)

    # Load GDP indicator(s)
    ## WDI
    ds = Dataset(DATA_DIR / "garden/worldbank_wdi/2023-05-29/wdi")
    tb_gdp_wdi = ds["wdi"].reset_index()[["year", "country", "ny_gdp_pcap_pp_kd"]]
    tb_gdp_wdi = tb_gdp_wdi.rename(
        columns={
            "ny_gdp_pcap_pp_kd": "gdp_per_capita_wdi",
            "country": "location",
        }
    )
    ## PWT
    ds = Dataset(DATA_DIR / "garden/ggdc/2022-11-28/penn_world_table")
    tb_gdp_pwt = ds["penn_world_table"].reset_index()[["year", "country", "rgdpo_pc"]]
    tb_gdp_pwt = tb_gdp_pwt.rename(
        columns={
            "rgdpo_pc": "gdp_per_capita_pwt",
            "country": "location",
        }
    )
    ## MDP
    ds = Dataset(DATA_DIR / "garden/ggdc/2020-10-01/ggdc_maddison")
    tb_gdp_mdp = ds["maddison_gdp"].reset_index()[["year", "country", "gdp_per_capita"]]
    tb_gdp_mdp = tb_gdp_mdp.rename(
        columns={
            "gdp_per_capita": "gdp_per_capita_mdp",
            "country": "location",
        }
    )
    tb_gdp_mdp = tb_gdp_mdp[tb_gdp_mdp["year"] >= 1950]

    # Load geographic info
    ## file from https://gist.github.com/metal3d/5b925077e66194551df949de64e910f6 (later harmonized)
    geo = pd.read_csv(
        Path(__file__).parent / "owidle_countries.csv",
    )
    geo = Table(geo)

    # Combine
    tb = tb.merge(geo, left_on="location", right_on="Country", how="left")
    tb = tb.merge(tb_gdp_wdi, on=["year", "location"], how="left")
    tb = tb.merge(tb_gdp_pwt, on=["year", "location"], how="left")
    tb = tb.merge(tb_gdp_mdp, on=["year", "location"], how="left")

    # Remove NaNs
    tb = tb.dropna(subset=["Latitude", "Longitude"])

    # Get indicator df
    tb_indicator = tb[
        ["year", "location", "value", "gdp_per_capita_wdi", "gdp_per_capita_pwt", "gdp_per_capita_mdp"]
    ].rename(
        columns={
            "value": "population",
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

    return tb_indicator, df_geo


@st.cache_data
def get_all_distances():
    # Create columns with lat and lon
    df = GEO.copy()
    df["lat"] = df["geometry"].y
    df["lon"] = df["geometry"].x

    # Initialise geod
    geod = pyproj.Geod(ellps="WGS84")

    # Formula to estimate distance in km
    def calculate_distance(lat1, lon1, lat2, lon2):
        _, _, distance = geod.inv(lon1, lat1, lon2, lat2)
        return distance // 1000  # Convert meters to kilometers

    # Get distances for each pair of countries
    distances = []
    for (_, row1), (_, row2) in product(df.iterrows(), repeat=2):
        distance_km = calculate_distance(
            lat1=row1["lat"],
            lon1=row1["lon"],
            lat2=row2["lat"],
            lon2=row2["lon"],
        )
        distances.append(
            {
                "origin": row1["location"],
                "target": row2["location"],
                "distance": distance_km,
            }
        )
    # Create dataframe from list
    distances = pd.DataFrame(distances)

    # Filter own country
    distances = distances[distances["origin"] != distances["target"]]

    # Keep relevant columns, set index
    distances = distances.set_index("origin")

    # Set type
    distances = distances.astype({"distance": "int"})

    # Ensure distances are correct
    assert not (
        distances["distance"] > MAX_DISTANCE_ON_EARTH
    ).any(), f"Unexpected error! Some countries have a distance greater than the maximum distance on Earth ({MAX_DISTANCE_ON_EARTH} km)"
    # Filter own country
    return distances


# Get data
DATA, GEO = load_data("cached")
OPTIONS = sorted(DATA["location"].unique())
# Distances
DISTANCES = get_all_distances()

##########################################
#
## PICK SOLUTION
#
##########################################


@st.cache_data
def pick_solution(difficuty_level: int, seed: int | None = None):
    df_ = DATA.drop_duplicates(subset="location")
    if seed is None:
        seed = (dt.datetime.now(dt.timezone.utc).date() - dt.date(1993, 7, 13)).days
    if difficuty_level == 2:
        seed = 2 * seed * seed
    return df_["location"].sample(random_state=seed).item()


@st.cache_data
def pick_solution_year():
    seed = (dt.datetime.now(dt.timezone.utc).date() - dt.date(1993, 7, 13)).days
    df_ = DATA[DATA["location"] == SOLUTION]
    return df_["year"].max().item(), df_["year"].min().item(), df_["year"].sample(random_state=seed).item()


# Arbitrary daily solution
SOLUTION = pick_solution(st.session_state.owidle_difficulty)
# Year solution
YEAR_MAX, YEAR_MIN, SOLUTION_YEAR = pick_solution_year()

##########################################
#
## INDICATORS
#
##########################################
# Find indicator for this solution (we prioritise PWD > MDP > WDI)
gdp_indicator_titles = {
    "gdp_per_capita_pwt": "GDP per capita (constant 2017 intl-$)",
    "gdp_per_capita_mdp": "GDP per capita (constant 2011 intl-$)",
    "gdp_per_capita_wdi": "GDP per capita (constant 2017 intl-$)",
}
gdp_indicators = list(gdp_indicator_titles.keys())
GDP_INDICATOR = None
for ind in gdp_indicators:
    s = set(
        DATA.dropna(
            subset=[ind],
        )["location"].unique()
    )
    if SOLUTION in s:
        GDP_INDICATOR = ind
        break
if GDP_INDICATOR is None:
    SOLUTION_HAS_GDP = False
    GDP_INDICATOR = gdp_indicators[0]
else:
    SOLUTION_HAS_GDP = True


##########################################
#
# COMPARE GUESS WITH SOLUTION
#
##########################################
def calculate_flat_earth_bearing(lat1, lon1, lat2, lon2):
    # Longitude and latitude differences
    dLon = lon2 - lon1
    dLat = lat2 - lat1

    if abs(dLon) > 180:
        dLon = -(360 - abs(dLon)) if dLon > 0 else (360 - abs(dLon))

    # Calculate approximate bearing
    theta = math.atan2(dLon, dLat)

    # Convert from radians to degrees
    bearing = math.degrees(theta)

    # Adjust so that 0 degrees is North
    bearing = (bearing + 360) % 360

    return bearing


def distance_to_solution(country_selected: str) -> Tuple[str, str, str]:
    """Estimate distance (km) from guessed to solution, including direction.

    ref: https://stackoverflow.com/a/47780264
    """
    # If user has guessed the correct country
    st.session_state.user_has_succeded_country = False
    if country_selected == SOLUTION:
        st.session_state.user_has_succeded_country = True
        return "0", "🎉", "100"
    # Estimate distance
    # st.write(GEO)
    # GEO_DIST = cast(gpd.GeoDataFrame, GEO.to_crs(3310))
    # GEO = cast(gpd.GeoDataFrame, GEO.to_crs(3310))
    df_geo = GEO.copy()
    # df_geo = cast(gpd.GeoDataFrame, df_geo.to_crs(epsg=3395))
    solution = df_geo.loc[df_geo["location"] == SOLUTION, "geometry"]
    guess = df_geo.loc[df_geo["location"] == country_selected, "geometry"]

    # Estimate direction
    guess = guess.item()
    solution = solution.item()

    # Use Geodesic to calculate distance
    # More details:
    # - https://geographiclib.sourceforge.io/Python/doc/examples.html#initializing
    if USE_WGS84:
        geod = Geodesic.WGS84  # type: ignore
        # geod.Inverse returns a Geodesic dictionary (https://geographiclib.sourceforge.io/Python/doc/interface.html#dict)
        print("----------------")
        print(guess.y, guess.x, solution.y, solution.x)
        print("----------------")
        g = geod.Inverse(
            lat1=guess.y,
            lon1=guess.x,
            lat2=solution.y,
            lon2=solution.x,
        )
        bearing = g["azi1"]

        # Get arrow
        if (bearing > -22.5) & (bearing <= 22.5):
            arrow = "⬆️"
        elif (bearing > 22.5) & (bearing <= 67.5):
            arrow = "↗️"
        elif (bearing > 67.5) & (bearing <= 112.5):
            arrow = "➡️"
        elif (bearing > 112.5) & (bearing <= 157.5):
            arrow = "↘️"
        elif (bearing > 157.5) | (bearing <= -157.5):
            arrow = "⬇️"
        elif (bearing > -157.5) & (bearing <= -112.5):
            arrow = "↙️"
        elif (bearing > -112.5) & (bearing <= -67.5):
            arrow = "⬅️"
        else:
            arrow = "↖️"
    else:
        bearing = calculate_flat_earth_bearing(
            lat1=guess.y,
            lon1=guess.x,
            lat2=solution.y,
            lon2=solution.x,
        )

        # Get arrow
        arrow_idx = int(((bearing + 22.5) // 45) % 8)
        arrows = [
            "⬆️",
            "↗️",
            "➡️",
            "↘️",
            "⬇️",
            "↙️",
            "⬅️",
            "↖️",
        ]
        arrow = arrows[arrow_idx]

    # Get distance
    dist = DISTANCES.loc[SOLUTION]
    distance = dist[dist["target"] == country_selected]["distance"].item()
    # Initial bearing angle (angle from guess to solution in degrees)

    # Estimate score
    score = int(round(100 - (distance / MAX_DISTANCE_ON_EARTH) * 100, 0))
    # Only 100 if correct, not if 99.9%
    if score == 100:
        score = 99
    # Ensure string types
    score = str(score)
    distance = str(int(distance))

    return distance, arrow, score


def distance_to_solution_year(year_selected: int) -> Tuple[str, str, str]:
    st.session_state.user_has_succeded_year = False
    diff = SOLUTION_YEAR - year_selected
    if diff == 0:
        st.session_state.user_has_succeded_year = True
        return "0", "🎉", "100"
    elif (diff > 0) and (diff <= 5):
        arrows = "🔥"
    elif (diff > 5) and (diff <= 15):
        arrows = "🔼"
    elif (diff > 15) and (diff <= 30):
        arrows = "⏫️🔼"
    elif diff > 30:
        arrows = "⏫️⏫️⏫️"
    elif (diff < 0) and (diff >= -5):
        arrows = "🔥"
    elif (diff < -5) and (diff >= -15):
        arrows = "🔽"
    elif (diff < -15) and (diff >= -30):
        arrows = "⏬️🔽"
    else:
        arrows = "⏬️⏬️⏬️"
    score = int(round(100 - (abs(diff) / (YEAR_MAX - YEAR_MIN)) * 100, 0))
    # Only 100 if correct, not even if 99.9%
    if score == 100:
        score = 99
    score = str(score)
    return str(abs(diff)), arrows, score


# Actions once user clicks on "GUESS" button
def guess() -> None:
    """Actions performed once the user clicks on 'GUESS' button."""
    # HARD mode
    if st.session_state.owidle_difficulty == 2:
        # If guess is None (either country or year), don't do anything
        if (st.session_state.guess_year_last_submitted is None) or (st.session_state.guess_last_submitted is None):
            pass
        else:
            st.session_state.user_has_guessed = True
            # Check if guess was already submitted
            # if st.session_state.guess_last_submitted in [guess["name"] for guess in st.session_state.guesses]:
            #     st.toast("⚠️ You have already guessed this country!")
            # else:
            # Estimate distance from correct answer
            distance, direction, score = distance_to_solution(st.session_state.guess_last_submitted)
            distance_year, direction_year, score_year = distance_to_solution_year(
                st.session_state.guess_year_last_submitted
            )
            # Update has_succeded flag
            st.session_state.user_has_succeded = (
                st.session_state.user_has_succeded_country and st.session_state.user_has_succeded_year
            )
            # Add to session state
            st.session_state.guess_last = {
                "name": st.session_state.guess_last_submitted,
                "year": st.session_state.guess_year_last_submitted,
                "distance": distance,
                "direction": direction,
                "score": score,
                "distance_year": distance_year,
                "direction_year": direction_year,
                "score_year": score_year,
            }
            st.session_state.guesses[st.session_state.num_guesses] = st.session_state.guess_last
            # Increment number of guesses
            st.session_state.num_guesses += 1
    # STANDARD/EASY mode
    else:
        # If guess is None, don't do anything
        if st.session_state.guess_last_submitted is None:
            pass
        else:
            st.session_state.user_has_guessed = True
            # Check if guess was already submitted
            if st.session_state.guess_last_submitted in [guess["name"] for guess in st.session_state.guesses]:
                st.toast("⚠️ You have already guessed this country!")
            else:
                # Estimate distance from correct answer
                distance, direction, score = distance_to_solution(st.session_state.guess_last_submitted)
                # Update has_succeded flag
                st.session_state.user_has_succeded = st.session_state.user_has_succeded_country
                # Add to session state
                st.session_state.guess_last = {
                    "name": st.session_state.guess_last_submitted,
                    "distance": distance,
                    "direction": direction,
                    "score": score,
                    "year": "",
                    "distance_year": "",
                    "direction_year": "",
                    "score_year": "",
                }
                st.session_state.guesses[st.session_state.num_guesses] = st.session_state.guess_last
                # Increment number of guesses
                st.session_state.num_guesses += 1


##########################################
#
# PLOT CHART
#
# Define functions, actual plot
#
##########################################
def _plot_chart(
    countries_guessed: List[str],
    solution: str,
    column_indicator: str,
    title: str,
    column_country: str,
):
    # Filter out solution countri if given within guessed countries
    countries_guessed = [c for c in countries_guessed if c != solution]
    countries_to_plot = [solution] + countries_guessed

    # Filter only data for countries to plot
    df = DATA[DATA["location"].isin(countries_to_plot)].reset_index(drop=True)

    # Sort
    priority = {c: i for i, c in enumerate(countries_to_plot)}
    df["priority"] = df[column_country].map(priority)
    df = df.sort_values(["priority", "year"])

    # Map locations to lcolor
    color_map = dict(zip(countries_to_plot, COLORS))
    color_map["?"] = color_map[solution]

    # Map locations to line dash
    line_dash_map = {
        **{c: "dashdot" for c in countries_guessed},
        solution: "solid",
        "?": "solid",
    }

    # Hide country name if user has not succeded yet.
    if not st.session_state.user_has_succeded:
        df[column_country] = df[column_country].replace({solution: "?"})

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
        line_shape="spline",
        range_y=[0, df[column_indicator].max() * 1.1],
    )

    # Remove axis labels
    fig.update_layout(xaxis_title=None, yaxis_title=None)
    # Legends
    fig.update_layout(
        legend=dict(
            orientation="h",
        )
    )

    st.plotly_chart(
        fig,
        theme="streamlit",
        use_container_width=True,
    )


def _plot_chart_hard(
    countries_guessed: List[str],
    years_guessed: List[str],
    solution: str,
    column_indicator: str,
    title: str,
    column_country: str,
):
    # Filter out solution countri if given within guessed countries
    countries_guessed = [c for c in countries_guessed]
    countries_to_plot = [solution] + [c for c in countries_guessed if c != solution]
    solution_year = solution + " (" + str(SOLUTION_YEAR) + ")"
    rows_to_plot = [solution_year] + [ll + " (" + str(y) + ")" for ll, y in zip(countries_guessed, years_guessed)]
    DATA["locationyear"] = DATA["location"] + " (" + DATA["year"].astype(str) + ")"

    # Filter only data for countries to plot
    df = DATA[DATA["locationyear"].isin(rows_to_plot)].reset_index(drop=True)
    # st.write(DATA)
    # Sort
    priority = {c: i for i, c in enumerate(rows_to_plot)}
    df["priority"] = df[column_country].map(priority)
    df = df.sort_values(["priority", "year"])

    # Map locations to lcolor
    color_map = dict(zip(countries_to_plot, COLORS))
    color_map["?"] = color_map[solution]

    # Map locations to line dash
    pattern_map = {
        **{c: "/" for c in countries_guessed},
        solution: "",
        "?": "",
    }

    # Hide country name if user has not succeded yet.
    if not st.session_state.user_has_succeded:
        if st.session_state.user_has_succeded_year:
            df["locationyear"] = df["locationyear"].replace({solution_year: f"? ({SOLUTION_YEAR})"})
            df[column_country] = df[column_country].replace({solution: "?"})
        elif st.session_state.user_has_succeded_country:
            df["year"] = df["year"].replace({SOLUTION_YEAR: "?"})
            df["locationyear"] = df["locationyear"].replace({solution_year: f"{solution} ?"})
        else:
            df[column_country] = df[column_country].replace({solution: "?"})
            df["year"] = df["year"].replace({SOLUTION_YEAR: "?"})
            df["locationyear"] = df["locationyear"].replace({solution_year: "?"})

    # Change location column name to "Country"
    df["Country"] = df[column_country]

    # Drop NaN
    df = df.dropna(subset=[column_indicator])

    # Create plotly figure & plot
    fig = px.bar(
        data_frame=df,
        x="locationyear",
        y=column_indicator,
        title=title,
        color="Country",
        color_discrete_map=color_map,
        pattern_shape="Country",
        pattern_shape_map=pattern_map,
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
def plot_chart_population(countries_guessed: List[str], years_guessed: List[str], solution: str):
    """Plot timeseries."""
    if st.session_state.owidle_difficulty == 2:
        _plot_chart_hard(
            countries_guessed,
            years_guessed=years_guessed,
            solution=solution,
            column_indicator="population",
            title="Population",
            column_country="location",
        )
    else:
        _plot_chart(
            countries_guessed,
            solution=solution,
            column_indicator="population",
            title="Population",
            column_country="location",
        )


@st.cache_data
def plot_chart_gdp_pc(countries_guessed: List[str], years_guessed: List[str], solution: str):
    """Plot timeseries."""
    if st.session_state.owidle_difficulty == 2:
        _plot_chart_hard(
            countries_guessed,
            years_guessed=years_guessed,
            solution=solution,
            column_indicator=GDP_INDICATOR,
            title=gdp_indicator_titles[GDP_INDICATOR],
            column_country="location",
        )
    else:
        _plot_chart(
            countries_guessed,
            solution=solution,
            column_indicator=GDP_INDICATOR,
            title=gdp_indicator_titles[GDP_INDICATOR],
            column_country="location",
        )


def display_metadata(metadata):
    if not isinstance(metadata, dict):
        metadata = metadata.to_dict()
    ds = pd.Series(metadata).astype(str)
    ds.name = "value"
    st.table(data=ds)


with st.container(border=True):
    countries_guessed = [guess["name"] for guess in st.session_state.guesses if guess["name"] != ""]
    if st.session_state.owidle_difficulty == 2:
        years_guessed = [guess["year"] for guess in st.session_state.guesses if guess["year"] != ""]
    else:
        years_guessed = []
    if not SOLUTION_HAS_GDP:
        st.warning("We don't have GDP data for this country!")
    col1, col2 = st.columns(2)
    with col1:
        plot_chart_population(countries_guessed, years_guessed, solution=SOLUTION)
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
        plot_chart_gdp_pc(countries_guessed, years_guessed, solution=SOLUTION)
        with st.expander("Sources"):
            try:
                metadata = DATA[GDP_INDICATOR].metadata.to_dict()
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
#
# INPUT FROM USER
#
##########################################
with st.form("form_guess", border=False, clear_on_submit=True):
    col_guess_1, col_guess_2 = st.columns([4, 1])
    # EASY MODE: Filter options
    ## Only consider options within the radius of the last guess
    ## If there are less than NUM_COUNTRIES_EASY_MODE, show the NUM_COUNTRIES_EASY_MODE closest ones.
    if (st.session_state.owidle_difficulty == 0) and (st.session_state.guess_last is not None):
        distances = DISTANCES.loc[st.session_state.guess_last["name"]]
        options = distances.loc[
            distances["distance"] <= int(st.session_state.guess_last["distance"]), "target"
        ].tolist()
        if len(options) <= NUM_COUNTRIES_EASY_MODE:
            options = distances.sort_values("distance").head(NUM_COUNTRIES_EASY_MODE)["target"].tolist()
        else:
            options = distances.loc[
                distances["distance"] <= int(st.session_state.guess_last["distance"]), "target"
            ].tolist()
        options = sorted(options)
    else:
        options = OPTIONS

    # Show dropdown for options
    if st.session_state.owidle_difficulty == 2:
        # Get default for country selector if we got it right
        if st.session_state.user_has_succeded_country:
            country = st.session_state.get("guess_last_submitted")
            index = options.index(country)
        else:
            index = None

        col1, col2 = st.columns([1, 1])
        with col1:
            value = st.selectbox(
                label="Guess a country",
                placeholder="Choose a country... ",
                options=options,
                label_visibility="collapsed",
                index=index,
                key="guess_last_submitted",
            )
        with col2:
            st.slider(
                label="Year",
                min_value=YEAR_MIN,
                max_value=YEAR_MAX,
                value=st.session_state.get("guess_year_last_submitted", (YEAR_MAX + YEAR_MIN) // 2),
                label_visibility="collapsed",
                key="guess_year_last_submitted",
            )
    else:
        col1, col2 = st.columns([1, 1])
        value = st.selectbox(
            label="Guess a country",
            placeholder="Choose a country... ",
            options=options,
            label_visibility="collapsed",
            index=None,
            key="guess_last_submitted",
        )

    # Disable button if user has finished their guesses or has succeeded
    disabled = (st.session_state.num_guesses >= NUM_GUESSES) or st.session_state.user_has_succeded
    # Label for button
    if st.session_state.user_has_succeded:
        label = "YOU GUESSED IT!"
    elif st.session_state.num_guesses >= NUM_GUESSES:
        label = "MAYBE NEXT TIME!"
    else:
        label = f"GUESS {st.session_state.num_guesses + 1}/{NUM_GUESSES}"

    # Show button
    btn = st.form_submit_button(
        label=label,
        type="primary",
        use_container_width=True,
        on_click=lambda: guess(),
        disabled=disabled,
    )

##########################################
#
# SHOW GUESSES (this far)
#
##########################################
guesses_display = []
num_guesses_bound = min(st.session_state.num_guesses, NUM_GUESSES)
for i in range(num_guesses_bound):
    # LAYOUT IF HARD MODE
    if st.session_state.owidle_difficulty == 2:
        col1, col2 = st.columns([2, 1])
        with col1.container(border=True):
            col11, col12, col13 = st.columns([30, 10, 7])
            col11.markdown(f"**{st.session_state.guesses[i]['name']}**")
            col12.markdown(f"{st.session_state.guesses[i]['distance']}km")
            col13.markdown(st.session_state.guesses[i]["direction"])
        with col2.container(border=True):
            col21, col22 = st.columns(2)
            col21.markdown(f"**{st.session_state.guesses[i]['year']}**")
            if i == st.session_state.num_guesses - 1:
                col22.markdown(
                    st.session_state.guesses[i]["direction_year"],
                    help="🔥: up to ±5 years\n\n🔽/🔼: between ±5 and ±15 years\n\n⏬️/⏫️🔼: between ±15 and ±30 years\n\n⏬️⏬️⏬️/⏫️⏫️⏫️: More than 30 (or less than -30) years difference",
                )
            else:
                col22.markdown(st.session_state.guesses[i]["direction_year"])
    # LAYOUT OTHERWISE
    else:
        with st.container(border=True):
            col1, col2, col3, col4 = st.columns([30, 10, 7, 7])
            with col1:
                # st.text(st.session_state.guesses[i]["name"])
                st.markdown(f"**{st.session_state.guesses[i]['name']}**")
            with col2:
                st.markdown(f"{st.session_state.guesses[i]['distance']}km")
            with col3:
                st.markdown(st.session_state.guesses[i]["direction"])
            with col4:
                st.markdown(f"{st.session_state.guesses[i]['score']}%")

##########################################
#
# SHOW REMAINING GUESSES BOXES
#
##########################################
for i in range(num_guesses_bound, NUM_GUESSES):
    with st.container(border=True):
        st.empty()

##########################################
#
# FINAL MESSAGE
#
##########################################
LANGUAGE_SHARE = "markdown"


def _convert_score_to_emojis(score: int) -> str:
    """Return mosaic score.

    🟩: 20%
    🟨: 10%
    # 🟧: 5% (NOT IN USE). should only be used if we define a 15% square
    ⬛: 0%
    """
    # Get number of green squares
    num_g = score // 20
    score -= num_g * 20
    # Get number of yellow squares
    num_y = score // 10
    score -= num_y * 10
    # Get number of orange squares
    # num_o = score // 5
    # score -= num_o * 5
    # Get number of black squares
    num_b = max(5 - (num_g + num_y), 0)

    # Return mosaic
    mosaic = "🟩" * num_g + "🟨" * num_y + "⬛" * num_b
    return mosaic


def _convert_year_score_to_emojis(score: int) -> str:
    """Return mosaic score.

    🟢: 20%
    🟡: 10%
    # 🟧: 5% (NOT IN USE). should only be used if we define a 15% square
    ⚫: 0%
    """
    # Get number of green squares
    num_g = score // 20
    score -= num_g * 20
    # Get number of yellow squares
    num_y = score // 10
    score -= num_y * 10
    # Get number of orange squares
    # num_o = score // 5
    # score -= num_o * 5
    # Get number of black squares
    num_b = max(5 - (num_g + num_y), 0)

    # Return mosaic
    mosaic = "🟢" * num_g + "🟡" * num_y + "⚫" * num_b
    return mosaic


def get_score_mosaic():
    """Get mosaic of score.

    The mosaic provides a visual representation of the score for each round played.
    """
    scores = [int(guess["score"]) for guess in st.session_state.guesses if guess["score"] != ""]
    mosaic = "\n".join([_convert_score_to_emojis(score) for score in scores])
    return mosaic


def get_year_score_mosaic():
    """Get mosaic of score (years).

    The mosaic provides a visual representation of the score for each round played.
    """
    scores = [int(guess["score"]) for guess in st.session_state.guesses if guess["score"] != ""]
    mosaic = "\n".join([_convert_year_score_to_emojis(score) for score in scores])
    return mosaic


def get_score_mosaic_hard():
    # Distance
    scores_dist = [int(guess["score"]) for guess in st.session_state.guesses if guess["score"] != ""]
    # Years
    scores_year = [int(guess["score_year"]) for guess in st.session_state.guesses if guess["score_year"] != ""]
    # Combine into string
    mosaic = "\n".join(
        [
            _convert_score_to_emojis(dist) + _convert_year_score_to_emojis(year)
            for dist, year in zip(scores_dist, scores_year)
        ]
    )
    return mosaic


# st.write(st.session_state.guesses)

# HARD MODE
if st.session_state.owidle_difficulty == 2:
    mosaic_hard = get_score_mosaic_hard()
    ## Successful
    if st.session_state.user_has_succeded:
        st.balloons()
        st.success("🎉 You have guessed the correct country! 🎉")
        col, _ = st.columns(2)
        with col:
            s = []
            s2 = []
            for count, i in enumerate(range(num_guesses_bound)):
                _s = f"{st.session_state.guesses[i]['distance']}km"
                s.append(_s)
                _s = f"{st.session_state.guesses[i]['year']}"
                s2.append(_s)

            r = "round" if num_guesses_bound == 1 else "rounds"
            s = f"owidle#{OWID_NUM} {num_guesses_bound}/6 (Hard)\n" + mosaic_hard + "\nhttp://etl.owid.io/wizard/owidle"
            st.subheader("Share it")
            st.code(s, language=LANGUAGE_SHARE)
    ## Unsuccessful
    elif st.session_state.num_guesses >= NUM_GUESSES:
        st.error(f"The correct answer was **{SOLUTION}** in **{SOLUTION_YEAR}**. Better luck next time!")
        st.subheader("Share it")
        s = f"owidle#{OWID_NUM}: Failed (Hard)\n" + mosaic_hard + "\nhttp://etl.owid.io/wizard/owidle"
        st.code(s, language=LANGUAGE_SHARE)

# EASY/MID MODE
else:
    mosaic = get_score_mosaic()
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
                f"owidle#{OWID_NUM} {num_guesses_bound}/6 ({DIF_LVLS[st.session_state.owidle_difficulty]})\n"
                + mosaic
                + "\nhttp://etl.owid.io/wizard/owidle"
            )
            st.subheader("Share it")
            st.code(s, language=LANGUAGE_SHARE)
    ## Unsuccessful
    elif st.session_state.num_guesses >= NUM_GUESSES:
        st.error(f"The correct answer was **{SOLUTION}**. Better luck next time!")
        st.subheader("Share it")
        s = (
            f"owidle#{OWID_NUM}: Failed ({DIF_LVLS[st.session_state.owidle_difficulty]})\n"
            + mosaic
            + "\nhttp://etl.owid.io/wizard/owidle"
        )
        st.code(s, language=LANGUAGE_SHARE)


if st.session_state.user_has_succeded or (st.session_state.num_guesses >= NUM_GUESSES):
    # Explanation on how scores are estimated
    with st.expander("How is the score estimated?"):
        st.markdown("### Easy and Standard modes")
        st.markdown(
            "The distance score is calculated as the ratio between the distance from the guessed country to the solution country and the maximum distance on Earth (~20,000 km)."
        )
        st.markdown(
            "We create a 5-square visual representation of the score where 🟩 = 20%, 🟨 = 10% and ⬛ = 0%. For example, a score of 70% will be represented as 🟩🟩🟩🟨⬛."
        )

        st.markdown("### Hard mode")
        st.markdown(
            "The distance score is calculated as the ratio between the distance from the guessed country to the solution country and the maximum distance on Earth (~20,000 km)."
        )
        st.markdown(
            "The year score is calculated as the ratio between the difference in years from the guessed year to the solution year and the maximum difference in years (depends on year coverage for that country)."
        )
        st.markdown(
            "We create a 2 x 5-slot visual representation of the score where (i) 🟩 = 20%, 🟨 = 10% and ⬛ = 0% (for distance) and 🟢 = 20%, 🟡 = 10%, ⚫ = 0% (for years). For example, a distance score of 70% and year score of 30% will be represented as 🟩🟩🟩🟨⬛🟢🟡⚫⚫⚫."
        )
        st.markdown("Note that in hard mode, scores are hidden as you guess.")
        # .
    st.stop()
