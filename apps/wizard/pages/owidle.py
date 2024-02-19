"""Game owidle."""
import datetime as dt
from pathlib import Path
from typing import List, Tuple, cast

import geopandas as gpd
import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st
from owid.catalog import Dataset
from st_pages import add_indentation

from etl.paths import DATA_DIR

##########################################
# CONFIG PAGE & SESSION STATE INIT
##########################################
st.set_page_config(page_title="Wizard: owidle", layout="wide", page_icon="🪄")
add_indentation()
st.title(body="👾 :rainbow[OWIDLE] ")

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
@st.cache_data
def load_data() -> Tuple[pd.DataFrame, gpd.GeoDataFrame, str]:
    # Load indicator
    ds = Dataset(DATA_DIR / "garden" / "un" / "2022-07-11" / "un_wpp")
    tb = ds["population"].reset_index()
    tb = tb.loc[
        (tb["metric"] == "population") & (tb["sex"] == "all") & (tb["age"] == "all") & (tb["variant"] == "estimates"),
        ["year", "location", "value"],
    ]
    df = pd.DataFrame(tb)

    # Load geographic info
    ## file from https://gist.github.com/metal3d/5b925077e66194551df949de64e910f6 (later harmonized)
    geo = pd.read_csv(
        Path(__file__).parent / "owidle_countries.csv",
    )

    # Combine
    df = df.merge(geo, left_on="location", right_on="Country", how="left")
    # Remove NaNs
    df = df.dropna(subset=["Latitude", "Longitude"])

    # Get indicator df
    df_indicator = df[
        [
            "year",
            "location",
            "value",
        ]
    ]

    # Get geographic df
    df_geo = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df.Longitude, y=df.Latitude),
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
    seed = (dt.date.today() - dt.date(2020, 1, 1)).days
    solution = df["location"].sample(random_state=seed).item()

    return df_indicator, df_geo, solution


DATA, GEO, SOLUTION = load_data()
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


def vector_calc(lat, long, ht):
    """
    Calculates the vector from a specified point on the Earth's surface to the North Pole.
    """
    a = 6378137.0  # Equatorial radius of the Earth
    b = 6356752.314245  # Polar radius of the Earth

    e_squared = 1 - ((b**2) / (a**2))  # e is the eccentricity of the Earth
    n_phi = a / (np.sqrt(1 - (e_squared * (np.sin(lat) ** 2))))

    x = (n_phi + ht) * np.cos(lat) * np.cos(long)
    y = (n_phi + ht) * np.cos(lat) * np.sin(long)
    z = ((((b**2) / (a**2)) * n_phi) + ht) * np.sin(lat)

    x_npole = 0.0
    y_npole = 6378137.0
    z_npole = 0.0

    v = ((x_npole - x), (y_npole - y), (z_npole - z))

    return v


def angle_calc(lat1, long1, lat2, long2, ht1=0, ht2=0):
    """
    Calculates the angle between the vectors from 2 points to the North Pole.
    """
    # Convert from degrees to radians
    lat1_rad = (lat1 / 180) * np.pi
    long1_rad = (long1 / 180) * np.pi
    lat2_rad = (lat2 / 180) * np.pi
    long2_rad = (long2 / 180) * np.pi

    v1 = vector_calc(lat1_rad, long1_rad, ht1)
    v2 = vector_calc(lat2_rad, long2_rad, ht2)

    # The angle between two vectors, vect1 and vect2 is given by:
    # arccos[vect1.vect2 / |vect1||vect2|]
    dot = np.dot(v1, v2)  # The dot product of the two vectors
    v1_mag = np.linalg.norm(v1)  # The magnitude of the vector v1
    v2_mag = np.linalg.norm(v2)  # The magnitude of the vector v2

    theta_rad = np.arccos(dot / (v1_mag * v2_mag))
    # Convert radians back to degrees
    theta = (theta_rad / np.pi) * 180

    return theta


def calc_bearing(lat1, long1, lat2, long2):
    import math

    # Convert latitude and longitude to radians
    lat1 = math.radians(lat1)
    long1 = math.radians(long1)
    lat2 = math.radians(lat2)
    long2 = math.radians(long2)

    # Calculate the bearing
    bearing = math.atan2(
        math.sin(long2 - long1) * math.cos(lat2),
        math.cos(lat1) * math.sin(lat2) - math.sin(lat1) * math.cos(lat2) * math.cos(long2 - long1),
    )

    # Convert the bearing to degrees
    bearing = math.degrees(bearing)

    # Make sure the bearing is positive
    bearing = (bearing + 360) % 360

    return bearing


def distance_to_solution(country_selected: str) -> Tuple[str, str]:
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
    direction = get_flat_earth_bearing(guess.x, guess.y, solution.x, solution.y)
    st.write(GEO.loc[GEO["location"] == country_selected])
    st.write(guess)
    st.write(guess.x, guess.y)
    st.write(GEO.loc[GEO["location"] == SOLUTION])
    st.write(solution)
    st.write(solution.x, solution.y)
    st.write(direction)
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

    # Estimate distance
    GEO_DIST = cast(gpd.GeoDataFrame, GEO.to_crs(3310))
    solution = GEO_DIST.loc[GEO_DIST["location"] == SOLUTION, "geometry"]
    guess = GEO_DIST.loc[GEO_DIST["location"] == country_selected, "geometry"]
    distance = int((solution.distance(guess, align=False) / 1e3).round().item())
    distance = str(distance)

    if country_selected == SOLUTION:
        st.session_state.user_has_succeded = True
        return "0", "🎉"
    return direction, arrow


# Actions once user clicks on "GUESS" button
def guess() -> None:
    """Actions performed once the user clicks on 'GUESS' button."""
    # If guess is None, don't do anything
    if st.session_state.guess_last is None:
        pass
    else:
        # Estimate distance from correct answer
        distance, direction = distance_to_solution(st.session_state.guess_last)
        # Add to session state
        st.session_state.guesses[st.session_state.num_guesses] = {
            "guess": st.session_state.guess_last,
            "distance": distance,
            "direction": direction,
        }
        # Increment number of guesses
        st.session_state.num_guesses += 1


##########################################
# PLOT CHART
##########################################
@st.cache_data
def plot_chart(countries_guessed: List[str]):
    """Plot timeseries."""
    # Get data
    countries_to_plot = [SOLUTION] + countries_guessed

    # COLORS =
    df = DATA[DATA["location"].isin(countries_to_plot)].reset_index(drop=True)
    df["location"] = df["location"].replace({SOLUTION: "?"})

    # Create plotly figure & plot
    fig = px.line(df, x="year", y="value", color="location")
    st.plotly_chart(fig, theme=None, use_container_width=True)


##########################################
# PLOT CHART
##########################################
with st.container(border=True):
    countries_guessed = [guess["guess"] for guess in st.session_state.guesses]
    plot_chart(countries_guessed)
    st.empty()

##########################################
# INPUT FROM USER
##########################################
with st.form("form_guess", border=False):
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
    # with col_guess_2:
    btn = st.form_submit_button(
        "GUESS",
        type="primary",
        use_container_width=True,
        on_click=lambda: guess(),
        disabled=(st.session_state.num_guesses >= NUM_GUESSES) or st.session_state.user_has_succeded,
    )

##########################################
# SHOW GUESSES (this far)
##########################################
guesses_display = []
for i in range(min(st.session_state.num_guesses, NUM_GUESSES)):
    with st.container(border=True):
        col1, col2, col3 = st.columns([4, 1, 1])
        with col1:
            # st.text(st.session_state.guesses[i]["guess"])
            st.markdown(f"**{st.session_state.guesses[i]['guess']}**")
        with col2:
            st.text(f"{st.session_state.guesses[i]['distance']} km")
        with col3:
            st.text(st.session_state.guesses[i]["direction"])


##########################################
# SHOW REMAINING GUESSES BOXES
##########################################
for i in range(min(st.session_state.num_guesses, NUM_GUESSES), NUM_GUESSES):
    with st.container(border=True):
        st.empty()

##########################################
# FINAL MESSAGE
##########################################
## Successful
if st.session_state.user_has_succeded:
    st.balloons()
    st.success("🎉 You have guessed the correct country! 🎉")
    st.stop()
## Unsuccessful
elif st.session_state.num_guesses >= NUM_GUESSES:
    st.error(f"The correct answer was {SOLUTION}. Better luck next time!")
    st.stop()
