"""Game owidle."""
import datetime as dt
from math import atan2, degrees
from pathlib import Path
from typing import Tuple

import geopandas as gpd
import pandas as pd
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
st.session_state.guesses = st.session_state.get("guesses", default_guess)
st.session_state.num_guesses = st.session_state.get("num_guesses", 0)
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
        (tb["metric"] == "population") & (tb["sex"] == "all") & (tb["age"] == "all"),
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
    df_geo = df_geo.to_crs(3310)

    # Arbitrary daily solution
    seed = (dt.date.today() - dt.date(2020, 1, 1)).days
    solution = df["location"].sample(random_state=seed).item()

    return df_indicator, df_geo, solution


DATA, GEO, SOLUTION = load_data()
st.write(SOLUTION)
OPTIONS = sorted(DATA["location"].unique())


##########################################
# COMPARE GUESS WITH SOLUTION
##########################################
def get_flat_earth_bearing(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    REF: https://github.com/gerardrbentley/streamlit_worldle/blob/main/streamlit_app.py
    Flat-earther bearing

    For globe projected initial or final bearing: https://www.movable-type.co.uk/scripts/latlong.html
    # y = sin(lon2 - lon1) * cos(lat2)
    # x = cos(lat1) * sin(lat2) - sin(lat1) * cos(lat2) * cos(lon2 - lon1)
    # angle = atan2(x, y)
    # bearing = (degrees(angle) + 360) % 360  # in degrees
    """
    relative_lat = lat2 - lat1
    relative_lon = lon2 - lon1
    angle = atan2(relative_lon, relative_lat)

    return degrees(angle)


def distance_to_solution(country_selected: str) -> Tuple[str, str]:
    """Estimate distance (km) from guessed to solution, including direction."""
    # Estimate distance
    solution = GEO.loc[GEO["location"] == SOLUTION, "geometry"]
    guess = GEO.loc[GEO["location"] == country_selected, "geometry"]
    distance = int((solution.distance(guess, align=False) / 1e3).round().item())
    distance = str(distance)
    # Estimate direction
    guess = guess.item()
    solution = solution.item()
    direction = get_flat_earth_bearing(guess.x, guess.y, solution.x, solution.y)
    st.write(direction)
    if (direction > -22.5) & (direction <= 22.5):
        direction = "⬆️"
    elif (direction > 22.5) & (direction <= 67.5):
        direction = "↗️"
    elif (direction > 67.5) & (direction <= 112.5):
        direction = "➡️"
    elif (direction > 112.5) & (direction <= 157.5):
        direction = "↘️"
    elif (direction > 157.5) | (direction <= -157.5):
        direction = "⬇️"
    elif (direction > -157.5) & (direction <= -112.5):
        direction = "↙️"
    elif (direction > -112.5) & (direction <= -67.5):
        direction = "⬅️"
    else:
        direction = "↖️"

    if country_selected == SOLUTION:
        st.session_state.user_has_succeded = True
        return "0", "🎉"
    return distance, direction


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
def plot_chart():
    pass


##########################################
# PLOT CHART
##########################################
with st.container(border=True):
    plot_chart()
    st.empty()

##########################################
# INPUT FROM USER
##########################################
with st.form("form_guess", border=False):
    col_guess_1, col_guess_2 = st.columns([4, 1])
    with col_guess_1:
        value = st.selectbox(
            label="Guess a country",
            placeholder="Choose a country... ",
            options=OPTIONS,
            label_visibility="collapsed",
            index=None,
            key="guess_last",
        )
    with col_guess_2:
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
