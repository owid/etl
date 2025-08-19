import matplotlib.pyplot as plt
import owid.catalog.processing as pr
from owid.catalog import Table

from etl.helpers import PathFinder

paths = PathFinder(__file__)

YEAR = 2023  # year of the data

# these are the causes of death we are using for the 2023 version
CAUSES_OF_DEATH = [
    "heart disease",
    "cancer",
    "accidents",
    "stroke",
    "respiratory",
    "alzheimers",
    "diabetes",
    "kidney",
    "liver",
    "covid",
    "suicide",
    "influenza",
    "drug overdose",
    "homicide",
    "terrorism",
]

WAR_DEATHS_2023 = 0  # from CDC Wonder database
TERRORISM_DEATHS_2023 = 16  # from Global Terrorism Index


def create_tb_death(tb_leading_causes, tb_ext_causes):
    drug_od_deaths = tb_ext_causes[tb_ext_causes["cause_of_death_code"] == "X42"]["deaths"].iloc[0]
    ext_causes_gb = tb_ext_causes[["deaths", "icd_sub_chapter"]].groupby("icd_sub_chapter").sum().reset_index()
    homicide_deaths = ext_causes_gb[ext_causes_gb["icd_sub_chapter"] == "Assault"]["deaths"].iloc[0]

    terrorism_deaths = TERRORISM_DEATHS_2023

    deaths = [
        {"cause": "drug overdose", "year": 2023, "deaths": drug_od_deaths},
        {"cause": "homicide", "year": 2023, "deaths": homicide_deaths},
        {"cause": "terrorism", "year": 2023, "deaths": terrorism_deaths},
    ]

    tb_death = pr.concat([tb_leading_causes, Table(deaths)])

    return tb_death


def add_shares(tb, columns=None):
    """Add shares of columns to DataFrame."""
    if columns is None:
        columns = ["mentions", "deaths"]

    for col in columns:
        total = tb[col].sum()
        if total == 0:
            tb[f"{col}_share"] = 0
        else:
            tb[f"{col}_share"] = round(
                (tb[col] / total) * 100, 3
            )  # convert to percentage, round to three decimal places

    return tb


# This function is not used in the garden step, but kept in for easy plotting & evaluation of results
def plot_media_deaths(media_deaths_df, columns=None, bar_labels=None, title=None):
    fixed_colors = {
        "heart disease": "#1f77b4",  # Blue
        "cancer": "#ff7f0e",  # Orange
        "accidents": "#2ca02c",  # Green
        "stroke": "#d62728",  # Red
        "respiratory": "#9467bd",  # Purple
        "alzheimers": "#8c564b",  # Brown
        "diabetes": "#e377c2",  # Pink
        "kidney": "#7f7f7f",  # Gray
        "liver": "#bcbd22",  # Olive
        "covid": "#17becf",  # Teal
        "suicide": "#aec7e8",  # Light blue
        "influenza": "#ffbb78",  # Light orange
        "drug overdose": "#98df8a",  # Light green
        "homicide": "#ff9896",  # Light red
        "terrorism": "#c5b0d5",  # Light purple
        "war": "#c49c94",  # Light brown
        "hiv": "#f7b6d2",  # Light pink
        "malaria": "#c7c7c7",  # Light gray
        "tb": "#dbdb8d",  # Light olive
        "diarrhea": "#9edae5",  # Light teal
    }

    if columns is None:
        columns = ["deaths_share", "mentions_share"]
        bar_labels = ["Deaths", "Mentions"]
    if bar_labels is None:
        bar_labels = columns  # fallback to original column names
    if title is None:
        title = f"Media Mentions of Causes of Death in {YEAR}"

    mm_plot = media_deaths_df[["cause"] + columns].transpose()
    mm_plot.columns = mm_plot.iloc[0]
    mm_plot = mm_plot.drop(mm_plot.index[0])  # drop the first row which is the cause names
    mm_plot.index = bar_labels

    ordered_cols = [cause for cause in CAUSES_OF_DEATH if cause in mm_plot.columns]

    # Ensure the causes in mm_plot.columns match the fixed color order
    color_order = [fixed_colors[cause] for cause in ordered_cols]

    # Plot with fixed colors and cause order
    mm_plot = mm_plot[ordered_cols]  # Reorder the columns in desired order
    ax = mm_plot.plot(kind="bar", stacked=True, color=color_order)

    ax.set_xticklabels(ax.get_xticklabels(), rotation=0)

    plt.ylabel("Share")
    plt.title(title)
    plt.legend(title="Cause of death", bbox_to_anchor=(1.05, 1), loc="upper left")
    plt.tight_layout()

    for i, row in enumerate(mm_plot.values):
        cumulative = 0
        for j, value in enumerate(row):
            if value > 2:
                ax.text(
                    x=i,  # bar index (x position)
                    y=cumulative + value / 2,  # vertical position in the middle of the bar segment
                    s=f"{round(value, 1)}%",  # label with one decimal place as percent
                    ha="center",
                    va="center",
                    fontsize=8,
                )
            cumulative += value

    plt.show()


def run() -> None:
    ds_leading_causes = paths.load_dataset("leading_causes")
    ds_ext_causes = paths.load_dataset("external_causes")
    ds_media_mentions = paths.load_dataset("media_deaths")

    # load tables
    tb_leading_causes = ds_leading_causes["leading_causes"].reset_index()
    tb_ext_causes = ds_ext_causes["external_causes"].reset_index()
    tb_mm = ds_media_mentions["media_deaths"].reset_index()

    tb_leading_causes = tb_leading_causes.drop(columns=["full_icd_code", "crude_rate"], errors="raise")

    tb_deaths = create_tb_death(tb_leading_causes, tb_ext_causes)

    # filter only on causes of death we are interested in
    tb_mm = tb_mm[tb_mm["cause"].isin(CAUSES_OF_DEATH)]

    tb_mm = pr.merge(left=tb_mm, right=tb_deaths, on=["cause", "year"], how="left")

    sources = tb_mm["source"].unique().tolist()

    # add shares to media mentions table
    tb_mm["mentions_share"] = 0.0
    tb_mm["deaths_share"] = 0.0
    for source in sources:
        tb_s = tb_mm[tb_mm["source"] == source]
        tb_s = add_shares(tb_s, columns=["mentions", "deaths"])
        tb_mm.update(tb_s)

    # pivot table
    tb_mm = tb_mm.pivot(
        index=["cause", "year", "deaths", "deaths_share"], columns="source", values=["mentions", "mentions_share"]
    ).reset_index()

    tb_mm.columns = [
        "cause",
        "year",
        "deaths",
        "deaths_share",
        "fox_mentions",
        "nyt_mentions",
        "wapo_mentions",
        "us_mentions",
        "fox_share",
        "nyt_share",
        "wapo_share",
        "us_share",
    ]

    # US collection is not used in grapher
    tb_mm = tb_mm.drop(columns=["us_mentions", "us_share"], errors="raise")

    tb_mm["nyt_over_under"] = tb_mm["nyt_share"] / tb_mm["deaths_share"]
    # set values smaller 1 to negative reciprocal
    tb_mm.loc[tb_mm["nyt_over_under"] < 1, "nyt_over_under"] = (
        -1 / tb_mm.loc[tb_mm["nyt_over_under"] < 1, "nyt_over_under"]
    ).round(2)

    # format table
    tb = tb_mm.format(["cause", "year"], short_name="media_deaths")

    ds_garden = paths.create_dataset(tables=[tb], check_variables_metadata=True)
    ds_garden.save()
