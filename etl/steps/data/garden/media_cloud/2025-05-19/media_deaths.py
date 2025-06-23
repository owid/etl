# top 10 causes of death in US:
# 1 heart disease, I00-I09,I11,I13,I20-I51
# 2 cancer C00-C97
# 3 accidents (unintentional injuries), V01-X59,Y85-Y86
# 4 stroke, I60-69
# 5 chronic lower respiratory diseases, (COPD, chronic bronchitis, emphysema, asthma) J40-J47
# 6 Alzheimer's disease, G30
# 7 diabetes, E10-E14
# 8 Kidney disease and failure, N00-N07,N17-N19,N25-N27
# 9 chronic liver disease and cirrhosis, K70,K73-K74
# 10 COVID-19
# 11 Suicide, *U03,X60-X84,Y87.0
# 12 Influenza and pneumoia J09, J18

# in addition:
# drug overdose X42
# homicide (X80-Y09)
# terrorism (Z65.4)
# War (Z65.5)

import datetime as dt
import os
import time

import matplotlib.pyplot as plt
import mediacloud.api
import pandas as pd
from dotenv import load_dotenv
from media_deaths_queries import create_full_queries, create_queries, create_single_queries

from etl.helpers import PathFinder

PATHS = PathFinder(__file__)

load_dotenv()

MC_API_TOKEN = os.getenv("MC_API_TOKEN")
NYT_API_TOKEN = os.getenv("NYT_API_TOKEN")

YEAR = 2023
TODAY_STR = dt.date.today().strftime("%Y_%m_%d")
SAVE_TOKEN = f"{TODAY_STR}_"

MENTIONS_PATH = f"/Users/tunaacisu/Data/Media Deaths/mentions_{SAVE_TOKEN}.csv"

CAUSES_OF_DEATH_PATH = f"/Users/tunaacisu/Data/Media Deaths/Underlying Cause of Death_{YEAR}.csv"
EXTERNAL_CAUSES_PATH = f"/Users/tunaacisu/Data/Media Deaths/Underlying Cause of Death_{YEAR}_external_factors.csv"

MEDIA_MENTIONS_PATH = f"/Users/tunaacisu/Data/Media Deaths/media_mentions_{YEAR}_{SAVE_TOKEN}.csv"

NYT_ID = 1
WAPO_ID = 2
GUARDIAN_ID = 300560
FOX_ID = 1092

US_NATIONAL_COLLECTION_ID = 34412234


QUERIES = create_queries()
STR_QUERIES = create_full_queries()
SINGLE_QUERIES = create_single_queries()

# Initialize the Media Cloud API client
search_api = mediacloud.api.SearchApi(MC_API_TOKEN)


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
    "war",
    "hiv",
    "malaria",
    "tb",
    "diarrhea",
]

CAUSES_MAP = {
    "#Diseases of heart (I00-I09,I11,I13,I20-I51)": "heart disease",
    "#Malignant neoplasms (C00-C97)": "cancer",
    "#Accidents (unintentional injuries) (V01-X59,Y85-Y86)": "accidents",
    "#Cerebrovascular diseases (I60-I69)": "stroke",
    "#Chronic lower respiratory diseases (J40-J47)": "respiratory",
    "#Alzheimer disease (G30)": "alzheimers",
    "#Diabetes mellitus (E10-E14)": "diabetes",
    "#Nephritis, nephrotic syndrome and nephrosis (N00-N07,N17-N19,N25-N27)": "kidney",
    "#Chronic liver disease and cirrhosis (K70,K73-K74)": "liver",
    "#COVID-19 (U07.1)": "covid",
    "#Intentional self-harm (suicide) (*U03,X60-X84,Y87.0)": "suicide",
    "#Influenza and pneumonia (J09-J18)": "influenza",
}

WAR_DEATHS = 0  # from CDC Wonder database
TERRORISM_DEATHS_2023 = 16  # from Global Terrorism Index
TERRORISM_DEATHS_2022 = 11  # from Global Terrorism Index


def get_start_end(year):
    return (dt.date(year, 1, 1), dt.date(year, 12, 31))


def global_causes_mentions(year):
    death_df = load_gbd_data()
    death_df = death_df[death_df["year"] == year]

    source_ids = [NYT_ID, GUARDIAN_ID, WAPO_ID, FOX_ID]
    sources = ["The New York Times", "The Guardian", "The Washington Post", "Fox News"]

    mentions_ls = []

    for s_id, s_name in zip(source_ids, sources):
        mentions = get_mentions_from_source(
            [s_id], s_name, STR_QUERIES, death_df=death_df, verbose=True, causes_to_exclude=["war"], year=year
        )
        mentions_ls.append(mentions.copy(deep=True))

    all_mentions = pd.concat(
        mentions_ls,
        ignore_index=True,
    )

    all_mentions["year"] = year

    mentions_pv = pivot_media_mentions(all_mentions)

    plot_media_deaths(
        mentions_pv,
        columns=[
            "deaths_share",
            "mentions_share The New York Times",
            "mentions_share The Washington Post",
            "mentions_share Fox News",
        ],
        bar_labels=["Deaths", "NYT", "WaPo", "Fox News"],
        title=f"Media Mentions of Causes of Death - Global ({year})",
    )

    return all_mentions


def load_gbd_data(year=None):
    ds_cause = PATHS.load_dataset("gbd_cause")
    ds_hierarchy = PATHS.load_dataset("cause_hierarchy")

    tb_c = ds_cause.read("gbd_cause_deaths")
    tb_c_rel = tb_c[(tb_c["metric"] == "Number") & (tb_c["age"] == "All ages") & (tb_c["country"] == "World")]
    tb_c_rel = tb_c_rel.drop(columns=["metric", "age"])

    tb_h = ds_hierarchy.read("cause_hierarchy")

    tb_h[["level1", "level2", "level3", "level4"]] = tb_h["cause_outline"].str.split(".", expand=True)

    # only include level 3 causes
    card_codes = [
        "Aortic aneurysm",
        "Atrial fibrillation and flutter",
        "Cardiomyopathy and myocarditis",
        "Endocarditis",
        "Hypertensive heart disease",
        "Ischemic heart disease",
        "Lower extremity peripheral arterial disease",
        "Non-rheumatic valvular heart disease",
        "Other cardiovascular and circulatory diseases",
        "Pulmonary Arterial Hypertension",
        "Rheumatic heart disease",
    ]  # without stroke
    stroke_codes = ["Stroke"]
    resp_codes = [
        "Asthma",
        "Chronic obstructive pulmonary disease",
        "Interstitial lung disease and pulmonary sarcoidosis",
        "Other chronic respiratory diseases",
        "Pneumoconiosis",
    ]
    liver_codes = ["Cirrhosis and other chronic liver diseases"]
    accid_codes = [
        "Adverse effects of medical treatment",
        "Animal contact",
        "Drowning",
        "Environmental heat and cold exposure",
        "Exposure to forces of nature",
        "Exposure to mechanical forces",
        "Falls",
        "Fire, heat, and hot substances",
        "Foreign body",
        "Other transport injuries",
        "Other unintentional injuries",
        "Poisonings",
        "Road injuries",
    ]  # includes transport injuries and unintentional injuries
    alzh_codes = ["Alzheimer's disease and other dementias"]
    drug_use_codes = ["Drug use disorders"]  # without alcoholism
    suicide_codes = ["Self-harm"]
    homicide_codes = ["Interpersonal violence"]  # not including war or police violence
    war_terrorism_codes = ["Conflict and terrorism"]
    cancer_codes = [
        "Bladder cancer",
        "Brain and central nervous system cancer",
        "Breast cancer",
        "Cervical cancer",
        "Colon and rectum cancer",
        "Esophageal cancer",
        "Eye cancer",
        "Gallbladder and biliary tract cancer",
        "Hodgkin lymphoma",
        "Kidney cancer",
        "Larynx cancer",
        "Leukemia",
        "Lip and oral cavity cancer",
        "Liver cancer",
        "Malignant neoplasm of bone and articular cartilage",
        "Malignant skin melanoma",
        "Mesothelioma",
        "Multiple myeloma",
        "Nasopharynx cancer",
        "Neuroblastoma and other peripheral nervous cell tumors",
        "Non-Hodgkin lymphoma",
        "Non-melanoma skin cancer",
        "Other malignant neoplasms",
        "Other neoplasms",
        "Other pharynx cancer",
        "Ovarian cancer",
        "Pancreatic cancer",
        "Prostate cancer",
        "Soft tissue and other extraosseous sarcomas",
        "Stomach cancer",
        "Testicular cancer",
        "Thyroid cancer",
        "Tracheal, bronchus, and lung cancer",
        "Uterine cancer",
    ]
    covid_codes = ["COVID-19"]
    kidney_codes = ["Chronic kidney disease", "Acute glomerulonephritis"]
    diabetes_codes = ["Diabetes mellitus"]
    # influenza & pneumonia as upper and lower respiratory diseases
    infl_codes = ["Lower respiratory infections", "Upper respiratory infections"]

    # additional causes
    hiv_codes = ["HIV/AIDS"]
    malaria_codes = ["Malaria"]
    tb_codes = ["Tuberculosis"]
    diarrhea_codes = ["Diarrheal diseases"]
    # acute_hep_codes = ["Acute hepatitis"]

    # Map each cause in CAUSES_OF_DEATH to its corresponding code list
    codes = {
        "heart disease": card_codes,
        "cancer": cancer_codes,
        "accidents": accid_codes,
        "stroke": stroke_codes,
        "respiratory": resp_codes,
        "alzheimers": alzh_codes,
        "diabetes": diabetes_codes,
        "kidney": kidney_codes,
        "liver": liver_codes,
        "covid": covid_codes,
        "suicide": suicide_codes,
        "influenza": infl_codes,
        "drug overdose": drug_use_codes,
        "homicide": homicide_codes,
        "terrorism": war_terrorism_codes,
        # "war": war_terrorism_codes,
        "hiv": hiv_codes,
        "malaria": malaria_codes,
        "tb": tb_codes,
        "diarrhea": diarrhea_codes,
    }

    all_codes = [code for c_list in codes.values() for code in c_list]
    tb_c_rel = tb_c_rel[tb_c_rel["cause"].isin(all_codes)]
    tb_c_rel["mapped_cause"] = tb_c_rel["cause"].apply(lambda x: find_mapping_cause(x, codes))

    gb = tb_c_rel.groupby(["mapped_cause", "country", "year"])
    tb_c = gb.sum().reset_index().drop(columns="cause")
    tb_c = tb_c.rename(columns={"value": "deaths", "mapped_cause": "cause"}, errors="raise")

    if not year:
        return tb_c
    else:
        return tb_c[tb_c["year"] == year]


def find_mapping_cause(cause, map):
    for key, cause_list in map.items():
        if cause in cause_list:
            return key


def plot_single_vs_multiple_mentions():
    death_df = create_death_df()
    start_time = time.time()

    source_ids = [NYT_ID, GUARDIAN_ID, WAPO_ID, FOX_ID]
    sources = ["The New York Times", "The Guardian", "The Washington Post", "Fox News"]

    mult_mentions = []

    exclude_causes = ["war", "hiv", "malaria", "tb", "diarrhea"]

    for s_id, s_name in zip(source_ids, sources):
        mentions = get_mentions_from_source(
            [s_id],
            s_name,
            STR_QUERIES,
            death_df=death_df,
            verbose=True,
            causes_to_exclude=exclude_causes,
        )
        mult_mentions.append(mentions.copy(deep=True))
        print(f"Time taken for {s_name}: {time.time() - start_time:.2f} seconds")
    mult_mentions_df = pd.concat(mult_mentions, ignore_index=True)
    mult_mentions_df["multiple_mentions"] = mult_mentions_df["mentions"]
    mult_mentions_df["multiple_mentions_share"] = mult_mentions_df["mentions_share"]
    mult_mentions_df = mult_mentions_df.drop(columns=["mentions", "mentions_share"])

    single_mentions = []
    for s_id, s_name in zip(source_ids, sources):
        mentions = get_mentions_from_source(
            [s_id],
            s_name,
            SINGLE_QUERIES,
            death_df=death_df,
            verbose=True,
            causes_to_exclude=exclude_causes,
        )
        single_mentions.append(mentions.copy(deep=True))
        print(f"Time taken for {s_name}: {time.time() - start_time:.2f} seconds")
    single_mentions_df = pd.concat(single_mentions, ignore_index=True)
    single_mentions_df["single_mentions"] = single_mentions_df["mentions"]
    single_mentions_df["single_mentions_share"] = single_mentions_df["mentions_share"]
    single_mentions_df = single_mentions_df.drop(columns=["mentions", "mentions_share"])

    # merge the two DataFrames on 'cause' and 'source' and deaths and deaths_share columns
    media_deaths_df = pd.merge(
        mult_mentions_df,
        single_mentions_df,
        on=["cause", "source", "deaths", "deaths_share"],
        how="left",
    )

    # plot single and multiple mentions for each source
    for source in sources:
        source_df = media_deaths_df[media_deaths_df["source"] == source]
        if source_df.empty:
            print(f"No data for {source}")
            continue

        plot_media_deaths(
            source_df,
            columns=["multiple_mentions_share", "single_mentions_share"],
            bar_labels=["Multiple Mentions", "Single Mentions"],
            title=f"Media Mentions of Causes of Death - {source} ({YEAR})",
        )


def create_death_df():
    df_ext_causes = pd.read_csv(EXTERNAL_CAUSES_PATH)
    df_ext_causes = df_ext_causes.replace("Suppressed", pd.NA)
    df_ext_causes["Deaths"] = df_ext_causes["Deaths"].astype("Int64")

    drug_od_deaths = df_ext_causes[df_ext_causes["Cause of death Code"] == "X42"]["Deaths"].iloc[0]
    ext_causes_gb = (
        df_ext_causes[df_ext_causes["Deaths"] != "Suppressed"].groupby("ICD Sub-Chapter").sum().reset_index()
    )
    homicide_deaths = ext_causes_gb[ext_causes_gb["ICD Sub-Chapter"] == "Assault"]["Deaths"].iloc[0]

    if YEAR == 2022:
        terrorism_deaths = TERRORISM_DEATHS_2022
    elif YEAR == 2023:
        terrorism_deaths = TERRORISM_DEATHS_2023
    else:
        terrorism_deaths = 0

    deaths = [
        {"cause": "drug overdose", "deaths": drug_od_deaths},
        {"cause": "homicide", "deaths": homicide_deaths},
        {"cause": "terrorism", "deaths": terrorism_deaths},
        {"cause": "war", "deaths": WAR_DEATHS},
    ]

    df_causes_of_death = pd.read_csv(CAUSES_OF_DEATH_PATH)
    for _, row in df_causes_of_death.head(12).iterrows():
        cause = row["15 Leading Causes of Death"]
        deaths.append({"cause": CAUSES_MAP[cause], "deaths": row["Deaths"]})

    return pd.DataFrame(deaths)


def query_results(query, source_ids, year=YEAR, collection_ids=None):
    start_date, end_date = get_start_end(year)
    if collection_ids:
        results = search_api.story_count(
            query=query, start_date=start_date, end_date=end_date, collection_ids=collection_ids
        )
    else:
        results = search_api.story_count(query=query, start_date=start_date, end_date=end_date, source_ids=source_ids)
    return results["relevant"]


def query_stories(query, source_ids, year=YEAR, collection_ids=None):
    start_date, end_date = get_start_end(year)
    if collection_ids:
        results = search_api.story_list(
            query=query, start_date=start_date, end_date=end_date, collection_ids=collection_ids
        )
    else:
        results = search_api.story_list(query=query, start_date=start_date, end_date=end_date, source_ids=source_ids)
    return pd.DataFrame(results[0])


def query_all_stories(query, source_ids, year=YEAR, timeout=35, collection_ids=None):
    start_date, end_date = get_start_end(year)
    all_stories = []
    more_stories = True
    pagination_token = None
    page = []
    while more_stories:
        if collection_ids:
            page, pagination_token = search_api.story_list(
                query,
                collection_ids=collection_ids,
                start_date=start_date,
                end_date=end_date,
                pagination_token=pagination_token,
            )
        else:
            page, pagination_token = search_api.story_list(
                query,
                source_ids=source_ids,
                start_date=start_date,
                end_date=end_date,
                pagination_token=pagination_token,
            )
        all_stories += page
        more_stories = pagination_token is not None
        time.sleep(timeout)  # Sleep to avoid hitting API rate limits
    return pd.DataFrame(all_stories)


def stories_to_csv(
    queries: dict, timeout: int = 35, source_ids=[NYT_ID], source_name="NYT", title: str = "", verbose=True
) -> None:
    if not title:
        title = f"stories_{source_name}_{YEAR}"
    for name, query in queries.items():
        stories = query_all_stories(query, source_ids)
        stories.to_csv(f"/Users/tunaacisu/Data/Media Deaths/{source_name}_articles/{name}_{title}_{SAVE_TOKEN}.csv")
        if verbose:
            print(
                f"Saved {len(stories)} stories for query: {name} to {source_name}_articles/{name}_{title}_{SAVE_TOKEN}.csv"
            )
        time.sleep(timeout)  # Sleep to avoid hitting API rate limits


def print_example_stories(queries: dict, timeout: int = 35, source_ids=[NYT_ID], n_stories: int = 10) -> None:
    for name, query in queries.items():
        stories = query_stories(query, source_ids)
        print(f"Example stories for query: {name}")
        for r, story in stories.head(n_stories).iterrows():  # Print top 10 stories
            print(f"{story['title']} - {story['url']}")
            print("\n")
        time.sleep(timeout)  # Sleep to avoid hitting API rate limits


def add_shares(media_deaths_df, columns=None):
    """Add shares of columns to DataFrame."""
    if columns is None:
        columns = ["mentions", "deaths"]

    for col in columns:
        total = media_deaths_df[col].sum()
        if total == 0:
            media_deaths_df[col] = 0
        else:
            media_deaths_df[f"{col}_share"] = round(
                (media_deaths_df[col] / total) * 100, 3
            )  # convert to percentage, round to three decimal places

    return media_deaths_df


def excluding_war():
    death_df = create_death_df()

    queries_excl_war = STR_QUERIES
    queries_incl_war = STR_QUERIES.copy()
    queries_incl_war["homicide"] = STR_QUERIES["homicide"].split(" NOT ")[0]

    cdc_excludes = ["war", "hiv", "malaria", "tb", "diarrhea"]  # causes to exclude from the analysis

    source_ids = [NYT_ID, WAPO_ID, FOX_ID]
    sources = ["The New York Times", "The Washington Post", "Fox News"]

    mentions_ls = []
    stories_ls = []

    for s_id, s_name in zip(source_ids, sources):
        mentions = get_mentions_from_source(
            [s_id], s_name, queries_incl_war, death_df=death_df, verbose=True, causes_to_exclude=cdc_excludes, year=YEAR
        )
        mentions_ls.append(mentions.copy(deep=True))
        stories = query_all_stories({"homicide": queries_incl_war["homicide"]}, source_ids=[s_id], timeout=30)
        stories_ls.append(stories.copy(deep=True))

    stories_war_df = pd.concat(stories_ls, ignore_index=True)

    sources_no_war = [f"{s} (no war)" for s in sources]
    stories_no_war_ls = []

    for s_id, s_name in zip(source_ids, sources_no_war):
        mentions = get_mentions_from_source(
            [s_id], s_name, queries_excl_war, death_df=death_df, verbose=True, causes_to_exclude=cdc_excludes, year=YEAR
        )
        mentions_ls.append(mentions.copy(deep=True))
        stories = query_all_stories({"homicide": queries_excl_war["homicide"]}, source_ids=[s_id], timeout=30)
        stories_no_war_ls.append(stories.copy(deep=True))

    stories_no_war_df = pd.concat(stories_no_war_ls, ignore_index=True)

    if not stories_war_df.empty:
        titles = stories_war_df["title"].tolist()
        stories_excluded = stories_no_war_df[stories_no_war_df["title"].isin(titles)]
        return stories_excluded

    all_mentions = pd.concat(mentions_ls, ignore_index=True)

    pv = pivot_media_mentions(all_mentions)

    for s_name in sources:
        plot_media_deaths(
            pv,
            columns=[
                f"mentions_share {s_name}",
                f"mentions_share {s_name} (no war)",
            ],
            bar_labels=[s_name, f"{s_name} (no war)"],
            title=f"Media Mentions of Causes of Death - {YEAR} (US)",
        )


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

    # mm_plot.plot(kind="bar", stacked=True, colormap="tab20")
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


def get_mentions_from_source(
    source_ids: list,
    source_name: str,
    queries: dict,
    death_df,
    year=YEAR,
    verbose: bool = False,
    collection_ids=None,
    causes_to_exclude=[],  # causes to exclude from analysis
):
    """
    Get mentions of causes of death from a specific source.
    Args:
        source_ids (list): List of source IDs to query.
        source_name (str): Name of the source.
        queries (dict): Dictionary of queries to run.
        death_df (pd.DataFrame): DataFrame containing causes of death and their respective death counts.
        year (int): Year to query for.
        verbose (bool): Whether to print verbose output.
        collection_ids (list): List of collection IDs to query.
        causes_to_exclude (list): List of causes to exclude from the analysis.
    Returns:
        pd.DataFrame: DataFrame containing the results of the queries."""
    query_count = []

    for name, query in queries.items():
        if name in causes_to_exclude:
            if verbose:
                print(f"Skipping {name} as it is excluded from the analysis.")
            continue
        cnt = query_results(query, source_ids, collection_ids=collection_ids, year=year)
        n_deaths = death_df[death_df["cause"] == name]["deaths"].iloc[0]
        if verbose:
            print(f"Q: {query}")
            print(f"{name}: {cnt}")
        query_count.append(
            {
                "cause": name,
                "mentions": cnt,
                "deaths": n_deaths,
                "source": source_name,
            }
        )
    df_results = pd.DataFrame(query_count)
    df_results = add_shares(df_results)
    return df_results


def filter_mentions_on_source(media_mentions_df, source_name: str) -> pd.DataFrame:
    """Filter media mentions DataFrame on a specific source."""
    if "source" not in media_mentions_df.columns:
        raise ValueError("The DataFrame must contain a 'source' column.")

    filtered_df = media_mentions_df[media_mentions_df["source"] == source_name].copy()
    if filtered_df.empty:
        print(f"No mentions found for source: {source_name}")
    return filtered_df.reset_index(drop=True)


def pivot_media_mentions(media_deaths_df):
    """Pivot media mentions to have mentions per outlet as columns."""
    media_pv = media_deaths_df.pivot(
        index=["cause", "deaths_share"], columns=["source"], values=["mentions_share"]
    ).reset_index()

    media_pv.columns = [" ".join(col).strip() for col in media_pv.columns.values]

    return media_pv


def run() -> None:
    use_saved_data = False  # set to True to use saved data, False to query the API
    find_stories = False  # set to True to find stories, False to skip finding stories

    all_queries = STR_QUERIES  # queries to use

    source_ids = [NYT_ID, GUARDIAN_ID, WAPO_ID, FOX_ID]
    sources = ["The New York Times", "The Guardian", "The Washington Post", "Fox News"]

    if use_saved_data:
        # Load saved data
        all_mentions = pd.read_csv(MEDIA_MENTIONS_PATH)

    else:
        death_df = create_death_df()

        all_queries = STR_QUERIES

        cdc_excludes = ["war", "hiv", "malaria", "tb", "diarrhea"]  # causes to exclude from the analysis

        mentions_ls = []

        # querying single sources
        for s_id, s_name in zip(source_ids, sources):
            mentions = get_mentions_from_source(
                [s_id], s_name, all_queries, death_df=death_df, verbose=True, causes_to_exclude=cdc_excludes, year=YEAR
            )
            mentions_ls.append(mentions.copy(deep=True))

        collection_ids = [US_NATIONAL_COLLECTION_ID]
        collections = ["US National Collection"]

        # querying collections
        for c_id, c_name in zip(collection_ids, collections):
            mentions = get_mentions_from_source(
                [],
                c_name,
                all_queries,
                death_df=death_df,
                verbose=True,
                causes_to_exclude=cdc_excludes,
                collection_ids=collection_ids,
            )
            mentions_ls.append(mentions.copy(deep=True))

        all_mentions = pd.concat(mentions_ls, ignore_index=True)

    if find_stories:
        # Save stories to csv
        stories_to_csv(all_queries, source_ids=[NYT_ID], source_name="NYT")

        # Find stories for each source
        print_example_stories(all_queries, source_ids=[NYT_ID], n_stories=5)
        print_example_stories(all_queries, source_ids=[GUARDIAN_ID], n_stories=5)
        print_example_stories(all_queries, source_ids=[WAPO_ID], n_stories=5)
        print_example_stories(all_queries, source_ids=[FOX_ID], n_stories=5)

    # plot media deaths:

    title_str = f"Media Mentions, Causes of Death (w/o war) - {YEAR}"

    for s_name in sources:
        mentions = filter_mentions_on_source(all_mentions, s_name)
        if not mentions.empty:
            plot_media_deaths(mentions, title=f"{title_str} - {s_name}")

    mentions_pv = pivot_media_mentions(all_mentions)
    plot_media_deaths(
        mentions_pv,
        columns=[
            "deaths_share",
            "mentions_share The New York Times",
            "mentions_share The Washington Post",
            "mentions_share Fox News",
        ],
        bar_labels=["Deaths", "NYT", "WaPo", "Fox News"],
        title=f"Media Mentions of Causes of Death - {YEAR} (US)",
    )

    # combine dataframes and save to csv
    if not use_saved_data:
        all_mentions["year"] = YEAR

        # save to csv
        all_mentions.to_csv(MEDIA_MENTIONS_PATH)
