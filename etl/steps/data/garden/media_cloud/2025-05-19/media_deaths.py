# top 10 causes of death:
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

load_dotenv()

MC_API_TOKEN = os.getenv("MC_API_TOKEN")
NYT_API_TOKEN = os.getend("NYT_API_TOKEN")

YEAR = 2023

CAUSES_OF_DEATH_PATH = f"/Users/tunaacisu/Data/Media Deaths/Underlying Cause of Death_{YEAR}.csv"
EXTERNAL_CAUSES_PATH = f"/Users/tunaacisu/Data/Media Deaths/Underlying Cause of Death_{YEAR}_external_factors.csv"

MEDIA_MENTIONS_PATH = f"/Users/tunaacisu/Data/Media Deaths/media_mentions_{YEAR}.csv"

NYT_ID = 1
WAPO_ID = 2
GUARDIAN_ID = 300560

US_NATIONAL_COLLECTION_ID = 34412234

START_CURR_YEAR = dt.date(YEAR, 1, 1)
END_CURR_YEAR = dt.date(YEAR, 12, 31)

START_2022 = dt.date(2022, 1, 1)
END_2022 = dt.date(2022, 12, 31)

START_2023 = dt.date(2023, 1, 1)
END_2023 = dt.date(2023, 12, 31)

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

HEALTH_QUERY = "health OR disease OR illness OR sick OR sickness OR injury OR injuries OR fatality OR fatalities OR mortality OR mortalities OR drug OR hospital OR clinic OR doctor OR physician OR nurse OR medical OR healthcare OR treatment OR therapy OR care OR patient OR patients OR prescription OR medication OR vaccine OR vaccination OR immunization OR outbreak OR epidemic OR pandemic"

DEATH_QUERY = "death OR died OR dead OR fatality OR fatalities OR mortality OR mortalities OR dies OR dying OR die OR fatal OR deadliness OR lethality OR lethal OR kill OR killed OR killing OR killer OR kills"


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
    for i, row in df_causes_of_death.head(12).iterrows():
        cause = row["15 Leading Causes of Death"]
        deaths.append({"cause": CAUSES_MAP[cause], "deaths": row["Deaths"]})

    return pd.DataFrame(deaths)


def query_results(query, source_ids, start_date=START_CURR_YEAR, end_date=END_CURR_YEAR, collection_ids=None):
    if collection_ids:
        results = search_api.story_count(
            query=query, start_date=start_date, end_date=end_date, collection_ids=collection_ids
        )
    else:
        results = search_api.story_count(query=query, start_date=start_date, end_date=end_date, source_ids=source_ids)
    return results["relevant"]


def query_stories(query, source_ids, start_date=START_CURR_YEAR, end_date=END_CURR_YEAR, collection_ids=None):
    if collection_ids:
        results = search_api.story_list(
            query=query, start_date=start_date, end_date=end_date, collection_ids=collection_ids
        )
    else:
        results = search_api.story_list(query=query, start_date=start_date, end_date=end_date, source_ids=source_ids)
    return pd.DataFrame(results[0])


def query_all_stories(
    query, source_ids, start_date=START_CURR_YEAR, end_date=END_CURR_YEAR, timeout=35, collection_ids=None
):
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


def stories_to_csv(queries: dict, timeout: int = 35, source_ids=[NYT_ID], source_name="NYT", title: str = "") -> None:
    if not title:
        title = f"stories_{source_name}_{YEAR}"
    for name, query in queries.items():
        stories = query_all_stories(query, source_ids)
        stories.to_csv(f"/Users/tunaacisu/Data/Media Deaths/{source_name}_articles/{name}_{title}.csv")
        time.sleep(timeout)  # Sleep to avoid hitting API rate limits


def print_example_stories(queries: dict, timeout: int = 35, source_ids=[NYT_ID], n_stories: int = 10) -> None:
    for name, query in queries.items():
        stories = query_stories(query, source_ids)
        print(f"Example stories for query: {name}")
        for r, story in stories.head(n_stories).iterrows():  # Print top 10 stories
            print(f"{story['title']} - {story['url']}")
            print("\n")
        time.sleep(timeout)  # Sleep to avoid hitting API rate limits


def add_shares(media_deaths_df: pd.DataFrame):
    total_mentions = media_deaths_df["mentions"].sum()
    total_mentions_incl_health = media_deaths_df["mentions_incl_health"].sum()
    total_deaths = media_deaths_df["deaths"].sum()

    media_deaths_df["mentions_share"] = (
        media_deaths_df["mentions"] / total_mentions
    ) * 100  # share of mentions compared mentions of all causes of death

    media_deaths_df["mentions_share_incl_health"] = (
        media_deaths_df["mentions_incl_health"] / total_mentions_incl_health
    ) * 100

    media_deaths_df["deaths_share"] = (
        media_deaths_df["deaths"] / total_deaths
    ) * 100  # share of deaths compared to all deaths

    return media_deaths_df


def plot_media_deaths(media_deaths_df, columns=None, bar_labels=None, title=None):
    if columns is None:
        columns = ["deaths_share", "mentions_share", "mentions_share_incl_health"]
        bar_labels = ["Deaths", "Mentions", "Mentions keyword + health"]
    if bar_labels is None:
        bar_labels = columns  # fallback to original column names
    if title is None:
        title = f"Media Mentions of Causes of Death in {YEAR}"

    mm_plot = media_deaths_df[["cause"] + columns].transpose()
    mm_plot.columns = mm_plot.iloc[0]
    mm_plot = mm_plot.drop(mm_plot.index[0])  # drop the first row which is the cause names
    mm_plot.index = bar_labels

    ax = mm_plot.plot(kind="bar", stacked=True, colormap="tab20")

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
    start_date=None,
    end_date=None,
    verbose: bool = False,
    collection_ids=None,
):
    causes_to_exclude = ["war"]
    query_count = []

    for name, query in queries.items():
        if name in causes_to_exclude:
            if verbose:
                print(f"Skipping {name} as it is excluded from the analysis.")
            continue
        if start_date is not None and end_date is not None:
            cnt = query_results(
                query, source_ids, start_date=start_date, end_date=end_date, collection_ids=collection_ids
            )
            cnt_incl_health = query_results(
                f"({query}) AND ({HEALTH_QUERY} OR {DEATH_QUERY})",
                source_ids,
                start_date=start_date,
                end_date=end_date,
                collection_ids=collection_ids,
            )
        else:
            cnt = query_results(query, source_ids, collection_ids=collection_ids)
            cnt_incl_health = query_results(
                f"({query}) AND ({HEALTH_QUERY} OR {DEATH_QUERY})", source_ids, collection_ids=collection_ids
            )

        n_deaths = death_df[death_df["cause"] == name]["deaths"].iloc[0]
        if verbose:
            print(f"{name}: {cnt}, {cnt_incl_health}")
        query_count.append(
            {
                "cause": name,
                "mentions": cnt,
                "mentions_incl_health": cnt_incl_health,
                "deaths": n_deaths,
                "source": source_name,
            }
        )
    df_results = pd.DataFrame(query_count)
    df_results = add_shares(df_results)
    return df_results


def pivot_media_mentions(media_deaths_df: pd.DataFrame):
    """Pivot media mentions to have mentions per outlet as columns."""
    media_pv = media_deaths_df.pivot(
        index=["cause", "deaths_share"], columns=["source"], values=["mentions_share"]
    ).reset_index()

    media_pv.columns = [" ".join(col).strip() for col in media_pv.columns.values]

    return media_pv


def run() -> None:
    use_saved_data = True  # set to True to use saved data, False to query the API

    if use_saved_data:
        # Load saved data
        media_deaths_df = pd.read_csv(MEDIA_MENTIONS_PATH)

        nyt_mentions = media_deaths_df[media_deaths_df["source"] == "The New York Times"]
        guardian_mentions = media_deaths_df[media_deaths_df["source"] == "The Guardian"]
        wapo_mentions = media_deaths_df[media_deaths_df["source"] == "The Washington Post"]
        collection_mentions = media_deaths_df[media_deaths_df["source"] == "US National Collection"]

    #
    else:
        death_df = create_death_df()

        all_queries = create_queries()

        nyt_mentions = get_mentions_from_source(
            source_ids=[NYT_ID], source_name="The New York Times", queries=all_queries, death_df=death_df, verbose=True
        )

        guardian_mentions = get_mentions_from_source(
            source_ids=[GUARDIAN_ID],
            source_name="The Guardian",
            queries=all_queries,
            death_df=death_df,
            verbose=True,
        )

        wapo_mentions = get_mentions_from_source(
            source_ids=[WAPO_ID],
            source_name="The Washington Post",
            queries=all_queries,
            death_df=death_df,
            verbose=True,
        )

        collection_mentions = get_mentions_from_source(
            source_ids=[],
            source_name="US National Collection",
            queries=all_queries,
            death_df=death_df,
            collection_ids=[US_NATIONAL_COLLECTION_ID],
            verbose=True,
        )

    # plot media deaths:

    title_str = f"Media Mentions, Causes of Death (w/o war) - {YEAR}"

    plot_media_deaths(guardian_mentions, title=f"{title_str} - The Guardian")
    plot_media_deaths(nyt_mentions, title=f"{title_str} - The New York Times")
    plot_media_deaths(wapo_mentions, title=f"{title_str} - The Washington Post")
    plot_media_deaths(collection_mentions, title=f"{title_str} - US National Collection")

    # combine dataframes and save to csv
    if not use_saved_data:
        all_mentions = [nyt_mentions, guardian_mentions, wapo_mentions, collection_mentions]
        media_deaths_df = pd.concat(all_mentions, ignore_index=True)
        media_deaths_df["year"] = YEAR

        # save to csv
        media_deaths_df.to_csv(MEDIA_MENTIONS_PATH)


def create_queries():
    query_heart = '"heart disease" OR "heart attack" OR "cardiac arrest" OR "myocardial infarction" OR "coronary artery disease" OR "arrhythmia" OR "heart failure" OR "congestive heart failure" OR "valvular heart disease" OR "pericarditis" OR "endocarditis" OR "cardiomyopathy"'
    query_cancer = '"cancer" OR "malignant neoplasm" OR "tumor" OR "tumour" OR "carcinoma" OR "sarcoma" OR "leukemia" OR "lymphoma" OR "melanoma" OR "mass lesion" OR "oncology" OR "chemotherapy" OR "radiation therapy" OR "immunotherapy" OR "targeted therapy" OR "biopsy" OR "oncogene" OR "carcinogenesis" OR "metastasis" OR "remission" OR "carcogenesis" OR "carcinogenic" OR "carcinogen"'
    query_accidents = '"road accident" OR "car crash" OR "vehicle accident" OR "traffic collision" OR "car accident" OR "motorcycle accident" OR "hit and run" OR "plane crash" OR "train accident" OR "boat accident" OR "airplane accident" OR "industrial accident" OR "workplace accident" OR "falling object" OR "electrocution" OR "burn injury" OR "drowning"'  # OR "drunk driving" OR "collision"
    query_stroke = '"stroke" OR "cerebrovascular disease" OR "brain attack" OR "transient ischemic attack" OR "cerebral infarction" OR "brain hemorrhage" OR "subarachnoid hemorrhage" OR "intracerebral hemorrhage" OR "cerebral thrombosis" OR "cerebral embolism" OR "brain ischemia" OR "brain injury" OR "T.I.A."'
    query_respiratory = '"chronic obstructive pulmonary disease" OR "COPD" OR "chronic bronchitis" OR "emphysema" OR "asthma" OR "respiratory failure" OR "lung disease" OR "pulmonary disease" OR "respiratory illness" OR "respiratory disease" OR "respiratory tract infection"'  # should we add smoking/ tobacco use?
    query_alzheimers = '"Alzheimer" OR "dementia" OR "Alzheimer\'s disease"'
    query_diabetes = '"diabetes" OR "insulin" OR "hyperglycemia" OR "diabetic"'
    query_kidney = '"kidney disease" OR "renal failure" OR "chronic kidney disease" OR "end-stage renal disease" OR "nephropathy" OR "dialysis"'
    query_liver = '"liver disease" OR "cirrhosis" OR "chronic liver disease" OR "hepatitis" OR "liver failure"'  # should we add alcoholism/ alcohol use
    query_covid = '"COVID-19" OR "coronavirus" OR "SARS-CoV-2"'
    query_suicide = '"suicide" OR "self-harm" OR "self-inflicted injury" OR "suicidal"'
    query_influenza = (
        '"influenza" OR " flu " OR "respiratory infection" OR "pneumonia" OR "lung infection" OR "bronchopneumonia"'
    )
    query_drug = '"drug use" OR "overdose" OR "drug-related" OR "substance use disorder" OR "substance abuse" OR "addiction" OR "opioid" OR "heroin" OR "fentanyl" OR "morphine" OR "cocaine" OR "drug abuse" OR "illicit drug"'
    query_homocide = '"homicide" OR "assault" OR "shooting" OR "murder" OR "manslaughter" OR "violent crime" OR "domestic violence" OR "gang violence" OR "knife attack" OR "stabbing" OR "lynching" OR "execution"'  # OR "killer" OR "killing"
    query_terrorism = '"terrorism" OR "terrorist" OR "extremism" OR "radicalization" OR "suicide bomb" OR "car bomb" OR "hostage" OR "terror attack" OR "terror plot" OR "terror cell" OR "terror network"'
    query_war = '"war" OR "warfare" OR "armed conflict" OR "military operation" OR "combat" OR "invasion" OR "occupation" OR "insurgency" OR "guerrilla warfare" OR "airstrike" OR "bombing" OR "siege" OR "troops" OR "military intervention" OR "peacekeeping" OR "ceasefire" OR "truce" OR "hostilities" OR "genocide" OR "ethnic cleansing" NOT "trade war"'

    all_queries = {
        "heart disease": query_heart,
        "cancer": query_cancer,
        "accidents": query_accidents,
        "stroke": query_stroke,
        "respiratory": query_respiratory,
        "alzheimers": query_alzheimers,
        "diabetes": query_diabetes,
        "kidney": query_kidney,
        "liver": query_liver,
        "covid": query_covid,
        "suicide": query_suicide,
        "influenza": query_influenza,
        "drug overdose": query_drug,
        "homicide": query_homocide,
        "terrorism": query_terrorism,
        "war": query_war,
    }

    return all_queries
