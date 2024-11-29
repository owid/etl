import pandas as pd
import streamlit as st
from st_aggrid import AgGrid
from st_aggrid.grid_options_builder import GridOptionsBuilder
from structlog import get_logger

from apps.wizard.app_pages.indicator_search import data
from apps.wizard.app_pages.indicator_search import embeddings as emb
from apps.wizard.utils.components import Pagination, st_horizontal, st_multiselect_wider, tag_in_md

# Initialize log.
log = get_logger()

# PAGE CONFIG
st.set_page_config(
    page_title="Wizard: Indicator Search",
    page_icon="🪄",
    layout="wide",
)

########################################################################################################################
# FUNCTIONS
########################################################################################################################


def st_display_indicators(indicators: list[dict]):
    df = pd.DataFrame(indicators)

    df["link"] = df.apply(lambda x: f"http://staging-site-indicator-search/admin/variables/{x['variableId']}/", axis=1)

    df["catalogPath"] = df["catalogPath"].str.replace("grapher/", "")

    df = df.drop(columns=["variableId"])

    def make_bold(value):
        return "font-weight: bold;"

    styled_df = df.style.map(make_bold, subset=["name"])

    # df.style.format({"name": markdown_to_html_link}, escape=False)

    column_config = {
        "link": st.column_config.LinkColumn("Open", display_text="Open"),
        # "name": st.column_config.TextColumn("Name"),
    }

    # html = styled_df.to_html(escape=False, index=False)
    # st.markdown(html, unsafe_allow_html=True)
    # st.markdown(df.to_html(escape=False), unsafe_allow_html=True)

    st.dataframe(
        styled_df,
        column_order=["link", "name", "description", "catalogPath", "n_charts", "similarity"],
        use_container_width=True,
        hide_index=True,
        height=2000,
        column_config=column_config,
    )
    return

    # :material/person
    authors = ", ".join([tag_in_md(a, "gray", ":material/person") for a in indicator["authors"]])
    score = round(indicator["similarity"] * 100)

    # Get edit URLs
    # url_gdoc = f"https://docs.google.com/document/d/{indicator['id']}/edit"
    url_admin = f"http://staging-site-master/admin/gdocs/{indicator['id']}/preview"

    with st.container(border=True):
        # If public, display special header (inc multimedia content if indicator is public)
        if indicator["is_public"]:
            st.markdown(f"#### [{indicator['title']}]({indicator['url']})")

            # Display header 'Author | Date'
            date_str = indicator["date_published"].strftime("%B %d, %Y")
            date_str = tag_in_md(date_str, "green", ":material/calendar_month")
            # header = f"by **{authors}** | published **{date_str}** | [view]({indicator['url']})"
            st.markdown(f"by {authors} | {date_str} | [:material/edit: edit]({url_admin})")

            # Show multimedia content if available (image, video)
            if indicator["url_img_desktop"] is not None:
                st.image(indicator["url_img_desktop"], use_container_width=True)
            elif indicator["url_vid"] is not None:
                st.video(indicator["url_vid"])
        # Display only authors if not public
        else:
            st.markdown(f"#### {indicator['title']}")
            st.write(f":red[(Draft)] {authors} | [:material/edit: edit]({url_admin})")

        # Render text
        text = indicator["markdown"].replace("$", r"\$")
        st.caption(text)

        # Score
        st.write(f"**Similarity Score:** {score}%")


def split_input_string(input_string: str) -> tuple[str, list[str], list[str]]:
    """Break input string into query, includes and excludes."""
    # Break input string into query, includes and excludes
    query = []
    includes = []
    excludes = []
    for term in input_string.split():
        if term.startswith("+"):
            includes.append(term[1:].lower())
        elif term.startswith("-"):
            excludes.append(term[1:].lower())
        else:
            query.append(term)

    return " ".join(query), includes, excludes


def indicator_query(indicator: dict) -> str:
    return indicator["name"] + " " + indicator["description"] + " " + (indicator["catalogPath"] or "")


########################################################################################################################
# Get embedding model.
MODEL = emb.get_model()
# Fetch all data indicators.
indicators = data.get_data_indicators()

# Create an embedding for each indicator.
embeddings = emb.get_indicators_embeddings(MODEL, indicators)
########################################################################################################################


########################################################################################################################
# RENDER
########################################################################################################################

# Streamlit app layout.
st.title(":material/search: Indicator search")

# Box for input text.
input_string = st.text_input(
    label="Enter a word or phrase to find the most similar indicators.",
    placeholder="Type something...",
    help="Write any text to find the most similar indicators.",
)

st_multiselect_wider()
with st_horizontal():
    pass

if input_string:
    if len(input_string) < 3:
        st.warning("Please enter at least 3 characters.")
    else:
        # Break input string into query, includes and excludes
        query, includes, excludes = split_input_string(input_string)

        # Get the sorted indicators.
        sorted_dis = emb.get_sorted_documents_by_similarity(MODEL, query, docs=indicators, embeddings=embeddings)

        # Display the sorted documents.
        # TODO: This could be enhanced in different ways:
        #   * Add a color to similarity score.
        #   * Show the part of the text that justifies the score (this may also slow down the search).

        # Filter indicators
        selection = st.segmented_control(
            "Indicator status",
            ["All", "Used in charts"],
            selection_mode="single",
            default="All",
            label_visibility="collapsed",
        )

        # Filter DIs
        match selection:
            case "All":
                filtered_dis = sorted_dis
            case "Used in charts":
                filtered_dis = [di for di in sorted_dis if di["is_public"]]
            # case "Drafts":
            #     filtered_dis = [di for di in sorted_dis if not di["is_public"]]
            case _:
                filtered_dis = sorted_dis

        # Filter includes and excludes
        for include in includes:
            filtered_dis = [di for di in filtered_dis if include in indicator_query(di)]

        for exclude in excludes:
            filtered_dis = [di for di in filtered_dis if exclude not in indicator_query(di)]

        # Show items (only current page)
        st_display_indicators(filtered_dis[:50])
