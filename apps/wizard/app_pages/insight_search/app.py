import streamlit as st
from structlog import get_logger

from apps.wizard.app_pages.insight_search import data
from apps.wizard.utils import embeddings as emb
from apps.wizard.utils.components import Pagination, st_horizontal, st_multiselect_wider, tag_in_md

# Initialize log.
log = get_logger()

# PAGE CONFIG
st.set_page_config(
    page_title="Wizard: Insight Search",
    page_icon="🪄",
)


########################################################################################################################
# FUNCTIONS
########################################################################################################################


def st_display_insight(insight: data.Insight):
    assert insight.similarity is not None

    # :material/person
    authors = ", ".join([tag_in_md(a, "gray", ":material/person") for a in insight.authors])
    score = round(insight.similarity * 100)

    # Get edit URLs
    # url_gdoc = f"https://docs.google.com/document/d/{insight.id}/edit"
    url_admin = f"https://admin.owid.io/admin/gdocs/{insight.id}/preview"

    with st.container(border=True):
        # If public, display special header (inc multimedia content if insight is public)
        if insight.is_public:
            st.markdown(f"#### [{insight.title}]({insight.url})")

            # Display header 'Author | Date'
            date_str = insight.date_published.strftime("%B %d, %Y")
            date_str = tag_in_md(date_str, "green", ":material/calendar_month")
            # header = f"by **{authors}** | published **{date_str}** | [view]({insight.url})"
            st.markdown(f"by {authors} | {date_str} | [:material/edit: edit]({url_admin})")

            # Show multimedia content if available (image, video)
            if insight.url_img_desktop is not None:
                st.image(insight.url_img_desktop, use_container_width=True)
            elif insight.url_vid is not None:
                st.video(insight.url_vid)
        # Display only authors if not public
        else:
            st.markdown(f"#### {insight.title}")
            st.write(f":red[(Draft)] {authors} | [:material/edit: edit]({url_admin})")

        # Render text
        text = insight.markdown.replace("$", r"\$")
        st.caption(text)

        # Score
        st.write(f"**Similarity Score:** {score}%")


def get_authors_with_DIs(insights: list[data.Insight]) -> set[str]:
    return set(author for insight in insights for author in insight.authors)


@st.cache_data(show_spinner=False, max_entries=1)
def get_and_fit_model(insights: list[data.Insight]) -> emb.EmbeddingsModel:
    # Get embedding model.
    model = emb.EmbeddingsModel(emb.get_model())
    # Create an embedding for each insight.
    model.fit(insights)
    return model


########################################################################################################################
# Fetch all data insights.
insights = data.get_data_insights()
# Get embedding model.
model = get_and_fit_model(insights)
# Available authors
authors = get_authors_with_DIs(insights)

########################################################################################################################


########################################################################################################################
# RENDER
########################################################################################################################

# Streamlit app layout.
st.title(":material/search: DI search")

# Other interesting links
with st.popover("Additional resources"):
    st.markdown(
        """

        - [**Topic diversity**](http://analytics/analytics?sql=--%0D%0A--+Table+of+topics+%22neglected%22+by+our+published%2Fscheduled+data+insights%0D%0A--%0D%0A--+Notes%3A%0D%0A--+++-+%60n_insights_per_1m_views_365d%60+column+represents+the+%23+of+published%2Fscheduled+data%0D%0A--+++++insights+per+1+million+page+views+on+the+topic+in+the+past+365+days.%0D%0A--+++-+views_365d+represents+all+views+on+the+topic+%28including+articles%2C+charts%2C+data%0D%0A--+++++insights%2C+explorers%2C+topic+pages%29.%0D%0A--+++-+published+and+scheduled+data+insights+are+counted%2C+draft+data+insights+are+not.%0D%0A--+%0D%0A%0D%0Awith%0D%0A%0D%0Atopics+as+%28%0D%0A++select+%0D%0A++++topic%2C%0D%0A++++sum%28views_365d%29+as+views_365d%0D%0A++from+pages%0D%0A++join+page_x_topic+using%28url%29%0D%0A++group+by+topic%0D%0A%29%2C%0D%0A%0D%0Acounts+as+%28%0D%0A++select+topic%2C+count%28*%29+as+n_insights%0D%0A++from+%28%0D%0A++++select+unnest%28topics%29+as+topic+%0D%0A++++from+data_insights++--+alternatives%3A+articles%2C+charts%2C+explorers%0D%0A++++--+filter+by+author%3A%0D%0A++++--+where+list_contains%28authors%2C+%27Hannah+Ritchie%27%29%0D%0A++++--+filter+by+days+since+published%3A%0D%0A++++--+where+published_at+%3E+CURRENT_DATE%28%29+-+INTERVAL+90+DAY%0D%0A++%29%0D%0A++group+by+topic%0D%0A%29%0D%0A%0D%0Aselect%0D%0A++topic%2C%0D%0A++views_365d%2C%0D%0A++COALESCE%28n_insights%2C+0%29+as+n_insights%2C%0D%0A++COALESCE%28round%281e6+*+n_insights+%2F+views_365d%2C+1%29%2C+0%29+as+n_insights_per_1m_views_365d%0D%0Afrom+topics%0D%0Aleft+join+counts+using%28topic%29%0D%0Aorder+by+views_365d+desc) (Datasette): Check which topics we've covered so far — and which have been neglected — to find new ideas.
        - **Country diversity** (Instagram): Look at which countries we have referenced in our Instagram posts. IG posts originate from a subset of DIs, therefore these can be a good indicator of which countries we are focusing on.
            - [Countries covered by Instagram posts](https://admin.owid.io/admin/charts/8259/edit)
            - [Average country share of mentions in a post](https://admin.owid.io/admin/charts/8260/edit)
        """
    )
# Box for input text.
input_string = st.text_input(
    label="Enter a word or phrase to find the most similar insights.",
    placeholder="Type something...",
    help="Write any text to find the most similar data insights.",
)

st_multiselect_wider()
with st_horizontal():
    input_authors = st.multiselect(
        label="Authors",
        options=authors,
        help="Show only insights by selected authors.",
        placeholder="Filter by author(s)",
    )

if input_string or (input_authors != []):
    if (len(input_string) < 3) and (len(input_authors) == 0):
        st.warning("Please enter at least 3 characters or one author.")
    else:
        # Get the sorted DIs.
        sorted_dis: list[data.Insight] = model.get_sorted_documents_by_similarity(input_string)

        # Display the sorted documents.
        # TODO: This could be enhanced in different ways:
        #   * Add a color to similarity score.
        #   * Show the part of the text that justifies the score (this may also slow down the search).

        # Filter DIs by publication status
        options = ["All", "Published", "Drafts"]
        selection = st.segmented_control(
            "Publication status",
            options,
            selection_mode="single",
            default="All",
            label_visibility="collapsed",
        )

        # Filter DIs
        match selection:
            case "All":
                filtered_dis = sorted_dis
            case "Published":
                filtered_dis = [di for di in sorted_dis if di.is_public]
            case "Drafts":
                filtered_dis = [di for di in sorted_dis if not di.is_public]
            case _:
                filtered_dis = sorted_dis

        # Filter DIs by author
        if input_authors:
            filtered_dis = [di for di in filtered_dis if any(author in di.authors for author in input_authors)]

        # Use pagination
        items_per_page = 100
        pagination = Pagination(
            items=filtered_dis,
            items_per_page=items_per_page,
            pagination_key=f"pagination-di-search-{input_string}",
        )

        if len(filtered_dis) > items_per_page:
            pagination.show_controls(mode="bar")

        # Show items (only current page)
        for item in pagination.get_page_items():
            st_display_insight(item)
