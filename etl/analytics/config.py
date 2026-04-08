"""Configuration variables for analytics."""

from datetime import datetime

# First day when we started collecting chart render views.
DATE_MIN = "2024-11-01"
# Current date.
DATE_MAX = str(datetime.today().date())
# Base url for Datasette csv queries.
DATASETTE_URL = "http://analytics.owid.io"
ANALYTICS_URL = f"{DATASETTE_URL}/analytics"
ANALYTICS_CSV_URL = f"{ANALYTICS_URL}.csv"
ANALYTICS_JSON_URL = f"{ANALYTICS_URL}.json"

# Maximum number of rows that a single Datasette csv call can return.
MAX_DATASETTE_N_ROWS = 10000

# Base OWID URL, used to find views in articles and topic pages.
OWID_BASE_URL = "https://ourworldindata.org/"

# Base URL for grapher charts.
GRAPHERS_BASE_URL = OWID_BASE_URL + "grapher/"


# Complete list of component types in the posts_gdocs_links.
# Each component type corresponds to a way in which a gdoc can be linked to another piece of content (e.g. a grapher chart, or an explorer).
COMPONENT_TYPES_ALL = [
    # Gdoc (usually of a topic page, but also possibly articles, e.g. https://ourworldindata.org/human-development-index ) cites a grapher chart as part of the all-charts block.
    # NOTE: Only charts that are specifically cited as top charts will be included.
    # To link gdocs of topic pages with charts that appear in the all-charts block, we need to use tags.
    "all-charts",
    # Gdoc (usually of an article) embeds a grapher chart.
    "chart",
    # Gdoc (only the SDG tracker) embeds grapher charts in a special story style.
    "chart-story",
    # Gdoc of homepage links to a few explorers (showing a thumbnail, no data).
    "explorer-tiles",
    # Gdoc of data insights cites the grapher-url in the metadata.
    # We can use this field to connect data insights to the original grapher chart.
    # NOTE: Not all data insights can be linked to a grapher chart (either because grapher-url is not defined, or because it uses a custom static visualization).
    "front-matter",
    # Gdoc of homepage cites other content (usually other gdocs).
    "homepage-intro",
    # Gdoc of homepage cites key indicators (usually grapher charts).
    "key-indicator",
    # Gdoc of a topic page cites key insights (usually grapher charts).
    "key-insights",
    # Gdoc (usually of an article) cites a narrative chart.
    "narrative-chart",
    # Gdoc of team page cites a person.
    "person",
    # Gdoc of homepage or author page cites topics to be shown in a "row of pills".
    "pill-row",
    # Gdoc (usually of articles) cites content (usually URLs, but also grapher charts) that appear in a special box.
    # NOTE: When it's linked to a chart, the box displays a small thumbnail of the grapher chart.
    "prominent-link",
    # Gdoc (usually topic pages) cite other content (usually articles).
    "research-and-writing",
    # Gdoc (of any kind of content) cites a URL (usually an external URL, a grapher chart, or an explorer).
    "span-link",
    # Gdoc of a topic page cites other content (usually related topics).
    "topic-page-intro",
    # Gdoc (articles about our site, or data insights) cite a video.
    "video",
]
# Component types to consider when linking gdocs with views (of charts, explorers, or narrative charts).
COMPONENT_TYPES_TO_LINK_GDOCS_WITH_VIEWS = [
    # NOTE: 'all-charts' only includes top charts in the all-charts block. To link charts with topic pages, we need to use tags.
    # 'all-charts',
    "chart",
    "chart-story",
    # 'explorer-tiles',
    "front-matter",
    # 'homepage-intro',
    "key-indicator",
    "key-insights",
    "narrative-chart",
    # 'person',
    # 'pill-row',
    # 'prominent-link',
    # 'research-and-writing',
    # NOTE: Grapher charts are often cited as URLs. It's unclear whether we want to count these references. But I'd say that in most cases, we'd prefer to ignore these. For example, when counting views of articles that use charts, we should count articles that display the chart, but not articles that simply cite the URL of the chart. Therefore, for now, ignore 'span-link'.
    # 'span-link',
    # 'topic-page-intro',
    # 'video',
]
# Dictionary that maps the type of a gdoc post (as defined in the posts_gdocs DB table) to its base URL; adding the post's slug to this base URL gives the complete URL to the corresponding post.
POST_TYPE_TO_URL = {
    "about-page": OWID_BASE_URL,
    "article": OWID_BASE_URL,
    "author": f"{OWID_BASE_URL}team/",
    "data-insight": f"{OWID_BASE_URL}data-insights/",
    # Fragments are used just a handful of times, and seems to be used for data pages FAQ.
    # It's not clear to me how to link them to specific posts, so, ignore them for now.
    "fragment": None,
    # Gdocs of type 'homepage' have an arbitrary slug "owid-homepage". They will be manually fixed later on.
    "homepage": None,
    "linear-topic-page": OWID_BASE_URL,
    "topic-page": OWID_BASE_URL,
}
# Dictionary that maps a link type (as defined in the posts_gdocs_links DB table) to the base url; adding the target to this base url gives a full URL to the linked content (e.g. a grapher chart).
POST_LINK_TYPES_TO_URL = {
    "grapher": OWID_BASE_URL + "grapher/",
    # Narrative charts, which don't have a public URL.
    # They are handled separately.
    "narrative-charts": OWID_BASE_URL + "grapher/",
    "explorer": OWID_BASE_URL + "explorers/",
    "gdoc": "https://docs.google.com/document/d/",
    # A "url" link type links to an arbitrary URL.
    # NOTE: In a handful of cases, there are links of type "url" and component type "chart". It's not clear to me why they are not of link types "grapher" with type "chart". But what's clear is that, when the link type is "grapher" the target corresponds to a slug, and when the link type is "url" the target is a full URL to a grapher chart. So, simply map these urls to their targets without modification.
    "url": "",
}
