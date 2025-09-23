"""
Experiment with Metabase API. To be moved to analytics/metabase/api.py module."""

# %% Imports
import datetime

from metabase_api import Metabase_API

from etl.config import METABASE_API_KEY, METABASE_URL_LOCAL

# %% Initialize Metabase API
mb = Metabase_API(METABASE_URL_LOCAL, api_key=METABASE_API_KEY)

# %% Get all cards
# ref: https://www.metabase.com/docs/latest/api#tag/apicard/get/api/card/
cards = mb.get("/api/card/")


# %% Exploring one card and its properties
card = cards[0]
name = card["name"]
description = card["description"]
dataset_query = card["dataset_query"]["native"]["query"]
card_filters = card["dataset_query"]["native"]["template-tags"]
card_id = card["id"]
card_type = card["type"]  # e.g. "question"
card_display = card["display"]  # e.g. "line" or "table"

## Construct URL to the card (is this stable? couldn't find any field in card providing slug)
url = f"{METABASE_URL_LOCAL}/question/{card_id}-{name.replace(' ', '-').lower()}"

# Show all keys
sorted(card.keys())

# %% get the data from the card result
mb.get_card_data(card_id=card_id)


# %% Copy card
mb.copy_card(
    source_card_id=185,
    destination_collection_id=61,
)

# %% Create a new card (question)
COLLECTION_EXPERT_ID = 61  # Expert collection

DATABASE_ID = 2  # Semantic Layer database

QUESTION_NAME = "My new question"
QUESTION_TIMESTAMP = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
QUESTION_TITLE = f"🤖 {QUESTION_NAME} ({QUESTION_TIMESTAMP})"

mb.create_card(
    # card_name=f"{QUESTION_TITLE} (1)",
    collection_id=COLLECTION_EXPERT_ID,
    # If you are providing only this argument, the keys 'name', 'dataset_query' and 'display' are required (https://github.com/metabase/metabase/blob/master/docs/api-documentation.md#post-apicard).
    custom_json={
        "name": QUESTION_TITLE,
        "description": "Question created by Expert.",
        "type": "question",
        "dataset_query": {
            "type": "native",
            "database": DATABASE_ID,
            "native": {"query": "SELECT 1 AS one, 2 AS two"},
        },
        "display": "table",
    },
)

# %%
