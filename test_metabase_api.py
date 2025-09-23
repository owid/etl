"""
Experiment with Metabase API. To be moved to analytics/metabase/api.py module."""

from metabase_api import Metabase_API

from etl.config import METABASE_API_KEY

mb = Metabase_API(METABASE_URL_LOCAL, api_key=METABASE_API_KEY)

# Get all cards
# ref: https://www.metabase.com/docs/latest/api#tag/apicard/get/api/card/
cards = mb.get("/api/card/")


# Exploring one card and its properties
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

# get the data from the card result
mb.get_card_data(card_id=card_id)
