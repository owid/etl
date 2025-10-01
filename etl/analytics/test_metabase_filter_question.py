from etl.analytics.metabase import COLLECTION_EXPERT_ID, DATABASE_ID, mb_cli

mb = mb_cli()
q1 = mb.get_item_info("card", 174)
query1 = q1["dataset_query"]["native"]["query"]
q2 = mb.get_item_info("card", 258)
query2 = q2["dataset_query"]["native"]["query"]

targs = q1["dataset_query"]["native"]
question = mb.create_card(
    # card_name=f"{QUESTION_TITLE} (1)",
    collection_id=COLLECTION_EXPERT_ID,
    # If you are providing only this argument, the keys 'name', 'dataset_query' and 'display' are required (https://github.com/metabase/metabase/blob/master/docs/api-documentation.md#post-apicard).
    custom_json={
        "name": "test",
        "description": "description",
        "type": "question",
        "dataset_query": {
            "type": "native",
            "database": DATABASE_ID,
            "native": {
                "template-tags": targs["template-tags"],
                "query": query1,
            },
        },
        "display": "table",
    },
    return_card=True,
    # **kwargs,
)
