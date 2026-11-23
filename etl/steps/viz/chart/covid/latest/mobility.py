from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# View config shared by every view, which the YAML can't set because the views are generated from the table.
MOBILITY_CONFIG_DEFAULT = {
    "subtitle": "This data shows how community movement in specific locations has changed relative to the period before the pandemic.",
    "note": "It's not recommended to compare levels across countries; local differences in categories could be misleading.",
    "originUrl": "ourworldindata.org/coronavirus",
    "minTime": "earliest",
    "maxTime": "latest",
    "hideAnnotationFieldsInTitle": {"time": True},
    "addCountryMode": "change-country",
}


def run() -> None:
    # Load the table the views are generated from.
    ds = paths.load_dataset("google_mobility")
    tb = ds.read("google_mobility", load_data=False)

    # Create and save the multidim.
    c = paths.create_chart(
        config=paths.load_config(),
        tb=tb,
        common_view_config=MOBILITY_CONFIG_DEFAULT,
    )
    c.save()
