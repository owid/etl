from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# NOTE: "VA" is not defined in the source file and only appears on Seasonal Influenza rows. I've emailed WHO (vpdata@who.int), but until we hear back, keep the raw code visible rather than guessing a label.
# So until then, show the previous release's flu data here instead, whose only categories are a plain Yes/No.
FLU_DESCRIPTION = "Seasonal Influenza vaccine"
FLU_FALLBACK_VERSION = "2025-07-15"
CURRENT_VERSION = "2026-10-07"

MULTIDIM_CONFIG = {
    "hasMapTab": True,
    "chartTypes": [],
    "tab": "map",
    "map": {
        "colorScale": {
            "customCategoryColors": {
                "Entire country": "#38AABA",
                "Not routinely administered": "#E77969",
                "Regions of the country": "#E9AD6F",
                "Specific risk groups": "#A2559C",
                "Demonstration projects": "#D7191C",
                "Adolescents": "#C8ADF5",
                "High risk areas": "#286BBB",
                "During outbreaks": "#398724",
                "Maternal vaccination only": "#C15FA8",
                "Infant antibodies only": "#3DA68C",
                "Both": "#38AABA",
            },
        },
    },
}


def run() -> None:
    # Load configuration from adjacent yaml file.
    config = paths.load_config()

    # Add views for all dimensions
    # NOTE: using load_data=False which only loads metadata significantly speeds this up
    ds = paths.load_dataset("vaccination_introductions", version=CURRENT_VERSION)
    tb = ds.read("vaccination_introductions", load_data=False)

    # All vaccines except seasonal influenza come from the current release.
    descriptions = sorted(
        {tb[col].m.dimensions["description"] for col in tb.columns if tb[col].m.dimensions} - {FLU_DESCRIPTION}
    )

    # Seasonal influenza comes from the previous release instead -- see FLU_DESCRIPTION note above.
    ds_flu = paths.load_dataset("vaccination_introductions", version=FLU_FALLBACK_VERSION)
    tb_flu = ds_flu.read("vaccination_introductions", load_data=False)

    # Create and save chart
    c = paths.create_chart(
        config=config,
        tb=[tb, tb_flu],
        indicator_names="intro",
        dimensions=[{"description": descriptions}, {"description": [FLU_DESCRIPTION]}],
        indicators_slug="vaccine",
        common_view_config=MULTIDIM_CONFIG,
        catalog_path_full=True,
    )
    c.save()
