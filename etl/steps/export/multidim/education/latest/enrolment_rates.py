"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Default collection config
    config = paths.load_collection_config()

    # Load grapher dataset.
    ds_sdgs = paths.load_dataset("education_sdgs")
    tb_sdgs = ds_sdgs.read("education_sdgs", load_data=True)

    ds_opri = paths.load_dataset("education_opri")
    tb_opri = ds_opri.read("education_opri", load_data=True)

    ds_wdi = paths.load_dataset("education_wdi")
    tb_wdi = ds_wdi.read("education_wdi", load_data=True)

    # List of columns to select from tb_opri
    cols_opri = [
        "total_net_enrolment_rate__primary__both_sexes__pct",
        "total_net_enrolment_rate__primary__male__pct",
        "total_net_enrolment_rate__primary__female__pct",
        "total_net_enrolment_rate__lower_secondary__both_sexes__pct",
        "total_net_enrolment_rate__lower_secondary__male__pct",
        "total_net_enrolment_rate__lower_secondary__female__pct",
        "total_net_enrolment_rate__upper_secondary__both_sexes__pct",
        "total_net_enrolment_rate__upper_secondary__male__pct",
        "total_net_enrolment_rate__upper_secondary__female__pct",
        "gross_enrolment_ratio__primary__both_sexes__pct",
        "gross_enrolment_ratio__primary__male__pct",
        "gross_enrolment_ratio__primary__female__pct",
        "gross_enrolment_ratio__lower_secondary__both_sexes__pct",
        "gross_enrolment_ratio__lower_secondary__male__pct",
        "gross_enrolment_ratio__lower_secondary__female__pct",
        "gross_enrolment_ratio__upper_secondary__both_sexes__pct",
        "gross_enrolment_ratio__upper_secondary__male__pct",
        "gross_enrolment_ratio__upper_secondary__female__pct",
    ]

    cols_sdgs = [
        "gross_enrolment_ratio_for_tertiary_education__both_sexes__pct__ger_5t8",
        "gross_enrolment_ratio_for_tertiary_education__female__pct__ger_5t8_f",
        "gross_enrolment_ratio_for_tertiary_education__male__pct__ger_5t8_f",
        "net_enrolment_rate__pre_primary__both_sexes__pct__ner_02_cp",
        "net_enrolment_rate__pre_primary__female__pct__ner_02_f_cp",
        "net_enrolment_rate__pre_primary__male__pct__ner_02_f_cp",
    ]

    cols_wdi = [
        "se_pre_enrr",
        "se_pre_enrr_ma",
        "se_pre_enrr_fe",
    ]

    # Select only the relevant columns from tb_sdgs
    tb_opri = tb_opri[cols_opri]
    tb_sdgs = tb_sdgs[cols_sdgs]
    tb_wdi = tb_wdi[cols_wdi]

    tb = pr.concat([tb_opri, tb_sdgs, tb_wdi])
    print(tb)

    #
    # (optional) Adjust dimensions if needed
    #

    #
    # Create collection object
    #
    c = paths.create_collection(
        config=config,
        short_name="enrolment_rates",
        tb=tb,
        # indicator_names=[],
        # dimensions={},
    )

    #
    # (optional) Edit views
    #
    for view in c.views:
        # if view.dimension["sex"] == "male":
        #     view.config["title"] = "Something else"
        pass

    #
    # Save garden dataset.
    #
    c.save()


def adjust_dimensions_corruption(tb):
    """
    Add dimensions to enrolment table columns.

    """

    indicator_name = "enrolment_rates"

    # Helper maps for pattern matching

    level_keywords = {
        "pre_primary": "Pre-Primary",
        "primary": "Primary",
        "lower_secondary": "Lower Secondary",
        "upper_secondary": "Upper Secondary",
        "tertiary": "Tertiary",
    }

    sex_keywords = {"both_sexes": "Both", "male": "Male", "female": "Female", "ma": "Male", "fe": "Female"}

    # Initialize mappings
    level_mapping = {}
    enrolment_type_mapping = {}
    sex_mapping = {}
    cols_to_add_dimensions = [col for col in tb.columns if col not in ["country", "year"]]

    # Iterate and populate mappings
    for var in cols_to_add_dimensions:
        # --- Level ---
        level = None
        for key, val in level_keywords.items():
            if key in var:
                level = val
                break
        level_mapping[var] = level or "Unknown"

        # --- Enrolment Type ---
        if "gross" in var:
            enrolment_type_mapping[var] = "Gross"
        elif "net" in var:
            enrolment_type_mapping[var] = "Net"
        else:
            enrolment_type_mapping[var] = "Gross"  # fallback for `se_pre_enrr`

        # --- Sex ---
        sex = None
        for key, val in sex_keywords.items():
            if f"__{key}__" in var or var.endswith(f"_{key}"):
                sex = val
                break
        sex_mapping[var] = sex or "Both"  # fallback for non-disaggregated vars

        for col in tb.columns:
            if col in ["country", "year"]:
                continue

            # Set short name
            tb[col].metadata.original_short_name = indicator_name
            tb[col].metadata.dimensions = {}

            # Set dimensions
            tb[col].metadata.dimensions["gender"] = sex_mapping[col]
            tb[col].metadata.dimensions["level"] = level_mapping[col]

    # Add dimension definitions at table level
    if isinstance(tb.metadata.dimensions, list):
        tb.metadata.dimensions.extend(
            [
                {"name": "Gender", "slug": "gender"},
                {"name": "Level of education", "slug": "level"},
                {"name": "Enrolment type", "slug": "enrolment_type"},
            ]
        )

    return tb
