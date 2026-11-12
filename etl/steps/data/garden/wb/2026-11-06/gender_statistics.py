"""Load a meadow dataset and create a garden dataset."""

import json

import pandas as pd
from owid.catalog import Table
from owid.catalog import processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Women, Business and the Law (WBL 1.0) topic scores, and the yes/no questions each one is the share of (times 100).
# NOTE: Gender Statistics dropped all of these in its October 2026 release, when WBL moved to its 2.0 framework, so we
# take them from the WBL website's 1971-2024 historical panel instead (see the women_business_and_the_law meadow step,
# which also explains why the 2026 report isn't used yet). On the next update, check whether Gender Statistics publishes
# them again, and whether the World Bank has published a corrected WBL 1.0 history.
WBL_TOPICS = {
    "sg_law_indx_mo": ["sg_loc_live_eq", "sg_hme_trvl_eq", "sg_apl_pspt_eq", "sg_ctr_trvl_eq"],
    "sg_law_indx_wp": ["sg_get_jobs_eq", "sg_law_nodc_hr", "sg_leg_sxhr_em", "sg_pen_sxhr_em"],
    "sg_law_indx_py": ["sg_law_eqrm_wk", "sg_ngt_work_eq", "sg_dng_work_dn_eq", "sg_ind_work_eq"],
    "sg_law_indx_mr": ["sg_law_obhb_mr_no", "sg_hld_head_eq", "sg_leg_dvaw", "sg_obt_dvrc_eq", "sg_rem_rigt_eq"],
    "sg_law_indx_pr": ["sh_mmr_leve_al", "sh_mmr_leve_gt", "sh_ptr_leve_al", "sh_par_leve_al", "sg_dml_prgw"],
    "sg_law_indx_en": ["sg_law_crdd_gr", "sg_cnt_sign_eq", "sg_bus_regt_eq", "sg_opn_bank_eq"],
    "sg_law_indx_as": ["sg_own_prrt_im", "sg_iht_asst_pt_eq", "sg_iht_asst_eq", "sg_law_asst_ar", "sg_law_nmcn"],
    "sg_law_indx_pe": ["sg_age_fupn_eq", "sg_age_papn_eq", "sg_age_mret_eq", "sg_abs_penb"],
}
WBL_QUESTIONS = [question for questions in WBL_TOPICS.values() for question in questions]
# WBL leave lengths, in calendar days.
WBL_LEAVE_DAYS = ["sh_mmr_leve", "sh_ptr_leve", "sh_par_leve", "sh_par_leve_fe", "sh_par_leve_ma"]
WBL_COLUMNS = ["sg_law_indx"] + list(WBL_TOPICS) + WBL_QUESTIONS + WBL_LEAVE_DAYS
WBL_SCORES = ["sg_law_indx"] + list(WBL_TOPICS)

# World Bank groups for which Gender Statistics published averages of the WBL scores, and their names in our data.
# NOTE: WBL lists high-income OECD members as a separate group ("High income: OECD"), so these regions exclude them,
# exactly as the averages Gender Statistics published did. For the same reason, North America can't be rebuilt.
WBL_REGIONS = {
    "East Asia & Pacific": "East Asia and Pacific (WB)",
    "Europe & Central Asia": "Europe and Central Asia (WB)",
    "Latin America & Caribbean": "Latin America and Caribbean (WB)",
    "South Asia": "South Asia (WB)",
    "Sub-Saharan Africa": "Sub-Saharan Africa (WB)",
}
# Series the producer ships with no data at all, which we drop.
# NOTE: In the October 2026 release, these "female share of graduates" series are empty, although the previous release
# had data for them. On the next update, check whether the data is back (and remove them from this list).
EMPTY_SERIES = [
    "se_ter_grad_fe_ag_zs",
    "se_ter_grad_fe_ed_zs",
    "se_ter_grad_fe_en_zs",
    "se_ter_grad_fe_hl_zs",
    "se_ter_grad_fe_hu_zs",
    "se_ter_grad_fe_ot_zs",
    "se_ter_grad_fe_sc_zs",
    "se_ter_grad_fe_ss_zs",
    "se_ter_grad_fe_sv_zs",
]

WBL_INCOME_GROUPS = {
    "High income": "High-income countries",
    "Upper middle income": "Upper-middle-income countries",
    "Lower middle income": "Lower-middle-income countries",
    "Low income": "Low-income countries",
}


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("gender_statistics")

    # Read table from meadow dataset.
    tb = ds_meadow.read("gender_statistics")

    # Load Women, Business and the Law (WBL 1.0) data.
    ds_wbl = paths.load_dataset("women_business_and_the_law")
    tb_wbl = ds_wbl.read("women_business_and_the_law")

    # Retrieve snapshot.
    snap = paths.load_snapshot("gender_statistics.zip")
    # Load metadata from snapshot.
    tb_meta = snap.read_in_archive("Gender_StatsSeries.csv", low_memory=False, safe_types=False)
    tb_meta = tb_meta[
        [
            "Series Code",
            "Indicator Name",
            "Long definition",
            "Unit of measure",
            "Aggregation method",
            "Limitations and exceptions",
            "Notes from original source",
            "General comments",
            "Source",
        ]
    ]
    tb_meta = tb_meta.rename(columns={"Series Code": "indicator_code"})

    # Some series have no name in the series file, so fall back to the name given in the data file.
    # NOTE: In the October 2026 release, this affects 14 of the 15 "other COVID-19 pandemic-related outcomes" series.
    tb_names = snap.read_in_archive("Gender_StatsCSV.csv", usecols=["Indicator Code", "Indicator Name"])
    names = tb_names.drop_duplicates().set_index("Indicator Code")["Indicator Name"]
    tb_meta["Indicator Name"] = tb_meta["Indicator Name"].fillna(tb_meta["indicator_code"].map(names))
    assert tb_meta["Indicator Name"].notna().all(), "Series with no name in either the series or the data file."

    #
    # Process data.
    #
    sanity_check_inputs(tb=tb, tb_wbl=tb_wbl)

    # Harmonize country names (both tables share the mapping, so unused names are checked across both below).
    check_unused_country_names(tb=tb, tb_wbl=tb_wbl)
    tb = paths.regions.harmonize_names(tb, warn_on_unused_countries=False)

    # Pivot table to have indicators as columns
    tb = tb.pivot(index=["country", "year"], columns="indicator_code", values="value")
    tb = tb.reset_index()

    # Add metadata to indicators
    tb = add_metadata(tb, tb_meta)

    # Add WBL 1.0 indicators, under the codes Gender Statistics used for them.
    tb_wbl = prepare_wbl(tb_wbl)
    tb = pr.merge(tb, tb_wbl, on=["country", "year"], how="outer")

    # Improve table format.
    tb = tb.format(["country", "year"], short_name=paths.short_name)

    # Drop series that have no data at all.
    empty = sorted(column for column in tb.columns if tb[column].isna().all())
    assert empty == EMPTY_SERIES, f"Series with no data changed. Now empty: {empty}"
    tb = tb.drop(columns=empty)

    # Remove known errors in the source data (see the .corrections.yml file).
    tb = paths.apply_corrections(tb)

    # Combine maternity and paternity leave indicators (time only available TO each parent)
    tb["total_maternity_leave_to"] = tb["sh_mmr_leve"] + tb["sh_par_leve_fe"]
    tb["total_paternity_leave_to"] = tb["sh_par_leve_ma"] + tb["sh_ptr_leve"]

    # Combine maternity and paternity leave indicators (time available days FOR each parent)
    tb["total_maternity_leave_for"] = tb["sh_mmr_leve"] + tb["sh_par_leve_fe"] + tb["sh_par_leve"]
    tb["total_paternity_leave_for"] = tb["sh_par_leve_ma"] + tb["sh_ptr_leve"] + tb["sh_par_leve"]

    # The producer descriptions of these sums, and of the WBL indicators they combine, are in the .meta.yml file.

    sanity_check_outputs(tb)

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()


def sanity_check_inputs(tb: Table, tb_wbl: Table) -> None:
    """Check the Gender Statistics and WBL inputs before processing."""
    # Gender Statistics no longer has the WBL 1.0 indicators; if they come back, decide which source to keep.
    gs_codes = set(tb["indicator_code"].astype(str).str.lower().str.replace(".", "_", regex=False))
    overlap = sorted(gs_codes & set(WBL_COLUMNS))
    assert not overlap, f"Gender Statistics publishes WBL 1.0 indicators again: {overlap}"

    # WBL must have one row per economy and report year, with the columns we expect.
    assert set(tb_wbl.columns) == {"country", "report_year", "region", "income_group"} | set(WBL_COLUMNS), (
        "Unexpected WBL columns."
    )
    regions = set(tb_wbl["region"].dropna().astype(str))
    assert regions == set(WBL_REGIONS) | {"High income: OECD", "Middle East & North Africa"}, f"WBL regions: {regions}"
    income_groups = set(tb_wbl["income_group"].dropna().astype(str))
    assert income_groups <= set(WBL_INCOME_GROUPS) | {"Not classified"}, f"WBL income groups: {income_groups}"
    assert not tb_wbl.duplicated(subset=["country", "report_year"]).any(), "Duplicate WBL economy-years."
    assert tb_wbl[WBL_COLUMNS].notna().all().all(), "Missing values in WBL data."

    # Answers are yes or no; scores are within 0-100; leave lengths are non-negative.
    for column in WBL_QUESTIONS:
        values = set(tb_wbl[column].astype(str).unique())
        assert values <= {"Yes", "No"}, f"Unexpected answers in {column}: {values}"
    scores = tb_wbl[["sg_law_indx"] + list(WBL_TOPICS)]
    assert ((scores >= 0) & (scores <= 100)).all().all(), "WBL score outside 0-100."
    assert (tb_wbl[WBL_LEAVE_DAYS] >= 0).all().all(), "Negative WBL leave length."

    # Each topic score is the share of yes answers in that topic (catches a misassigned column header).
    for topic, questions in WBL_TOPICS.items():
        share_yes = (tb_wbl[questions] == "Yes").mean(axis=1) * 100
        assert (abs(tb_wbl[topic] - share_yes) < 1e-3).all(), f"{topic} doesn't match its yes/no answers."
    # The index is the average of the eight topic scores.
    assert (abs(tb_wbl["sg_law_indx"] - tb_wbl[list(WBL_TOPICS)].mean(axis=1)) < 1e-3).all(), "WBL index mismatch."

    # Coverage: 190 economies in every report year.
    n_economies = tb_wbl.groupby("report_year")["country"].nunique()
    assert (n_economies == 190).all(), f"Unexpected number of WBL economies: {n_economies[n_economies != 190]}"


def check_unused_country_names(tb: Table, tb_wbl: Table) -> None:
    """Warn about names in the countries file that neither Gender Statistics nor WBL use."""
    names = set(tb["country"].astype(str)) | set(tb_wbl["country"].astype(str))
    unused = sorted(set(json.loads(paths.country_mapping_path.read_text())) - names)
    if unused:
        paths.log.warning(f"Unused names in the countries file: {unused}")


def prepare_wbl(tb_wbl: Table) -> Table:
    """Turn WBL answers into 1/0, assign each report to its data year, and harmonize country names."""
    tb_wbl = tb_wbl.copy()
    for column in WBL_QUESTIONS:
        tb_wbl[column] = (tb_wbl[column] == "Yes").astype("Int8").copy_metadata(tb_wbl[column])

    # Each report describes the laws in force on October 1 of the previous year, which is how Gender Statistics
    # dated these indicators (for example, the 2026 report is shown as 2025).
    tb_wbl["year"] = tb_wbl["report_year"] - 1
    tb_wbl = tb_wbl.drop(columns=["report_year"])

    tb_wbl = paths.regions.harmonize_names(
        tb_wbl, warn_on_unused_countries=False, warn_on_unknown_excluded_countries=False
    )

    # Rebuild the averages of the WBL scores that Gender Statistics used to publish for World Bank groups.
    tb_wbl = add_wbl_averages(tb_wbl)

    return tb_wbl


def add_wbl_averages(tb_wbl: Table) -> Table:
    """Add unweighted averages of the WBL scores for the world, World Bank regions, and income groups.

    This is how the World Bank computes them: the averages Gender Statistics published until 2025 are reproduced exactly
    (to 1e-3). Like WBL's own panel, we apply each economy's classification in the latest report year to all years.
    """
    latest = tb_wbl.sort_values("year").groupby("country", observed=True).tail(1)[["country", "region", "income_group"]]
    tb_wbl = pr.merge(tb_wbl.drop(columns=["region", "income_group"]), latest, on="country", how="left")

    groups = [tb_wbl.assign(group="World")]
    groups.append(tb_wbl.assign(group=tb_wbl["region"].astype(str).map(WBL_REGIONS)))
    groups.append(tb_wbl.assign(group=tb_wbl["income_group"].astype(str).map(WBL_INCOME_GROUPS)))
    tb_groups = pr.concat(groups, ignore_index=True).dropna(subset=["group"])
    tb_averages = tb_groups.groupby(["group", "year"], as_index=False, observed=True)[WBL_SCORES].mean()
    tb_averages = tb_averages.rename(columns={"group": "country"})

    tb_wbl = pr.concat([tb_wbl.drop(columns=["region", "income_group"]), tb_averages], ignore_index=True)

    return tb_wbl


def sanity_check_outputs(tb: Table) -> None:
    """Check the combined output table."""
    wbl = tb[WBL_COLUMNS].dropna(how="all")
    years = set(wbl.index.get_level_values("year"))
    assert years == set(range(1970, 2024)), f"Unexpected WBL years: {sorted(years)}"
    for column in WBL_QUESTIONS:
        assert set(wbl[column].dropna().unique()) <= {0, 1}, f"{column} is not 1/0."
    # Each total is at least as long as each of its parts.
    assert (tb["total_maternity_leave_for"].dropna() >= tb["total_maternity_leave_to"].dropna()).all()
    assert (tb["total_paternity_leave_for"].dropna() >= tb["total_paternity_leave_to"].dropna()).all()


def add_metadata(tb: Table, tb_meta: pd.DataFrame) -> Table:
    """Add metadata to table columns using World Bank metadata.

    Args:
        tb: Table with data columns to add metadata to
        tb_meta: DataFrame with metadata information

    Returns:
        Table with updated metadata
    """
    # Create a mapping from indicator_code to metadata
    metadata_dict = tb_meta.set_index("indicator_code").to_dict("index")

    for column in tb.columns:
        if column not in ["country", "year"]:
            # Get the column metadata object
            meta = tb[column].metadata

            if column in metadata_dict:
                meta_info = metadata_dict[column]

                # Set title from Indicator Name (some producer names contain double spaces).
                indicator_name = " ".join(str(meta_info.get("Indicator Name", column)).split())
                meta.title = indicator_name

                # Set source attribution
                source = meta_info.get("Source", "")
                if source and pd.notna(source):
                    cleaned_source = clean_source_field(source)
                    if cleaned_source:
                        meta.presentation = {"attribution": cleaned_source}

                # Handle units
                meta.unit, meta.short_unit = clean_unit(meta_info.get("Unit of measure", ""), title=meta.title)
                if meta.unit == "%":
                    meta.display = {"numDecimalPlaces": 1}

                # Build description from producer
                description_parts = []

                # Add long definition
                long_def = meta_info.get("Long definition", "")
                if pd.notna(long_def) and long_def:
                    description_parts.append(f"**Definition:** {long_def}")

                # Add aggregation method
                agg_method = meta_info.get("Aggregation method", "")
                if pd.notna(agg_method) and agg_method:
                    description_parts.append(f"**Aggregation method:** {agg_method}")

                # Add limitations and exceptions
                limitations = meta_info.get("Limitations and exceptions", "")
                if pd.notna(limitations) and limitations:
                    description_parts.append(f"**Limitations and exceptions:** {limitations}")

                # Add notes from original source
                notes = meta_info.get("Notes from original source", "")
                if pd.notna(notes) and notes:
                    description_parts.append(f"**Notes from original source:** {notes}")

                # Add general comments
                comments = meta_info.get("General comments", "")
                if pd.notna(comments) and comments:
                    description_parts.append(f"**General comments:** {comments}")

                # Combine all parts
                if description_parts:
                    meta.description_from_producer = "\n\n".join(description_parts)
                else:
                    meta.description_from_producer = f"World Bank indicator: {column}"

                # Fix detected typo:
                meta.description_from_producer = meta.description_from_producer.replace("inidcator", "indicator")
            else:
                # Column not found in metadata - set defaults
                meta.title = column
                meta.unit = ""
                meta.short_unit = ""
                meta.description_from_producer = f"World Bank indicator: {column}"

    return tb


def clean_unit(unit_of_measure, title: str) -> tuple[str, str]:
    """Return (unit, short_unit) from the producer's unit, or from the indicator name when the producer gives none.

    Many series have no unit in the series file, but their name ends with it, e.g. "... (% of women age 15-49)" or
    "... (per 100,000 female population)".
    """
    import re

    unit = str(unit_of_measure).strip() if pd.notna(unit_of_measure) else ""
    if "percent" in unit.lower() or "%" in unit:
        return "%", "%"
    # Labels that say nothing about the unit (counts, scores and indexes, whose scale is in the name).
    if unit in ("Unit", "Number", "Count", "Score") or unit.startswith(("Index", "Number of 15-year old women")):
        unit = ""
    if unit == "Persons":
        unit = "people"
    if unit:
        # Lowercase "Per 1000 live births", "Years", etc., but not acronyms.
        if len(unit) > 1 and unit[1].islower():
            unit = unit[0].lower() + unit[1:]
        return unit, ""

    # Fall back to the unit given in the indicator name.
    if "(%" in title:
        return "%", "%"
    match = re.search(r"\(([^()]*)\)\s*$", title)
    if match:
        unit_in_title = match.group(1).strip()
        if unit_in_title in ("days", "calendar days"):
            return "days", ""
        if unit_in_title.startswith("per "):
            return unit_in_title, ""
    return "", ""


def clean_source_field(source: str) -> str:
    """Clean up source field by simplifying to key organization names."""
    if not source or pd.isna(source):
        return ""

    import re

    # Create a simple lookup table for common sources to clean names
    source_mappings = {
        # World Bank variants
        "world bank": "World Bank",
        "wb": "World Bank",
        "world bank group": "World Bank",
        "wbg": "World Bank",
        "doing business": "World Bank",
        "enterprise surveys": "World Bank",
        "women, business and the law": "Women, Business and the Law",
        "entrepreneurship survey": "World Bank",
        "global findex": "World Bank",
        # UN variants
        "united nations population division": "UN Population Division via World Bank",
        "world population prospects": "UN Population Division via World Bank",
        "world urbanization prospects": "UN Population Division via World Bank",
        "world marriage data": "UN Population Division via World Bank",
        "trends in international migrant stock": "UN Population Division via World Bank",
        "united nations": "UN via World Bank",
        # UNESCO variants
        "unesco institute for statistics": "UNESCO via World Bank",
        "uis": "UNESCO via World Bank",
        "stat bulk data download service": "UNESCO via World Bank",
        # WHO variants
        "world health organization": "WHO via World Bank",
        "global health observatory": "WHO via World Bank",
        "global health estimates": "WHO via World Bank",
        "maternal mortality": "WHO via World Bank",
        "trends in maternal mortality": "WHO via World Bank",
        # UNICEF variants
        "un children's fund": "UNICEF via World Bank",
        "unicef data": "UNICEF via World Bank",
        "state of the world's children": "UNICEF via World Bank",
        "childinfo": "UNICEF via World Bank",
        # ILO variants
        "international labour organization": "ILO via World Bank",
        "ilostat": "ILO via World Bank",
        "labour force statistics": "ILO via World Bank",
        "ilo modelled estimates": "ILO via World Bank",
        # Other UN agencies
        "international monetary fund": "IMF via World Bank",
        "joint united nations programme on hiv/aids": "UNAIDS via World Bank",
        "unaids": "UNAIDS via World Bank",
        "international telecommunication union": "ITU via World Bank",
        "inter-parliamentary union": "IPU via World Bank",
        "un women": "UN Women via World Bank",
        "unfpa": "UNFPA via World Bank",
        "unodc": "UNODC via World Bank",
        # Survey programs
        "demographic and health surveys": "DHS via World Bank",
        "dhs program": "DHS via World Bank",
        "multiple indicator cluster surveys": "MICS via World Bank",
        # Other organizations
        "organisation for economic co-operation and development": "OECD via World Bank",
        "organization for economic co-operation and development": "OECD via World Bank",
        "eurostat": "Eurostat via World Bank",
        "human mortality database": "Human Mortality Database via World Bank",
        "luxembourg income study": "Luxembourg Income Study via World Bank",
    }

    # Convert to lowercase for matching
    source_lower = source.lower()

    # Special handling for World Bank methodology citations
    if "world bank" in source_lower and ("methodology" in source_lower or "based on" in source_lower):
        # Extract author and year from citations like "Filmer et al. (2018)"
        author_year_match = re.search(r"([A-Za-z]+(?:\s+et\s+al\.)?)\s+\((\d{4})\)", source)
        if author_year_match:
            author = author_year_match.group(1)
            year = author_year_match.group(2)
            return f"World Bank based on methodology in {author} ({year})"

    # Find matching organizations
    found_orgs = set()
    for key, org_name in source_mappings.items():
        if key in source_lower:
            found_orgs.add(org_name)

    # Handle joint initiatives specifically
    if "joint" in source_lower and ("unicef" in source_lower or "who" in source_lower or "world bank" in source_lower):
        if "malnutrition" in source_lower or "jme" in source_lower:
            found_orgs = {"UNICEF-WHO-World Bank JME"}
        elif "child mortality" in source_lower:
            found_orgs = {"UN IGME"}
        elif "water" in source_lower and "sanitation" in source_lower:
            found_orgs = {"WHO-UNICEF JMP"}

    # Special cases for household surveys
    if "household surveys" in source_lower:
        found_orgs.add("Household surveys")

    if "national statistical offices" in source_lower or "country official statistics" in source_lower:
        found_orgs.add("National statistical offices")

    # If no specific organizations found, try to extract the first meaningful part
    if not found_orgs:
        # Remove all URLs and technical metadata first
        clean_text = re.sub(r"https?://[^\s,;)\]]+", "", source)
        clean_text = re.sub(r"\([^)]*\)", "", clean_text)
        clean_text = re.sub(r"uri:.*?(?=[,;]|$)", "", clean_text, flags=re.IGNORECASE)
        clean_text = re.sub(r"note:.*?(?=[,;]|$)", "", clean_text, flags=re.IGNORECASE)
        clean_text = re.sub(r"publisher:.*?(?=[,;]|$)", "", clean_text, flags=re.IGNORECASE)
        clean_text = re.sub(r"type:.*?(?=[,;]|$)", "", clean_text, flags=re.IGNORECASE)
        clean_text = re.sub(r"data accessed:.*?(?=[,;]|$)", "", clean_text, flags=re.IGNORECASE)

        # Take the first substantial part
        parts = re.split(r"[;,\n]", clean_text)
        for part in parts:
            part = part.strip()
            if len(part) > 10 and not re.match(r"^\s*(staff|estimates|based|data|compiled)", part, re.IGNORECASE):
                found_orgs.add(part[:50])  # Truncate if too long
                break

    if not found_orgs:
        return ""

    # "united nations" also matches the full names of UN agencies, so drop the generic UN when an agency was found.
    if "UN via World Bank" in found_orgs and any(org.startswith("UN") for org in found_orgs - {"UN via World Bank"}):
        found_orgs.discard("UN via World Bank")

    # Return up to 2 organizations, sorted for consistency
    result_orgs = sorted(list(found_orgs))[:2]
    return "; ".join(result_orgs)
