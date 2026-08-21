"""The Metadata Diff outputs: a reviewer punch-list and a ready-to-execute PR brief.

Pure Markdown builders — no Streamlit, no DB — so they can be unit-tested, and so the shape of the
brief is decided in one place. Each takes `resolved` rows: one per change group, carrying the group
itself plus the review decision (`label`, `comment`, `stale`, `scope`) the page collected.
"""

from typing import Any

from apps.wizard.app_pages.metadata_diff.core import (
    CHART_FIELD_PREFIX,
    OVERRIDE_TARGET,
    ChangeGroup,
    ViewDiff,
    affected_charts,
    as_bullets,
    as_plaintext,
    dims_str,
    distinct_garden_datasets,
    distinct_indicator_short_names,
    field_label,
    group_usage,
    override_snippet,
    parse_catalog_path,
    yaml_field_snippet,
)

# Statuses a resolved row can carry, and how they route into the brief's sections.
_STATUS_TO_DB = {"✅ Approve": "approved", "🚩 Flag": "flagged"}


def decision(r: dict[str, Any]) -> str:
    """The reviewer's call for one change — approved | flagged | pending | stale — used to route it
    into the PR brief's Apply / Hold / Pending sections."""
    if r["stale"]:
        return "stale"
    return _STATUS_TO_DB.get(str(r.get("label") or r["seed_label"]), "pending")


def review_markdown(
    catalog_path: str,
    baseline_name: str,
    resolved: list[dict[str, Any]],
    usage: dict[int, dict[str, list[dict[str, Any]]]],
) -> str:
    """Compile the reviewer's sign-off + comments into a copy-pasteable punch-list for the author."""

    def _label(r: dict[str, Any]) -> str:
        if r["stale"]:
            return "⚠️ edited since review"
        return str(r.get("label") or r["seed_label"])

    flagged = sum(1 for r in resolved if _label(r).startswith("🚩"))
    lines = [f"# Metadata review — `{catalog_path}`", "", f"_Baseline: {baseline_name}_", ""]
    lines.append(f"**{len(resolved)} distinct change(s)** — {flagged} flagged.")
    lines.append("")
    for r in resolved:
        g = r["g"]
        status = _label(r)
        comment = str(r.get("comment") or "").strip()
        scope = "shared indicator metadata" if g.affects_indicator else "MDim override"
        reach = f"{len(g.view_dims)} view(s)"
        if g.affects_indicator:
            n_charts = len(affected_charts(g, usage))
            if n_charts:
                reach += f", {n_charts} chart(s)"
        lines.append(f"## {field_label(g.field)} — {status}")
        lines.append(f"- **Scope:** {scope}; affects {reach}")
        views = "; ".join(dims_str(d) for d in g.view_dims[:8]) + (" …" if len(g.view_dims) > 8 else "")
        lines.append(f"- **Views:** {views}")
        lines.append(f"- **Before:** {as_plaintext(g.old)}")
        lines.append(f"- **After:** {as_plaintext(g.new)}")
        if comment:
            lines.append(f"- **💬 Comment:** {comment}")
        lines.append("")
    return "\n".join(lines)


_BRIEF_LEGEND = [
    "**▶ To open the PR:** copy this whole brief, paste it to Claude Code, and ask it to open the PR — it "
    "carries the changes, the checks to run, and a ready-to-paste PR description.",
    "",
    "**How to action each change (from the review decisions):**",
    "- ✅ **Approve → add to the PR** — apply the edit shown.",
    "- 🚩 **Flag → hold** — do NOT add; the reviewer wants a change (see the note).",
    "- ⏳ **Pending → do nothing** — not reviewed yet; leave as-is.",
    "",
    "_The **value** to set is exact. The **location** (file + key) is a best guess from the indicator's "
    "catalogPath — confirm it against the metadata build before committing, since a value set via "
    "`definitions`/anchors, `shared.meta.yml`, Jinja, or the step `.py` can live elsewhere._",
    "",
]


def yaml_block(field_name: str, value: Any) -> list[str]:
    return ["```yaml", yaml_field_snippet(field_name, value), "```"]


def garden_location_lines(g: ChangeGroup, reach: str) -> list[str]:
    """File + key hint for a shared indicator field, from its catalogPath.

    Two cases. If the identical text change lands on a single indicator, point at that variable's key.
    If it lands on *several* indicators (the fingerprint of a shared `definitions.*`/anchor — one Jinja
    template renders into many variables), point at the definition instead of guessing a variable, and
    flag the diff-observed reach as a floor. The single-variable key is a wrong, misleading target for a
    shared-definition edit, which is exactly the mistake this tool exists to prevent.

    A third case sits on top of both: the group is keyed on the text, so the same edit made in two
    different garden datasets arrives as one group. Naming only the first dataset's file would send the
    author to fix half the change, so every dataset carrying it is named and the shared-definition claim
    is dropped — separate files cannot share a `definitions.*` block."""
    parsed = parse_catalog_path(g.catalog_path)
    garden_dirs = distinct_garden_datasets(g.catalog_paths) or ([parsed[0]] if parsed else [])
    garden_dir = parsed[0] if parsed else None
    table = parsed[1] if parsed else None
    if len(garden_dirs) > 1:
        files = ", ".join(f"`{d}.meta.yml`" for d in garden_dirs)
        return [
            f"- **Files ({len(garden_dirs)} datasets):** {files} — or their `.meta.override.yml`",
            f"- **The identical text was edited in {len(garden_dirs)} separate garden datasets**, so this is "
            "that many edits, not one: no `definitions.*` block is shared across datasets. Make the change "
            "in each file, and rebuild each dataset.",
            f"- **Reach (observed in this diff):** {reach}.",
        ]
    file_line = (
        f"- **File (best guess):** `{garden_dir}.meta.yml` — or `{garden_dir}.meta.override.yml`"
        if garden_dir
        else "- **Where:** the indicator's garden `.meta.yml` (catalogPath unavailable)"
    )

    shared_names = distinct_indicator_short_names(g.catalog_paths)
    if len(shared_names) > 1:
        preview = ", ".join(f"`{n}`" for n in shared_names[:6]) + (" …" if len(shared_names) > 6 else "")
        dont_edit = f"`tables.{table}.variables.<short>`" if table else "any single variable"
        return [
            file_line,
            f"- **Likely a shared definition/anchor** — the identical text renders on at least "
            f"{len(shared_names)} indicators ({preview}) *within this MDim alone*, which happens through a "
            "shared `definitions.*` (Jinja) block or `shared.meta.yml`, not a per-variable field.",
            f"- **Find it:** grep the garden `.meta.yml` for the changed text and edit the `definitions.` "
            f"entry (or `shared.meta.yml`) — do **not** edit {dont_edit} directly.",
            f"- **Reach (observed in this diff):** {reach} — **treat as a floor.** This diff only sees the "
            "indicators used by this MDim; the definition is typically referenced by many more, so grep it "
            "to get the real count before deciding. (A branched definition changes only the matching branch "
            "— e.g. wealth views, not income — so verify which branch you edited.)",
        ]
    if parsed:
        return [
            file_line,
            f"- **Key:** `tables.{table}.variables.{parsed[2]}`",
            f"- **Reach (observed in this diff):** {reach}.",
        ]
    return [
        file_line,
        f"- **Reach (observed in this diff):** {reach}.",
    ]


# Indicator fields that a chart's own text can inherit. A chart that sets the corresponding key in
# its config patch is "shielded" — it keeps its own text and does NOT change with the indicator edit
# (see the edit-faust-metadata skill's per-field inheritance analysis).
INHERITED_TO_CHART_TEXT = {"titlePublic": "title", "descriptionShort": "subtitle"}

# Surfaces the blast radius here does NOT cover, and where to get each one. Named explicitly because
# an unlisted surface reads as "nothing else is affected", which is the one wrong signal to send.
_UNCOVERED_SURFACES = [
    "**Narrative charts** — children of an affected chart or MDim view. They inherit the parent's text, "
    "but the stored merged config can be stale, so an inheriting child keeps showing the OLD text until "
    "its patch is re-saved. A child that overrides the field keeps its own text permanently.",
    "**Explorer views** — deliberately not queried here (explorers are being phased out); legacy "
    "CSV-backed explorers are invisible to the DB tables entirely.",
    "**Data insights, static viz, key-chart slots, article links & embeds** — not queried here. Embeds "
    "don't break, but the text a reader sees changes.",
]


def changed_text_lines(g: ChangeGroup) -> list[str]:
    """The exact text that changed, as a diff — the right payload for a shared-definition edit.

    For a shared `definitions.*` edit the pastable full-field YAML is actively wrong: the diffed value
    is the *rendered* output, so pasting it under a variable hardcodes rendered text and destroys the
    Jinja branches for every other dimension. What the executor needs is the one line to find and
    replace inside the definition, so we emit only the changed bullet(s) as a diff."""
    old, new = as_bullets(g.old), as_bullets(g.new)
    reordered = False
    if isinstance(old, list) and isinstance(new, list):
        old_set = {str(x).strip() for x in old}
        new_set = {str(x).strip() for x in new}
        removed = [str(x) for x in old if str(x).strip() not in new_set]
        added = [str(x) for x in new if str(x).strip() not in old_set]
        # A reorder edits no bullet, so membership finds nothing to show. Emitting nothing would leave the
        # executor a detected change with no instruction, so both orders go in and the note says which.
        if not removed and not added and [str(x).strip() for x in old] != [str(x).strip() for x in new]:
            reordered = True
            removed, added = [str(x) for x in old], [str(x) for x in new]
    else:
        removed = [str(old)] if str(old).strip() else []
        added = [str(new)] if str(new).strip() else []
    if not removed and not added:
        return []
    lead = (
        "- **The bullets were reordered** — no bullet's text changed, so reorder them to match the new "
        "order below (in the definition, not in a variable):"
        if reordered
        else "- **The text that changed** — find this inside the definition and replace it "
        "(do not paste a rendered value into a variable, it would break the Jinja branches):"
    )
    out = [
        lead,
        "```diff",
    ]
    out += [f"- {t}" for t in removed]
    out += [f"+ {t}" for t in added]
    out.append("```")
    return out


def surface_lines(g: ChangeGroup, usage: dict[int, dict[str, list[dict[str, Any]]]], scope: str) -> list[str]:
    """Name every chart and MDim this change lands on — not just a count.

    A count ("10 charts") is not something an author can check. Applying to all means those specific
    charts change, so the brief lists them by slug, and flags any chart that does NOT render a data page
    (a multi-indicator chart has no single data page, so a WYSK edit is invisible to its readers). When
    the change is scoped, the same list is what *keeps* the old text — equally worth seeing."""
    imp = group_usage(g, usage)
    charts, mdims = imp.get("charts", []), imp.get("mdims", [])
    if not charts and not mdims:
        return ["- **Affected surfaces:** none — no published chart or other MDim uses these indicators."]

    verb = "will change" if scope != "scoped" else "keep the old text (scoped)"
    out: list[str] = []
    if charts:
        out.append(f"- **Charts that {verb} ({len(charts)}):**")
        for c in sorted(charts, key=lambda c: str(c.get("slug") or "")):
            slug = c.get("slug") or f"chart {c.get('chartId')}"
            note = (
                ""
                if c.get("has_data_page", True)
                else " — multi-indicator chart: shown under *Learn more about this data*, not on a data page"
            )
            out.append(f"  - [`{slug}`](https://ourworldindata.org/grapher/{slug}){note}")
    if mdims:
        out.append(f"- **Other MDims that {verb} ({len(mdims)}):**")
        for m in sorted(mdims, key=lambda m: str(m.get("slug") or "")):
            out.append(f"  - `{m.get('slug') or m.get('catalogPath')}`")
    # These fields also feed a chart's title/subtitle by inheritance, and a chart carrying its own
    # value for that field is *shielded* — it keeps its current text. We list usage, not inheritance,
    # so for those fields the list above is an upper bound on the charts whose visible text changes.
    if g.field in INHERITED_TO_CHART_TEXT and charts:
        out.append(
            f"- _⚠️ Upper bound: `{field_label(g.field)}` also feeds the chart's "
            f"{INHERITED_TO_CHART_TEXT[g.field]} by inheritance, and a chart that sets its own "
            f"{INHERITED_TO_CHART_TEXT[g.field]} in its config keeps that text. This list is indicator "
            "usage, not per-field inheritance — for the exact set, run "
            f"`blast_radius.py --field {INHERITED_TO_CHART_TEXT[g.field]}` (edit-faust-metadata skill)._"
        )
    return out


def pending_lines(header: str, rows: list[dict[str, Any]]) -> list[str]:
    out = [header]
    for r in rows:
        tag = "edited since review" if r["stale"] else "not reviewed"
        out.append(f"- {field_label(r['g'].field)} — {tag}")
    out.append("")
    return out


def change_one_liner(g: ChangeGroup) -> str:
    """One-line summary of an approved change for the PR-description draft."""
    label = field_label(g.field)
    if g.field.startswith(CHART_FIELD_PREFIX):
        return f"**{label}** — chart config (edited on the chart itself, not the ETL repo)"
    where = "shared indicator metadata" if g.affects_indicator else "MDim-level override"
    return f"**{label}** — {where}"


def ship_section(approved_groups: list[ChangeGroup], baseline_name: str) -> list[str]:
    """The 'best of both' tail of the brief: a rigor checklist (blast radius, make check, metadata quality
    checks, staging rebuild + verify, Codex) plus a ready-to-paste PR description — so the copied brief is a
    complete, rigorous spec for opening the PR, not just a list of edits."""
    # Distinct garden datasets touched by shared-indicator edits, for concrete rebuild/upsert commands.
    # Every dataset in the group, not the group's first: one group can span two datasets (it is keyed on
    # the text), and a rebuild list missing one of them leaves that edit unpublished on staging.
    datasets: list[str] = []
    for g in approved_groups:
        dirs = distinct_garden_datasets(g.catalog_paths)
        if not dirs:
            parsed = parse_catalog_path(g.catalog_path)
            dirs = [parsed[0]] if parsed else []
        for d in dirs:
            ds = d.replace("etl/steps/data/garden/", "")
            if ds not in datasets:
                datasets.append(ds)
    if datasets:
        build = "\n".join(
            f"  - `.venv/bin/etlr garden/{ds} grapher/{ds} --private` → "
            f"`STAGING=1 .venv/bin/etlr grapher://grapher/{ds} --grapher`"
            for ds in datasets
        )
    else:
        build = (
            "  - rebuild the edited garden step(s), then `STAGING=1 .venv/bin/etlr grapher://grapher/<step> --grapher`"
        )

    shared = any(g.affects_indicator for g in approved_groups)
    shared_def = any(len(distinct_indicator_short_names(g.catalog_paths)) > 1 for g in approved_groups)
    blast = (
        "shared indicator metadata — reaches every chart / MDim view using the indicator(s); see per-change **Reach** above"
        if shared
        else "contained — no surface beyond the target is affected"
    )
    if shared_def:
        blast += " — includes a **shared definition/anchor** edit reaching multiple indicators (the **Reach** counts above are floors)"
    fields = ", ".join(sorted({field_label(g.field) for g in approved_groups}))

    out = [
        "## 🚀 Ship it — run before opening the PR",
        "_Scope every check to the edited text only._",
        "- [ ] **Blast radius** reviewed (see *Reach* above) — apply-to-all vs scope decided",
        *(
            [
                "- [ ] **Shared definition** — confirmed the edit is in `definitions.*` / `shared.meta.yml` "
                "(not a single variable), and checked every indicator & dimension branch it renders on"
            ]
            if shared_def
            else []
        ),
        "- [ ] `make check`",
        "- [ ] **Typos** — `/check-metadata-typos`",
        "- [ ] **Jinja spacing** — `/check-metadata-spacing`",
        "- [ ] **Style guide** — `/check-metadata-style`",
        "- [ ] **Claims vs the producer** — `/adversarial-data-review` (only the new/edited text, against the source's docs)",
        "- [ ] **Rebuild + upsert to staging:**",
        build,
        "- [ ] **Verify on staging** — indicator metadata API / data page",
        *(
            [
                "- [ ] **Surfaces this brief did NOT check** — sweep them before merge:",
                *[f"  - {s}" for s in _UNCOVERED_SURFACES],
                "  - Full reference sweep: `find-chart-references` skill. Per-field inheritance (which "
                "surfaces an edit actually reaches, which are shielded): `blast_radius.py --field <f>` "
                "(edit-faust-metadata skill).",
            ]
            if shared
            else []
        ),
        "- [ ] **Open the PR** with the description below; post a bare `@codex review`; run the pr-babysitter loop",
        "",
        "## 📝 PR description (draft — paste as the PR body)",
        # Attribution is mandatory on anything posted to GitHub under a human's identity. Left as
        # placeholders on purpose: the tool can't know which assistant/model opens the PR, nor whose
        # handle is at the wheel, and a wrong @-tag pings a real person.
        "> _Written by <assistant> <model name> — @<handle> at the wheel (fill these in)._",
        "",
        f"Update user-facing metadata ({fields}) — {len(approved_groups)} change(s), reviewed and approved in the "
        "Metadata Diff tool.",
        "",
        "**Changes**",
        *[f"- {change_one_liner(g)}" for g in approved_groups],
        "",
        f"**Blast radius:** {blast}."
        + (
            " Counts cover published charts and MDims; narrative charts, explorer views, data insights "
            "and static viz were checked separately (see below)."
            if shared
            else ""
        ),
        "**Checks:** `make check` · typos · Jinja spacing · style guide · claims-vs-producer.",
        f"**Reviewed against baseline:** {baseline_name} (Metadata Diff tool).",
        "**Verification:** staging preview — <link>.",
        "",
        "**Still open**",
        "- _Handed off / Proposed / Unverified — fill from the checklist results before merge._",
        "",
    ]
    return out


def pr_brief_markdown(
    catalog_path: str,
    baseline_name: str,
    resolved: list[dict[str, Any]],
    usage: dict[int, dict[str, list[dict[str, Any]]]],
) -> str:
    """Decision-grouped PR brief for an MDim: **Apply** (approved) carries a turnkey edit — a pastable
    YAML value with a best-guess file+key, or a scoped `.py` override; **Hold** (flagged) carries the
    reviewer's note and is explicitly not applied; **Pending** is listed for reference only."""
    approved = [r for r in resolved if decision(r) == "approved"]
    flagged = [r for r in resolved if decision(r) == "flagged"]
    pending = [r for r in resolved if decision(r) in ("pending", "stale")]

    lines = [
        f"# PR brief — `{catalog_path}`",
        "",
        f"_Baseline: {baseline_name}. ✅ {len(approved)} to apply · 🚩 {len(flagged)} on hold · "
        f"⏳ {len(pending)} pending._",
        "",
        *_BRIEF_LEGEND,
        f"## ✅ Apply — add to the PR ({len(approved)})",
    ]
    if not approved:
        lines.append("_Nothing approved yet._")
    for r in approved:
        g = r["g"]
        field = g.field
        lines.append(f"### {field_label(field)}")
        if not g.affects_indicator:
            lines.append(
                f"- **Where:** MDim-level field in `{catalog_path}` — set it on the view(s) in this MDim's step."
            )
            lines += yaml_block(field, g.new)
        elif r["scope"] == "scoped" and field in OVERRIDE_TARGET:
            n_c = len(r["charts"])
            others = f"the {n_c} other chart(s) keep the old text" if n_c else "no other surface changes"
            lines.append(
                "- **Where:** scope to these views — add an override in this MDim's `.py` step **and** revert "
                f"the shared change in the indicator's garden `.meta.yml` ({others})."
            )
            lines.append("```python")
            for dims in g.view_dims[:8]:
                lines.append(override_snippet(ViewDiff(dimensions=dims), field, g.new))
            if len(g.view_dims) > 8:
                lines.append(f"# … and {len(g.view_dims) - 8} more view(s) — same override")
            lines.append("```")
            lines += surface_lines(g, usage, "scoped")
        else:
            imp = group_usage(g, usage)
            n_c, n_m = len(affected_charts(g, usage)), len(imp.get("mdims", []))
            reach = f"{n_c} chart(s)" + (f" · {n_m} other MDim(s)" if n_m else "") + f" · {len(g.view_dims)} view(s)"
            lines += garden_location_lines(g, reach)
            # Applying to all means these specific charts change — name them, so the author can check.
            lines += surface_lines(g, usage, "all")
            # For a shared definition the pastable full-field YAML would break the Jinja branches, so
            # show the changed line instead; a plain per-variable field still gets the pastable block.
            # The changed line is always the safe, minimal edit. The full rendered field is kept for
            # reference but explicitly NOT pastable unless the field is authored literally — most
            # description_key fields are lists of `{definitions.*}` refs, and overwriting them with a
            # rendered value silently drops every other definition and Jinja branch.
            lines += changed_text_lines(g)
            lines.append(
                "- _Full rendered value, for reference — do **not** paste it over the field unless "
                "the field is authored literally (no `{definitions.*}` refs, no Jinja):_"
            )
            lines += yaml_block(field, g.new)
        lines.append("")

    if flagged:
        lines.append(f"## 🚩 Hold — flagged, do NOT add ({len(flagged)})")
        for r in flagged:
            g = r["g"]
            comment = str(r.get("comment") or "").strip()
            lines.append(f"### {field_label(g.field)}")
            if comment:
                lines.append(f"- **Reviewer:** {comment}")
            lines.append(f"- **Proposed:** {as_plaintext(g.old)} → {as_plaintext(g.new)}")
            lines.append("")

    if pending:
        lines += pending_lines(f"## ⏳ Pending — no action ({len(pending)})", pending)
    if approved:
        lines += ship_section([r["g"] for r in approved], baseline_name)
    return "\n".join(lines)


def chart_pr_brief_markdown(
    chart: dict[str, Any],
    baseline_name: str,
    resolved: list[dict[str, Any]],
    usage: dict[int, dict[str, list[dict[str, Any]]]],
    catalog_root: str,
) -> str:
    """Decision-grouped PR brief for a standalone chart. Approved indicator-layer changes carry a turnkey
    garden YAML edit; approved chart-config changes (title/subtitle/footnote) are flagged as NOT an ETL
    edit (they belong on the chart itself). Flagged → hold; pending → no action."""
    approved = [r for r in resolved if decision(r) == "approved"]
    flagged = [r for r in resolved if decision(r) == "flagged"]
    pending = [r for r in resolved if decision(r) in ("pending", "stale")]
    slug = chart.get("slug")

    lines = [
        f"# PR brief — chart `{slug}`",
        "",
        f"_Baseline: {baseline_name}. ✅ {len(approved)} to apply · 🚩 {len(flagged)} on hold · "
        f"⏳ {len(pending)} pending._",
        "",
        *_BRIEF_LEGEND,
        f"## ✅ Apply — add to the PR ({len(approved)})",
    ]
    if not approved:
        lines.append("_Nothing approved yet._")
    for r in approved:
        g = r["g"]
        field = g.field
        lines.append(f"### {field_label(field)}")
        if field.startswith(CHART_FIELD_PREFIX):
            lines.append(
                "- ⚠️ **Not an ETL edit** — this is the chart's own config (title/subtitle/footnote). Change "
                "it on the chart itself (grapher admin / chart-diff), not in the ETL repo."
            )
            lines.append(f"- **Set to:** {as_plaintext(g.new)}")
        elif g.affects_indicator:
            imp = group_usage(g, usage)
            n_c, n_m = len(affected_charts(g, usage)), len(imp.get("mdims", []))
            # `"..." or "no other surface"` never reached the fallback — a non-empty f-string is truthy, so
            # a change reaching nothing else read as "0 other chart(s)". Say it in words instead.
            if n_c or n_m:
                reach = f"{n_c} other chart(s)" + (f" · {n_m} MDim(s)" if n_m else "")
            else:
                reach = "no other surface"
            lines += garden_location_lines(g, reach)
            lines += surface_lines(g, usage, "all")
            # The changed line is always the safe, minimal edit. The full rendered field is kept for
            # reference but explicitly NOT pastable unless the field is authored literally — most
            # description_key fields are lists of `{definitions.*}` refs, and overwriting them with a
            # rendered value silently drops every other definition and Jinja branch.
            lines += changed_text_lines(g)
            lines.append(
                "- _Full rendered value, for reference — do **not** paste it over the field unless "
                "the field is authored literally (no `{definitions.*}` refs, no Jinja):_"
            )
            lines += yaml_block(field, g.new)
        else:
            lines.append("- **Where:** the indicator's garden `.meta.yml`.")
            lines += yaml_block(field, g.new)
        lines.append("")

    if flagged:
        lines.append(f"## 🚩 Hold — flagged, do NOT add ({len(flagged)})")
        for r in flagged:
            g = r["g"]
            lines.append(f"### {field_label(g.field)}")
            lines.append(f"- **Proposed:** {as_plaintext(g.old)} → {as_plaintext(g.new)}")
            lines.append("")

    if pending:
        lines += pending_lines(f"## ⏳ Pending — no action ({len(pending)})", pending)
    if approved:
        lines += ship_section([r["g"] for r in approved], baseline_name)
    return "\n".join(lines)


# The reviewer only accepts or rejects — the scope decision belongs to the author (View diff toggle).
