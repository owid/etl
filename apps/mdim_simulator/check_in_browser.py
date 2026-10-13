"""Build the MDim simulator, click through it in headless Chromium like a user would, and check what happens.

Builds its own pages (an empty one, one pre-filled with "Number of people using the Internet", and one reopened
from the setup the run copies out), prints one line per check, and exits with an error if any fails. Screenshots
and setups land in ai/mdim_simulator/. Not a unit test (pytest only collects tests/): run it by hand after
changing the page. Needs Tailscale, because the page loads Grapher from the Tailnet.

    .venv/bin/python apps/mdim_simulator/check_in_browser.py
"""

import importlib.util
import json
import re
import sys
from pathlib import Path

import yaml
from playwright.sync_api import Page, sync_playwright

from etl.viz.chart.core.utils import create_chart_from_config

OUT = Path(__file__).parents[2] / "ai" / "mdim_simulator"
PAGE = OUT / "check.html"
BLANK = OUT / "check_blank.html"
# Free-form "What you should know": an intro paragraph, a wrapped line and nested bullets, none of which may be lost.
FREE_FORM_WYSK = (
    "Some context before the points.\n"
    "- A first point\n  that wraps onto a second line.\n"
    "- A second point:\n  - with a nested item\n  - and another one"
)
REOPENED = OUT / "check_reopened.html"
SCREENSHOTS = OUT / "screenshots"

# build.py is a script, not a package module: load it from its file.
_spec = importlib.util.spec_from_file_location("mdim_simulator_build", Path(__file__).parent / "build.py")
assert _spec and _spec.loader
build = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(build)

failures: list[str] = []


def check(label: str, ok: bool, detail: object = "") -> None:
    print(f"{'✓' if ok else '✗'} {label}" + (f": {detail}" if detail != "" else ""))
    if not ok:
        failures.append(label)


def box_of(locator) -> dict:
    """An element's bounding box; Playwright returns None for elements that aren't rendered."""
    box = locator.bounding_box()
    assert box is not None, f"{locator} is not rendered"
    return box


def saved(page: Page) -> dict:
    """The setup the main check page keeps in the browser, under that page's own storage key."""
    return json.loads(
        page.evaluate("localStorage.getItem(Object.keys(localStorage).find((k) => k.includes(':check:')))")
    )


def copied_setup(page: Page) -> str:
    page.click("#copy")
    page.wait_for_timeout(500)
    return page.evaluate("navigator.clipboard.readText()")


def build_pages() -> None:
    """A page pre-filled with the newest "Number of people using the Internet", and an empty one."""
    _, number_path = build.latest_indicator(*build.EXAMPLE_INDICATOR)
    setup = {
        "title": {"title": "Internet users"},
        "topic_tags": ["Internet", "Technological Change"],
        "dimensions": [
            {"slug": "metric", "name": "Metric", "choices": [{"slug": "number_of_people", "name": "Number of people"}]}
        ],
        "views": [
            {
                "dimensions": {"metric": "number_of_people"},
                "indicators": {"y": [{"catalogPath": number_path}]},
                "metadata": {"description_key": FREE_FORM_WYSK},
            }
        ],
    }
    OUT.mkdir(parents=True, exist_ok=True)
    setup_path = OUT / "check_setup.yml"
    setup_path.write_text(yaml.safe_dump(setup, sort_keys=False))
    build.build(PAGE, setup_path)
    build.build(BLANK)


def pick_option(page: Page, dropdown: int, option: str) -> None:
    page.click(f"#mdim-bar .md-dropdown >> nth={dropdown}")
    page.click(f".md-menu:not([hidden]) .md-option >> text={option}")


def main() -> None:
    SCREENSHOTS.mkdir(parents=True, exist_ok=True)
    build_pages()
    with sync_playwright() as p:
        browser = p.chromium.launch()
        ctx = browser.new_context(viewport={"width": 1340, "height": 1000})
        ctx.grant_permissions(["clipboard-read", "clipboard-write"])
        page = ctx.new_page()
        errors: list[str] = []
        warnings: list[str] = []
        page.on("console", lambda m: errors.append(m.text) if m.type == "error" else None)
        page.on("console", lambda m: warnings.append(m.text) if m.type == "warning" else None)
        page.on("pageerror", lambda e: errors.append(str(e)))

        # An empty page says where to start.
        page.goto(BLANK.as_uri())
        page.wait_for_timeout(2500)
        check("an empty page says where to start", "Start by linking an indicator" in page.inner_text("#chart"))
        check("an empty page has no title yet", page.inner_text("#mdim-title") == "Untitled MDim")

        page.goto(PAGE.as_uri())
        page.wait_for_selector("#chart svg", timeout=30000)
        page.wait_for_timeout(1500)
        page.screenshot(path=SCREENSHOTS / "initial.png")
        check("starting chart is drawn", "Number of people using the Internet" in page.inner_text("#chart"))
        check("the last dropdown can't be removed", page.locator(".dim-head .btn-icon").first.is_disabled())
        wysk = page.inner_text("#wysk-list")
        check(
            "free-form WYSK is shown whole",
            all(
                part in wysk
                for part in ("Some context before the points.", "wraps onto a second line", "and another one")
            ),
        )
        # The page starts from the newest WDI version; the share indicator sits in the same dataset.
        number_path = page.locator("table.combos tbody tr >> nth=0").locator("td.indicator input").input_value()
        share_path = number_path.replace("#it_net_user_zs_number", "#it_net_user_zs")
        wdi_dataset = "data://" + number_path.split("#")[0].rsplit("/", 1)[0]
        check(
            "the first view is linked to the starting indicator",
            number_path.endswith("#it_net_user_zs_number"),
            number_path,
        )

        # Title, title variant and topic tags.
        page.locator("#page-fields input").first.fill("Internet use")
        page.locator("#page-fields .link-btn").click()
        page.locator("#page-fields input").nth(1).fill("ITU via World Bank")
        header = page.inner_text(".mdim-header__left")
        check("title and variant in the header", "Internet use" in header and "ITU via World Bank" in header)
        check(
            "topic tags link to their topic pages",
            page.get_attribute("#topic-tags a >> nth=0", "href") == "https://ourworldindata.org/internet",
        )
        page.select_option("#tags-editor select", label="Loneliness & Social Connections")
        check("a topic tag can be added", "Loneliness & Social Connections" in page.inner_text("#topic-tags"))

        # A second option, with a description.
        page.click("text=+ Add option")
        page.locator(".choice-row input.choice-name").nth(1).fill("Share of population")
        check(
            "descriptions stay hidden until asked for",
            page.locator(".choice-row input.choice-description:visible").count() == 0,
        )
        page.locator(".choice-row >> nth=1").locator(".link-btn", has_text="+ Add description").click()
        page.locator(".choice-row >> nth=1").locator('input[placeholder^="Shown under the option"]').fill(
            "Share of the population who used the Internet in the last three months."
        )
        rows = page.locator("table.combos tbody tr")
        check("one combination per option", rows.count() == 2)

        # Linking: chart links are refused, catalog paths accepted.
        indicator_input = rows.nth(1).locator("td.indicator input")
        indicator_input.fill("https://ourworldindata.org/grapher/share-of-individuals-using-the-internet")
        indicator_input.press("Enter")
        page.wait_for_timeout(1000)
        check("chart links are refused", "Chart links aren't accepted" in rows.nth(1).locator(".status").inner_text())
        indicator_input.fill(share_path)
        indicator_input.press("Enter")
        page.wait_for_selector("table.combos tbody tr:nth-child(2) .status.ok", timeout=20000)
        check("catalog paths are accepted", share_path in rows.nth(1).locator(".status").inner_text())

        # Switching views with the dropdown.
        page.click("#mdim-bar .md-dropdown >> nth=0")
        page.wait_for_selector(".md-menu:not([hidden])")
        page.screenshot(path=SCREENSHOTS / "menu.png")
        page.click(".md-menu:not([hidden]) .md-option >> text=Share of population")
        check("menu closes after a pick", page.locator(".md-menu:not([hidden])").count() == 0)
        page.wait_for_function(
            "document.querySelector('#chart').innerText.includes('Share of the population')", timeout=30000
        )
        page.wait_for_timeout(1500)
        check(
            "the chart follows the dropdown", "Share of the population using the Internet" in page.inner_text("#chart")
        )

        # FAUST: fields start from the indicator, and the chart updates without reloading.
        page.click("#text-editor-toggle")
        page.wait_for_selector("#text-subtitle")
        check(
            "edit fields start from the indicator",
            page.input_value("#text-title") == "Share of the population using the Internet",
        )
        page.fill("#text-subtitle", "Share of people who used the Internet in the last three months")
        page.wait_for_timeout(400)
        check("subtitle updates live", "Share of people who used the Internet" in page.inner_text("#chart"))
        check(
            "style hint for a missing period",
            "End the subtitle with punctuation" in page.inner_text("#text-subtitle ~ .hints"),
        )
        page.fill("#text-subtitle", "Share of people who used the Internet in the last three months.")
        check("hint goes away once fixed", page.inner_text("#text-subtitle ~ .hints") == "")

        # WYSK: edit a point in place, then add one.
        page.click("#wysk-list li >> nth=0 >> .wysk-bullet")
        page.fill(
            ".wysk-edit", "An Internet user is anyone who has gone online from any location in the last three months."
        )
        page.keyboard.press("Enter")
        check(
            "a point is edited in place",
            page.inner_text("#wysk-list li >> nth=0").startswith("An Internet user is anyone"),
        )
        bullets_before = page.locator("#wysk-list li").count()
        page.click(".add-bullet")
        page.fill(".wysk-edit", "A new point about this view.")
        page.keyboard.press("Enter")
        check("a point is added", page.locator("#wysk-list li").count() == bullets_before + 1)
        check("WYSK marked as edited", page.locator("#wysk-tools .edited").count() == 1)

        # Footnote, undo, and the suggest-for-the-indicator checkbox.
        page.fill("#text-note", "Data for 2025 is provisional.")
        page.wait_for_timeout(300)
        check("footnote shows on the chart", "provisional" in page.inner_text("#chart"))
        page.click("#tools-note .link-btn")
        page.wait_for_timeout(300)
        check("undo removes it", "provisional" not in page.inner_text("#chart"))
        page.fill("#text-note", "Data for 2025 is provisional.")
        page.check("#tools-subtitle input[type=checkbox]")
        page.screenshot(path=SCREENSHOTS / "text_editing.png")

        # Text edits belong to their view only.
        pick_option(page, 0, "Number of people")
        page.wait_for_function(
            "document.querySelector('#chart').innerText.includes('Number of people using')", timeout=30000
        )
        page.wait_for_timeout(800)
        check("the other view keeps its own text", "provisional" not in page.inner_text("#chart"))
        # Both views: the share view's edits, and the pre-filled free-form WYSK on the number view.
        check("edited views are marked in the table", page.locator("table.combos .text-tag").count() == 2)
        check("text updates without reloading the chart", not [w for w in warnings if "Live text" in w])
        page.click("#text-editor-toggle")

        # Default countries: ticked in the chart, shared by all views.
        page.wait_for_selector("#selection-note:not([hidden])", timeout=10000)
        page.locator("#chart .entity-selector").get_by_text("France", exact=True).first.click()
        page.wait_for_timeout(900)
        check("ticked countries become the default", "France" in saved(page)["defaultSelection"])
        pick_option(page, 0, "Share of population")
        page.wait_for_function("document.querySelector('#chart').innerText.includes('Share of')", timeout=30000)
        page.wait_for_timeout(1200)
        check(
            "the default carries over to other views",
            "France" in page.locator("#chart .entity-selector").inner_text().split("Filter by type")[0],
        )

        # Settings shared by every view.
        chart_text = page.inner_text("#chart")
        check("time range hidden after the title", "1990 to 2025" not in chart_text.split("\n")[0])
        check("footer links to the main topic", "OurWorldinData.org/internet" in chart_text)
        page.locator("#chart-settings label", has_text="Slope chart").locator("input").check()
        page.wait_for_timeout(2000)
        check("a chart type can be added", "SlopeChart" in saved(page)["chartTypes"])
        page.locator("#chart-settings label", has_text="change the country").locator("input").check()
        page.wait_for_timeout(1000)
        check("change-country mode is saved", saved(page)["changeCountry"] is True)
        page.locator("#chart-settings label", has_text="change the country").locator("input").uncheck()
        page.wait_for_timeout(1000)

        # The view that opens first.
        rows.nth(1).locator("td.default-col input").check()
        opens_first = [rows.nth(i).locator("td.default-col input").is_checked() for i in range(2)]
        check("exactly one view opens first", opens_first == [False, True], opens_first)

        # Default tab and map colours.
        page.click("#text-editor-toggle")
        map_tab = page.locator("#chart").get_by_role("tab", name="Map")
        if not map_tab.count():
            map_tab = page.locator("#chart").get_by_text("Map", exact=True)
        map_tab.first.click()
        page.wait_for_selector("#map-palette", timeout=10000)
        check("the open tab becomes the default tab", saved(page)["defaultTab"] == "map")
        page.select_option("#map-palette", "Blues")
        page.select_option("#map-strategy", "equalSizeBins-percent")
        page.wait_for_timeout(600)
        check("map colours marked as edited", page.locator("#tools-map .edited").count() == 1)
        page.screenshot(path=SCREENSHOTS / "map_colours.png")

        # Number rounding, and undoing it, as the chart shows it (France's value in the countries panel).
        def france_value() -> str:
            panel = page.inner_text("#chart .entity-selector")
            match = re.search(r"(?m)^France\xa0?\n([^\n]+)$", panel)
            return match.group(1).strip() if match else ""

        original_value = france_value()
        page.select_option("#rounding-mode", "significantFigures")
        page.fill("#rounding-digits", "1")
        page.wait_for_timeout(1500)
        check("rounding marked as edited", page.locator("#tools-rounding .edited").count() == 1)
        rounded_value = france_value()
        check(
            "the chart shows the new rounding", rounded_value != original_value, f"{original_value} -> {rounded_value}"
        )
        page.click("#tools-rounding .link-btn")
        page.wait_for_timeout(1500)
        check("undo redraws the chart", france_value() == original_value, france_value())
        page.select_option("#rounding-mode", "significantFigures")
        page.fill("#rounding-digits", "2")
        page.wait_for_timeout(600)
        page.click("#text-editor-toggle")

        # Dropdown description and an option group.
        first_card = page.locator(".dim-card >> nth=0")
        first_card.locator(".choice-fields .link-btn", has_text="+ Add description").first.click()
        first_card.locator(".choice-fields input.choice-description").first.fill("What the chart counts")
        first_card.locator(".choice-row >> nth=1").locator(".link-btn", has_text="+ Add group").click()
        first_card.locator(".choice-row >> nth=1").locator("input[list]").fill("Relative")
        page.click("#mdim-bar .md-dropdown >> nth=0")
        check(
            "the menu shows the dropdown description",
            page.inner_text(".md-menu:not([hidden]) .md-menu__dimension-description") == "What the chart counts",
        )
        groups = page.locator(".md-menu:not([hidden]) .md-menu__group-label").all_inner_texts()
        check("the menu shows option groups", groups == ["Relative"], groups)
        page.screenshot(path=SCREENSHOTS / "menu_groups.png")
        page.keyboard.press("Escape")

        # A second dropdown: existing links move to its first option.
        page.click("#add-dim")
        page.locator(".dim-head input").nth(1).fill("Sex")
        check("links survive a new dropdown", "2 linked" in page.inner_text("#panel-summary"))
        page.locator(".dim-card").nth(1).locator("text=+ Add option").click()
        pick_option(page, 1, "Option 2")
        page.wait_for_timeout(500)
        check("unlinked combinations say so", "No indicator linked" in page.inner_text("#chart"))

        # Copy the setup.
        setup_text = copied_setup(page)
        check("the setup is copied", page.inner_text("#copy-status").startswith("Copied"))
        setup_path = OUT / "last_setup.yml"
        setup_path.write_text(setup_text)

        # Resize, fold, reload.
        before = box_of(page.locator("#panel-body"))["height"]
        handle = box_of(page.locator("#panel-resize"))
        page.mouse.move(handle["x"] + handle["width"] / 2, handle["y"] + 6)
        page.mouse.down()
        page.mouse.move(handle["x"] + handle["width"] / 2, handle["y"] - 150, steps=5)
        page.mouse.up()
        check("the panel can be made taller", box_of(page.locator("#panel-body"))["height"] > before + 100)
        page.click("#panel-toggle")
        page.wait_for_timeout(300)
        check("the panel folds into one row", box_of(page.locator("#panel"))["height"] < 60)
        page.reload()
        page.wait_for_timeout(3000)
        check(
            "the setup survives a reload",
            "2 dropdowns · 4 combinations · 2 linked" in page.inner_text("#panel-summary"),
        )

        # Each page keeps its own work: the empty page is still empty.
        page.goto(BLANK.as_uri())
        page.wait_for_timeout(2500)
        check("sketches are saved separately", "0 linked" in page.inner_text("#panel-summary"))

        # A copied setup reopens unchanged, including a "What you should know" given as a list.
        reopen = yaml.safe_load(setup_text)
        reopen["views"][0].setdefault("metadata", {})["description_key"] = ["A point given as a list item.", "Another."]
        reopen_path = OUT / "check_reopen.yml"
        reopen_path.write_text(yaml.safe_dump(reopen, sort_keys=False, allow_unicode=True))
        build.build(REOPENED, reopen_path)
        page.goto(REOPENED.as_uri())
        page.wait_for_selector("#chart svg", timeout=30000)
        page.wait_for_timeout(1500)
        reopened = yaml.safe_load(copied_setup(page))
        check("a copied setup reopens unchanged", reopened == reopen)
        check(
            "a list-form WYSK stays a list",
            reopened["views"][0]["metadata"]["description_key"] == ["A point given as a list item.", "Another."],
        )

        check("no console errors", not errors, errors or "")
        browser.close()

    # What Claude receives has to be a valid ETL chart config, with the edits where ETL expects them.
    setup = yaml.safe_load(setup_text)
    shared = setup["definitions"]["common_views"][0]["config"]
    share = next(v for v in setup["views"] if v["dimensions"]["metric"] == "share_of_population")
    number = next(v for v in setup["views"] if v["dimensions"]["metric"] == "number_of_people")
    check("first view exported", setup["default_dimensions"]["metric"] == "share_of_population")
    check(
        "shared settings exported",
        shared["tab"] == "map" and "SlopeChart" in shared["chartTypes"] and shared["yAxis"] == {"min": 0},
    )
    check("text overrides exported on their view", share["config"]["note"] == "Data for 2025 is provisional.")
    check("rounding exported on the indicator", share["indicators"]["y"][0]["display"]["numSignificantFigures"] == 2)
    check("untouched maps export nothing", "map" not in (number.get("config") or {}))
    check("suggestions are listed for Claude", "Suggested for the indicator everywhere" in setup_text)
    check(
        "a suggestion names its view and carries that view's text",
        "(from Metric = Share of population" in setup_text
        and "Share of people who used the Internet in the last three months." in setup_text.split("Suggested for")[1],
    )
    check(
        "free-form WYSK comes back unchanged",
        number.get("metadata", {}).get("description_key") == FREE_FORM_WYSK,
        number.get("metadata", {}).get("description_key"),
    )
    chart = create_chart_from_config(
        setup, dependencies={wdi_dataset}, catalog_path="worldbank_wdi/latest/internet_users#internet_users"
    )
    check("ETL accepts the copied setup unedited", len(chart.views) == 2)

    if failures:
        sys.exit(f"{len(failures)} check(s) failed: {', '.join(failures)}")
    print("All checks passed.")


if __name__ == "__main__":
    main()
