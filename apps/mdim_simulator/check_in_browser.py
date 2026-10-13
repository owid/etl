"""Click through the MDim simulator in headless Chromium, like a user would, and save screenshots.

Not a unit test (pytest only collects tests/): run it by hand after changing the page.
Needs Tailscale, because the page loads Grapher from the Tailnet.

    .venv/bin/python apps/mdim_simulator/build.py
    .venv/bin/python apps/mdim_simulator/check_in_browser.py
"""

from pathlib import Path

from playwright.sync_api import sync_playwright

PAGE = Path(__file__).parents[2] / "ai" / "mdim_simulator" / "index.html"
SCREENSHOTS = PAGE.parent / "screenshots"
URL = PAGE.as_uri()


def box_of(locator) -> dict:
    """An element's bounding box; Playwright returns None for elements that aren't rendered."""
    box = locator.bounding_box()
    assert box is not None, f"{locator} is not rendered"
    return box


def main() -> None:
    SCREENSHOTS.mkdir(parents=True, exist_ok=True)
    with sync_playwright() as p:
        browser = p.chromium.launch()
        ctx = browser.new_context(viewport={"width": 1340, "height": 1000})
        ctx.grant_permissions(["clipboard-read", "clipboard-write"])
        page = ctx.new_page()
        errors = []
        page.on("console", lambda m: errors.append(m.text) if m.type == "error" else None)
        page.on("pageerror", lambda e: errors.append(str(e)))

        page.goto(URL)
        page.wait_for_selector("#chart svg", timeout=30000)
        page.wait_for_timeout(1500)
        page.screenshot(path=SCREENSHOTS / "t1_initial.png")
        print("1 initial title:", page.locator("#chart").inner_text()[:60].replace("\n", " | "))
        print("  summary:", page.inner_text("#panel-summary"))

        print("  header:", page.inner_text(".mdim-header__left").replace("\n", " | "))
        page.locator("#page-fields input").first.fill("Internet use")
        page.locator("#page-fields .link-btn").click()
        page.locator("#page-fields input").nth(1).fill("ITU via World Bank")
        print(
            "  header after edit:",
            page.inner_text(".mdim-header__left").replace("\n", " | "),
            "| tab title:",
            page.title(),
        )

        print(
            "  topic tags:",
            page.inner_text("#topic-tags").replace("\n", " | "),
            "| first link:",
            page.get_attribute("#topic-tags a >> nth=0", "href"),
        )
        page.select_option("#tags-editor select", label="Loneliness & Social Connections")
        print("  topic tags after adding one:", page.inner_text("#topic-tags").replace("\n", " | "))
        page.screenshot(path=SCREENSHOTS / "t0_header.png", clip={"x": 0, "y": 0, "width": 1340, "height": 260})

        # Add a second option and rename it.
        page.click("text=+ Add option")
        page.locator(".choice-row input.choice-name").nth(1).fill("Share of population")
        print(
            "  description fields visible before asking:",
            page.locator(".choice-row input.choice-description:visible").count(),
        )
        page.locator(".choice-row .link-btn").nth(1).click()
        page.locator(".choice-row input.choice-description").nth(1).fill(
            "Share of the population who used the Internet in the last three months."
        )
        print(
            "  description fields visible after adding one:",
            page.locator(".choice-row input.choice-description:visible").count(),
        )
        rows = page.locator("table.combos tbody tr")
        print("2 rows:", rows.count(), "| summary:", page.inner_text("#panel-summary"))

        # A chart link is refused.
        row2 = rows.nth(1)
        row2.locator("input").fill("https://ourworldindata.org/grapher/share-of-individuals-using-the-internet")
        row2.locator("input").press("Enter")
        page.wait_for_timeout(1000)
        print("3 chart link status:", row2.locator(".status").inner_text())

        # A catalog path is accepted.
        row2.locator("input").fill("grapher/worldbank_wdi/2026-07-27/wdi/wdi#it_net_user_zs")
        row2.locator("input").press("Enter")
        page.wait_for_selector("table.combos tbody tr:nth-child(2) .status.ok", timeout=20000)
        print("4 catalog path status:", row2.locator(".status").inner_text().replace("\n", " | "))
        print("  summary:", page.inner_text("#panel-summary"))

        # Switch the view with the dropdown above the chart.
        page.click("#mdim-bar .md-dropdown >> nth=0")
        page.wait_for_selector(".md-menu:not([hidden])")
        page.screenshot(path=SCREENSHOTS / "t1b_menu.png")
        page.click(".md-menu:not([hidden]) .md-option >> text=Share of population")
        print("  menu closed after pick:", page.locator(".md-menu:not([hidden])").count() == 0)
        page.wait_for_function(
            "document.querySelector('#chart').innerText.includes('Share of the population')", timeout=30000
        )
        page.wait_for_timeout(1500)
        page.screenshot(path=SCREENSHOTS / "t2_share.png")
        print("5 after switch:", page.locator("#chart").inner_text()[:60].replace("\n", " | "))
        print("  wysk first bullet:", page.locator("#wysk-list li").first.inner_text()[:80])

        # Add a second dropdown: existing links should survive on its first option.
        page.click("#add-dim")
        page.locator(".dim-head input").nth(1).fill("Sex")
        print("6 after 2nd dropdown:", page.inner_text("#panel-summary"))
        page.screenshot(path=SCREENSHOTS / "t3_two_dims.png")

        # Unlinked combination.
        page.locator(".dim-card").nth(1).locator("text=+ Add option").click()
        page.click("#mdim-bar .md-dropdown >> nth=1")
        page.click(".md-menu:not([hidden]) .md-option >> nth=1")
        page.wait_for_timeout(500)
        print("7 unlinked view:", page.locator("#chart").inner_text()[:80])

        # Copy the setup.
        page.click("#copy")
        page.wait_for_timeout(500)
        print("8 copy status:", page.inner_text("#copy-status"))
        print(page.evaluate("navigator.clipboard.readText()"))

        # Resize then fold.
        before = box_of(page.locator("#panel-body"))["height"]
        box = box_of(page.locator("#panel-resize"))
        page.mouse.move(box["x"] + box["width"] / 2, box["y"] + 6)
        page.mouse.down()
        page.mouse.move(box["x"] + box["width"] / 2, box["y"] - 150, steps=5)
        page.mouse.up()
        after = box_of(page.locator("#panel-body"))["height"]
        print(f"9 panel body height {before:.0f} -> {after:.0f}")
        page.screenshot(path=SCREENSHOTS / "t4_taller.png")
        page.click("#panel-toggle")
        page.wait_for_timeout(300)
        print("10 folded panel height:", box_of(page.locator("#panel"))["height"])
        page.screenshot(path=SCREENSHOTS / "t5_folded.png")

        # Reload: setup is remembered.
        page.reload()
        page.wait_for_timeout(3000)
        print(
            "11 after reload:",
            page.inner_text("#panel-summary"),
            "| open:",
            page.evaluate("document.getElementById('panel').classList.contains('open')"),
        )

        print("console errors:", errors or "none")
        browser.close()


if __name__ == "__main__":
    main()
