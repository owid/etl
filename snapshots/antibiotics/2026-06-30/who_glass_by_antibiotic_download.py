"""Automate downloading WHO GLASS 'by antibiotic' data from the GLASS AMR dashboard.

Downloads all slices from the section:
  "Global maps of testing coverage by bacterial pathogen and antibiotic group"

Output folder structure (matching what the snapshot script expects):
  <output_dir>/glass_by_antibiotic/<syndrome>/<pathogen>/<antibiotic>/<year>.csv

Then zip and upload:
  cd <output_dir> && zip -r glass_by_antibiotic.zip glass_by_antibiotic/
  .venv/bin/etls antibiotics/2026-06-30/who_glass_by_antibiotic --path-to-file <output_dir>/glass_by_antibiotic.zip

Install playwright (once):
  uv add --dev playwright
  .venv/bin/playwright install chromium

Usage:
  # Step 1 — discover selector IDs (opens a real browser so you can see the page)
  .venv/bin/python snapshots/antibiotics/2026-06-30/who_glass_by_antibiotic_download.py --discover

  # Step 2 — download everything (creates ~/Downloads/glass_by_antibiotic/)
  .venv/bin/python snapshots/antibiotics/2026-06-30/who_glass_by_antibiotic_download.py \\
      --output-dir ~/Downloads

  # Step 3 — zip and snapshot
  cd ~/Downloads && zip -r glass_by_antibiotic.zip glass_by_antibiotic/
  .venv/bin/etls antibiotics/2026-06-30/who_glass_by_antibiotic --path-to-file ~/Downloads/glass_by_antibiotic.zip
"""

import asyncio
import re
from pathlib import Path

import click
import structlog
from playwright.async_api import Browser, Page, async_playwright
from tqdm import tqdm

log = structlog.get_logger()

# ── Dashboard URL ─────────────────────────────────────────────────────────────
GLASS_URL = "https://worldhealthorg.shinyapps.io/glass-dashboard/#!/amr"

# ── Selector IDs — fill these in after running --discover ────────────────────
# These are the HTML element IDs of the Shiny input controls and download button
# inside the "Global maps" section. Run with --discover to find the correct values.
SEL_SYNDROME = "amr-gc_pathogen_anti-infsys-select"
SEL_PATHOGEN = "amr-gc_pathogen_anti-pathogen-select"
SEL_ANTIBIOTIC = "amr-gc_pathogen_anti-antibiotic-select"
SEL_YEAR = "amr-gc_pathogen_anti-year-select"
SEL_DOWNLOAD = "amr-gc_pathogen_anti-dl-data"
# Region is fixed to World (001) — not iterated
SEL_REGION = "amr-gc_pathogen_anti-region-select"
REGION_WORLD = "001"

# ── Valid (syndrome, pathogen, antibiotic) combinations ───────────────────────
# Derived from the GLASS AMR surveillance protocol (only specific pathogen ×
# antibiotic pairs are monitored per syndrome). Hardcoded to avoid brute-forcing
# ~1,792 combinations when only 18 are real.
# Values must match the Shiny dropdown values exactly (verified 2026-07-01).
VALID_COMBOS: list[tuple[str, str, str]] = [
    # Bloodstream
    ("BLOOD", "Acinetobacter spp.", "Carbapenems"),
    ("BLOOD", "Salmonella spp.", "Fluoroquinolones"),
    ("BLOOD", "Staphylococcus aureus", "Methicillin-resistance"),
    ("BLOOD", "Escherichia coli", "Third-generation cephalosporins"),
    ("BLOOD", "Escherichia coli", "Carbapenems"),
    ("BLOOD", "Klebsiella pneumoniae", "Third-generation cephalosporins"),
    ("BLOOD", "Klebsiella pneumoniae", "Carbapenems"),
    ("BLOOD", "Streptococcus pneumoniae", "Penicillins"),
    # Gastrointestinal
    ("STOOL", "Salmonella spp.", "Fluoroquinolones"),
    ("STOOL", "Shigella spp.", "Third-generation cephalosporins"),
    # Urinary tract
    ("URINE", "Escherichia coli", "Fluoroquinolones"),
    ("URINE", "Escherichia coli", "Sulfonamides and trimethoprim"),
    ("URINE", "Escherichia coli", "Third-generation cephalosporins"),
    ("URINE", "Klebsiella pneumoniae", "Fluoroquinolones"),
    ("URINE", "Klebsiella pneumoniae", "Sulfonamides and trimethoprim"),
    ("URINE", "Klebsiella pneumoniae", "Third-generation cephalosporins"),
    # Gonorrhoea
    ("UROGENITAL", "Neisseria gonorrhoeae", "Macrolides"),
    ("UROGENITAL", "Neisseria gonorrhoeae", "Third-generation cephalosporins"),
]

# ── Timeouts ──────────────────────────────────────────────────────────────────
PAGE_LOAD_TIMEOUT = 120_000  # ms — Shiny apps are slow to boot
SHINY_REACT_DELAY = 1_500  # ms — wait after changing a filter
DOWNLOAD_TIMEOUT = 60_000  # ms

# ── Shiny initialisation marker ───────────────────────────────────────────────
# A string that only appears on the page after Shiny has fully connected.
SHINY_READY_TEXT = "Global maps of testing coverage"


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────


def slugify(text: str) -> str:
    """Convert a label to a filesystem-safe slug (lowercase, underscores)."""
    return re.sub(r"[^a-z0-9]+", "_", text.strip().lower()).strip("_")


async def wait_for_shiny(page: Page) -> None:
    """Wait until Shiny has finished its initial render."""
    await page.wait_for_function(
        "() => !document.body.classList.contains('shiny-busy')",
        timeout=PAGE_LOAD_TIMEOUT,
    )
    await page.wait_for_selector(f"text={SHINY_READY_TEXT}", timeout=PAGE_LOAD_TIMEOUT)
    await page.wait_for_timeout(2_000)


async def get_select_options(page: Page, selector_id: str) -> list[tuple[str, str]]:
    """Return (value, label) pairs for a Shiny selectize input.

    Opens the selectize dropdown via JS to force all server-side options to render,
    reads the option elements, then closes it again.
    """
    # Open the selectize dropdown — this triggers server-side options to load
    await page.evaluate(
        "id => { const el = document.querySelector('#' + id); if (el && el.selectize) el.selectize.open(); }",
        selector_id,
    )
    await page.wait_for_timeout(1_000)

    # Read options from the rendered dropdown list
    pairs = await page.evaluate(
        """id => {
            const el = document.querySelector('#' + id);
            if (!el) return [];
            // Prefer selectize internal options (server-side) over DOM <option> elements
            if (el.selectize) {
                return Object.values(el.selectize.options).map(o => [o.value, (o.text || o.label || o.value).trim()]);
            }
            return Array.from(el.options).map(o => [o.value, o.text.trim()]);
        }""",
        selector_id,
    )

    # Close the dropdown
    await page.evaluate(
        "id => { const el = document.querySelector('#' + id); if (el && el.selectize) el.selectize.close(); }",
        selector_id,
    )

    return [(v, lbl) for v, lbl in pairs if v]


async def set_select(page: Page, selector_id: str, value: str) -> None:
    """Set a Shiny selectize input using Shiny's JS API and wait for reactivity to settle."""
    # Shiny's selectize hides the underlying <select> — use Shiny.setInputValue() to
    # push the value directly into the reactive graph, bypassing the invisible element.
    await page.evaluate(
        "([id, val]) => Shiny.setInputValue(id, val, {priority: 'event'})",
        [selector_id, value],
    )
    await page.wait_for_timeout(SHINY_REACT_DELAY)
    await page.wait_for_function(
        "() => !document.body.classList.contains('shiny-busy')",
        timeout=30_000,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Discovery mode
# ─────────────────────────────────────────────────────────────────────────────


async def run_discover() -> None:
    """
    Open the GLASS dashboard with a visible browser, wait for Shiny to load,
    then print every <select>/selectize control and download button on the page.
    Use this output to identify the correct selector IDs for the constants above.
    """
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=False)  # always visible in discover
        page = await browser.new_page()

        log.info("Navigating to dashboard", url=GLASS_URL)
        await page.goto(GLASS_URL, timeout=PAGE_LOAD_TIMEOUT)
        log.info("Waiting for Shiny to fully load")
        await wait_for_shiny(page)
        log.info("Shiny ready")

        # All <select> elements
        selects = await page.eval_on_selector_all(
            "select",
            """els => els.map(el => ({
                id: el.id,
                name: el.name,
                optionCount: el.options.length,
                firstOptions: Array.from(el.options).map(o => o.value).slice(0, 6),
                visible: el.offsetParent !== null,
            }))""",
        )
        log.info("=== <select> elements ===")
        for s in selects:
            vis = "visible" if s["visible"] else "hidden"
            log.info(
                "select", id=s["id"], visibility=vis, option_count=s["optionCount"], first_options=s["firstOptions"]
            )

        # selectize wrappers (Shiny hides <select> behind selectize by default)
        selectize = await page.eval_on_selector_all(
            ".selectize-control",
            """els => els.map(el => {
                const sel = el.previousElementSibling;
                return {
                    wrapsId: sel ? sel.id : null,
                    placeholder: el.querySelector('.selectize-input')?.innerText?.trim(),
                };
            })""",
        )
        log.info("=== selectize wrappers (Shiny dropdowns) ===")
        for si in selectize:
            log.info("selectize", wraps_id=si["wrapsId"], placeholder=si["placeholder"])

        # Download buttons / links
        downloads = await page.eval_on_selector_all(
            "a[download], button, a.btn",
            """els => els
                .filter(el => {
                    const t = (el.innerText || el.title || el.id || '').toLowerCase();
                    return t.includes('download') || t.includes('csv') || t.includes('data');
                })
                .map(el => ({
                    tag: el.tagName,
                    id: el.id,
                    text: el.innerText?.trim().slice(0, 60),
                    href: el.href || null,
                }))""",
        )
        log.info("=== Download buttons / links ===")
        for d in downloads:
            log.info("download_element", tag=d["tag"], id=d["id"], text=d["text"], href=d["href"])

        log.info("Use the IDs above to fill in the SEL_* constants at the top of this file. Press Ctrl+C to close.")
        # Keep browser open for manual inspection
        try:
            await asyncio.sleep(300)
        except asyncio.CancelledError:
            pass
        await browser.close()


# ─────────────────────────────────────────────────────────────────────────────
# Download mode
# ─────────────────────────────────────────────────────────────────────────────


async def download_all(output_dir: Path, headless: bool, resume: bool) -> None:
    """Iterate all (syndrome × pathogen × antibiotic × year) and download each CSV."""
    missing = [
        name
        for name, val in [
            ("SEL_SYNDROME", SEL_SYNDROME),
            ("SEL_PATHOGEN", SEL_PATHOGEN),
            ("SEL_ANTIBIOTIC", SEL_ANTIBIOTIC),
            ("SEL_YEAR", SEL_YEAR),
            ("SEL_DOWNLOAD", SEL_DOWNLOAD),
        ]
        if val.startswith("TODO_")
    ]
    if missing:
        raise click.ClickException(
            f"Selector IDs not yet configured: {missing}\n"
            "Run with --discover to identify them, then edit the SEL_* constants "
            "at the top of this script."
        )

    output_dir.mkdir(parents=True, exist_ok=True)

    async with async_playwright() as pw:
        browser: Browser = await pw.chromium.launch(headless=headless)
        context = await browser.new_context(accept_downloads=True)
        page = await context.new_page()

        log.info("Navigating to dashboard", url=GLASS_URL)
        await page.goto(GLASS_URL, timeout=PAGE_LOAD_TIMEOUT)
        await wait_for_shiny(page)
        log.info("Shiny ready — reading filter options")

        # Fix region to World before reading options (some dropdowns are region-dependent)
        await set_select(page, SEL_REGION, REGION_WORLD)

        years = await get_select_options(page, SEL_YEAR)
        year_values = [v for v, _ in years]
        total = len(VALID_COMBOS) * len(year_values)
        log.info("Download plan", years=year_values, valid_combos=len(VALID_COMBOS), total_downloads=total)

        done = skipped = errors = 0
        prev_syn = prev_path = prev_anti = None  # track last-set values to skip redundant calls

        all_items = [(s, p, a, y) for s, p, a in VALID_COMBOS for y in year_values]
        progress = tqdm(all_items, unit="file", ncols=90)

        for syn_val, path_val, anti_val, year_val in progress:
            syn_slug = slugify(syn_val)
            path_slug = slugify(path_val)
            anti_slug = slugify(anti_val)
            year_slug = slugify(year_val)

            dest_dir = output_dir / syn_slug / path_slug / anti_slug
            dest_file = dest_dir / f"{year_slug}.csv"
            label = f"{syn_slug}/{path_slug}/{anti_slug}/{year_slug}"

            progress.set_description(label[:50])

            if resume and dest_file.exists():
                skipped += 1
                continue

            try:
                # Only re-set filters that actually changed to save time
                if syn_val != prev_syn:
                    await set_select(page, SEL_SYNDROME, syn_val)
                    prev_syn = syn_val
                    prev_path = prev_anti = None  # dependents must be reset
                if path_val != prev_path:
                    await set_select(page, SEL_PATHOGEN, path_val)
                    prev_path = path_val
                    prev_anti = None
                if anti_val != prev_anti:
                    await set_select(page, SEL_ANTIBIOTIC, anti_val)
                    prev_anti = anti_val
                await set_select(page, SEL_YEAR, year_val)

                dest_dir.mkdir(parents=True, exist_ok=True)

                async with page.expect_download(timeout=DOWNLOAD_TIMEOUT) as dl_info:
                    await page.click(f"#{SEL_DOWNLOAD}")
                dl = await dl_info.value
                failure = await dl.failure()
                if failure:
                    raise RuntimeError(f"browser download error: {failure}")
                await dl.save_as(dest_file)
                done += 1

            except Exception as exc:
                log.error("Download failed", label=label, error=str(exc))
                errors += 1
                prev_syn = prev_path = prev_anti = None  # force full reset after error
                try:
                    await page.goto(GLASS_URL, timeout=PAGE_LOAD_TIMEOUT)
                    await wait_for_shiny(page)
                    await scroll_to_section_and_init(page)
                except Exception:
                    pass

        await browser.close()

    log.info("Finished", downloaded=done, skipped=skipped, errors=errors)
    if errors:
        log.warning("Some downloads failed — re-run with --resume to retry only the failed slices.")


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────


async def scroll_to_section_and_init(page: Page) -> None:
    """Scroll the gc_pathogen_anti section into view to trigger Shiny's lazy initialisation."""
    await page.eval_on_selector(
        f"#{SEL_SYNDROME}",
        "el => el.scrollIntoView({behavior: 'instant', block: 'center'})",
    )
    await page.wait_for_timeout(2_000)
    # Set region, then fire a dummy syndrome change to kick the reactive chain
    await page.evaluate(
        "([id, val]) => Shiny.setInputValue(id, val, {priority: 'event'})",
        [SEL_REGION, REGION_WORLD],
    )
    await page.wait_for_timeout(SHINY_REACT_DELAY)
    # Re-assert the current syndrome so Shiny re-runs its observers
    current_syn = await page.eval_on_selector(f"#{SEL_SYNDROME}", "el => el.value")
    await page.evaluate(
        "([id, val]) => Shiny.setInputValue(id, val, {priority: 'event'})",
        [SEL_SYNDROME, current_syn],
    )
    await page.wait_for_function("() => !document.body.classList.contains('shiny-busy')", timeout=30_000)
    await page.wait_for_timeout(2_000)


async def print_options() -> None:
    """Load the page and print all available values for each of the 4 filter dropdowns."""
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        page = await browser.new_page()
        log.info("Navigating to dashboard", url=GLASS_URL)
        await page.goto(GLASS_URL, timeout=PAGE_LOAD_TIMEOUT)
        await wait_for_shiny(page)
        await scroll_to_section_and_init(page)

        # Year and syndrome are independent — read them directly
        year_opts = await get_select_options(page, SEL_YEAR)
        syn_opts = await get_select_options(page, SEL_SYNDROME)
        log.info("Year options", count=len(year_opts), options=[{"value": v, "label": lbl} for v, lbl in year_opts])
        log.info("Syndrome options", count=len(syn_opts), options=[{"value": v, "label": lbl} for v, lbl in syn_opts])

        # Pathogen depends on syndrome — collect across all syndromes
        all_pathogens: dict[str, str] = {}
        for syn_val, _ in syn_opts:
            await set_select(page, SEL_SYNDROME, syn_val)
            for v, lbl in await get_select_options(page, SEL_PATHOGEN):
                all_pathogens[v] = lbl
        log.info(
            "Pathogen options",
            count=len(all_pathogens),
            options=[{"value": v, "label": lbl} for v, lbl in all_pathogens.items()],
        )

        # Antibiotic depends on syndrome + pathogen — collect across all combinations
        all_antibiotics: dict[str, str] = {}
        for syn_val, _ in syn_opts:
            await set_select(page, SEL_SYNDROME, syn_val)
            path_opts = await get_select_options(page, SEL_PATHOGEN)
            for path_val, _ in path_opts:
                await set_select(page, SEL_PATHOGEN, path_val)
                for v, lbl in await get_select_options(page, SEL_ANTIBIOTIC):
                    all_antibiotics[v] = lbl
        log.info(
            "Antibiotic options",
            count=len(all_antibiotics),
            options=[{"value": v, "label": lbl} for v, lbl in all_antibiotics.items()],
        )

        await browser.close()


@click.command()
@click.option(
    "--discover",
    is_flag=True,
    default=False,
    help="Print all select/download element IDs on the page and exit (opens real browser).",
)
@click.option(
    "--print-options",
    "do_print_options",
    is_flag=True,
    default=False,
    help="Print all available dropdown values for each filter and exit.",
)
@click.option(
    "--output-dir",
    default=str(Path.home() / "Downloads"),
    show_default=True,
    type=click.Path(),
    help="Parent directory; a 'glass_by_antibiotic' subfolder will be created inside it.",
)
@click.option(
    "--headless/--no-headless",
    default=True,
    help="Run browser headlessly (default) or with a visible window.",
)
@click.option(
    "--resume/--no-resume",
    default=True,
    help="Skip files that already exist in output-dir (default: on).",
)
def main(discover: bool, do_print_options: bool, output_dir: str, headless: bool, resume: bool) -> None:
    """Download all WHO GLASS antibiotic CSV slices from the GLASS AMR dashboard."""
    if discover:
        asyncio.run(run_discover())
    elif do_print_options:
        asyncio.run(print_options())
    else:
        asyncio.run(download_all(Path(output_dir) / "glass_by_antibiotic", headless=headless, resume=resume))


if __name__ == "__main__":  # not outdated as this is a standalone script
    main()
