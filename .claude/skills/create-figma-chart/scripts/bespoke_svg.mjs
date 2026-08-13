#!/usr/bin/env node
/**
 * Export an OWID bespoke visualization as chart-only SVG(s) for import into a Figma template.
 *
 * Two routes (see ../BESPOKE-SVG.md):
 *
 *   --demo <project>     drive the local dev demo page, which mounts every variant in a Shadow DOM
 *   --article <url>      drive a published article, mounting the bundle ourselves if the page's own
 *                        hydration doesn't fire
 *
 * Writes <base>.svg (or <base>-1.svg, <base>-2.svg, ... for a split viz) plus <base>.boxes.json,
 * a sidecar holding each viz's bounding box relative to the chart container so Step 5 can place a
 * multi-part viz without re-measuring it.
 *
 * Requires puppeteer-core and a system Chrome; install into a scratchpad, not the project:
 *   npm init -y && npm install puppeteer-core@23
 *
 * The in-page serializer below mirrors owid-grapher's bespoke/shared/exportSvg.ts. It is a copy
 * rather than an import because production serves built bundles, not that source file. If
 * exportSvg.ts changes -- especially STYLE_PROPS -- re-copy it.
 */

import { access, mkdir, writeFile } from "node:fs/promises"
import { join, resolve } from "node:path"

const CHROME_PATHS = [
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    "/Applications/Chromium.app/Contents/MacOS/Chromium",
    "/usr/bin/google-chrome",
    "/usr/bin/chromium",
    "/usr/bin/chromium-browser",
]

const USAGE = `
Usage:
  bespoke_svg.mjs --demo <project> [--variant <name>] [options]
  bespoke_svg.mjs --article <url> --bundle <name> --variant <name> [--config <json>] [options]

Options:
  --width <px>       viewport width  (default 1200) -- this is the aspect control
  --height <px>      viewport height (default 800)
  --out <dir>        output directory (default .)
  --base <name>      output filename stem (default derived from project/bundle + variant)
  --demo-url <url>   dev server base (default http://localhost:8089)
  --chrome <path>    Chrome executable
  --timeout <ms>     render wait budget (default 20000)
  --keep-open        leave the browser open (debugging)
`.trim()

function parseArgs(argv) {
    const opts = {
        width: 1200,
        height: 800,
        out: ".",
        demoUrl: "http://localhost:8089",
        timeout: 20000,
    }
    const flags = {
        "--demo": "demo",
        "--article": "article",
        "--bundle": "bundle",
        "--variant": "variant",
        "--config": "config",
        "--width": "width",
        "--height": "height",
        "--out": "out",
        "--base": "base",
        "--demo-url": "demoUrl",
        "--chrome": "chrome",
        "--timeout": "timeout",
    }
    for (let i = 0; i < argv.length; i++) {
        const arg = argv[i]
        if (arg === "--help" || arg === "-h") return { help: true }
        if (arg === "--keep-open") {
            opts.keepOpen = true
            continue
        }
        const key = flags[arg]
        if (!key) throw new Error(`Unknown argument: ${arg}\n\n${USAGE}`)
        const value = argv[++i]
        if (value === undefined) throw new Error(`${arg} needs a value`)
        opts[key] =
            key === "width" || key === "height" || key === "timeout"
                ? Number(value)
                : value
    }
    if (!opts.demo && !opts.article)
        throw new Error(`Pass --demo <project> or --article <url>\n\n${USAGE}`)
    if (opts.demo && opts.article)
        throw new Error("--demo and --article are mutually exclusive")
    if (opts.article && !opts.bundle)
        throw new Error("--article needs --bundle (from the gdoc JSON)")
    return opts
}

async function findChrome(explicit) {
    const candidates = explicit ? [explicit] : CHROME_PATHS
    for (const path of candidates) {
        try {
            await access(path)
            return path
        } catch {
            // try the next one
        }
    }
    throw new Error(
        `No Chrome found. Pass --chrome <path>. Looked in:\n  ${candidates.join("\n  ")}`
    )
}

/**
 * Mirrors bespoke/shared/exportSvg.ts. Runs in the page; returns one entry per viz <svg>.
 * `rootSelector` is resolved in the page, and shadow roots are traversed.
 */
function serializeInPage(rootSelector, minVizSize) {
    const STYLE_PROPS = [
        "fill",
        "fill-opacity",
        "fill-rule",
        "stroke",
        "stroke-width",
        "stroke-opacity",
        "stroke-linecap",
        "stroke-linejoin",
        "stroke-dasharray",
        "stroke-dashoffset",
        "stroke-miterlimit",
        "opacity",
        "color",
        "visibility",
        "mix-blend-mode",
        "paint-order",
        "font-family",
        "font-size",
        "font-weight",
        "font-style",
        "font-variant",
        "letter-spacing",
        "word-spacing",
        "text-anchor",
        "text-decoration",
        "dominant-baseline",
        "alignment-baseline",
    ]

    const host = document.querySelector(rootSelector)
    if (!host) return { error: `No element matches ${rootSelector}` }

    const svgs = []
    const collect = (node) => {
        for (const el of node.querySelectorAll("*")) {
            if (el instanceof SVGSVGElement) {
                const rect = el.getBoundingClientRect()
                if (Math.max(rect.width, rect.height) >= minVizSize)
                    svgs.push(el)
            }
            if (el.shadowRoot) collect(el.shadowRoot)
        }
    }
    const searchRoot = host.shadowRoot ?? host
    collect(searchRoot)
    if (host instanceof SVGSVGElement) svgs.unshift(host)

    if (svgs.length === 0) return { error: "No viz <svg> found" }

    const hostRect = host.getBoundingClientRect()

    const serialize = (source) => {
        const clone = source.cloneNode(true)
        const sourceNodes = [source, ...source.querySelectorAll("*")]
        const cloneNodes = [clone, ...clone.querySelectorAll("*")]
        // Back-to-front so removing a hidden node doesn't shift later indices.
        for (let i = sourceNodes.length - 1; i >= 0; i--) {
            const dest = cloneNodes[i]
            if (!(dest instanceof SVGElement)) continue
            const style = getComputedStyle(sourceNodes[i])
            if (style.display === "none") {
                dest.remove()
                continue
            }
            let inline = dest.getAttribute("style") ?? ""
            for (const prop of STYLE_PROPS) {
                const value = style.getPropertyValue(prop)
                if (value && value !== "normal" && value !== "auto")
                    inline += `${prop}:${value};`
            }
            if (inline) dest.setAttribute("style", inline)
        }
        clone.setAttribute("xmlns", "http://www.w3.org/2000/svg")
        if (!clone.getAttribute("viewBox")) {
            const rect = source.getBoundingClientRect()
            clone.setAttribute("viewBox", `0 0 ${rect.width} ${rect.height}`)
        }
        return new XMLSerializer().serializeToString(clone)
    }

    return {
        container: { width: hostRect.width, height: hostRect.height },
        vizzes: svgs.map((svg) => {
            const rect = svg.getBoundingClientRect()
            return {
                markup: serialize(svg),
                box: {
                    x: rect.x - hostRect.x,
                    y: rect.y - hostRect.y,
                    width: rect.width,
                    height: rect.height,
                },
            }
        }),
    }
}

/** Wait until a viz-sized <svg> exists under the selector, shadow roots included. */
async function waitForViz(page, selector, minVizSize, timeout) {
    await page.waitForFunction(
        (sel, min) => {
            const host = document.querySelector(sel)
            if (!host) return false
            const root = host.shadowRoot ?? host
            const found = []
            const walk = (node) => {
                for (const el of node.querySelectorAll("*")) {
                    if (el instanceof SVGSVGElement) {
                        const r = el.getBoundingClientRect()
                        if (Math.max(r.width, r.height) >= min) found.push(el)
                    }
                    if (el.shadowRoot) walk(el.shadowRoot)
                }
            }
            walk(root)
            return found.length > 0
        },
        { timeout, polling: 250 },
        selector,
        minVizSize
    )
}

async function runDemo(page, opts) {
    const url = `${opts.demoUrl.replace(/\/$/, "")}/${opts.demo}/demo`
    await page.goto(url, { waitUntil: "networkidle2", timeout: opts.timeout })
    // The demo page wraps each variant in div.variant#variant--<name>.
    const selector = opts.variant
        ? `#variant--${opts.variant}`
        : ".variant:first-of-type"
    await page.waitForSelector(selector, { timeout: opts.timeout })
    await waitForViz(page, selector, 60, opts.timeout)
    return selector
}

async function runArticle(page, opts) {
    await page.goto(opts.article, {
        waitUntil: "networkidle2",
        timeout: opts.timeout,
    })
    const selector = "[class*=bespoke-component]"
    await page.waitForSelector(selector, { timeout: opts.timeout })
    await page.evaluate((sel) => {
        document
            .querySelector(sel)
            ?.scrollIntoView({ block: "center", behavior: "instant" })
    }, selector)

    // The page may hydrate on its own. Only mount by hand if it doesn't.
    let rendered = true
    try {
        await waitForViz(page, selector, 60, 5000)
    } catch {
        rendered = false
    }

    if (!rendered) {
        // Mount into a Shadow DOM, as production does: each bundle injects its own CSS scoped to
        // :host, so mounting into a plain element renders the component unstyled -- and the
        // serializer reads computed styles, so the SVG would come out with the wrong paint.
        const config = opts.config ? JSON.parse(opts.config) : {}
        await page.evaluate(
            async (sel, bundle, variant, cfg) => {
                const el = document.querySelector(sel)
                const root = el.shadowRoot ?? el.attachShadow({ mode: "open" })
                const mountPoint = document.createElement("div")
                root.appendChild(mountPoint)
                const mod = await import(`/assets/bespoke/${bundle}/index.js`)
                window.__bespokeUnmount = mod.mount(mountPoint, {
                    variant,
                    config: { ...cfg, urlSync: false },
                })
            },
            selector,
            opts.bundle,
            opts.variant,
            config
        )
        await waitForViz(page, selector, 60, opts.timeout)
    }
    return selector
}

async function main() {
    const opts = parseArgs(process.argv.slice(2))
    if (opts.help) {
        console.log(USAGE)
        return
    }

    const executablePath = await findChrome(opts.chrome)
    const { default: puppeteer } = await import("puppeteer-core")

    const browser = await puppeteer.launch({
        executablePath,
        headless: "new",
        defaultViewport: { width: opts.width, height: opts.height },
    })

    try {
        const page = await browser.newPage()
        page.on("console", (msg) => {
            if (msg.type() === "error") console.error(`[page] ${msg.text()}`)
        })

        const selector = opts.demo
            ? await runDemo(page, opts)
            : await runArticle(page, opts)

        const result = await page.evaluate(serializeInPage, selector, 60)
        if (result.error) throw new Error(`In-page: ${result.error}`)

        const stem =
            opts.base ??
            [opts.demo ?? opts.bundle, opts.variant].filter(Boolean).join("-")
        const outDir = resolve(opts.out)
        await mkdir(outDir, { recursive: true })

        const written = []
        for (const [i, viz] of result.vizzes.entries()) {
            const name =
                result.vizzes.length > 1 ? `${stem}-${i + 1}.svg` : `${stem}.svg`
            await writeFile(join(outDir, name), viz.markup, "utf8")
            written.push({ file: name, box: viz.box })
        }
        await writeFile(
            join(outDir, `${stem}.boxes.json`),
            JSON.stringify(
                { container: result.container, vizzes: written },
                null,
                2
            ),
            "utf8"
        )

        console.log(
            `Wrote ${written.length} SVG${written.length === 1 ? "" : "s"} to ${outDir}`
        )
        for (const { file, box } of written)
            console.log(
                `  ${file}  ${Math.round(box.width)}x${Math.round(box.height)} at (${Math.round(box.x)}, ${Math.round(box.y)})`
            )
        console.log(`  ${stem}.boxes.json`)
    } finally {
        if (!opts.keepOpen) await browser.close()
    }
}

main().catch((err) => {
    console.error(err.message ?? err)
    process.exit(1)
})
