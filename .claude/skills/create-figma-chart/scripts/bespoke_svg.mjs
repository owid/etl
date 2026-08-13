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
import { createRequire } from "node:module"
import { join, resolve } from "node:path"
import { pathToFileURL } from "node:url"

/**
 * Load puppeteer-core from the CURRENT WORKING DIRECTORY, not from next to this file.
 * The script lives in the skill directory (which has no node_modules) while the dependency is
 * installed in a throwaway scratchpad, so a bare `import "puppeteer-core"` resolves from the
 * script's own folder and fails. Run the script from wherever you installed it.
 */
async function loadPuppeteer() {
    try {
        const requireFromCwd = createRequire(
            pathToFileURL(join(process.cwd(), "package.json"))
        )
        const entry = requireFromCwd.resolve("puppeteer-core")
        return (await import(pathToFileURL(entry).href)).default
    } catch (err) {
        throw new Error(
            "Could not load puppeteer-core from " +
                process.cwd() +
                "\nInstall it there first:  npm init -y && npm install puppeteer-core@23" +
                "\n(original: " +
                (err.message ?? err) +
                ")"
        )
    }
}

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

Sizing options -- --demo only. Passing any of them mounts our own container instead of reading
the demo page's, so the geometry is ours rather than the page's. They cannot be honored on
--article, which exports the page's own mount at the page's own size; passing one there is an
error rather than a silent no-op.
  --viz-width <px>   width of the mounted container (default: --width). These components lay
                     out from container width, so this is what shapes the export
  --viz-height <px>  height of the mounted container. Components using useParentSize() take their
                     height from it too -- but only if the component has no hard-coded height
  --viz-css <css>    CSS injected into the Shadow DOM after mounting. The escape hatch for a
                     component that pins its chart height in SCSS, e.g.
                     '.food-trade-captioned-chart__chart-area{height:896px!important}'

Options:
  --width <px>       viewport width  (default 1200) -- this is the aspect control
  --config <json>    mount with this config instead of the variant's own demoConfig. On --article
                     it applies only if the page does not hydrate itself
  --height <px>      viewport height (default 800)
  --out <dir>        output directory (default .)
  --base <name>      output filename stem (default derived from project/bundle + variant)
  --demo-url <url>   dev server base (default http://localhost:8089)
  --chrome <path>    Chrome executable
  --timeout <ms>     render wait budget (default 20000)
  --block-index <n>  which bespoke component on the article to export, 0-based. Required when the
                     page has more than one: the container exposes its size, not its bundle, so
                     --bundle cannot disambiguate
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
        "--viz-width": "vizWidth",
        "--viz-height": "vizHeight",
        "--viz-css": "vizCss",
        "--block-index": "blockIndex",
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
            ["width", "height", "timeout", "vizWidth", "vizHeight", "blockIndex"].includes(key)
                ? Number(value)
                : value
    }
    if (!opts.demo && !opts.article)
        throw new Error(`Pass --demo <project> or --article <url>\n\n${USAGE}`)
    if (opts.demo && opts.article)
        throw new Error("--demo and --article are mutually exclusive")
    if (opts.article && !opts.bundle)
        throw new Error("--article needs --bundle (from the gdoc JSON)")
    // The article route exports the page's own mount, whose container the page sizes. Honoring a
    // sizing option there would mean re-mounting a component that may already have rendered, so
    // reject the combination instead of accepting the flag and ignoring it -- a silently
    // wrong-sized SVG is the failure this whole route is trying to avoid.
    if (opts.article) {
        const ignored = [
            ["--viz-width", opts.vizWidth],
            ["--viz-height", opts.vizHeight],
            ["--viz-css", opts.vizCss],
        ]
            .filter(([, value]) => value !== undefined)
            .map(([flag]) => flag)
        if (ignored.length)
            throw new Error(
                `${ignored.join(", ")} cannot be honored on --article: the page owns the ` +
                    `container, so the export comes out at the page's geometry.\n` +
                    `To choose the size, check the project out and use --demo instead.`
            )
    }
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

        // These components draw side labels OUTSIDE their own <svg> bounds and rely on the page not
        // clipping. A standalone SVG clips at its viewBox, so the labels would be cut. Widen the
        // viewBox to the union of the content's own bbox.
        const vb = source.getAttribute("viewBox")
        const rect = source.getBoundingClientRect()
        let vx0 = 0, vy0 = 0, vw = rect.width, vh = rect.height
        if (vb) {
            const [vx, vy, w, h] = vb.trim().split(/[\s,]+/).map(Number)
            vx0 = vx; vy0 = vy; vw = w; vh = h
        }
        let x0 = vx0, y0 = vy0, x1 = vx0 + vw, y1 = vy0 + vh
        try {
            const bb = source.getBBox()          // user units, union of all rendered children
            const PAD = 2
            x0 = Math.min(x0, bb.x - PAD)
            y0 = Math.min(y0, bb.y - PAD)
            x1 = Math.max(x1, bb.x + bb.width + PAD)
            y1 = Math.max(y1, bb.y + bb.height + PAD)
        } catch {
            // getBBox throws on a detached or empty tree; keep the declared box.
        }
        clone.setAttribute("viewBox", `${x0} ${y0} ${x1 - x0} ${y1 - y0}`)
        clone.setAttribute("width", `${x1 - x0}`)
        clone.setAttribute("height", `${y1 - y0}`)

        // That widening moved the file's origin off the element's top-left and grew it past the
        // element's rect -- by each viz's OWN label overhang, so by a different amount per part of
        // a split viz. Report where the WRITTEN file sits, in page px, or placing the parts from
        // their on-page rects shifts them relative to each other. Scale is per axis so a
        // non-uniform mapping degrades gracefully; under the default uniform one both agree.
        const scaleX = vw ? rect.width / vw : 1
        const scaleY = vh ? rect.height / vh : 1
        return {
            markup: new XMLSerializer().serializeToString(clone),
            offsetX: (x0 - vx0) * scaleX,
            offsetY: (y0 - vy0) * scaleY,
            width: (x1 - x0) * scaleX,
            height: (y1 - y0) * scaleY,
        }
    }

    return {
        container: { width: hostRect.width, height: hostRect.height },
        vizzes: svgs.map((svg) => {
            const rect = svg.getBoundingClientRect()
            const out = serialize(svg)
            return {
                markup: out.markup,
                // Where the element sits on the page, before the viewBox widening.
                box: {
                    x: rect.x - hostRect.x,
                    y: rect.y - hostRect.y,
                    width: rect.width,
                    height: rect.height,
                },
                // Where the written SVG sits. Place a multi-part viz from THIS one.
                exportBox: {
                    x: rect.x - hostRect.x + out.offsetX,
                    y: rect.y - hostRect.y + out.offsetY,
                    width: out.width,
                    height: out.height,
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

    // The demo page mounts each variant itself, in div.variant#variant--<name>, at the width its
    // own grid gives it. That is only usable when both the view AND the size it happens to render
    // at are the ones we want -- so any of --config/--viz-* means we mount our own instead.
    // Reading the page's mount while a sizing option was passed would export the page's geometry
    // under the caller's numbers.
    const ownMount =
        opts.config !== undefined ||
        opts.vizWidth !== undefined ||
        opts.vizHeight !== undefined ||
        opts.vizCss !== undefined

    if (!ownMount) {
        const selector = opts.variant
            ? `#variant--${opts.variant}`
            : ".variant:first-of-type"
        await page.waitForSelector(selector, { timeout: opts.timeout })
        await waitForViz(page, selector, 60, opts.timeout)
        return selector
    }

    // Mount our own instance on the dev-server origin (same-origin, so the module and its CSS
    // resolve) into our own Shadow DOM. The demo page is still the host because it carries the
    // dev-only global stylesheet.
    await page.waitForSelector(".variant", { timeout: opts.timeout })
    const selector = "#bespoke-svg-target"
    await page.evaluate(
        async (sel, project, variant, cfg, hostWidth, hostHeight, extraCss) => {
            const host = document.createElement("div")
            host.id = sel.replace("#", "")
            // Fixed width, not 100%: these components lay out from their container width, so the
            // page's own padding must not decide what the export looks like.
            host.style.cssText =
                `position:relative;width:${hostWidth}px;background:#fff` +
                (hostHeight ? `;height:${hostHeight}px` : "")
            document.body.insertBefore(host, document.body.firstChild)
            const root = host.attachShadow({ mode: "open" })
            const mountPoint = document.createElement("div")
            root.appendChild(mountPoint)
            const mod = await import(`/${project}/index.js`)
            if (!mod.VARIANTS?.length)
                throw new Error(
                    `${project} exports no VARIANTS, so there is no variant to mount.`
                )
            const entry = variant
                ? mod.VARIANTS.find((v) => v.name === variant)
                : mod.VARIANTS[0]
            if (!entry)
                throw new Error(
                    `Unknown variant "${variant}". ${project} has: ` +
                        mod.VARIANTS.map((v) => v.name).join(", ")
                )
            // Without --config, reuse the variant's own demoConfig -- the exact view the demo page
            // shows -- so a sizing-only run re-renders the same chart at our geometry rather than
            // an empty config's default view.
            window.__bespokeUnmount = mod.mount(mountPoint, {
                variant: entry.name,
                config: { ...(cfg ?? entry.demoConfig ?? {}), urlSync: false },
            })
            if (extraCss) {
                const style = document.createElement("style")
                style.textContent = extraCss
                root.appendChild(style)
            }
        },
        selector,
        opts.demo,
        opts.variant,
        opts.config ? JSON.parse(opts.config) : null,
        opts.vizWidth ?? opts.width,
        opts.vizHeight,
        opts.vizCss
    )
    await waitForViz(page, selector, 60, opts.timeout)
    // Injected CSS changes the container size, so the component re-lays out asynchronously via its
    // ResizeObserver. Give that a beat before serializing, or the old geometry is captured.
    if (opts.vizCss) await new Promise((r) => setTimeout(r, 1200))
    return selector
}

async function runArticle(page, opts) {
    await page.goto(opts.article, {
        waitUntil: "networkidle2",
        timeout: opts.timeout,
    })
    const baseSelector = "[class*=bespoke-component]"
    await page.waitForSelector(baseSelector, { timeout: opts.timeout })

    // An article can carry several bespoke components, and the container does NOT say which bundle
    // it is: the class carries the block's *size* (`bespoke-component--<size>`), never its bundle.
    // So --bundle cannot disambiguate, and taking the first match would silently export a different
    // chart than the one asked for -- plausible-looking and wrong. Make the caller choose.
    const blocks = await page.$$eval(baseSelector, (els) =>
        els.map((el, i) => ({ i, className: el.className, text: (el.textContent ?? "").trim().slice(0, 60) }))
    )
    if (blocks.length > 1 && opts.blockIndex === undefined) {
        const list = blocks.map(b => `  [${b.i}] class="${b.className}" text="${b.text}"`).join("\n")
        throw new Error(
            `This article has ${blocks.length} bespoke components and the container does not expose ` +
            `its bundle, so --bundle cannot pick one.\nRe-run with --block-index <n>:\n${list}`
        )
    }
    const index = opts.blockIndex ?? 0
    if (index >= blocks.length)
        throw new Error(`--block-index ${index} out of range: the article has ${blocks.length} bespoke component(s)`)

    // Tag the chosen block so every later step addresses it and nothing else.
    const selector = "#bespoke-svg-target"
    await page.evaluate((base, i, id) => {
        const el = document.querySelectorAll(base)[i]
        el.id = id.replace("#", "")
        el.scrollIntoView({ block: "center", behavior: "instant" })
    }, baseSelector, index, selector)

    // The page may hydrate on its own. Only mount by hand if it doesn't.
    let rendered = true
    try {
        await waitForViz(page, selector, 60, 5000)
    } catch {
        rendered = false
    }

    // --config only reaches a component we mount ourselves, and whether we do is the page's
    // choice, not ours. Say so rather than leaving the caller to assume their config was applied.
    if (rendered && opts.config)
        console.warn(
            "Note: the page hydrated the component itself, so --config was NOT applied -- " +
                "this is the view the article publishes."
        )

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
    const puppeteer = await loadPuppeteer()

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
            written.push({ file: name, box: viz.box, exportBox: viz.exportBox })
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
        // Report the written file's own box, not the element's: they differ by the viewBox widening,
        // and it is the file that gets imported.
        for (const { file, exportBox } of written)
            console.log(
                `  ${file}  ${Math.round(exportBox.width)}x${Math.round(exportBox.height)} at (${Math.round(exportBox.x)}, ${Math.round(exportBox.y)})`
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
