const PACKAGE_URL = "__PACKAGE_BASE__/grapher.standalone.min.js";
// Filled in by build.py: the pre-filled state (null for an empty page), the pre-filled indicators' data, an
// example catalog path for empty fields, and the name this sketch is saved under in the browser.
const INITIAL_STATE = __INITIAL_STATE__;
const INITIAL_INDICATORS = JSON.parse(document.getElementById("initial-data").textContent);
const EXAMPLE_PATH = __EXAMPLE_PATH__;
const DEFAULT_SELECTION = __DEFAULT_SELECTION__;
const GRAPHER_SCHEMA_VERSION = __GRAPHER_SCHEMA_VERSION__;
// Valid topic tags (schemas/dataset-schema.json) and the topic page each one links to.
const TOPIC_TAGS = __TOPIC_TAGS__;
const TAG_SLUGS = __TAG_SLUGS__;
const STORAGE_KEY = __STORAGE_KEY__;
const API = "https://api.ourworldindata.org/v1/indicators";
const DATASETTE = "https://datasette-public.owid.io/owid/variables.json";
const CATALOG = "https://catalog.ourworldindata.org";

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------
const newId = () => Math.random().toString(36).slice(2, 10);

function defaultState() {
  if (INITIAL_STATE) return JSON.parse(JSON.stringify(INITIAL_STATE));
  // An empty page: one dropdown with one option, and nothing linked yet.
  const dimId = newId(), choiceId = newId();
  return {
    open: true,
    title: "",
    titleVariant: "",
    topicTags: [],
    dims: [{ id: dimId, name: "Dropdown 1", choices: [{ id: choiceId, name: "Option 1" }] }],
    links: {},
    selected: { [dimId]: choiceId },
  };
}

function loadState() {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (raw) {
      const s = JSON.parse(raw);
      if (Array.isArray(s.dims) && s.links && typeof s.links === "object") return { open: true, selected: {}, title: "", titleVariant: "", topicTags: [], ...s };
    }
  } catch (err) { warnNotSaved(err); }
  return defaultState();
}

let state = loadState();
if (!state.dims.length) state.dims.push({ id: newId(), name: "Dropdown 1", choices: [{ id: newId(), name: "Option 1" }] });
// Text overrides, per view (keyed like links). Older saved setups have none.
state.textMode ??= false;
state.textEdits ??= {};
state.textEdits.views ??= {};
state.suggest ??= {};
state.defaultSelection ??= [...DEFAULT_SELECTION];
state.defaultKey ??= null; // the combination the MDim opens on; null means the first one
state.chartTypes ??= ["LineChart", "DiscreteBar"];
state.hasMapTab ??= true;
state.defaultTab ??= "chart";
state.hideTimeInTitle ??= true;
state.changeCountry ??= false;

// The sketch lives only in the browser, so a failure to save (private window, blocked storage, full quota) is
// shown rather than swallowed.
function warnNotSaved(err) {
  console.warn("Could not save the sketch", err);
  const warning = document.getElementById("storage-warning");
  if (warning) warning.hidden = false;
}

function saveState() {
  try { localStorage.setItem(STORAGE_KEY, JSON.stringify(state)); } catch (err) { warnNotSaved(err); }
}

// Every combination of options, as arrays of choices in dropdown order.
function combinations() {
  let combos = [[]];
  for (const dim of state.dims) {
    combos = combos.flatMap((combo) => dim.choices.map((choice) => [...combo, choice]));
  }
  return combos;
}
const comboKey = (combo) => combo.map((c) => c.id).join("|");

function normalizeSelection() {
  const selected = {};
  for (const dim of state.dims) {
    const current = state.selected[dim.id];
    selected[dim.id] = dim.choices.some((c) => c.id === current) ? current : dim.choices[0]?.id;
  }
  state.selected = selected;
}

function currentCombo() {
  normalizeSelection();
  const combo = state.dims.map((dim) => dim.choices.find((c) => c.id === state.selected[dim.id]));
  return combo.every(Boolean) ? combo : null;
}

function pruneLinks() {
  const keys = new Set(combinations().map(comboKey));
  for (const key of Object.keys(state.links)) if (!keys.has(key)) delete state.links[key];
  for (const key of Object.keys(state.textEdits.views)) if (!keys.has(key)) delete state.textEdits.views[key];
  for (const key of Object.keys(state.suggest)) if (!keys.has(key)) delete state.suggest[key];
}

// Re-key links when a dropdown is added or removed, so existing work survives.
function rekeyLinks(transform) {
  const rekey = (byKey) => {
    const out = {};
    for (const [key, value] of Object.entries(byKey)) {
      const newKey = transform(key === "" ? [] : key.split("|"));
      if (newKey !== null && !(newKey in out)) out[newKey] = value;
    }
    return out;
  };
  state.links = rekey(state.links);
  state.textEdits.views = rekey(state.textEdits.views);
  state.suggest = rekey(state.suggest);
  if (state.defaultKey !== null) state.defaultKey = transform(state.defaultKey === "" ? [] : state.defaultKey.split("|"));
}

// ---------------------------------------------------------------------------
// Indicators: resolve what was pasted, fetch metadata and data
// ---------------------------------------------------------------------------
const indicatorCache = new Map();
const metaCache = new Map();
for (const { meta, csv } of INITIAL_INDICATORS) {
  indicatorCache.set(meta.id, Promise.resolve({ meta, csv }));
  metaCache.set(meta.id, Promise.resolve(meta));
}

async function fetchJson(url) {
  const res = await fetch(url);
  if (!res.ok) throw Object.assign(new Error(`HTTP ${res.status}`), { status: res.status });
  return res.json();
}

// The indicator API leaves out an indicator's own chart settings (title, subtitle, note, map colours from its
// metadata's grapher_config), which an MDim view inherits. The catalog's table metadata has them.
const tableMetaCache = new Map();
async function withChartSettings(meta) {
  const [table, field] = String(meta.catalogPath || "").split("#");
  if (!table || !field || meta.presentation?.grapherConfig) return meta;
  if (!tableMetaCache.has(table)) tableMetaCache.set(table, fetchJson(`${CATALOG}/${table}.meta.json`));
  try {
    const tableMeta = await tableMetaCache.get(table);
    const grapherConfig = tableMeta.fields?.[field]?.presentation?.grapher_config;
    if (grapherConfig) meta.presentation = { ...(meta.presentation || {}), grapherConfig };
  } catch (err) {
    tableMetaCache.delete(table);
    console.warn(`Could not load the chart settings of ${meta.catalogPath}`, err);
    meta.chartSettingsMissing = true;
  }
  return meta;
}

function getMeta(id) {
  if (!metaCache.has(id)) {
    const p = fetchJson(`${API}/${id}.metadata.json`)
      // Only a 404 means the ID is wrong; anything else is the network or the API, worth trying again.
      .catch((err) => {
        if (err.status === 404) throw new Error(`No indicator with ID ${id}.`);
        throw Object.assign(new Error(`Couldn't load indicator ${id} (${err.message}). Try again.`), { transient: true });
      })
      .then(withChartSettings);
    p.catch(() => metaCache.delete(id));
    metaCache.set(id, p);
  }
  return metaCache.get(id);
}

function csvCell(value) {
  const s = String(value ?? "");
  return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}

function getIndicator(id) {
  if (!indicatorCache.has(id)) {
    const p = Promise.all([getMeta(id), fetchJson(`${API}/${id}.data.json`)]).then(([meta, data]) => {
      const entities = new Map(meta.dimensions.entities.values.map((e) => [e.id, e]));
      const rows = [`entityName,entityCode,entityId,year,ind_${id}`];
      for (let i = 0; i < data.values.length; i++) {
        const e = entities.get(data.entities[i]);
        rows.push([csvCell(e?.name), csvCell(e?.code), data.entities[i], data.years[i], data.values[i]].join(","));
      }
      return { meta, csv: rows.join("\n") };
    });
    p.catch(() => indicatorCache.delete(id));
    indicatorCache.set(id, p);
  }
  return indicatorCache.get(id);
}

async function resolveInput(raw) {
  const input = raw.trim();
  if (/ourworldindata\.org\/(grapher|explorers)\//i.test(input) || /\/admin\/charts\//i.test(input)) {
    throw new Error("Chart links aren't accepted. Paste the indicator's catalog path or ID instead.");
  }
  let match;
  if (/^\d+$/.test(input)) return Number(input);
  if ((match = input.match(/\/(?:variables|indicators)\/(\d+)/))) return Number(match[1]);
  if (input.includes("#")) {
    if (!input.startsWith("grapher/")) throw new Error("Use the full catalog path, starting with grapher/.");
    const rows = await fetchJson(`${DATASETTE}?catalogPath__exact=${encodeURIComponent(input)}&_shape=array&_col=id`);
    if (!rows.length) throw new Error("No published indicator has this catalog path.");
    return rows[0].id;
  }
  throw new Error("Paste a catalog path (grapher/…#…) or an indicator ID.");
}

// ---------------------------------------------------------------------------
// Chart
// ---------------------------------------------------------------------------
const chartEl = document.getElementById("chart");
const wyskEl = document.getElementById("wysk");
let grapher = null;
let loader = null;
let loaderReady = false;
let renderToken = 0;

function esc(text) {
  return String(text ?? "").replace(/[&<>"']/g, (ch) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[ch]);
}

function mdInline(text) {
  let html = esc(text);
  html = html.replace(/\*\*(.+?)\*\*/g, "<strong>$1</strong>");
  html = html.replace(/\[([^\]]+)\]\(#dod:[^)]+\)/g, '<span class="dod">$1</span>');
  return html.replace(/\[([^\]]+)\]\(([^)\s]+)\)/g, '<a href="$2" target="_blank" rel="noopener">$1</a>');
}

// "What you should know" as editable blocks, losing nothing. A description_key is a list or a markdown string
// (see CLAUDE.md). A string splits before each top-level "- " bullet: a block keeps its wrapped and nested
// lines, and text before the first bullet is a block of its own, so joining the blocks gives back the
// original. List items become "- " blocks. descriptionKeyValue() turns blocks back into the original's form.
function descriptionKeyBlocks(descriptionKey) {
  if (!descriptionKey) return [];
  // A one-item list is shown as a paragraph, as ETL and Grapher show it.
  if (Array.isArray(descriptionKey)) return descriptionKey.length === 1 ? [String(descriptionKey[0])] : descriptionKey.map((item) => `- ${item}`);
  const blocks = [];
  for (const line of String(descriptionKey).split("\n")) {
    if (line.startsWith("- ") || !blocks.length) blocks.push(line);
    else blocks[blocks.length - 1] += `\n${line}`;
  }
  return blocks;
}

function descriptionKeyValue(blocks, asList) {
  return asList ? blocks.map((b) => (b.startsWith("- ") ? b.slice(2) : b)) : blocks.join("\n");
}

// A pre-filled view records whether its text came as a list; text edited in the page follows the indicator.
const descriptionKeyAsList = (edits, meta) => edits?.descriptionKeyIsList ?? Array.isArray(meta.descriptionKey);

// A list keeps the shape ETL gives it: one item is a paragraph, several are all bullets.
const listShaped = (blocks) => (blocks.length === 1 ? [blockText(blocks[0])] : blocks.map((b) => (isBullet(b) ? b : `- ${b}`)));

// A block's text without its bullet, as markdown: continuation lines join the paragraph, nested "- " lines
// become a nested list.
function blockHtml(text) {
  const out = [];
  let nested = [];
  const flush = () => {
    if (nested.length) out.push(`<ul>${nested.map((n) => `<li>${mdInline(n)}</li>`).join("")}</ul>`);
    nested = [];
  };
  for (const line of text.split("\n")) {
    const item = line.match(/^\s+[-*] (.*)$/);
    if (item) { nested.push(item[1]); continue; }
    flush();
    out.push(line.trim() ? mdInline(line.trim()) : "<br>");
  }
  flush();
  return out.join(" ");
}

const isBullet = (block) => block.startsWith("- ");
const blockText = (block) => (isBullet(block) ? block.slice(2) : block);

function columnDef(meta) {
  const def = {
    slug: `ind_${meta.id}`, type: "Numeric", name: meta.name, unit: meta.unit, shortUnit: meta.shortUnit,
    display: meta.display, descriptionShort: meta.descriptionShort, descriptionKey: meta.descriptionKey,
    descriptionProcessing: meta.descriptionProcessing, descriptionFromProducer: meta.descriptionFromProducer,
    timespan: meta.timespan, origins: meta.origins,
  };
  return Object.fromEntries(Object.entries(def).filter(([, v]) => v !== undefined && v !== null && v !== ""));
}

// A view's chart text comes from the indicator alone: its grapher config if it has one,
// otherwise its public title and short description.
function viewConfig(meta) {
  const p = meta.presentation || {};
  const { dimensions, $schema, id, slug, ...gc } = p.grapherConfig || {};
  return {
    hasMapTab: true,
    ...gc,
    title: gc.title || p.titlePublic || meta.display?.name || meta.name,
    subtitle: gc.subtitle ?? meta.descriptionShort ?? "",
    selectedEntityNames: state.defaultSelection,
    dimensions: [{ property: "y", slug: `ind_${meta.id}` }],
  };
}

function showNotice(message) {
  disposeChart();
  chartEl.innerHTML = `<div class="notice">${message}</div>`;
}

function disposeChart() {
  try { loader?.dispose?.(); } catch (err) { console.warn(err); }
  loader = null;
  loaderReady = false;
}

// The countries ticked in Grapher's own "Add/remove countries and regions" panel are the MDim's
// default selection. Grapher keeps its state in MobX, which this page can't subscribe to, so poll it.
function syncSelection() {
  if (!loader || !loaderReady) return;
  let names;
  try { names = [...loader.grapherState.selection.selectedEntityNames]; } catch (err) { return; }
  if (!same(names, state.defaultSelection)) {
    state.defaultSelection = names;
    saveState();
  }
  let tab;
  try { tab = loader.grapherState.tab; } catch (err) { return; }
  if (tab && tab !== state.defaultTab) {
    state.defaultTab = tab;
    saveState();
    if (state.textMode) renderTextEditor(); // the map colours section depends on the tab
  }
}

// The reminder sits on the "Edit FAUST and WYSK" row, ending where the countries panel's content ends
// below it; it hides when the chart shows no such panel.
function positionSelectionNote() {
  document.getElementById("tab-note").hidden = !loader;
  const note = document.getElementById("selection-note");
  const panelEl = loader ? chartEl.querySelector(".side-panel .entity-selector") : null;
  if (!panelEl) { note.hidden = true; return; }
  note.hidden = false;
  const header = panelEl.querySelector(".overlay-header") || panelEl;
  const row = note.parentElement.getBoundingClientRect();
  note.style.marginRight = `${Math.max(0, row.right - header.getBoundingClientRect().right)}px`;
}

setInterval(() => { syncSelection(); positionSelectionNote(); }, 400);

// ---------------------------------------------------------------------------
// Settings shared by every view: tabs, the default tab, title annotations, footer link, y-axis
// ---------------------------------------------------------------------------
const CHART_TYPE_OPTIONS = [
  ["LineChart", "Line chart", "line"],
  ["DiscreteBar", "Bar chart", "discrete-bar"],
  ["SlopeChart", "Slope chart", "slope"],
  ["Dumbbell", "Dumbbell", "dumbbell"],
  ["Marimekko", "Marimekko", "marimekko"], // bar widths default to equal without a width (x) indicator
];

function validTabs() {
  const chartTabs = CHART_TYPE_OPTIONS.filter(([type]) => state.chartTypes.includes(type)).map(([, , tab]) => tab);
  return [...(chartTabs.length ? ["chart", ...chartTabs] : []), ...(state.hasMapTab ? ["map"] : []), "table"];
}

function normalizeDefaultTab() {
  // Saved setups may hold chart types the simulator no longer offers.
  state.chartTypes = state.chartTypes.filter((type) => CHART_TYPE_OPTIONS.some(([t]) => t === type));
  if (!validTabs().includes(state.defaultTab)) state.defaultTab = state.chartTypes.length ? "chart" : state.hasMapTab ? "map" : "table";
}

// The footer's "OurWorldinData.org/<topic>" link: always the main (first) topic tag's page.
function originUrl() {
  const slug = TAG_SLUGS[state.topicTags[0]];
  return slug ? `/${slug}` : undefined;
}

function sharedConfig() {
  const config = { chartTypes: [...state.chartTypes], hasMapTab: state.hasMapTab, tab: state.defaultTab };
  // Always explicit: an indicator's own chart settings often hide the time range, and only an explicit value
  // overrides them (Grapher deep-merges a view's config over its indicator's).
  config.hideAnnotationFieldsInTitle = { time: state.hideTimeInTitle };
  if (state.changeCountry) config.addCountryMode = "change-country";
  const url = originUrl();
  if (url) config.originUrl = url;
  config.yAxis = { min: 0 }; // always: every MDim's y-axis starts at zero
  return config;
}

function renderChartSettings() {
  const box = document.getElementById("chart-settings");
  box.innerHTML = "";
  const checkbox = (labelText, checked, onChange) => {
    const label = document.createElement("label");
    const input = document.createElement("input");
    input.type = "checkbox";
    input.checked = checked;
    input.addEventListener("change", () => {
      onChange(input.checked);
      normalizeDefaultTab();
      saveState();
      renderChartSettings();
      renderChart();
    });
    label.append(input, labelText);
    return label;
  };
  const note = (text) => {
    const p = document.createElement("p");
    p.className = "settings-note";
    p.textContent = text;
    return p;
  };
  // One labelled group per setting, so each choice says what it's about.
  const group = (title, ...children) => {
    const el = document.createElement("div");
    el.className = "settings-group";
    const heading = document.createElement("div");
    heading.className = "settings-group__title";
    heading.textContent = title;
    el.append(heading, ...children);
    return el;
  };

  const tabs = document.createElement("div");
  tabs.className = "settings-grid";
  for (const [type, name] of CHART_TYPE_OPTIONS) {
    tabs.append(checkbox(name, state.chartTypes.includes(type), (on) => {
      state.chartTypes = CHART_TYPE_OPTIONS.map(([t]) => t).filter((t) => (t === type ? on : state.chartTypes.includes(t)));
    }));
  }
  tabs.append(checkbox("Map", state.hasMapTab, (on) => { state.hasMapTab = on; }));

  const url = originUrl();
  box.append(
    group("Tabs", tabs, note("The same tabs in every view. The table is always there.")),
    group(
      "Title",
      checkbox("Hide the time range after the title, e.g. “1990 to 2025”", state.hideTimeInTitle, (on) => { state.hideTimeInTitle = on; }),
      note("Map and bar charts always show the year."),
    ),
    group("Countries", checkbox("Set selector to “Change country” instead of “Add/remove countries”", state.changeCountry, (on) => { state.changeCountry = on; })),
    note(`Always set: every y-axis starts at zero${url ? `, and the footer links to OurWorldinData.org${url}` : ""}.`),
  );
}

// ---------------------------------------------------------------------------
// Chart text: original (from the indicator), overrides, and what a view shows
// ---------------------------------------------------------------------------
const TEXT_FIELDS = ["title", "subtitle", "note"];
const FIELD_LABELS = {
  title: "Title", subtitle: "Subtitle", note: "Footnote", descriptionKey: "What you should know",
  rounding: "Number rounding", map: "Map colours",
};
const COLOR_SCHEMES = __COLOR_SCHEMES__;
// Grapher's automatic binning strategies. Manual bins are left to Claude, from the copied setup.
const BINNING_STRATEGIES = [
  ["auto", "Automatic"],
  ["log-auto", "Logarithmic, automatic"],
  ["log-1-2-5", "Logarithmic: 1, 2, 5, 10…"],
  ["log-1-3", "Logarithmic: 1, 3, 10…"],
  ["log-10", "Logarithmic: 1, 10, 100…"],
  ["equalSizeBins-few-bins", "Equal-size bins, few"],
  ["equalSizeBins-normal", "Equal-size bins"],
  ["equalSizeBins-many-bins", "Equal-size bins, many"],
  ["equalSizeBins-percent", "Equal-size bins for percentages"],
];
const MAP_KEYS = ["baseColorScheme", "binningStrategy"];
const same = (a, b) => JSON.stringify(a) === JSON.stringify(b);
let current = null; // { key, meta, label } of the view on screen, once its indicator has loaded

function originalRounding(meta) {
  const d = meta.display || {};
  const mode = d.roundingMode === "significantFigures" ? "significantFigures" : "decimalPlaces";
  return { mode, digits: mode === "significantFigures" ? d.numSignificantFigures ?? 3 : d.numDecimalPlaces ?? 2 };
}

// The map colour settings the editor handles, as the indicator's own chart config sets them (often none).
function originalMap(meta) {
  const colorScale = viewConfig(meta).map?.colorScale || {};
  return Object.fromEntries(MAP_KEYS.filter((k) => colorScale[k] !== undefined).map((k) => [k, colorScale[k]]));
}

function originalTexts(meta) {
  const cfg = viewConfig(meta);
  return {
    title: cfg.title || "", subtitle: cfg.subtitle || "", note: cfg.note || "",
    descriptionKey: descriptionKeyBlocks(meta.descriptionKey),
    rounding: originalRounding(meta),
    map: originalMap(meta),
  };
}

function roundingDisplay(rounding) {
  return rounding.mode === "significantFigures"
    ? { roundingMode: "significantFigures", numSignificantFigures: rounding.digits }
    : { roundingMode: "decimalPlaces", numDecimalPlaces: rounding.digits };
}

function colorScaleFrom(map) {
  const out = {};
  if (map.baseColorScheme) out.baseColorScheme = map.baseColorScheme;
  if (map.binningStrategy) out.binningStrategy = map.binningStrategy;
  return out;
}


// A view's own text wins over the indicator's, as a view's config does in the ETL file.
function resolvedTexts(key, meta) {
  const original = originalTexts(meta);
  const own = state.textEdits.views[key] || {};
  return Object.fromEntries(Object.keys(original).map((f) => [f, own[f] ?? original[f]]));
}

function setText(field, value) {
  setViewText(current.key, current.meta, field, value);
}

// The same, for any view: "Manage WYSK" edits views that aren't on screen.
function setViewText(key, meta, field, value) {
  const views = state.textEdits.views;
  const own = (views[key] ||= {});
  if (field === "descriptionKey" && descriptionKeyAsList(own, meta)) value = listShaped(value);
  if (same(value, originalTexts(meta)[field])) {
    delete own[field];
    dropSuggestion(key, field);
  } else own[field] = value;
  if (!("descriptionKey" in own)) delete own.descriptionKeyIsList;
  if (!Object.keys(own).length) delete views[key];
  saveState();
}

// A "suggest for the indicator everywhere" belongs to the change it was ticked on.
function dropSuggestion(key, field) {
  if (!state.suggest[key]) return;
  delete state.suggest[key][field];
  if (!Object.keys(state.suggest[key]).length) delete state.suggest[key];
}

function undoText(field) {
  const views = state.textEdits.views;
  if (!views[current.key]) return;
  delete views[current.key][field];
  dropSuggestion(current.key, field);
  if (field === "descriptionKey") delete views[current.key].descriptionKeyIsList;
  if (!Object.keys(views[current.key]).length) delete views[current.key];
  saveState();
}

function textHints(field, text) {
  const t = text.trim();
  const hints = [];
  if (field === "subtitle" && t) {
    if (!/[.!?]$/.test(t)) hints.push("End the subtitle with punctuation, normally a period.");
    if (/^The\s/.test(t)) hints.push("Try dropping “The” at the start, unless the sentence needs it.");
  }
  if (field === "title" && /\.$/.test(t)) hints.push("Chart titles don't end with a period.");
  return hints;
}

// "Edited · Undo" and the suggest-for-the-indicator checkbox, for one field of the current view.
function fieldTools(field) {
  const wrap = document.createElement("span");
  wrap.className = "field-tools";
  if (!current) return wrap;
  const edited = !same(resolvedTexts(current.key, current.meta)[field], originalTexts(current.meta)[field]);
  if (!edited) return wrap;
  const badge = document.createElement("span");
  badge.className = "edited";
  badge.textContent = "Edited";
  const undo = document.createElement("button");
  undo.type = "button";
  undo.className = "link-btn";
  undo.textContent = "Undo";
  undo.addEventListener("click", () => {
    undoText(field);
    afterTextChange({ rebuildFields: true });
    // These two are drawn from the chart's config, not set live, so the chart has to be redrawn.
    if (field === "rounding" || field === "map") scheduleRemount();
  });
  const suggest = document.createElement("label");
  const box = document.createElement("input");
  box.type = "checkbox";
  // Kept per view, so the suggestion carries this view's text even when other views share the indicator.
  box.checked = Boolean(state.suggest[current.key]?.[field]);
  box.addEventListener("change", () => {
    const s = (state.suggest[current.key] ||= {});
    if (box.checked) s[field] = true;
    else delete s[field];
    if (!Object.keys(s).length) delete state.suggest[current.key];
    saveState();
  });
  suggest.append(box, "Suggest for the indicator everywhere");
  suggest.title = "Claude will check which other charts use this indicator before changing it";
  wrap.append(badge, undo, suggest);
  return wrap;
}

// Push the current view's text into the chart on screen, without reloading it.
function applyTextToChart() {
  if (!loader || !current) return;
  const t = resolvedTexts(current.key, current.meta);
  try {
    const gs = loader.grapherState;
    gs.title = t.title;
    gs.subtitle = t.subtitle;
    gs.note = t.note;
  } catch (err) {
    console.warn("Live text update failed; reloading the chart", err);
    renderChart();
  }
}

function afterTextChange({ rebuildFields = false } = {}) {
  applyTextToChart();
  renderWysk();
  if (rebuildFields) renderTextEditor();
  else for (const field of [...TEXT_FIELDS, "rounding", "map"]) document.getElementById(`tools-${field}`)?.replaceChildren(...fieldTools(field).childNodes);
}

let remountTimer;
function scheduleRemount() {
  clearTimeout(remountTimer);
  remountTimer = setTimeout(() => renderChart({ keepEditor: true }), 350);
}

function autoGrow(textarea) {
  textarea.style.height = "auto";
  textarea.style.height = `${textarea.scrollHeight}px`;
}

function renderWysk() {
  const list = document.getElementById("wysk-list");
  const tools = document.getElementById("wysk-tools");
  list.innerHTML = "";
  tools.innerHTML = "";
  document.getElementById("wysk-manage").disabled = !Object.values(state.links).some((l) => l?.id);
  if (!current) { wyskEl.hidden = true; return; }
  const blocks = resolvedTexts(current.key, current.meta).descriptionKey;
  const editing = state.textMode;
  wyskEl.hidden = !editing && blocks.length === 0;
  wyskEl.classList.toggle("editing", editing);
  wyskEl.querySelector(".add-bullet")?.remove();

  // Text before the first bullet is shown as a paragraph inside the list, without a bullet.
  const itemFor = (block) => {
    const li = document.createElement("li");
    if (!isBullet(block)) li.className = "wysk-prose";
    return li;
  };
  if (!editing) {
    for (const block of blocks) {
      const li = itemFor(block);
      li.innerHTML = blockHtml(blockText(block));
      list.append(li);
    }
    return;
  }
  tools.append(fieldTools("descriptionKey"));

  const save = (next) => { setText("descriptionKey", next.filter((b) => blockText(b).trim())); afterTextChange(); };
  blocks.forEach((block, i) => {
    const li = itemFor(block);
    const view = document.createElement("span");
    view.className = "wysk-bullet";
    view.tabIndex = 0;
    view.title = "Click to edit (Shift+Enter for a new line)";
    view.innerHTML = blockHtml(blockText(block));
    const startEdit = () => {
      const ta = document.createElement("textarea");
      ta.className = "wysk-edit";
      ta.value = blockText(block);
      li.replaceChild(ta, view);
      autoGrow(ta);
      ta.focus();
      ta.addEventListener("input", () => autoGrow(ta));
      ta.addEventListener("keydown", (e) => {
        if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); ta.blur(); }
        if (e.key === "Escape") { ta.dataset.cancel = "1"; ta.blur(); }
      });
      ta.addEventListener("blur", () => {
        if (ta.dataset.cancel || ta.value === blockText(block)) return renderWysk();
        const next = [...blocks];
        next[i] = (isBullet(block) ? "- " : "") + ta.value;
        save(next);
      });
    };
    view.addEventListener("click", (e) => { if (!e.target.closest("a")) startEdit(); });
    view.addEventListener("keydown", (e) => { if (e.key === "Enter") { e.preventDefault(); startEdit(); } });
    li.dataset.index = i;
    // The opening paragraph has no handle: it has to stay first.
    const handle = isBullet(block) ? dragHandle(li, i, blocks, save) : document.createElement("span");
    if (!isBullet(block)) handle.className = "wysk-handle wysk-handle--none";
    li.append(handle, view, removeButton("Remove this point", () => save(blocks.filter((_, j) => j !== i))));
    list.append(li);
  });

  const add = document.createElement("button");
  add.type = "button";
  add.className = "btn add-bullet";
  add.textContent = "+ Add point";
  add.addEventListener("click", () => {
    setText("descriptionKey", [...blocks, "- New point."]);
    afterTextChange();
    list.querySelector(`li[data-index="${blocks.length}"] .wysk-bullet`)?.click();
    list.querySelector(".wysk-edit")?.select();
  });
  wyskEl.append(add);
}

// Where the points start: after the paragraph a view's text may open with. Joined as one block of text, a
// paragraph placed after a point would become part of that point.
function firstPointIndex(blocks) {
  const i = blocks.findIndex(isBullet);
  return i === -1 ? blocks.length : i;
}

// Points move by dragging their handle; only the handle starts a drag, so clicking the text still edits it.
let wyskDragFrom = null;
function dragHandle(li, i, blocks, save) {
  const handle = document.createElement("span");
  handle.className = "wysk-handle";
  handle.title = "Drag to move this point";
  handle.addEventListener("mousedown", () => { li.draggable = true; });
  handle.addEventListener("mouseup", () => { li.draggable = false; });
  const clearMarks = () => document.querySelectorAll("#wysk-list li").forEach((el) => el.classList.remove("drop-before", "drop-after"));
  const dropsAfter = (e) => { const r = li.getBoundingClientRect(); return e.clientY > r.top + r.height / 2; };
  li.addEventListener("dragstart", (e) => {
    wyskDragFrom = i;
    e.dataTransfer.effectAllowed = "move";
    e.dataTransfer.setData("text/plain", String(i));
    li.classList.add("dragging");
  });
  li.addEventListener("dragend", () => { wyskDragFrom = null; li.draggable = false; li.classList.remove("dragging"); clearMarks(); });
  li.addEventListener("dragover", (e) => {
    if (wyskDragFrom === null) return;
    e.preventDefault();
    clearMarks();
    li.classList.add(dropsAfter(e) ? "drop-after" : "drop-before");
  });
  li.addEventListener("drop", (e) => {
    if (wyskDragFrom === null) return;
    e.preventDefault();
    const from = wyskDragFrom;
    let to = Math.max(dropsAfter(e) ? i + 1 : i, firstPointIndex(blocks));
    if (from < to) to--;
    wyskDragFrom = null;
    if (to === from) return renderWysk();
    const next = [...blocks];
    next.splice(to, 0, ...next.splice(from, 1));
    save(next);
  });
  return handle;
}

// ---------------------------------------------------------------------------
// "Manage WYSK": the points of every view in one list, each once, to edit, delete or add across views
// ---------------------------------------------------------------------------
const wyskManager = document.getElementById("wysk-manager");
let managerViews = []; // { key, label, meta } of each linked view, in combination order
let managerMissing = 0; // linked views whose indicator couldn't be loaded

const comboName = (combo) => combo.map((c) => c.name || "Untitled").join(" · ");
const managerLabel = (key) => managerViews.find((v) => v.key === key)?.label || "";

async function openWyskManager() {
  const linked = combinations().filter((c) => state.links[comboKey(c)]?.id);
  document.getElementById("wm-body").innerHTML = `<p class="hint">Loading the text of every view…</p>`;
  if (!wyskManager.open) wyskManager.showModal();
  const loaded = await Promise.allSettled(linked.map((c) => getMeta(state.links[comboKey(c)].id)));
  managerViews = linked.flatMap((c, i) => (loaded[i].status === "fulfilled" ? [{ key: comboKey(c), label: comboName(c), meta: loaded[i].value }] : []));
  managerMissing = linked.length - managerViews.length;
  renderWyskManager();
}

// Ticks for which views a change applies to, all ticked to start with, plus one to tick or untick them all.
function scopePicker(keys, onChange) {
  const el = document.createElement("div");
  el.className = "wm-scope";
  const tick = (text, cls) => {
    const label = document.createElement("label");
    if (cls) label.className = cls;
    const input = document.createElement("input");
    input.type = "checkbox";
    input.checked = true;
    label.append(input, text);
    el.append(label);
    return input;
  };
  const everyView = keys.length === managerViews.length;
  const all = keys.length > 1 ? tick(everyView ? "All views" : "All of these", "wm-scope__all") : null;
  const boxes = keys.map((key) => tick(managerLabel(key)));
  const selected = () => keys.filter((_, i) => boxes[i].checked);
  all?.addEventListener("change", () => { boxes.forEach((b) => { b.checked = all.checked; }); onChange(selected()); });
  boxes.forEach((b) => b.addEventListener("change", () => {
    if (all) all.checked = boxes.every((x) => x.checked);
    onChange(selected());
  }));
  return { el, selected };
}

const viewsCount = (n) => (n === 1 ? "1 view" : `${n} views`);

// Rewrite the points of some views, then redraw what shows them.
function changeViews(keys, edit) {
  for (const key of keys) {
    const view = managerViews.find((v) => v.key === key);
    const blocks = resolvedTexts(key, view.meta).descriptionKey;
    const next = edit(blocks).filter((b, i, all) => blockText(b).trim() && all.indexOf(b) === i);
    setViewText(key, view.meta, "descriptionKey", next);
  }
  renderWyskManager();
  if (current) afterTextChange();
}

function managerButton(text, onClick, primary = false) {
  const btn = document.createElement("button");
  btn.type = "button";
  btn.className = primary ? "btn btn-primary" : "btn";
  btn.textContent = text;
  btn.addEventListener("click", onClick);
  return btn;
}

function managerTextarea(value, placeholder) {
  const ta = document.createElement("textarea");
  ta.className = "wm-textarea";
  ta.value = value;
  ta.placeholder = placeholder;
  ta.addEventListener("input", () => autoGrow(ta));
  requestAnimationFrame(() => autoGrow(ta));
  return ta;
}

function pointRow(block, keys) {
  const li = document.createElement("li");
  li.className = "wm-point";
  const show = () => {
    li.innerHTML = "";
    const text = document.createElement("div");
    text.className = "wm-text";
    text.innerHTML = blockHtml(blockText(block));
    const meta = document.createElement("div");
    meta.className = "wm-meta";
    const on = document.createElement("span");
    on.className = "wm-on";
    on.textContent = keys.length === managerViews.length && keys.length > 1
      ? (keys.length === 2 ? "On both views" : `On all ${keys.length} views`)
      : `On: ${keys.map(managerLabel).join(", ")}`;
    const edit = document.createElement("button");
    edit.type = "button";
    edit.className = "link-btn";
    edit.textContent = "Edit";
    edit.addEventListener("click", () => openForm("edit"));
    const del = document.createElement("button");
    del.type = "button";
    del.className = "link-btn wm-delete";
    del.textContent = "Delete";
    // A point on a single view goes at once; otherwise, pick the views first.
    del.addEventListener("click", () => (keys.length === 1 ? changeViews(keys, (blocks) => blocks.filter((b) => b !== block)) : openForm("delete")));
    meta.append(on, edit, del);
    li.append(text, meta);
  };
  const openForm = (mode) => {
    li.innerHTML = "";
    li.classList.add("wm-point--open");
    const form = document.createElement("div");
    form.className = "wm-form";
    const ta = mode === "edit" ? managerTextarea(blockText(block), "The point's text") : null;
    if (mode === "delete") {
      const text = document.createElement("div");
      text.className = "wm-text";
      text.innerHTML = blockHtml(blockText(block));
      form.append(text);
    }
    const go = managerButton("", () => {
      const keysNow = scope.selected();
      if (!keysNow.length) return;
      if (mode === "delete") return changeViews(keysNow, (blocks) => blocks.filter((b) => b !== block));
      if (!ta.value.trim()) return;
      const replacement = (isBullet(block) ? "- " : "") + ta.value;
      changeViews(keysNow, (blocks) => blocks.map((b) => (b === block ? replacement : b)));
    }, true);
    const label = (n) => `${mode === "edit" ? "Save on" : "Delete from"} ${viewsCount(n)}`;
    const scope = scopePicker(keys, (sel) => { go.textContent = label(sel.length); go.disabled = !sel.length; });
    go.textContent = label(keys.length);
    const cancel = managerButton("Cancel", () => { li.classList.remove("wm-point--open"); show(); });
    const heading = document.createElement("div");
    heading.className = "wm-scope__label";
    heading.textContent = mode === "edit" ? "Change it on:" : "Delete it from:";
    const actions = document.createElement("div");
    actions.className = "wm-actions";
    actions.append(go, cancel);
    if (ta) form.append(ta);
    if (keys.length > 1) form.append(heading, scope.el);
    form.append(actions);
    li.append(form);
    ta?.focus();
  };
  show();
  return li;
}

function addPointForm() {
  const box = document.createElement("div");
  box.className = "wm-add";
  const heading = document.createElement("h3");
  heading.textContent = "Add a point";
  const ta = managerTextarea("", "The new point");
  const label = (n) => `Add to ${viewsCount(n)}`;
  const keys = managerViews.map((v) => v.key);
  const add = managerButton(label(keys.length), () => {
    const text = ta.value.trim();
    const keysNow = scope.selected();
    if (!text || !keysNow.length) return;
    const point = `- ${text}`;
    changeViews(keysNow, (blocks) => {
      const at = where.value === "top" ? firstPointIndex(blocks) : blocks.length;
      return [...blocks.slice(0, at), point, ...blocks.slice(at)];
    });
  }, true);
  const where = document.createElement("select");
  where.className = "wm-where";
  where.setAttribute("aria-label", "Where the new point goes");
  where.innerHTML = `<option value="bottom">At the bottom of the list</option><option value="top">At the top of the list</option>`;
  const scope = scopePicker(keys, (sel) => { add.textContent = label(sel.length); add.disabled = !sel.length; });
  box.append(heading, ta);
  if (keys.length > 1) {
    const on = document.createElement("div");
    on.className = "wm-scope__label";
    on.textContent = "Add it to:";
    box.append(on, scope.el);
  }
  const actions = document.createElement("div");
  actions.className = "wm-actions";
  actions.append(add, where);
  box.append(actions);
  return box;
}

function renderWyskManager() {
  const body = document.getElementById("wm-body");
  body.innerHTML = "";
  if (!managerViews.length) {
    body.innerHTML = managerMissing
      ? `<p class="hint">The text of the linked views couldn't be loaded. Check your connection, then open this again.</p>`
      : `<p class="hint">Link an indicator to a combination first.</p>`;
    return;
  }
  // Each point once, with the views that have it, in the order they first appear.
  const points = new Map();
  for (const view of managerViews) {
    for (const block of resolvedTexts(view.key, view.meta).descriptionKey) {
      if (!points.has(block)) points.set(block, []);
      if (!points.get(block).includes(view.key)) points.get(block).push(view.key);
    }
  }
  if (managerMissing) {
    body.insertAdjacentHTML("beforeend", `<p class="hint">${viewsCount(managerMissing)} couldn't be loaded and ${managerMissing === 1 ? "isn't" : "aren't"} listed.</p>`);
  }
  if (points.size) {
    const list = document.createElement("ul");
    list.className = "wm-points";
    for (const [block, keys] of points) list.append(pointRow(block, keys));
    body.append(list);
  } else body.insertAdjacentHTML("beforeend", `<p class="hint">No view has any points yet.</p>`);
  body.append(addPointForm());
}

document.getElementById("wysk-manage").addEventListener("click", openWyskManager);
document.getElementById("wm-close").addEventListener("click", () => wyskManager.close());
// A click on the dimmed backdrop lands on the dialog itself, outside its content box.
wyskManager.addEventListener("click", (e) => { if (e.target === wyskManager) wyskManager.close(); });

function renderTextEditor() {
  const toggle = document.getElementById("text-editor-toggle");
  const editor = document.getElementById("text-editor");
  const fields = document.getElementById("text-editor-fields");
  const context = document.getElementById("text-editor-context");
  toggle.textContent = state.textMode ? "Done editing" : "✎ Edit FAUST and WYSK";
  toggle.classList.toggle("btn-primary", state.textMode);
  editor.classList.toggle("open", state.textMode);
  fields.hidden = !state.textMode;
  context.innerHTML = state.textMode && current ? `Editing the text of <strong>${esc(current.label)}</strong>` : "";
  fields.innerHTML = "";
  if (!state.textMode) return;
  if (!current) {
    fields.innerHTML = `<p class="hint">Link an indicator to this combination to edit its text.</p>`;
    return;
  }
  const texts = resolvedTexts(current.key, current.meta);
  for (const field of TEXT_FIELDS) {
    const block = document.createElement("div");
    block.className = "text-field" + (field === "title" ? " text-field--wide" : "");
    const head = document.createElement("div");
    head.className = "text-field__head";
    const label = document.createElement("label");
    label.className = "text-field__label";
    label.htmlFor = `text-${field}`;
    label.textContent = FIELD_LABELS[field];
    const tools = fieldTools(field);
    tools.id = `tools-${field}`;
    head.append(label, tools);
    const input = document.createElement(field === "title" ? "input" : "textarea");
    input.id = `text-${field}`;
    if (field === "title") input.type = "text";
    else input.rows = 2;
    input.value = texts[field];
    input.placeholder = field === "note" ? "No footnote" : "";
    const hints = document.createElement("ul");
    hints.className = "hints";
    const showHints = () => { hints.innerHTML = textHints(field, input.value).map((h) => `<li>${esc(h)}</li>`).join(""); };
    showHints();
    input.addEventListener("input", () => {
      setText(field, input.value);
      afterTextChange();
      showHints();
    });
    input.addEventListener("change", () => renderCombos());
    block.append(head, input, hints);
    fields.append(block);
  }
  fields.append(roundingField(texts.rounding));
  if (state.hasMapTab) fields.append(mapField());
}

function fieldBlock(field, wide = false) {
  const block = document.createElement("div");
  block.className = "text-field" + (wide ? " text-field--wide" : "");
  const head = document.createElement("div");
  head.className = "text-field__head";
  const label = document.createElement("span");
  label.className = "text-field__label";
  label.style.fontSize = "13px";
  label.style.fontWeight = "700";
  label.textContent = FIELD_LABELS[field];
  const tools = fieldTools(field);
  tools.id = `tools-${field}`;
  head.append(label, tools);
  block.append(head);
  return block;
}

function roundingField(rounding) {
  const block = fieldBlock("rounding");
  const row = document.createElement("div");
  row.className = "field-row";
  const digits = document.createElement("input");
  digits.type = "number";
  digits.min = "0";
  digits.max = "10";
  digits.id = "rounding-digits";
  digits.value = String(rounding.digits);
  const mode = document.createElement("select");
  mode.id = "rounding-mode";
  mode.innerHTML = `<option value="decimalPlaces">decimal places</option><option value="significantFigures">significant figures</option>`;
  mode.value = rounding.mode;
  // Grapher needs at least one significant figure; decimal places can be zero.
  const lowest = () => (mode.value === "significantFigures" ? 1 : 0);
  digits.min = String(lowest());
  const update = () => {
    digits.min = String(lowest());
    const n = Math.max(lowest(), Math.min(10, Math.round(Number(digits.value) || 0)));
    setText("rounding", { mode: mode.value, digits: n });
    afterTextChange();
    scheduleRemount();
  };
  digits.addEventListener("input", update);
  digits.addEventListener("change", () => { digits.value = String(resolvedTexts(current.key, current.meta).rounding.digits); });
  mode.addEventListener("change", () => { update(); digits.value = String(resolvedTexts(current.key, current.meta).rounding.digits); });
  row.append("Round to", digits, mode);
  block.append(row);
  return block;
}

function mapField() {
  const block = fieldBlock("map", true);
  if (state.defaultTab !== "map") {
    const hint = document.createElement("p");
    hint.className = "hint";
    hint.style.margin = "0";
    hint.textContent = "Select the Map tab on the chart to edit its colours.";
    block.append(hint);
    return block;
  }
  const original = originalMap(current.meta);
  const map = resolvedTexts(current.key, current.meta).map;

  const palette = document.createElement("select");
  palette.id = "map-palette";
  // "No palette of its own" means the indicator's palette when it has one, Grapher's default otherwise.
  const inherited = original.baseColorScheme ? `Indicator's palette (${esc(original.baseColorScheme)})` : "Default palette";
  palette.innerHTML = `<option value="">${inherited}</option>` + COLOR_SCHEMES.map((c) => `<option>${esc(c)}</option>`).join("");
  palette.value = map.baseColorScheme || "";

  const strategy = document.createElement("select");
  strategy.id = "map-strategy";
  const options = original.binningStrategy === "manual" ? [["manual", "Manual bins from the indicator"], ...BINNING_STRATEGIES] : BINNING_STRATEGIES;
  strategy.innerHTML = options.map(([value, name]) => `<option value="${value}">${esc(name)}</option>`).join("");
  strategy.value = map.binningStrategy || "auto";

  // Keep only what differs from the indicator's own map settings, so an untouched map exports nothing.
  const update = () => {
    const next = { ...original };
    if (palette.value) next.baseColorScheme = palette.value;
    else delete next.baseColorScheme;
    if (strategy.value === (original.binningStrategy || "auto")) next.binningStrategy = original.binningStrategy;
    else next.binningStrategy = strategy.value;
    if (next.binningStrategy === undefined) delete next.binningStrategy;
    setText("map", next);
    afterTextChange();
    scheduleRemount();
  };
  palette.addEventListener("change", update);
  strategy.addEventListener("change", update);

  const row = document.createElement("div");
  row.className = "field-row";
  row.append("Palette", palette, "Bins", strategy);
  const note = document.createElement("p");
  note.className = "hint";
  note.style.margin = "8px 0 0";
  note.textContent = "If you want to set more specific bins, talk to Claude after copy-pasting your MDim setup.";
  block.append(row, note);
  return block;
}

document.getElementById("text-editor-toggle").addEventListener("click", () => {
  state.textMode = !state.textMode;
  saveState();
  renderTextEditor();
  renderWysk();
});


async function renderChart({ keepEditor = false } = {}) {
  const token = ++renderToken;
  const combo = currentCombo();
  const link = combo ? state.links[comboKey(combo)] : null;

  const clearCurrent = () => { current = null; renderWysk(); renderTextEditor(); };
  if (!combo) { clearCurrent(); return showNotice("Every dropdown needs at least one option."); }
  if (!link?.id) {
    clearCurrent();
    const nothingLinked = !Object.values(state.links).some((l) => l?.id);
    return showNotice(nothingLinked
      ? "Start by linking an indicator: paste its catalog path or ID in the Combinations table in the panel below."
      : "No indicator linked to this combination yet. Link one in the panel below.");
  }

  let indicator;
  try {
    indicator = await getIndicator(link.id);
  } catch (err) {
    if (token === renderToken) { clearCurrent(); showNotice(`Couldn't load indicator ${link.id}: ${esc(err.message)}`); }
    return;
  }
  if (token !== renderToken) return;
  const key = comboKey(combo);
  current = { key, meta: indicator.meta, label: combo.map((c, i) => `${state.dims[i].name}: ${c.name}`).join(" · ") || "this chart" };
  renderWysk();
  if (!keepEditor) renderTextEditor();

  if (!grapher) {
    return showNotice("This chart needs Tailscale. The Grapher package is served on OWID's private network: connect to Tailscale, then reload the page.");
  }
  disposeChart();
  chartEl.innerHTML = "";
  try {
    const { GrapherLoader, OwidTable } = grapher;
    // Grapher's fromTable takes rounding from the column, not from the chart's dimension, so the view's rounding
    // goes on the column here. The copied setup still puts it on the view's indicator, as ETL expects.
    const column = columnDef(indicator.meta);
    column.display = { ...(column.display || {}), ...roundingDisplay(resolvedTexts(comboKey(combo), indicator.meta).rounding) };
    const table = new OwidTable(indicator.csv, [column]);
    const texts = resolvedTexts(key, indicator.meta);
    const base = viewConfig(indicator.meta);
    const config = {
      ...base,
      ...sharedConfig(),
      title: texts.title,
      subtitle: texts.subtitle,
      note: texts.note,
      yAxis: { ...(base.yAxis || {}), ...sharedConfig().yAxis },
      hideAnnotationFieldsInTitle: { ...(base.hideAnnotationFieldsInTitle || {}), ...sharedConfig().hideAnnotationFieldsInTitle },
      map: { ...(base.map || {}), colorScale: { ...(base.map?.colorScale || {}), ...colorScaleFrom(texts.map) } },
      dimensions: [{ property: "y", slug: `ind_${indicator.meta.id}`, display: roundingDisplay(texts.rounding) }],
    };
    loaderReady = false;
    const mounted = GrapherLoader.fromTable({ config, data: table }).mount(chartEl);
    loader = mounted;
    // fromTable's data is already in memory, so `ready` resolves at once and never rejects.
    mounted.ready.then(() => { if (loader === mounted) loaderReady = true; });
  } catch (err) {
    // Building the chart can throw (the table, Grapher's state, mounting): say so instead of leaving it blank.
    console.error("Could not draw the chart", err);
    showNotice(`The chart couldn't be drawn: ${esc(err.message || err)}. Check the indicator linked to this combination.`);
  }
}

// ---------------------------------------------------------------------------
// Dropdowns above the chart
// ---------------------------------------------------------------------------
let openMenu = null;

function closeMenu({ focusButton = false } = {}) {
  if (!openMenu) return;
  const { menu, button } = openMenu;
  menu.hidden = true;
  button.setAttribute("aria-expanded", "false");
  if (focusButton) button.focus();
  openMenu = null;
}

document.addEventListener("pointerdown", (e) => {
  if (openMenu && !openMenu.wrap.contains(e.target)) closeMenu();
});
document.addEventListener("keydown", (e) => {
  if (e.key === "Escape" && openMenu) closeMenu({ focusButton: true });
});

function selectChoice(dim, choiceId) {
  state.selected[dim.id] = choiceId;
  saveState();
  closeMenu();
  renderBar();
  renderCombos();
  renderChart();
}

function renderHeader() {
  const title = state.title.trim() || "Untitled MDim";
  document.getElementById("mdim-title").textContent = title;
  document.getElementById("mdim-title-variant").textContent = state.titleVariant.trim();
  document.title = `${title} · MDim simulator`;
}

function renderTopicTags() {
  const block = document.getElementById("topic-tags-block");
  const box = document.getElementById("topic-tags");
  box.innerHTML = "";
  block.hidden = state.topicTags.length === 0;
  for (const tag of state.topicTags) {
    const slug = TAG_SLUGS[tag];
    const el = document.createElement(slug ? "a" : "span");
    el.className = "topic-tag";
    el.textContent = tag;
    if (slug) { el.href = `https://ourworldindata.org/${slug}`; el.target = "_blank"; el.rel = "noopener"; }
    box.append(el);
  }
}

function renderTagsEditor() {
  const box = document.getElementById("tags-editor");
  box.innerHTML = "";
  const chips = document.createElement("div");
  chips.className = "tag-chips";
  state.topicTags.forEach((tag, i) => {
    const chip = document.createElement("span");
    chip.className = "tag-chip" + (i === 0 ? " tag-chip--main" : "");
    chip.title = i === 0 ? "Main topic" : "";
    chip.append(tag, removeButton(`Remove the ${tag} tag`, () => {
      state.topicTags.splice(i, 1);
      saveState();
      renderTagsEditor();
      renderTopicTags();
      renderChartSettings();
      renderChart();
    }));
    chips.append(chip);
  });
  if (state.topicTags.length) box.append(chips);

  const picker = document.createElement("select");
  picker.className = "tag-picker";
  picker.setAttribute("aria-label", "Add a topic tag");
  picker.innerHTML = `<option value="">${state.topicTags.length ? "Add another topic tag…" : "Add a topic tag (the first one is the main topic)…"}</option>` +
    TOPIC_TAGS.filter((tag) => !state.topicTags.includes(tag)).map((tag) => `<option>${esc(tag)}</option>`).join("");
  picker.addEventListener("change", () => {
    if (!picker.value) return;
    state.topicTags.push(picker.value);
    saveState();
    renderTagsEditor();
    renderTopicTags();
    renderChartSettings();
    renderChart();
  });
  box.append(picker);
}

function renderPageFields() {
  const box = document.getElementById("page-fields");
  box.innerHTML = "";
  const titleInput = textInput(state.title, "MDim title, e.g. Internet users", (v) => { state.title = v; saveState(); renderHeader(); });
  titleInput.style.fontWeight = "700";
  // Optional, like an option's description: a small link until someone asks for the field.
  const variantInput = textInput(state.titleVariant, "Shown after the title, e.g. the source", (v) => { state.titleVariant = v; saveState(); renderHeader(); });
  variantInput.classList.add("choice-description");
  const addVariant = document.createElement("button");
  addVariant.type = "button";
  addVariant.className = "link-btn";
  addVariant.textContent = "+ Add title variant";
  const showVariant = (show) => { variantInput.hidden = !show; addVariant.hidden = show; };
  showVariant(Boolean(state.titleVariant.trim()));
  addVariant.addEventListener("click", () => { showVariant(true); variantInput.focus(); });
  variantInput.addEventListener("blur", () => { if (!variantInput.value.trim()) showVariant(false); });
  box.append(titleInput, variantInput, addVariant);
}

// The MDim's web address, as ETL's schema allows it: lowercase words joined by hyphens.
const SLUG_PATTERN = /^[a-z0-9]+(-[a-z0-9]+)*$/;
const validSlug = () => SLUG_PATTERN.test(state.slug || "");

function renderSlugField() {
  const box = document.getElementById("slug-field");
  box.innerHTML = "";
  const row = document.createElement("label");
  row.className = "slug-row";
  const prefix = document.createElement("span");
  prefix.className = "slug-row__prefix";
  prefix.textContent = "ourworldindata.org/grapher/";
  const input = textInput(state.slug || "", "e.g. internet-users", () => {});
  input.setAttribute("aria-label", "Web address");
  input.spellcheck = false;
  const note = document.createElement("p");
  note.className = "settings-note slug-note";
  const show = () => {
    const bad = Boolean(state.slug) && !validSlug();
    row.classList.toggle("invalid", bad);
    note.classList.toggle("invalid", bad);
    note.textContent = bad
      ? "Use lowercase letters, numbers and hyphens only, e.g. internet-users."
      : state.slug ? "Claude checks that it's free before building the MDim." : "Leave it empty and Claude will ask.";
  };
  input.addEventListener("input", () => {
    state.slug = input.value.trim();
    if (!state.slug) delete state.slug;
    saveState();
    show();
  });
  show();
  row.append(prefix, input);
  box.append(row, note);
}

function renderBar() {
  closeMenu();
  const bar = document.getElementById("mdim-bar");
  bar.innerHTML = "";
  const dims = state.dims.filter((dim) => dim.choices.length);
  if (!dims.length) return;
  const heading = document.createElement("span");
  heading.className = "mdim-bar__label";
  heading.textContent = "Configure the data";
  const group = document.createElement("div");
  group.className = "mdim-bar__dropdowns";
  bar.append(heading, group);

  for (const dim of dims) {
    const dimName = dim.name || "Untitled dropdown";
    const current = dim.choices.find((c) => c.id === state.selected[dim.id]) || dim.choices[0];
    const wrap = document.createElement("div");
    wrap.className = "md-dropdown-wrap";

    const button = document.createElement("button");
    button.className = "md-dropdown";
    button.type = "button";
    button.setAttribute("aria-haspopup", "listbox");
    button.setAttribute("aria-expanded", "false");
    button.title = `${dimName}: ${current.name || "Untitled option"}`;
    button.innerHTML = `<span class="md-dropdown__name">${esc(dimName)}</span><span class="md-dropdown__value">${esc(current.name || "Untitled option")}</span><span class="md-dropdown__caret"></span>`;

    const menu = document.createElement("div");
    menu.className = "md-menu";
    menu.hidden = true;
    menu.innerHTML = `<div class="md-menu__head"><span class="md-menu__title">${esc(dimName)}</span><button type="button" class="md-menu__close" aria-label="Close">×</button></div>`;
    if (dim.description?.trim()) {
      const description = document.createElement("p");
      description.className = "md-menu__dimension-description";
      description.textContent = dim.description.trim();
      menu.append(description);
    }
    // Options sharing a group sit under its heading; groups appear in the order of their first option.
    const groups = [];
    for (const choice of dim.choices) {
      const name = choice.group?.trim() || "";
      let group = groups.find((g) => g.name === name);
      if (!group) groups.push((group = { name, choices: [] }));
      group.choices.push(choice);
    }
    const items = [];
    for (const group of groups) {
      const groupEl = document.createElement("div");
      groupEl.className = "md-menu__group";
      if (group.name) {
        const groupLabel = document.createElement("div");
        groupLabel.className = "md-menu__group-label";
        groupLabel.textContent = group.name;
        groupEl.append(groupLabel);
      }
      const list = document.createElement("ul");
      list.className = "md-menu__list";
      list.setAttribute("role", "listbox");
      list.setAttribute("aria-label", group.name || dimName);
      for (const choice of group.choices) {
        const li = document.createElement("li");
        li.className = "md-option";
        li.setAttribute("role", "option");
        li.tabIndex = -1;
        li.dataset.choice = choice.id;
        li.setAttribute("aria-selected", String(choice.id === current.id));
        li.innerHTML = `<span class="md-option__radio" aria-hidden="true"></span><span><span class="md-option__name">${esc(choice.name || "Untitled option")}</span>${choice.description ? `<span class="md-option__description">${esc(choice.description)}</span>` : ""}</span>`;
        li.addEventListener("click", () => selectChoice(dim, choice.id));
        li.addEventListener("keydown", (e) => {
          const i = items.indexOf(li);
          if (e.key === "ArrowDown") { e.preventDefault(); items[Math.min(i + 1, items.length - 1)].focus(); }
          else if (e.key === "ArrowUp") { e.preventDefault(); items[Math.max(i - 1, 0)].focus(); }
          else if (e.key === "Enter" || e.key === " ") { e.preventDefault(); selectChoice(dim, choice.id); }
        });
        list.append(li);
        items.push(li);
      }
      groupEl.append(list);
      menu.append(groupEl);
    }
    menu.querySelector(".md-menu__close").addEventListener("click", () => closeMenu({ focusButton: true }));

    button.addEventListener("click", () => {
      const wasOpen = openMenu?.menu === menu;
      closeMenu();
      if (wasOpen) return;
      menu.hidden = false;
      button.setAttribute("aria-expanded", "true");
      openMenu = { menu, button, wrap };
      (items.find((li) => li.getAttribute("aria-selected") === "true") || items[0]).focus();
    });

    wrap.append(button, menu);
    group.append(wrap);
  }
}

// ---------------------------------------------------------------------------
// Panel: dropdown editor
// ---------------------------------------------------------------------------
// An optional field: a small "+ Add …" link until someone asks for it, folding back when left empty.
function optionalField(value, placeholder, linkText, onInput) {
  const input = textInput(value || "", placeholder, onInput);
  input.classList.add("choice-description");
  const link = document.createElement("button");
  link.type = "button";
  link.className = "link-btn";
  link.textContent = linkText;
  const show = (on) => { input.hidden = !on; link.hidden = on; };
  show(Boolean((value || "").trim()));
  link.addEventListener("click", () => { show(true); input.focus(); });
  input.addEventListener("blur", () => { if (!input.value.trim()) show(false); });
  return { input, link };
}

function textInput(value, placeholder, onInput) {
  const input = document.createElement("input");
  input.type = "text";
  input.value = value;
  input.placeholder = placeholder;
  input.addEventListener("input", () => onInput(input.value));
  return input;
}

function removeButton(label, onClick) {
  const btn = document.createElement("button");
  btn.className = "btn-icon";
  btn.title = label;
  btn.setAttribute("aria-label", label);
  btn.textContent = "×";
  btn.addEventListener("click", onClick);
  return btn;
}

// A dropdown's groups: the named ones, plus any an option uses. Options are kept in box order (no group first,
// then each group), so the editor, the dropdown menu and the copied setup all list them the same way.
function syncGroups(dim) {
  dim.groups = [...(dim.groups || [])];
  for (const c of dim.choices) {
    const g = c.group?.trim();
    if (g && !dim.groups.includes(g)) dim.groups.push(g);
  }
  const rank = (c) => (c.group?.trim() ? dim.groups.indexOf(c.group.trim()) : -1);
  dim.choices = dim.choices.map((c, i) => [c, i]).sort((a, b) => rank(a[0]) - rank(b[0]) || a[1] - b[1]).map(([c]) => c);
}

function structureChanged() {
  for (const dim of state.dims) syncGroups(dim);
  pruneLinks();
  normalizeSelection();
  saveState();
  renderDims();
  refresh();
}

function namesChanged() {
  saveState();
  renderBar();
  renderCombos();
  renderSummary();
}

function renderDims() {
  const list = document.getElementById("dims-list");
  list.innerHTML = "";
  state.dims.forEach((dim, dimIndex) => {
    const card = document.createElement("div");
    card.className = "dim-card";

    const head = document.createElement("div");
    head.className = "dim-head";
    // An MDim needs at least one dropdown, so the last one can't be removed.
    const remove = removeButton(`Remove the ${dim.name || "untitled"} dropdown`, () => {
      state.dims.splice(dimIndex, 1);
      rekeyLinks((parts) => parts.filter((_, i) => i !== dimIndex).join("|"));
      structureChanged();
    });
    if (state.dims.length === 1) {
      remove.disabled = true;
      remove.title = "An MDim needs at least one dropdown";
    }
    head.append(textInput(dim.name, "Dropdown name, e.g. Metric", (v) => { dim.name = v; namesChanged(); }), remove);
    card.append(head);
    const dimDescription = optionalField(dim.description, "Shown at the top of the dropdown menu", "+ Add description", (v) => {
      dim.description = v;
      saveState();
      renderBar();
    });
    const dimExtras = document.createElement("div");
    dimExtras.className = "choice-fields";
    dimExtras.style.margin = "-4px 0 10px";
    dimExtras.append(dimDescription.input, dimDescription.link);
    card.append(dimExtras);

    // Groups are outlined boxes with a heading; options outside every box have no group. Empty groups live in
    // dim.groups until an option is moved in.
    syncGroups(dim);
    const addGroup = document.createElement("button");
    addGroup.type = "button";
    addGroup.className = "link-btn add-group";
    addGroup.textContent = "+ Add group";
    addGroup.addEventListener("click", () => {
      let n = dim.groups.length + 1;
      while (dim.groups.includes(`Group ${n}`)) n++;
      dim.groups.push(`Group ${n}`);
      structureChanged();
      const heads = document.querySelectorAll(`#dims-list .dim-card:nth-child(${dimIndex + 1}) .group-head input`);
      heads[heads.length - 1]?.select();
    });
    card.append(addGroup);

    const optionRow = (choice) => {
      const row = document.createElement("div");
      row.className = "choice-row";
      row.dataset.choice = choice.id;
      const fields = document.createElement("div");
      fields.className = "choice-fields";
      const nameInput = textInput(choice.name, "Option name", (v) => { choice.name = v; namesChanged(); });
      nameInput.classList.add("choice-name");
      const description = optionalField(choice.description, "Shown under the option in the dropdown", "+ Add description", (v) => {
        choice.description = v;
        saveState();
        renderBar();
      });
      fields.append(nameInput, description.input, description.link);
      row.append(fields);
      if (dim.groups.length) {
        const picker = document.createElement("select");
        picker.className = "group-picker";
        picker.setAttribute("aria-label", "Group");
        picker.innerHTML = `<option value="">No group</option>` + dim.groups.map((g) => `<option>${esc(g)}</option>`).join("");
        picker.value = choice.group?.trim() || "";
        picker.addEventListener("change", () => {
          if (picker.value) choice.group = picker.value;
          else delete choice.group;
          structureChanged();
        });
        row.append(picker);
      }
      // A dropdown with no options has no combinations, which would drop every link, so its last option stays.
      const removeOption = removeButton(`Remove the ${choice.name || "untitled"} option`, () => {
        dim.choices.splice(dim.choices.indexOf(choice), 1);
        structureChanged();
      });
      if (dim.choices.length === 1) {
        removeOption.disabled = true;
        removeOption.title = "A dropdown needs at least one option";
      }
      row.append(removeOption);
      return row;
    };
    // `currentGroup` is read on click: a group can be renamed without the editor being redrawn.
    const addOption = (currentGroup) => {
      const button = document.createElement("button");
      button.className = "btn";
      button.textContent = "+ Add option";
      button.addEventListener("click", () => {
        const group = currentGroup();
        const choice = { id: newId(), name: `Option ${dim.choices.length + 1}`, ...(group ? { group } : {}) };
        dim.choices.push(choice);
        structureChanged();
        document.querySelector(`#dims-list [data-choice="${choice.id}"] input.choice-name`)?.select();
      });
      return button;
    };

    for (const choice of dim.choices.filter((c) => !c.group?.trim())) card.append(optionRow(choice));
    card.append(addOption(() => null));

    dim.groups.forEach((groupName, groupIndex) => {
      const box = document.createElement("div");
      box.className = "group-box";
      const head = document.createElement("div");
      head.className = "group-head";
      const label = document.createElement("span");
      label.className = "group-head__label";
      label.textContent = "Group";
      const nameInput = textInput(groupName, "Heading in the dropdown, e.g. Fossil fuels", (v) => {
        const old = dim.groups[groupIndex];
        dim.groups[groupIndex] = v;
        for (const c of dim.choices) if (c.group === old) c.group = v;
        // Keep this dropdown's "Group" pickers in step with the new name.
        for (const option of card.querySelectorAll(".group-picker option")) {
          if (option.value === old) { option.value = v; option.textContent = v; }
        }
        namesChanged();
      });
      nameInput.addEventListener("blur", () => { if (!nameInput.value.trim()) { nameInput.value = `Group ${groupIndex + 1}`; nameInput.dispatchEvent(new Event("input")); } });
      head.append(label, nameInput, removeButton(`Remove the ${groupName} group (its options stay, without a group)`, () => {
        const name = dim.groups[groupIndex];
        for (const c of dim.choices) if (c.group === name) delete c.group;
        dim.groups.splice(groupIndex, 1);
        structureChanged();
      }));
      box.append(head);
      for (const choice of dim.choices.filter((c) => c.group?.trim() === groupName)) box.append(optionRow(choice));
      box.append(addOption(() => dim.groups[groupIndex]));
      card.append(box);
    });

    list.append(card);
  });
}

document.getElementById("add-dim").addEventListener("click", () => {
  const choice = { id: newId(), name: "Option 1" };
  state.dims.push({ id: newId(), name: `Dropdown ${state.dims.length + 1}`, choices: [choice] });
  // Existing links move to the new dropdown's first option.
  rekeyLinks((parts) => [...parts, choice.id].join("|"));
  structureChanged();
  const cards = document.querySelectorAll("#dims-list .dim-card .dim-head input");
  cards[cards.length - 1]?.select();
});

// ---------------------------------------------------------------------------
// Panel: combinations and their indicators
// ---------------------------------------------------------------------------
const statusByKey = new Map();
const pendingByKey = new Map();

function statusHtml(status) {
  if (!status) return `<div class="status empty">Not linked yet</div>`;
  if (status.kind === "loading") return `<div class="status loading">Looking it up…</div>`;
  if (status.kind === "error") return `<div class="status error">${esc(status.message)}</div>`;
  const m = status.meta;
  const warning = m.chartSettingsMissing ? `<span class="status-warning">Its own chart settings couldn't be loaded, so the preview may differ.</span>` : "";
  return `<div class="status ok"><strong>${esc(m.display?.name || m.presentation?.titlePublic || m.name)}</strong><code>${esc(m.catalogPath)}</code>${warning}</div>`;
}

async function linkIndicator(key, raw, statusCell) {
  const token = (pendingByKey.get(key) || 0) + 1;
  pendingByKey.set(key, token);
  const setStatus = (status) => {
    if (pendingByKey.get(key) !== token) return;
    status ? statusByKey.set(key, status) : statusByKey.delete(key);
    if (statusCell.isConnected) statusCell.innerHTML = statusHtml(status);
  };

  // A suggestion is for the indicator it was made on, so it goes when the view's indicator changes.
  const dropSuggestions = () => { delete state.suggest[key]; };

  if (!raw.trim()) {
    delete state.links[key];
    dropSuggestions();
    saveState();
    setStatus(null);
    renderSummary();
    if (key === comboKey(currentCombo() || [])) renderChart();
    return;
  }
  setStatus({ kind: "loading" });
  try {
    const id = await resolveInput(raw);
    const meta = await getMeta(id);
    if (pendingByKey.get(key) !== token) return;
    if (state.links[key]?.id !== id) dropSuggestions();
    state.links[key] = { input: raw.trim(), id };
    saveState();
    setStatus({ kind: "ok", meta });
  } catch (err) {
    if (pendingByKey.get(key) !== token) return;
    // A network or API failure says nothing about the indicator already linked here, so it stays linked.
    const transient = err.transient || err.status || err instanceof TypeError;
    if (transient && state.links[key]?.id) {
      setStatus({ kind: "error", message: `${err.message} The previous indicator is still linked.` });
    } else {
      delete state.links[key];
      dropSuggestions();
      saveState();
      setStatus({ kind: "error", message: err.message });
    }
  }
  renderSummary();
  if (key === comboKey(currentCombo() || [])) renderChart();
}

function defaultComboKey() {
  const keys = combinations().map(comboKey);
  return keys.includes(state.defaultKey) ? state.defaultKey : keys[0];
}

function renderCombos() {
  const wrap = document.getElementById("combos");
  const combos = combinations();
  if (!combos.length) {
    wrap.innerHTML = `<p class="hint">Every dropdown needs at least one option.</p>`;
    return;
  }
  const current = currentCombo();
  const currentKey = current ? comboKey(current) : null;

  const table = document.createElement("table");
  table.className = "combos";
  const headCells = state.dims.map((d) => `<th>${esc(d.name || "Untitled dropdown")}</th>`).join("");
  table.innerHTML = `<thead><tr><th>Opens first</th>${headCells}<th>Indicator (catalog path or ID)</th><th>Linked to</th><th>Edited</th></tr></thead>`;
  const firstKey = defaultComboKey();
  const tbody = document.createElement("tbody");

  for (const combo of combos) {
    const key = comboKey(combo);
    const link = state.links[key];
    const tr = document.createElement("tr");
    if (key === currentKey) tr.className = "current";
    tr.innerHTML = combo.map((c) => `<td class="choice">${esc(c.name || "Untitled option")}</td>`).join("");
    const defaultCell = document.createElement("td");
    defaultCell.className = "default-col";
    const opensFirst = document.createElement("input");
    opensFirst.type = "checkbox";
    opensFirst.checked = key === firstKey;
    opensFirst.title = "The view readers see when they arrive";
    opensFirst.setAttribute("aria-label", "Opens first");
    opensFirst.addEventListener("change", () => {
      state.defaultKey = key; // exactly one view opens first: ticking another one moves it there
      saveState();
      renderCombos();
    });
    defaultCell.append(opensFirst);
    tr.prepend(defaultCell);

    const inputCell = document.createElement("td");
    inputCell.className = "indicator";
    const statusCell = document.createElement("td");
    statusCell.innerHTML = statusHtml(statusByKey.get(key) || (link ? { kind: "loading" } : null));

    let timer;
    const input = textInput(link?.input || "", `e.g. ${EXAMPLE_PATH}`, (v) => {
      clearTimeout(timer);
      timer = setTimeout(() => linkIndicator(key, v, statusCell), 600);
    });
    input.addEventListener("keydown", (e) => { if (e.key === "Enter") { clearTimeout(timer); linkIndicator(key, input.value, statusCell); } });
    inputCell.append(input);
    const textCell = document.createElement("td");
    textCell.className = "text-col";
    if (state.textEdits.views[key]) textCell.innerHTML = `<span class="text-tag" title="${esc(Object.keys(state.textEdits.views[key]).filter((f) => FIELD_LABELS[f]).map((f) => FIELD_LABELS[f]).join(", "))}">Edited</span>`;
    tr.append(inputCell, statusCell, textCell);

    // Clicking a row shows that combination in the chart.
    tr.addEventListener("click", (e) => {
      if (e.target.closest("input")) return;
      combo.forEach((choice, i) => { state.selected[state.dims[i].id] = choice.id; });
      saveState();
      renderBar();
      renderCombos();
      renderChart();
    });
    tbody.append(tr);

    if (link && !statusByKey.has(key)) {
      getMeta(link.id)
        .then((meta) => { statusByKey.set(key, { kind: "ok", meta }); })
        .catch((err) => { statusByKey.set(key, { kind: "error", message: err.message }); })
        .finally(() => { if (statusCell.isConnected) statusCell.innerHTML = statusHtml(statusByKey.get(key)); });
    }
  }
  table.append(tbody);
  wrap.innerHTML = "";
  wrap.append(table);
}

function renderSummary() {
  const combos = combinations();
  const linked = combos.filter((c) => state.links[comboKey(c)]?.id).length;
  const n = state.dims.length;
  document.getElementById("panel-summary").textContent =
    `${n} dropdown${n === 1 ? "" : "s"} · ${combos.length} combination${combos.length === 1 ? "" : "s"} · ${linked} linked`;
}

// ---------------------------------------------------------------------------
// Export: the setup in the shape of an ETL chart config
// ---------------------------------------------------------------------------
function slugify(text, fallback) {
  const slug = String(text || "").toLowerCase().normalize("NFKD").replace(/[̀-ͯ]/g, "").replace(/[^a-z0-9]+/g, "_").replace(/^_+|_+$/g, "");
  return slug || fallback;
}

function uniqueSlugs(items, fallbackPrefix) {
  const seen = new Map();
  return items.map((item, i) => {
    let slug = slugify(item.name, `${fallbackPrefix}_${i + 1}`);
    const count = (seen.get(slug) || 0) + 1;
    seen.set(slug, count);
    return count > 1 ? `${slug}_${count}` : slug;
  });
}

// Nested objects and lists of scalars as YAML lines, scalars written as JSON (valid YAML).
function yamlLines(value, indent) {
  const pad = " ".repeat(indent);
  const out = [];
  for (const [key, v] of Object.entries(value)) {
    if (v === undefined) continue;
    if (Array.isArray(v)) {
      if (!v.length) out.push(`${pad}${key}: []`);
      else out.push(`${pad}${key}:`, ...v.map((x) => `${pad}  - ${JSON.stringify(x)}`));
    } else if (v && typeof v === "object") out.push(`${pad}${key}:`, ...yamlLines(v, indent + 2));
    else out.push(`${pad}${key}: ${JSON.stringify(v)}`);
  }
  return out;
}

async function setupYaml() {
  const dimSlugs = uniqueSlugs(state.dims, "dropdown");
  const choiceSlugs = new Map();
  state.dims.forEach((dim) => uniqueSlugs(dim.choices, "option").forEach((slug, i) => choiceSlugs.set(dim.choices[i].id, slug)));

  const lines = [
    "# MDim setup, made with the MDim simulator.",
    "# Paste this to Claude to turn it into a viz://chart step.",
    "# Claude: whoever made this may not work in the ETL. Do the technical work yourself, ask only plain-language",
    "# questions, and follow the hand-off in the /sketch-mdim skill (step 5).",
    `grapher_schema: ${JSON.stringify(GRAPHER_SCHEMA_VERSION)}`,
    ...(state.slug
      ? [validSlug() ? `slug: ${JSON.stringify(state.slug)}` : `# slug: ${JSON.stringify(state.slug)} isn't a valid web address (lowercase words joined by hyphens)`]
      : []),
    "title:",
    `  title: ${JSON.stringify(state.title.trim())}`,
  ];
  if (state.titleVariant.trim()) lines.push(`  title_variant: ${JSON.stringify(state.titleVariant.trim())}`);
  if (state.topicTags.length) lines.push("topic_tags:", ...state.topicTags.map((tag) => `  - ${JSON.stringify(tag)}`));
  else lines.push("# topic_tags: add at least one, ETL requires it");
  if (state.defaultSelection.length) lines.push("default_selection:", ...state.defaultSelection.map((name) => `  - ${JSON.stringify(name)}`));
  else lines.push("# default_selection: tick at least one country in the chart");
  // The view that opens first has to be one that is exported, so it must have an indicator.
  const linked = (c) => Boolean(state.links[comboKey(c)]?.id);
  const chosenFirst = combinations().find((c) => comboKey(c) === defaultComboKey());
  const firstCombo = chosenFirst && linked(chosenFirst) ? chosenFirst : combinations().find(linked);
  if (firstCombo) {
    lines.push("default_dimensions:", ...firstCombo.map((c, i) => `  ${dimSlugs[i]}: ${choiceSlugs.get(c.id)}`));
  }
  const firstNote = chosenFirst && firstCombo !== chosenFirst
    ? "# The view set to open first has no indicator yet, so the first view with one opens first instead."
    : null;
  lines.push("definitions:", "  common_views:", "    - config:", ...yamlLines(sharedConfig(), 8));
  const textBlock = (edits, indent, meta) => {
    const out = [];
    const cfg = Object.fromEntries(TEXT_FIELDS.filter((f) => f in edits).map((f) => [f, edits[f]]));
    if (edits.map && Object.keys(colorScaleFrom(edits.map)).length) cfg.map = { colorScale: colorScaleFrom(edits.map) };
    if (Object.keys(cfg).length) out.push(`${indent}config:`, ...yamlLines(cfg, indent.length + 2));
    if ("descriptionKey" in edits) {
      out.push(`${indent}metadata:`, ...yamlLines({ description_key: descriptionKeyValue(edits.descriptionKey, descriptionKeyAsList(edits, meta)) }, indent.length + 2));
    }
    return out;
  };

  {
    lines.push("dimensions:");
    state.dims.forEach((dim, i) => {
      lines.push(`  - slug: ${dimSlugs[i]}`, `    name: ${JSON.stringify(dim.name)}`);
      if (dim.description?.trim()) lines.push(`    description: ${JSON.stringify(dim.description.trim())}`);
      lines.push("    choices:");
      for (const choice of dim.choices) {
        lines.push(`      - slug: ${choiceSlugs.get(choice.id)}`, `        name: ${JSON.stringify(choice.name)}`);
        if (choice.description?.trim()) lines.push(`        description: ${JSON.stringify(choice.description.trim())}`);
        if (choice.group?.trim()) lines.push(`        group: ${JSON.stringify(choice.group.trim())}`);
      }
    });
  }

  const unlinked = [];
  const viewLines = [];
  const changes = [];
  const suggestions = [];
  const suggested = new Set();
  for (const combo of combinations()) {
    const link = state.links[comboKey(combo)];
    const label = combo.map((c, i) => `${state.dims[i].name} = ${c.name}`).join(", ");
    if (!link?.id) { unlinked.push(label); continue; }
    let catalogPath;
    try { catalogPath = (await getMeta(link.id)).catalogPath; } catch { unlinked.push(`${label} (indicator ${link.id} could not be found)`); continue; }
    viewLines.push("  - dimensions:");
    combo.forEach((c, i) => viewLines.push(`      ${dimSlugs[i]}: ${choiceSlugs.get(c.id)}`));
    viewLines.push("    indicators:", "      y:", `        - catalogPath: ${JSON.stringify(catalogPath)}`);
    const meta = await getMeta(link.id);
    const ownEdits = state.textEdits.views[comboKey(combo)] || {};
    if (ownEdits.rounding) viewLines.push("          display:", ...yamlLines(roundingDisplay(ownEdits.rounding), 12));
    viewLines.push(...textBlock(ownEdits, "    ", meta));

    // What changed in this view, against the indicator's own text.
    const original = originalTexts(meta);
    const shown = resolvedTexts(comboKey(combo), meta);
    for (const field of Object.keys(original)) {
      if (same(original[field], shown[field])) continue;
      changes.push(`#   ${label} · ${FIELD_LABELS[field]}`, `#     was: ${JSON.stringify(original[field])}`, `#     now: ${JSON.stringify(shown[field])}`);
      const suggestedValue = JSON.stringify(field === "descriptionKey" ? descriptionKeyValue(shown[field], descriptionKeyAsList(state.textEdits.views[comboKey(combo)], meta)) : shown[field]);
      if (state.suggest[comboKey(combo)]?.[field] && !suggested.has(`${meta.id}|${field}|${suggestedValue}`)) {
        suggested.add(`${meta.id}|${field}|${suggestedValue}`);
        suggestions.push(`#   - ${meta.catalogPath} · ${FIELD_LABELS[field]} (from ${label}): ${suggestedValue}`);
      }
    }
  }
  lines.push(viewLines.length ? "views:" : "views: []", ...viewLines);
  if (unlinked.length) lines.push("# Combinations with no indicator yet:", ...unlinked.map((u) => `#   - ${u}`));
  if (firstNote) lines.push(firstNote);
  if (changes.length) lines.push("# Text changes, against each indicator's own text:", ...changes);
  if (suggestions.length) {
    lines.push("# Suggested for the indicator everywhere (Claude: check which other charts use it, with /edit-faust-metadata, before changing it):", ...suggestions);
  }
  return lines.join("\n") + "\n";
}

document.getElementById("copy").addEventListener("click", async () => {
  const status = document.getElementById("copy-status");
  const fallback = document.getElementById("copy-fallback");
  const text = await setupYaml();
  try {
    await navigator.clipboard.writeText(text);
    fallback.hidden = true;
    status.textContent = "Copied. Paste it to Claude.";
  } catch (err) {
    fallback.value = text;
    fallback.hidden = false;
    fallback.select();
    status.textContent = "Couldn't copy automatically: copy the text below.";
  }
  setTimeout(() => { status.textContent = ""; }, 4000);
});

// ---------------------------------------------------------------------------
// Panel open/close, and keeping the page clear of it
// ---------------------------------------------------------------------------
const panel = document.getElementById("panel");
const toggle = document.getElementById("panel-toggle");
function applyOpen() {
  panel.classList.toggle("open", state.open);
  toggle.setAttribute("aria-expanded", String(state.open));
}
toggle.addEventListener("click", () => { state.open = !state.open; saveState(); applyOpen(); });

// Drag the handle on the panel's top edge (or use the arrow keys on it) to make the panel taller or shorter.
const resizeHandle = document.getElementById("panel-resize");
const MIN_PANEL_HEIGHT = 120;
const maxPanelHeight = () => Math.max(MIN_PANEL_HEIGHT, window.innerHeight - 140);
const panelBody = document.getElementById("panel-body");
function applyHeight() {
  if (state.height) {
    state.height = Math.min(Math.max(state.height, MIN_PANEL_HEIGHT), maxPanelHeight());
    panel.style.setProperty("--panel-height", `${state.height}px`);
  }
}
resizeHandle.addEventListener("pointerdown", (e) => {
  e.preventDefault();
  const startY = e.clientY;
  const startHeight = panelBody.offsetHeight;
  resizeHandle.setPointerCapture(e.pointerId);
  panel.classList.add("resizing");
  const onMove = (ev) => { state.height = startHeight + (startY - ev.clientY); applyHeight(); };
  const onUp = () => {
    panel.classList.remove("resizing");
    resizeHandle.removeEventListener("pointermove", onMove);
    resizeHandle.removeEventListener("pointerup", onUp);
    resizeHandle.removeEventListener("pointercancel", onUp);
    saveState();
  };
  resizeHandle.addEventListener("pointermove", onMove);
  resizeHandle.addEventListener("pointerup", onUp);
  resizeHandle.addEventListener("pointercancel", onUp);
});
resizeHandle.addEventListener("keydown", (e) => {
  if (e.key !== "ArrowUp" && e.key !== "ArrowDown") return;
  e.preventDefault();
  state.height = panelBody.offsetHeight + (e.key === "ArrowUp" ? 40 : -40);
  applyHeight();
  saveState();
});
window.addEventListener("resize", applyHeight);
applyHeight();
new ResizeObserver(() => { document.body.style.paddingBottom = `${panel.offsetHeight + 24}px`; }).observe(panel);

function refresh() {
  renderBar();
  renderCombos();
  renderSummary();
  renderChart();
}

// ---------------------------------------------------------------------------
// Start
// ---------------------------------------------------------------------------
applyOpen();
renderHeader();
renderPageFields();
renderSlugField();
renderTopicTags();
renderTagsEditor();
normalizeDefaultTab();
renderChartSettings();
renderTextEditor();
pruneLinks();
normalizeSelection();
renderDims();
showNotice("Loading Grapher…");
renderBar();
renderCombos();
renderSummary();

try {
  grapher = await Promise.race([
    import(PACKAGE_URL),
    new Promise((_, reject) => setTimeout(() => reject(new Error("timeout")), 15000)),
  ]);
} catch (err) {
  console.error(err);
}
renderChart();
