/**
 * meta-yaml-hover — VSCode extension for OWID `*.meta.yml` files.
 *
 * Provides four things on metadata files:
 *   1. A hover popup that resolves Jinja-style placeholders and YAML aliases:
 *        - `{<dotted.path>}`  — resolved against the YAML root (e.g. `{definitions.foo}`,
 *                                `{tables.x.variables.y.title}`).
 *        - `{macros}`         — extracted as the literal `macros:` block (preserves Jinja).
 *        - `*<anchor>`         — resolves to the value declared at `&<anchor>` in the same file.
 *        - `<<: *<anchor>`     — same; the merge prefix is ignored, the alias hovers.
 *   2. Inline links inside the hover popup. Every reference appearing in the
 *      resolved value is rendered as a clickable `↗` link that jumps to the
 *      declaration line via a custom `meta-yaml-hover.revealLine` command.
 *   3. A `DefinitionProvider` so `Cmd+Click` / `F12` on the same tokens in the
 *      document jumps to their declaration line — the standard VSCode pattern.
 *      As an inverse, `Cmd+Click` on a *declaration* (a YAML key, or `&anchor`)
 *      returns every reference to it, so VSCode peeks the usages.
 *   4. A `ReferenceProvider` so `Shift+F12` / right-click → Find All References
 *      lists every usage of the placeholder or anchor under the cursor — works
 *      from both reference and declaration sites.
 *
 * Runtime placeholders such as `<<welfare_type>>`, `{TODAY}`, `{date_accessed}`,
 * `{LATEST_YEAR}`, etc. are *not* resolved — their values are injected at
 * step-execution time via Python's `yaml_params=...`, not declared in the YAML.
 *
 * The extension is intentionally same-file: no cross-file or `shared.meta.yml`
 * traversal in this version.
 */

import * as vscode from 'vscode';
import * as yaml from 'js-yaml';

/** Matches `{<dotted.path>}` placeholders. Capture group 1 = the dotted path. */
const PLACEHOLDER_RE = /\{([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*)\}/g;

/**
 * Matches YAML alias references `*<name>`. Capture group 1 = the anchor name.
 *
 * The lookbehind/lookahead ensure we don't confuse `*name` with markdown
 * emphasis (`*all*` → no match) or with `*` inside identifiers/expressions
 * (`foo*bar` → no match). Hyphens are allowed inside anchor names because
 * many OWID files use kebab-case anchors (e.g. `*variables-default`).
 */
const ALIAS_RE = /(?<![\w-])\*([A-Za-z_][A-Za-z0-9_-]*)(?![\w-])(?!\*)/g;

/** Custom command used by hover-popup links to navigate to a declaration line. */
const REVEAL_LINE_CMD = 'meta-yaml-hover.revealLine';

/** True iff `fileName` ends in `.meta.yml` / `.meta.yaml`. */
function isMetaYamlFile(fileName: string): boolean {
    return fileName.endsWith('.meta.yml') || fileName.endsWith('.meta.yaml');
}

/** Escape regex metacharacters in a literal string so it can be embedded in a `RegExp`. */
function escapeRegex(s: string): string {
    return s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

/**
 * Pull a top-level YAML block out of `docText` by raw-line scanning.
 *
 * Used as a fallback for `{macros}`: the macros block is a literal Jinja
 * string we want to display verbatim, not a parsed YAML object. We avoid
 * `yaml.load` here so that any unusual content (Jinja-only, malformed)
 * doesn't break the resolution.
 *
 * Returns the block content with the parent's two-space indent stripped,
 * or `undefined` if `key:` doesn't appear at column 0.
 */
function extractTopLevelBlock(docText: string, key: string): string | undefined {
    const lines = docText.split(/\r?\n/);
    const headerRe = new RegExp(`^${key}\\s*:\\s*(\\|[+-]?|>[+-]?)?\\s*$`);
    let startIdx = -1;
    for (let i = 0; i < lines.length; i++) {
        if (headerRe.test(lines[i])) {
            startIdx = i;
            break;
        }
    }
    if (startIdx === -1) {
        return undefined;
    }

    const captured: string[] = [];
    for (let i = startIdx + 1; i < lines.length; i++) {
        const line = lines[i];
        if (line.trim() === '') {
            captured.push('');
            continue;
        }
        if (/^\s/.test(line)) {
            captured.push(line.replace(/^ {0,2}/, ''));
        } else {
            break;
        }
    }
    while (captured.length > 0 && captured[captured.length - 1] === '') {
        captured.pop();
    }
    return captured.length > 0 ? captured.join('\n') : undefined;
}

/**
 * Walk a dotted path (`['definitions', 'foo', 'bar']`) through a parsed YAML
 * document and return the leaf value as a string.
 *
 * Strings are returned verbatim. Other values (lists, mappings) are
 * re-serialised with `yaml.dump` so the hover can show their structure.
 * Returns `undefined` if any segment is missing or YAML parsing fails.
 */
function resolveYamlPath(docText: string, segments: string[]): string | undefined {
    let parsed: unknown;
    try {
        parsed = yaml.load(docText);
    } catch {
        return undefined;
    }
    if (!parsed || typeof parsed !== 'object') {
        return undefined;
    }

    let node: unknown = parsed;
    for (const seg of segments) {
        if (node === null || typeof node !== 'object') {
            return undefined;
        }
        node = (node as Record<string, unknown>)[seg];
    }
    if (node === undefined || node === null) {
        return undefined;
    }
    if (typeof node === 'string') {
        return node;
    }
    try {
        return yaml.dump(node).trimEnd();
    } catch {
        return String(node);
    }
}

/**
 * Resolve a `{<dotted.path>}` placeholder to its value in the same file.
 *
 * The `{macros}` placeholder is special-cased to text extraction; everything
 * else walks the parsed YAML tree from the root.
 */
function resolvePlaceholder(docText: string, dottedPath: string): string | undefined {
    if (dottedPath === 'macros') {
        return extractTopLevelBlock(docText, 'macros');
    }
    const segments = dottedPath.split('.');
    if (segments.length === 0) {
        return undefined;
    }
    return resolveYamlPath(docText, segments);
}

/**
 * Resolve `*<anchor>` to the body of the matching `&<anchor>` declaration.
 *
 * Implemented by raw-line scanning rather than `yaml.load` so that the original
 * formatting (block scalar markers, Jinja, indentation) is preserved in the
 * hover. Walks lines after the anchor declaration as long as their indent is
 * deeper than the anchor's, then strips the anchor's indent + 2 from each.
 */
function resolveAnchor(docText: string, anchorName: string): string | undefined {
    const lines = docText.split(/\r?\n/);
    // The negative lookahead `(?![\w-])` prevents `&foo` from matching the
    // longer anchor `&foo-bar`.
    const anchorRe = new RegExp(`&${escapeRegex(anchorName)}(?![\\w-])(.*)$`);

    let anchorLineIdx = -1;
    let inlineRest = '';
    let anchorIndent = 0;
    for (let i = 0; i < lines.length; i++) {
        const m = lines[i].match(anchorRe);
        if (m) {
            anchorLineIdx = i;
            inlineRest = m[1];
            anchorIndent = (lines[i].match(/^(\s*)/)?.[1] ?? '').length;
            break;
        }
    }
    if (anchorLineIdx === -1) {
        return undefined;
    }

    const captured: string[] = [];
    // Strip leading whitespace and any block scalar indicator (`|`, `|-`, `>`, etc.)
    // that immediately follows the anchor name on the declaration line.
    const inlineTrimmed = inlineRest.replace(/^\s*([|>][+-]?)?\s*/, '');
    if (inlineTrimmed.trim() !== '') {
        captured.push(inlineTrimmed);
    }

    const stripCount = anchorIndent + 2;
    for (let i = anchorLineIdx + 1; i < lines.length; i++) {
        const line = lines[i];
        if (line.trim() === '') {
            captured.push('');
            continue;
        }
        const lineIndent = (line.match(/^(\s*)/)?.[1] ?? '').length;
        if (lineIndent > anchorIndent) {
            captured.push(line.slice(Math.min(stripCount, lineIndent)));
        } else {
            break;
        }
    }
    while (captured.length > 0 && captured[captured.length - 1] === '') {
        captured.pop();
    }
    return captured.length > 0 ? captured.join('\n') : undefined;
}

/**
 * Find the line where a dotted path is declared in the YAML.
 *
 * Walks the file as text rather than parsing, so it can return a raw line
 * number suitable for "go to definition". Each segment must be a direct child
 * of the previous one (matching at the same indent as the first child found
 * after entering the parent's scope) — this prevents a same-named key in an
 * unrelated nested branch from hijacking the search.
 */
function findKeyDeclLine(docText: string, dottedPath: string): number | undefined {
    const segments = dottedPath.split('.');
    if (segments.length === 0) {
        return undefined;
    }
    const lines = docText.split(/\r?\n/);
    let segIdx = 0;
    let parentIndent = -1;
    let childIndent = -1;
    for (let i = 0; i < lines.length; i++) {
        const line = lines[i];
        const m = line.match(/^(\s*)([A-Za-z_][A-Za-z0-9_]*)\s*:/);
        if (!m) {
            continue;
        }
        const indent = m[1].length;
        const key = m[2];

        if (segIdx === 0) {
            // Looking for the first segment as a top-level key.
            if (indent === 0 && key === segments[0]) {
                if (segments.length === 1) {
                    return i;
                }
                segIdx = 1;
                parentIndent = indent;
                childIndent = -1;
            }
            continue;
        }

        // Encountered a key at or above the parent's indent → left the scope.
        if (indent <= parentIndent) {
            return undefined;
        }
        // First child seen sets the immediate child indent for this scope.
        if (childIndent === -1) {
            childIndent = indent;
        }
        // Skip grandchildren (keys at deeper indents than direct children).
        if (indent !== childIndent) {
            continue;
        }
        if (key === segments[segIdx]) {
            if (segIdx === segments.length - 1) {
                return i;
            }
            segIdx++;
            parentIndent = indent;
            childIndent = -1;
        }
    }
    return undefined;
}

/** Find the line where `&<anchorName>` is declared. */
function findAnchorDeclLine(docText: string, anchorName: string): number | undefined {
    const lines = docText.split(/\r?\n/);
    const re = new RegExp(`&${escapeRegex(anchorName)}(?![\\w-])`);
    for (let i = 0; i < lines.length; i++) {
        if (re.test(lines[i])) {
            return i;
        }
    }
    return undefined;
}

/**
 * If the cursor sits on a *declaration* token, return what is being declared:
 * either a placeholder target (a YAML key, identified by its dotted path from
 * the document root) or a YAML anchor (`&name`). Returns `undefined` when the
 * cursor is somewhere else (a value, whitespace, a reference token, etc.).
 *
 * The dotted path for a YAML key is built by walking *up* from the cursor's
 * line: every prior line whose `key:` indent is strictly less than the
 * running minimum becomes the next ancestor segment, until indent 0. List
 * entries (`- key:`) are not specially handled — they will be picked up as
 * keys but the resulting path won't have a `[i]` index, which is fine because
 * `{...}` placeholders can't reference list items by index anyway.
 */
function getDeclAtCursor(
    document: vscode.TextDocument,
    position: vscode.Position,
): { kind: 'placeholder'; path: string } | { kind: 'anchor'; name: string } | undefined {
    const lineText = document.lineAt(position.line).text;
    const ch = position.character;

    // Anchor declaration: `&name` — match anywhere on the line.
    const anchorRe = /&([A-Za-z_][A-Za-z0-9_-]*)/g;
    let am: RegExpExecArray | null;
    while ((am = anchorRe.exec(lineText)) !== null) {
        const start = am.index;
        const end = start + am[0].length;
        if (ch >= start && ch <= end) {
            return { kind: 'anchor', name: am[1] };
        }
    }

    // YAML key declaration: cursor must be on the key text itself, before the colon.
    const km = lineText.match(/^(\s*)([A-Za-z_][A-Za-z0-9_]*)\s*:/);
    if (!km) {
        return undefined;
    }
    const indent = km[1].length;
    const key = km[2];
    const keyStart = indent;
    const keyEnd = indent + key.length;
    if (ch < keyStart || ch > keyEnd) {
        return undefined;
    }

    const segments = [key];
    let minIndent = indent;
    for (let i = position.line - 1; i >= 0 && minIndent > 0; i--) {
        const line = document.lineAt(i).text;
        const m = line.match(/^(\s*)([A-Za-z_][A-Za-z0-9_]*)\s*:/);
        if (!m) {
            continue;
        }
        const lineIndent = m[1].length;
        if (lineIndent < minIndent) {
            segments.unshift(m[2]);
            minIndent = lineIndent;
        }
    }
    return { kind: 'placeholder', path: segments.join('.') };
}

/**
 * Return every `{<dottedPath>}` occurrence in `document` as a `Location`.
 * Exact-path match only — sub-path references (e.g. `{definitions.foo.bar}`
 * for a key `definitions.foo`) are not included.
 */
function findPlaceholderRefs(document: vscode.TextDocument, dottedPath: string): vscode.Location[] {
    const docText = document.getText();
    const refs: vscode.Location[] = [];
    const re = new RegExp(PLACEHOLDER_RE.source, 'g');
    let m: RegExpExecArray | null;
    while ((m = re.exec(docText)) !== null) {
        if (m[1] === dottedPath) {
            const start = document.positionAt(m.index);
            const end = document.positionAt(m.index + m[0].length);
            refs.push(new vscode.Location(document.uri, new vscode.Range(start, end)));
        }
    }
    return refs;
}

/** Return every `*<anchorName>` occurrence in `document` as a `Location`. */
function findAnchorRefs(document: vscode.TextDocument, anchorName: string): vscode.Location[] {
    const docText = document.getText();
    const refs: vscode.Location[] = [];
    const re = new RegExp(ALIAS_RE.source, 'g');
    let m: RegExpExecArray | null;
    while ((m = re.exec(docText)) !== null) {
        if (m[1] === anchorName) {
            const start = document.positionAt(m.index);
            const end = document.positionAt(m.index + m[0].length);
            refs.push(new vscode.Location(document.uri, new vscode.Range(start, end)));
        }
    }
    return refs;
}

/**
 * Run `re` over `lineText` and return the match (with capture group 1 as
 * `name`) that contains the cursor at column `character`. Used to detect
 * whether the cursor sits on a placeholder/alias token.
 */
function findHoverMatch(
    lineText: string,
    character: number,
    re: RegExp,
): { start: number; end: number; name: string } | undefined {
    re.lastIndex = 0;
    let match: RegExpExecArray | null;
    while ((match = re.exec(lineText)) !== null) {
        const start = match.index;
        const end = start + match[0].length;
        if (character >= start && character <= end) {
            return { start, end, name: match[1] };
        }
    }
    return undefined;
}

/**
 * Build a `command:` URI that, when clicked in a trusted hover, fires the
 * `meta-yaml-hover.revealLine` command and reveals `(uri, line)` in an editor.
 */
function buildRevealUri(docUri: vscode.Uri, line: number): string {
    const args = encodeURIComponent(JSON.stringify([docUri.toString(), line]));
    return `command:${REVEAL_LINE_CMD}?${args}`;
}

/** Escape a string for safe insertion as HTML text or attribute value. */
function escapeHtml(s: string): string {
    return s
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;');
}

/**
 * Single-pass tokeniser for the resolved hover body. Each alternative is a
 * separate kind of inline token, identified later by which capture group
 * matched:
 *   group 1 — `{<dotted.path>}` placeholder
 *   group 2 — `*<anchor>` alias
 *   group 3 — `<% ... %>` Jinja statement OR `<# ... #>` Jinja comment
 *   (no group) — `<<...>>` Jinja interpolation (matched but not captured)
 */
const TOKEN_RE = new RegExp(
    [
        '\\{([A-Za-z_][A-Za-z0-9_]*(?:\\.[A-Za-z_][A-Za-z0-9_]*)*)\\}',
        '(?<![\\w-])\\*([A-Za-z_][A-Za-z0-9_-]*)(?![\\w-])(?!\\*)',
        '(<%[\\s\\S]*?%>|<#[\\s\\S]*?#>)',
        '<<[^<>]*?>>',
    ].join('|'),
    'g',
);

/**
 * Render a resolved value as HTML for the hover popup.
 *
 * Each token (placeholder, alias, Jinja statement/comment, interpolation)
 * gets an inline `<code>` wrapper so it stands out from the surrounding prose;
 * placeholders and aliases additionally become clickable `<a>` links to their
 * declaration line. Jinja control flow is wrapped in `<i>` only — no
 * monospace — so it reads as syntax rather than data.
 *
 * After tokenising, the rendered string is split per source line:
 *   - leading whitespace becomes `&nbsp;` so indentation survives in HTML
 *   - depth tracking through Jinja `<% if %>` / `<%- endif %>` / `<%- elif %>`
 *     adds two `&nbsp;` per nesting level to the front of every line, so
 *     conditions read as section headers and the body sits visibly indented
 *     under them.
 *   - lines are joined with `<br>` for visual breaks; if the next line starts
 *     a markdown list item (`- `, `* `, `+ `) we use `\n` instead so markdown
 *     can recognise the whole block as a bulleted list.
 */
function renderResolvedWithLinks(
    resolved: string,
    docUri: vscode.Uri,
    docText: string,
): string {
    let result = '';
    let lastIdx = 0;
    TOKEN_RE.lastIndex = 0;
    let m: RegExpExecArray | null;
    while ((m = TOKEN_RE.exec(resolved)) !== null) {
        result += escapeHtml(resolved.slice(lastIdx, m.index));
        const escaped = escapeHtml(m[0]);
        if (m[1] !== undefined) {
            // Placeholder
            const line = findKeyDeclLine(docText, m[1]);
            if (line !== undefined) {
                const uri = buildRevealUri(docUri, line);
                result += `<small><a href="${uri}" title="Go to definition (line ${line + 1})"><code>${escaped}</code>↗</a></small>`;
            } else {
                result += `<small><code>${escaped}</code></small>`;
            }
        } else if (m[2] !== undefined) {
            // YAML alias
            const line = findAnchorDeclLine(docText, m[2]);
            if (line !== undefined) {
                const uri = buildRevealUri(docUri, line);
                result += `<small><a href="${uri}" title="Go to definition (line ${line + 1})"><code>${escaped}</code>↗</a></small>`;
            } else {
                result += `<small><code>${escaped}</code></small>`;
            }
        } else if (m[3] !== undefined) {
            // Jinja control flow / comment — italic in the body font, no monospace
            result += `<i>${escaped}</i>`;
        } else {
            // Jinja interpolation `<<var>>` — runtime placeholder we can't resolve
            result += `<small><code>${escaped}</code></small>`;
        }
        lastIdx = m.index + m[0].length;
    }
    result += escapeHtml(resolved.slice(lastIdx));

    // Per-line post-processing: leading whitespace → &nbsp; (HTML collapses
    // consecutive spaces, so we have to opt out of that explicitly).
    const processedLines = result.split('\n').map((line) => {
        const lm = line.match(/^([ \t]*)(.*)$/);
        if (!lm) {
            return line;
        }
        const leading = lm[1].replace(/\t/g, '    ').replace(/ /g, '&nbsp;');
        return leading + lm[2];
    });

    /** True iff the post-processed line is *only* a Jinja statement/comment. */
    const isJinjaLine = (line: string): boolean =>
        /^(?:&nbsp;)*<i>[^<>]*<\/i>$/.test(line);

    /**
     * Classify a Jinja line as opening a scope (`if`, `for`, ...), closing
     * one (`endif`, ...), continuing one at the same level (`elif`, `else`),
     * or none of the above (a comment or `<% set %>`-style statement).
     */
    const jinjaControlKind = (line: string): 'open' | 'close' | 'middle' | 'none' => {
        const m = line.match(/^(?:&nbsp;)*<i>&lt;%-?\s*(\w+)/);
        if (!m) {
            return 'none';
        }
        const kw = m[1].toLowerCase();
        if (/^(?:if|for|with|block|macro)$/.test(kw)) {
            return 'open';
        }
        if (/^(?:endif|endfor|endwith|endblock|endmacro)$/.test(kw)) {
            return 'close';
        }
        if (/^(?:elif|else)$/.test(kw)) {
            return 'middle';
        }
        return 'none';
    };

    // Track Jinja nesting depth and prepend two `&nbsp;` per level to each
    // line. Jinja `if`/`for`/etc. open at the current depth and increment
    // for everything after; `endif`/`endfor` decrement before placing the
    // line so the closer aligns with its opener; `elif`/`else` sit at the
    // outer level but don't change the body depth.
    let depth = 0;
    const indentedLines = processedLines.map((line) => {
        let lineDepth: number;
        if (isJinjaLine(line)) {
            const kind = jinjaControlKind(line);
            if (kind === 'close') {
                depth = Math.max(0, depth - 1);
                lineDepth = depth;
            } else if (kind === 'middle') {
                lineDepth = Math.max(0, depth - 1);
            } else if (kind === 'open') {
                lineDepth = depth;
                depth += 1;
            } else {
                lineDepth = depth;
            }
        } else {
            lineDepth = depth;
        }
        return '&nbsp;&nbsp;'.repeat(lineDepth) + line;
    });

    // Join lines with `<br>` for hard breaks; switch to `\n` only when the
    // next line starts a *top-level* bulleted list (`- `, `* `, `+ ` at
    // column 0). Indented list lines carry an `&nbsp;` prefix from the
    // leading-whitespace pass, which would prevent markdown from recognising
    // them as list items — for those we keep `<br>` so the line break
    // survives instead of collapsing into a soft break.
    const pieces: string[] = [];
    for (let i = 0; i < indentedLines.length; i++) {
        pieces.push(indentedLines[i]);
        if (i < indentedLines.length - 1) {
            const next = indentedLines[i + 1];
            pieces.push(/^[-*+]\s/.test(next) ? '\n' : '<br>');
        }
    }
    return pieces.join('');
}

/**
 * Hover provider: shows the resolved value of `{<dotted.path>}` placeholders
 * and `*<anchor>` aliases when the cursor is on one. The popup carries an
 * `isTrusted = { enabledCommands: [REVEAL_LINE_CMD] }` so the inline
 * drill-down links can fire our command without user prompts.
 */
const hoverProvider: vscode.HoverProvider = {
    provideHover(document, position) {
        if (document.languageId !== 'yaml' || !isMetaYamlFile(document.fileName)) {
            return undefined;
        }

        const lineText = document.lineAt(position.line).text;
        const docText = document.getText();

        const placeholder = findHoverMatch(lineText, position.character, PLACEHOLDER_RE);
        if (placeholder) {
            const resolved = resolvePlaceholder(docText, placeholder.name);
            const md = new vscode.MarkdownString();
            md.isTrusted = { enabledCommands: [REVEAL_LINE_CMD] };
            md.supportHtml = true;
            md.appendMarkdown(`<small>PLACEHOLDER ·</small> <code>{${placeholder.name}}</code>\n\n---\n\n`);
            if (resolved === undefined) {
                md.appendMarkdown('_Not defined in this file._');
            } else {
                md.appendMarkdown(renderResolvedWithLinks(resolved, document.uri, docText));
            }
            return new vscode.Hover(md, new vscode.Range(
                position.line, placeholder.start,
                position.line, placeholder.end,
            ));
        }

        const alias = findHoverMatch(lineText, position.character, ALIAS_RE);
        if (alias) {
            const resolved = resolveAnchor(docText, alias.name);
            const md = new vscode.MarkdownString();
            md.isTrusted = { enabledCommands: [REVEAL_LINE_CMD] };
            md.supportHtml = true;
            md.appendMarkdown(`<small>ANCHOR ·</small> <code>*${alias.name}</code>\n\n---\n\n`);
            if (resolved === undefined) {
                md.appendMarkdown('_Anchor not defined in this file._');
            } else {
                md.appendMarkdown(renderResolvedWithLinks(resolved, document.uri, docText));
            }
            return new vscode.Hover(md, new vscode.Range(
                position.line, alias.start,
                position.line, alias.end,
            ));
        }

        return undefined;
    },
};

/**
 * Definition provider: powers `Cmd+Click` / `F12`. From a *reference* token
 * (`{path}` or `*anchor`) it jumps to the declaration. From a *declaration*
 * (a YAML key, or `&anchor`) it returns every reference — VSCode shows them
 * in its peek view, which gives you a full list when there are many usages.
 */
const definitionProvider: vscode.DefinitionProvider = {
    provideDefinition(document, position) {
        if (document.languageId !== 'yaml' || !isMetaYamlFile(document.fileName)) {
            return undefined;
        }

        const lineText = document.lineAt(position.line).text;
        const docText = document.getText();

        const placeholder = findHoverMatch(lineText, position.character, PLACEHOLDER_RE);
        if (placeholder) {
            const line = findKeyDeclLine(docText, placeholder.name);
            if (line !== undefined) {
                return new vscode.Location(document.uri, new vscode.Position(line, 0));
            }
        }

        const alias = findHoverMatch(lineText, position.character, ALIAS_RE);
        if (alias) {
            const line = findAnchorDeclLine(docText, alias.name);
            if (line !== undefined) {
                return new vscode.Location(document.uri, new vscode.Position(line, 0));
            }
        }

        // Reverse direction: cursor on a declaration → return all references.
        const decl = getDeclAtCursor(document, position);
        if (decl?.kind === 'placeholder') {
            const refs = findPlaceholderRefs(document, decl.path);
            if (refs.length > 0) {
                return refs;
            }
        }
        if (decl?.kind === 'anchor') {
            const refs = findAnchorRefs(document, decl.name);
            if (refs.length > 0) {
                return refs;
            }
        }

        return undefined;
    },
};

/**
 * Reference provider: powers `Shift+F12` / right-click → Find All References.
 * Works from both sides — on a declaration it lists every usage; on a usage
 * it lists every sibling usage. `context.includeDeclaration` is honoured so
 * VSCode's "include declaration" toggle behaves as expected.
 */
const referenceProvider: vscode.ReferenceProvider = {
    provideReferences(document, position, context) {
        if (document.languageId !== 'yaml' || !isMetaYamlFile(document.fileName)) {
            return undefined;
        }

        const lineText = document.lineAt(position.line).text;
        const docText = document.getText();

        const appendDecl = (
            refs: vscode.Location[],
            declLine: number | undefined,
        ): vscode.Location[] => {
            if (context.includeDeclaration && declLine !== undefined) {
                refs.push(new vscode.Location(document.uri, new vscode.Position(declLine, 0)));
            }
            return refs;
        };

        // Cursor on a declaration → references to it.
        const decl = getDeclAtCursor(document, position);
        if (decl?.kind === 'placeholder') {
            return appendDecl(findPlaceholderRefs(document, decl.path), findKeyDeclLine(docText, decl.path));
        }
        if (decl?.kind === 'anchor') {
            return appendDecl(findAnchorRefs(document, decl.name), findAnchorDeclLine(docText, decl.name));
        }

        // Cursor on a reference → its sibling references.
        const placeholder = findHoverMatch(lineText, position.character, PLACEHOLDER_RE);
        if (placeholder) {
            return appendDecl(findPlaceholderRefs(document, placeholder.name), findKeyDeclLine(docText, placeholder.name));
        }
        const alias = findHoverMatch(lineText, position.character, ALIAS_RE);
        if (alias) {
            return appendDecl(findAnchorRefs(document, alias.name), findAnchorDeclLine(docText, alias.name));
        }

        return undefined;
    },
};

/**
 * Extension activation. Registers the hover, definition, and reference
 * providers, plus the `revealLine` command used by clickable hover-popup links.
 */
export function activate(context: vscode.ExtensionContext) {
    context.subscriptions.push(
        vscode.languages.registerHoverProvider({ language: 'yaml' }, hoverProvider),
        vscode.languages.registerDefinitionProvider({ language: 'yaml' }, definitionProvider),
        vscode.languages.registerReferenceProvider({ language: 'yaml' }, referenceProvider),
        vscode.commands.registerCommand(REVEAL_LINE_CMD, async (uri: string, line: number) => {
            const target = vscode.Uri.parse(uri);
            const editor = await vscode.window.showTextDocument(target, { preserveFocus: false });
            const pos = new vscode.Position(line, 0);
            editor.selection = new vscode.Selection(pos, pos);
            editor.revealRange(new vscode.Range(pos, pos), vscode.TextEditorRevealType.InCenter);
        }),
    );
}

export function deactivate() { }
