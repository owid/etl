import * as vscode from 'vscode';
import * as yaml from 'js-yaml';

const PLACEHOLDER_RE = /\{([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*)\}/g;
const ALIAS_RE = /(?<![\w-])\*([A-Za-z_][A-Za-z0-9_-]*)(?![\w-])(?!\*)/g;
const REVEAL_LINE_CMD = 'meta-yaml-hover.revealLine';

function isMetaYamlFile(fileName: string): boolean {
    return fileName.endsWith('.meta.yml') || fileName.endsWith('.meta.yaml');
}

function escapeRegex(s: string): string {
    return s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

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

function resolveAnchor(docText: string, anchorName: string): string | undefined {
    const lines = docText.split(/\r?\n/);
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

        if (indent <= parentIndent) {
            return undefined;
        }
        if (childIndent === -1) {
            childIndent = indent;
        }
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

function buildRevealUri(docUri: vscode.Uri, line: number): string {
    const args = encodeURIComponent(JSON.stringify([docUri.toString(), line]));
    return `command:${REVEAL_LINE_CMD}?${args}`;
}

function escapeHtml(s: string): string {
    return s
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;');
}

const TOKEN_RE = new RegExp(
    [
        '\\{([A-Za-z_][A-Za-z0-9_]*(?:\\.[A-Za-z_][A-Za-z0-9_]*)*)\\}',
        '(?<![\\w-])\\*([A-Za-z_][A-Za-z0-9_-]*)(?![\\w-])(?!\\*)',
        '(<%[\\s\\S]*?%>|<#[\\s\\S]*?#>)',
        '<<[^<>]*?>>',
    ].join('|'),
    'g',
);

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
            const line = findKeyDeclLine(docText, m[1]);
            if (line !== undefined) {
                const uri = buildRevealUri(docUri, line);
                result += `<small><a href="${uri}" title="Go to definition (line ${line + 1})"><code>${escaped}</code>↗</a></small>`;
            } else {
                result += `<small><code>${escaped}</code></small>`;
            }
        } else if (m[2] !== undefined) {
            const line = findAnchorDeclLine(docText, m[2]);
            if (line !== undefined) {
                const uri = buildRevealUri(docUri, line);
                result += `<small><a href="${uri}" title="Go to definition (line ${line + 1})"><code>${escaped}</code>↗</a></small>`;
            } else {
                result += `<small><code>${escaped}</code></small>`;
            }
        } else if (m[3] !== undefined) {
            result += `<small><i>${escaped}</i></small>`;
        } else {
            result += `<small><code>${escaped}</code></small>`;
        }
        lastIdx = m.index + m[0].length;
    }
    result += escapeHtml(resolved.slice(lastIdx));

    const processedLines = result.split('\n').map((line) => {
        const lm = line.match(/^([ \t]*)(.*)$/);
        if (!lm) {
            return line;
        }
        const leading = lm[1].replace(/\t/g, '    ').replace(/ /g, '&nbsp;');
        return leading + lm[2];
    });
    const pieces: string[] = [];
    for (let i = 0; i < processedLines.length; i++) {
        pieces.push(processedLines[i]);
        if (i < processedLines.length - 1) {
            const next = processedLines[i + 1].replace(/^(?:&nbsp;)*/, '');
            pieces.push(/^[-*+]\s/.test(next) ? '\n' : '<br>');
        }
    }
    return pieces.join('');
}

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

        return undefined;
    },
};

export function activate(context: vscode.ExtensionContext) {
    context.subscriptions.push(
        vscode.languages.registerHoverProvider({ language: 'yaml' }, hoverProvider),
        vscode.languages.registerDefinitionProvider({ language: 'yaml' }, definitionProvider),
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
