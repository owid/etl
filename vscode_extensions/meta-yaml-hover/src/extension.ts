import * as vscode from 'vscode';
import * as yaml from 'js-yaml';

const PLACEHOLDER_RE = /\{([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*)\}/g;
const ALIAS_RE = /\*([A-Za-z_][A-Za-z0-9_]*)/g;

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
    const anchorRe = new RegExp(`&${escapeRegex(anchorName)}\\b(.*)$`);

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

function findHoverMatch(
    lineText: string,
    character: number,
    re: RegExp
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

const hoverProvider: vscode.HoverProvider = {
    provideHover(document, position) {
        if (document.languageId !== 'yaml' || !isMetaYamlFile(document.fileName)) {
            return undefined;
        }

        const lineText = document.lineAt(position.line).text;

        const placeholder = findHoverMatch(lineText, position.character, PLACEHOLDER_RE);
        if (placeholder) {
            const resolved = resolvePlaceholder(document.getText(), placeholder.name);
            const md = new vscode.MarkdownString();
            md.appendMarkdown(`**\`{${placeholder.name}}\`**\n\n`);
            if (resolved === undefined) {
                md.appendMarkdown('_Not defined in this file._');
            } else {
                md.appendCodeblock(resolved, 'yaml');
            }
            return new vscode.Hover(md, new vscode.Range(
                position.line, placeholder.start,
                position.line, placeholder.end
            ));
        }

        const alias = findHoverMatch(lineText, position.character, ALIAS_RE);
        if (alias) {
            const resolved = resolveAnchor(document.getText(), alias.name);
            const md = new vscode.MarkdownString();
            md.appendMarkdown(`**\`*${alias.name}\`** _(YAML anchor)_\n\n`);
            if (resolved === undefined) {
                md.appendMarkdown('_Anchor not defined in this file._');
            } else {
                md.appendCodeblock(resolved, 'yaml');
            }
            return new vscode.Hover(md, new vscode.Range(
                position.line, alias.start,
                position.line, alias.end
            ));
        }

        return undefined;
    },
};

export function activate(context: vscode.ExtensionContext) {
    context.subscriptions.push(
        vscode.languages.registerHoverProvider({ language: 'yaml' }, hoverProvider)
    );
}

export function deactivate() { }
