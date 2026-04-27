import * as vscode from 'vscode';
import * as yaml from 'js-yaml';

const PLACEHOLDER_RE = /\{(definitions(?:\.[A-Za-z_][A-Za-z0-9_]*)+|macros)\}/g;

function isMetaYamlFile(fileName: string): boolean {
    return fileName.endsWith('.meta.yml') || fileName.endsWith('.meta.yaml');
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

function resolveDefinitionsPath(docText: string, segments: string[]): string | undefined {
    let parsed: unknown;
    try {
        parsed = yaml.load(docText);
    } catch {
        return undefined;
    }
    if (!parsed || typeof parsed !== 'object') {
        return undefined;
    }

    let node: unknown = (parsed as Record<string, unknown>)['definitions'];
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
    const segments = dottedPath.split('.').slice(1);
    if (segments.length === 0) {
        return undefined;
    }
    return resolveDefinitionsPath(docText, segments);
}

const hoverProvider: vscode.HoverProvider = {
    provideHover(document, position) {
        if (document.languageId !== 'yaml' || !isMetaYamlFile(document.fileName)) {
            return undefined;
        }

        const lineText = document.lineAt(position.line).text;
        PLACEHOLDER_RE.lastIndex = 0;

        let match: RegExpExecArray | null;
        while ((match = PLACEHOLDER_RE.exec(lineText)) !== null) {
            const start = match.index;
            const end = start + match[0].length;
            if (position.character < start || position.character > end) {
                continue;
            }

            const path = match[1];
            const resolved = resolvePlaceholder(document.getText(), path);

            const md = new vscode.MarkdownString();
            md.appendMarkdown(`**\`{${path}}\`**\n\n`);
            if (resolved === undefined) {
                md.appendMarkdown('_Not defined in this file._');
            } else {
                md.appendCodeblock(resolved, 'yaml');
            }
            md.isTrusted = false;

            const range = new vscode.Range(
                position.line, start,
                position.line, end
            );
            return new vscode.Hover(md, range);
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
