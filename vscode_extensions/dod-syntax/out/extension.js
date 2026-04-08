"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function (o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
        desc = { enumerable: true, get: function () { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function (o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function (o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function (o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function (o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
Object.defineProperty(exports, "__esModule", { value: true });
exports.activate = activate;
exports.deactivate = deactivate;
const vscode = __importStar(require("vscode"));
function activate(context) {
    console.log('DOD Syntax extension activated.');
    // Create decoration type for DOD references with underline
    const dodDecorationFtype = vscode.window.createTextEditorDecorationType({
        textDecoration: 'underline',
        color: '#0078d4', // VS Code blue color
        cursor: 'pointer'
    });
    // Hover provider for DOD references
    const hoverProvider = {
        provideHover(document, position) {
            // Only process YAML and Python files
            const languageId = document.languageId;
            if (languageId !== 'yaml' && languageId !== 'python') {
                return undefined;
            }
            // Regex to match [title](#dod:key) pattern
            const dodRegex = /\[([^\]]+)\]\(#dod:([^)]+)\)/g;
            const line = document.lineAt(position.line);
            const lineText = line.text;
            let match;
            while ((match = dodRegex.exec(lineText)) !== null) {
                const matchStart = match.index;
                const matchEnd = match.index + match[0].length;
                // Check if the cursor position is within this match
                if (position.character >= matchStart && position.character <= matchEnd) {
                    // For Python files, only show hover if inside raw strings
                    if (languageId === 'python') {
                        const absolutePosition = document.offsetAt(new vscode.Position(position.line, matchStart));
                        if (!isInPythonRawString(document, absolutePosition)) {
                            continue;
                        }
                    }
                    const title = match[1];
                    const key = match[2];
                    // Create hover content with dummy text
                    const hoverContent = new vscode.MarkdownString();
                    hoverContent.appendMarkdown(`**${title}**\n\n`);
                    hoverContent.appendMarkdown(`*DOD Key:* \`${key}\`\n\n`);
                    hoverContent.appendMarkdown('This is a placeholder definition. ');
                    hoverContent.appendMarkdown('Future versions will fetch the actual definition from the database.');
                    // Create range for the entire match
                    const range = new vscode.Range(position.line, matchStart, position.line, matchEnd);
                    return new vscode.Hover(hoverContent, range);
                }
            }
            return undefined;
        }
    };
    // Function to check if a position is within a Python raw string
    function isInPythonRawString(document, position) {
        const text = document.getText();
        const beforeText = text.substring(0, position);
        // Find all raw string patterns before this position
        const rawStringRegex = /r('''|"""|'|")/g;
        let match;
        let inRawString = false;
        let stringDelimiter = '';
        while ((match = rawStringRegex.exec(beforeText)) !== null) {
            const delimiter = match[1];
            if (!inRawString) {
                // Starting a raw string
                inRawString = true;
                stringDelimiter = delimiter;
            }
            else if (delimiter === stringDelimiter) {
                // Ending the current raw string
                inRawString = false;
                stringDelimiter = '';
            }
        }
        return inRawString;
    }
    // Function to update decorations in the active editor
    function updateDecorations(editor) {
        if (!editor) {
            return;
        }
        const languageId = editor.document.languageId;
        if (languageId !== 'yaml' && languageId !== 'python') {
            return;
        }
        const decorations = [];
        const text = editor.document.getText();
        const dodRegex = /\[([^\]]+)\]\(#dod:([^)]+)\)/g;
        let match;
        while ((match = dodRegex.exec(text)) !== null) {
            // For Python files, only highlight if inside raw strings
            if (languageId === 'python' && !isInPythonRawString(editor.document, match.index)) {
                continue;
            }
            const startPos = editor.document.positionAt(match.index);
            const endPos = editor.document.positionAt(match.index + match[0].length);
            const range = new vscode.Range(startPos, endPos);
            const title = match[1];
            const key = match[2];
            decorations.push({
                range,
                hoverMessage: `DOD Reference: ${title} (${key})`
            });
        }
        editor.setDecorations(dodDecorationFtype, decorations);
    }
    // Register hover provider for YAML and Python files
    context.subscriptions.push(vscode.languages.registerHoverProvider({ language: 'yaml', scheme: 'file' }, hoverProvider));
    context.subscriptions.push(vscode.languages.registerHoverProvider({ language: 'python', scheme: 'file' }, hoverProvider));
    // Update decorations when the active editor changes
    context.subscriptions.push(vscode.window.onDidChangeActiveTextEditor(editor => {
        if (editor) {
            updateDecorations(editor);
        }
    }));
    // Update decorations when document content changes
    context.subscriptions.push(vscode.workspace.onDidChangeTextDocument(event => {
        const editor = vscode.window.activeTextEditor;
        if (editor && editor.document === event.document) {
            updateDecorations(editor);
        }
    }));
    // Update decorations for the current active editor
    if (vscode.window.activeTextEditor) {
        updateDecorations(vscode.window.activeTextEditor);
    }
}
function deactivate() { }
//# sourceMappingURL=extension.js.map
