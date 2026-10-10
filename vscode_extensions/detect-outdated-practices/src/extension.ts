import * as vscode from 'vscode';
import { detect } from './detector.mts';

// The rules themselves live in detector.mts, shared with the command-line checker (cli.mts), so the
// editor warnings and `node src/cli.mts <files>` can never disagree about what is outdated.

export function activate(context: vscode.ExtensionContext) {
    console.log('Detect Outdated Practices extension activated.');

    // Create a diagnostic collection
    const diagnosticCollection = vscode.languages.createDiagnosticCollection('outdated-practices');
    context.subscriptions.push(diagnosticCollection);

    // Function to analyze document and update diagnostics
    function updateDiagnostics(document: vscode.TextDocument) {
        // Only process Python files
        if (document.languageId !== 'python') {
            return;
        }
        if (!vscode.workspace.workspaceFolders?.[0]) {
            return;
        }

        const relativePath = vscode.workspace.asRelativePath(document.uri.fsPath, false);
        const diagnostics = detect(document.getText(), relativePath).map(finding => {
            const range = new vscode.Range(
                new vscode.Position(finding.line, finding.start),
                new vscode.Position(finding.line, finding.end)
            );
            const diagnostic = new vscode.Diagnostic(range, finding.message, vscode.DiagnosticSeverity.Warning);
            diagnostic.source = 'outdated-practices';
            return diagnostic;
        });

        diagnosticCollection.set(document.uri, diagnostics);
    }

    // Update diagnostics when document is opened or changed
    if (vscode.window.activeTextEditor) {
        updateDiagnostics(vscode.window.activeTextEditor.document);
    }

    // Listen for active editor changes
    context.subscriptions.push(
        vscode.window.onDidChangeActiveTextEditor(editor => {
            if (editor) {
                updateDiagnostics(editor.document);
            }
        })
    );

    // Listen for document changes
    context.subscriptions.push(
        vscode.workspace.onDidChangeTextDocument(event => {
            updateDiagnostics(event.document);
        })
    );

    // Listen for document open
    context.subscriptions.push(
        vscode.workspace.onDidOpenTextDocument(document => {
            updateDiagnostics(document);
        })
    );

    // Process all currently open documents
    vscode.workspace.textDocuments.forEach(document => {
        updateDiagnostics(document);
    });
}

export function deactivate() {
    // Cleanup is handled automatically by VS Code
}
