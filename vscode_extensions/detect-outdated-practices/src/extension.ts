import * as vscode from 'vscode';

/**
 * Configuration for outdated code practices
 * Add new patterns here with their associated warning messages
 */
interface OutdatedPattern {
    pattern: string | RegExp;
    message: string;
    severity: vscode.DiagnosticSeverity;
    /**
     * Optional: Restrict pattern to specific paths
     * If undefined, applies to all Python files
     * Can use glob patterns (e.g., "etl/steps/data/**")
     */
    scope?: string | string[];
}

const OUTDATED_PATTERNS: OutdatedPattern[] = [
    {
        // Matches dest_dir in various contexts:
        // - dest_dir, (with comma)
        // - dest_dir) (closing parenthesis)
        // - dest_dir: str (type annotation)
        // - create_dataset(dest_dir (function argument)
        // - "dest_dir" or 'dest_dir' (string literals)
        pattern: /dest_dir(?=[,):\s]|["'])/g,
        message: 'Use of `dest_dir` is outdated. Use paths.create_dataset, which does not need dest_dir.',
        severity: vscode.DiagnosticSeverity.Warning,
        scope: 'etl/steps/data/**'
    },
    {
        // Matches geo.harmonize_countries (from etl.data_helpers import geo)
        // Common patterns:
        // - geo.harmonize_countries(tb, ...)
        // - tb = geo.harmonize_countries(...)
        pattern: /geo\.harmonize_countries\(/g,
        message: '`geo.harmonize_countries` is outdated. Use `paths.regions.harmonize_names(tb)` instead.',
        severity: vscode.DiagnosticSeverity.Warning,
        scope: 'etl/steps/data/**'
    },
    {
        // Matches paths.load_dependency
        // Common patterns:
        // - paths.load_dependency("dataset_name")
        // - ds = paths.load_dependency(...)
        pattern: /paths\.load_dependency\(/g,
        message: '`paths.load_dependency` is outdated. Use `paths.load_dataset` or `paths.load_snapshot` instead.',
        severity: vscode.DiagnosticSeverity.Warning,
        scope: 'etl/steps/data/**'
    }
];

export function activate(context: vscode.ExtensionContext) {
    console.log('Detect Outdated Practices extension activated.');

    // Create a diagnostic collection
    const diagnosticCollection = vscode.languages.createDiagnosticCollection('outdated-practices');
    context.subscriptions.push(diagnosticCollection);

    /**
     * Check if a file path matches a glob pattern scope
     * @param filePath The file path to check (relative to workspace)
     * @param scope The scope pattern(s) to match against
     * @returns true if the file matches the scope, or if scope is undefined (global)
     */
    function matchesScope(filePath: string, scope?: string | string[]): boolean {
        // If no scope is defined, pattern applies to all files
        if (!scope) {
            return true;
        }

        // Get workspace folder to make relative paths
        const workspaceFolder = vscode.workspace.workspaceFolders?.[0];
        if (!workspaceFolder) {
            return false;
        }

        // Convert file URI to relative path
        const relativePath = vscode.workspace.asRelativePath(filePath, false);

        // Normalize path separators to forward slashes for consistency
        const normalizedPath = relativePath.replace(/\\/g, '/');

        // Handle single scope or array of scopes
        const scopes = Array.isArray(scope) ? scope : [scope];

        // Check if the file path matches any of the scope patterns
        for (const scopePattern of scopes) {
            // Convert glob pattern to regex
            // ** matches any number of directories
            // * matches any characters except /
            const regexPattern = scopePattern
                .replace(/\*\*/g, '§DOUBLESTAR§')  // Temporarily replace **
                .replace(/\*/g, '[^/]*')            // * matches anything except /
                .replace(/§DOUBLESTAR§/g, '.*')     // ** matches anything including /
                .replace(/\//g, '\\/')              // Escape forward slashes
                + '$';                              // Match to end of string

            const regex = new RegExp(regexPattern);
            if (regex.test(normalizedPath)) {
                return true;
            }
        }

        return false;
    }

    // Function to analyze document and update diagnostics
    function updateDiagnostics(document: vscode.TextDocument) {
        // Only process Python files
        if (document.languageId !== 'python') {
            return;
        }

        const diagnostics: vscode.Diagnostic[] = [];
        const text = document.getText();
        const lines = text.split('\n');

        // Check each line for outdated patterns
        for (let lineIndex = 0; lineIndex < lines.length; lineIndex++) {
            const line = lines[lineIndex];

            for (const outdatedPattern of OUTDATED_PATTERNS) {
                // Check if this pattern applies to the current file based on scope
                if (!matchesScope(document.uri.fsPath, outdatedPattern.scope)) {
                    continue;
                }

                const regex = typeof outdatedPattern.pattern === 'string'
                    ? new RegExp(outdatedPattern.pattern, 'g')
                    : new RegExp(outdatedPattern.pattern.source, 'g');

                let match: RegExpExecArray | null;
                while ((match = regex.exec(line)) !== null) {
                    const startPos = new vscode.Position(lineIndex, match.index);
                    const endPos = new vscode.Position(lineIndex, match.index + match[0].length);
                    const range = new vscode.Range(startPos, endPos);

                    const diagnostic = new vscode.Diagnostic(
                        range,
                        outdatedPattern.message,
                        outdatedPattern.severity
                    );

                    diagnostic.source = 'outdated-practices';
                    diagnostics.push(diagnostic);
                }
            }
        }

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
