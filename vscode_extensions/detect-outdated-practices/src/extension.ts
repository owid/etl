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
        // Matches geo.add_population_to_table (deprecated per its own docstring).
        // Common patterns:
        // - geo.add_population_to_table(tb=tb, ds_population=...)
        // - tb = geo.add_population_to_table(...)
        pattern: /geo\.add_population_to_table\(/g,
        message: '`geo.add_population_to_table` is outdated. Use `paths.regions.add_population(tb)` instead (auto-resolves the population dataset from the DAG; for per-capita indicators, prefer `paths.regions.add_per_capita(tb)`).',
        severity: vscode.DiagnosticSeverity.Warning,
        scope: 'etl/steps/data/**'
    },
    {
        // Matches geo.add_regions_to_table (deprecated per its own docstring).
        // Common patterns:
        // - geo.add_regions_to_table(tb=tb, ds_regions=..., regions=[...], aggregations=...)
        // - tb = geo.add_regions_to_table(...)
        pattern: /geo\.add_regions_to_table\(/g,
        message: '`geo.add_regions_to_table` is outdated. Use `paths.regions.add_aggregates(tb, ...)` instead (auto-resolves regions and income_groups from the DAG).',
        severity: vscode.DiagnosticSeverity.Warning,
        scope: 'etl/steps/data/**'
    },
    {
        // Matches geo.add_region_aggregates (older sibling of add_regions_to_table).
        // The docstring at etl/data_helpers/geo.py:286 says: "use the add_aggregates() method of the Regions class".
        pattern: /geo\.add_region_aggregates\(/g,
        message: '`geo.add_region_aggregates` is outdated. Use `paths.regions.add_aggregates(tb, ...)` instead (auto-resolves regions and income_groups from the DAG).',
        severity: vscode.DiagnosticSeverity.Warning,
        scope: 'etl/steps/data/**'
    },
    {
        // Matches geo.list_countries_in_region.
        // The docstring at etl/data_helpers/geo.py:121 says: "use the get_region() method of the Regions class".
        // The negative lookahead skips list_countries_in_region_that_must_have_data, which has its own warning below.
        pattern: /geo\.list_countries_in_region(?!_that_must_have_data)\(/g,
        message: '`geo.list_countries_in_region` is outdated. Use `paths.regions.get_region(<name>)` instead (auto-resolves regions from the DAG).',
        severity: vscode.DiagnosticSeverity.Warning,
        scope: 'etl/steps/data/**'
    },
    {
        // Matches geo.list_countries_in_region_that_must_have_data.
        // The docstring at etl/data_helpers/geo.py:178 says: "Currently no alternative is implemented." Flag anyway so the
        // call site is visible — users may need to inline the logic or wait for a replacement.
        pattern: /geo\.list_countries_in_region_that_must_have_data\(/g,
        message: '`geo.list_countries_in_region_that_must_have_data` is deprecated and no replacement is currently implemented. Inline the country-selection logic locally and flag this for follow-up.',
        severity: vscode.DiagnosticSeverity.Warning,
        scope: 'etl/steps/data/**'
    },
    {
        // Matches geo.interpolate_table.
        // The docstring at etl/data_helpers/geo.py:695 says: "Use `etl.data_helpers.misc.interpolate_table` instead".
        pattern: /geo\.interpolate_table\(/g,
        message: '`geo.interpolate_table` is outdated. Use `etl.data_helpers.misc.interpolate_table` instead.',
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
    },
    {
        // Matches if __name__ == "__main__" in snapshot files
        // Common patterns:
        // - if __name__ == "__main__":
        // - if __name__=="__main__":
        // - if "__main__" == __name__:
        pattern: /if\s+(__name__|["']__main__["'])\s*==\s*(["']__main__["']|__name__)/g,
        message: '`if __name__ == "__main__"` blocks are outdated in snapshot files. Remove it, as you no longer need it. You can now run snapshots directly with `etls` (or `etl snapshot`) command.',
        severity: vscode.DiagnosticSeverity.Warning,
        scope: 'snapshots/**'
    },
    {
        // Matches .set_index(...) used to finalize a table before create_dataset.
        // Common patterns:
        // - tb = tb.set_index(["country", "year"])
        // - tb = tb.set_index(["disease", "Year"], verify_integrity=False)
        // `Table.format()` is the modern way to set the index — it also sorts rows,
        // validates the key, and normalizes column names/types.
        //
        // The `[^()]*` captures the argument list (which never contains its own parens
        // in a finalize call) and the negative lookahead `(?!\s*[.\[])` skips calls that
        // are chained or subscripted — those are intermediate lookups/reshapes where
        // `format()` is NOT a replacement, e.g.:
        //   - .set_index("country")["pa_nus_prvt_pp"]        (subscript)
        //   - tb_meta.set_index("indicator_code").to_dict("index")  (method chain)
        pattern: /\.set_index\([^()]*\)(?!\s*[.\[])/g,
        message: '`set_index` is outdated for finalizing a table. Use `tb.format()` instead, which sets the index and also sorts rows, checks the key is unique, and normalizes column names/types. `format()` expects `country` and `year` by default; pass custom keys with `tb.format(["disease", "year"])`. For year-less tables use `set_index("country")` plus `tb.metadata.short_name`.',
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
