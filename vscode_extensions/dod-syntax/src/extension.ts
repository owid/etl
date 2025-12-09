import * as vscode from 'vscode';
import { spawn } from 'child_process';
import * as path from 'path';

export function activate(context: vscode.ExtensionContext) {
    console.log('DoD Syntax extension activated.');

    // Show activation message to user
    vscode.window.showInformationMessage('DoD Syntax extension is now active!');

    // Register a test command for debugging
    const testCommand = vscode.commands.registerCommand('dod-syntax.test', async () => {
        const editor = vscode.window.activeTextEditor;
        if (!editor) {
            vscode.window.showErrorMessage('No active editor');
            return;
        }
        
        const dodNames = await fetchAllDodNames();
        vscode.window.showInformationMessage(`DoD Syntax: Loaded ${dodNames.length} DoD names. Extension is working!`);
        console.log('DoD names sample:', dodNames.slice(0, 10));
    });
    context.subscriptions.push(testCommand);

    // Configuration: Set to false to disable caching and always fetch fresh DoDs
    const ENABLE_CACHE = true;

    // Cache for DoD definitions to avoid repeated database calls
    const dodCache = new Map<string, any>();

    // Cache for all DoD names (for autocomplete)
    let allDodNames: string[] = [];
    let dodNamesLoaded = false;

    // Function to fetch multiple DOD definitions from database in batch
    async function fetchDodDefinitions(dodKeys: string[]): Promise<Map<string, any>> {
        const results = new Map<string, any>();
        const keysToFetch: string[] = [];

        // Check cache first if enabled
        if (ENABLE_CACHE) {
            for (const key of dodKeys) {
                if (dodCache.has(key)) {
                    results.set(key, dodCache.get(key));
                } else {
                    keysToFetch.push(key);
                }
            }
        } else {
            keysToFetch.push(...dodKeys);
        }

        if (keysToFetch.length === 0) {
            return results;
        }

        return new Promise((resolve) => {
            // Find the workspace root to locate the Python script
            const workspaceFolder = vscode.workspace.workspaceFolders?.[0];
            if (!workspaceFolder) {
                // Return error results for all keys
                for (const key of keysToFetch) {
                    results.set(key, {
                        success: false,
                        error: 'No workspace folder found',
                        dod_name: key
                    });
                }
                resolve(results);
                return;
            }

            // Path to the Python script
            const scriptPath = path.join(workspaceFolder.uri.fsPath, 'vscode_extensions', 'dod-syntax', 'scripts', 'fetch_dod.py');

            // Try to use the virtual environment Python if available
            const venvPython = path.join(workspaceFolder.uri.fsPath, '.venv', 'bin', 'python3');
            const pythonCmd = require('fs').existsSync(venvPython) ? venvPython : 'python3';

            // Spawn Python process with all keys as arguments
            const pythonProcess = spawn(pythonCmd, [scriptPath, ...keysToFetch], {
                cwd: workspaceFolder.uri.fsPath
            });

            let stdout = '';
            let stderr = '';

            pythonProcess.stdout.on('data', (data) => {
                stdout += data.toString();
            });

            pythonProcess.stderr.on('data', (data) => {
                stderr += data.toString();
            });

            pythonProcess.on('close', (code) => {
                try {
                    if (code === 0 && stdout.trim()) {
                        const batchResult = JSON.parse(stdout.trim());
                        if (batchResult.success && batchResult.dods) {
                            // Process each DOD result
                            for (const [key, dodData] of Object.entries(batchResult.dods)) {
                                results.set(key, dodData);
                                // Cache the result for 5 minutes if caching is enabled
                                if (ENABLE_CACHE) {
                                    dodCache.set(key, dodData);
                                    setTimeout(() => dodCache.delete(key), 5 * 60 * 1000);
                                }
                            }
                        } else {
                            // Handle batch error
                            for (const key of keysToFetch) {
                                results.set(key, {
                                    success: false,
                                    error: batchResult.error || 'Unknown batch error',
                                    dod_name: key
                                });
                            }
                        }
                        resolve(results);
                    } else {
                        // Handle process error for all keys
                        for (const key of keysToFetch) {
                            results.set(key, {
                                success: false,
                                error: stderr || `Python process exited with code ${code}`,
                                dod_name: key
                            });
                        }
                        resolve(results);
                    }
                } catch (parseError) {
                    // Handle parse error for all keys
                    for (const key of keysToFetch) {
                        results.set(key, {
                            success: false,
                            error: `Failed to parse JSON response: ${parseError}`,
                            dod_name: key,
                            raw_output: stdout
                        });
                    }
                    resolve(results);
                }
            });

            pythonProcess.on('error', (error) => {
                // Handle spawn error for all keys
                for (const key of keysToFetch) {
                    results.set(key, {
                        success: false,
                        error: `Failed to spawn Python process: ${error.message}`,
                        dod_name: key
                    });
                }
                resolve(results);
            });
        });
    }

    // Backward compatibility: Single DoD fetch function
    async function fetchDodDefinition(dodKey: string): Promise<any> {
        const results = await fetchDodDefinitions([dodKey]);
        return results.get(dodKey);
    }

    // Function to fetch all DoD names for autocomplete
    async function fetchAllDodNames(): Promise<string[]> {
        if (dodNamesLoaded) {
            return allDodNames;
        }

        return new Promise((resolve) => {
            const workspaceFolder = vscode.workspace.workspaceFolders?.[0];
            if (!workspaceFolder) {
                resolve([]);
                return;
            }

            const scriptPath = path.join(workspaceFolder.uri.fsPath, 'vscode_extensions', 'dod-syntax', 'scripts', 'fetch_dod.py');
            const venvPython = path.join(workspaceFolder.uri.fsPath, '.venv', 'bin', 'python3');
            const pythonCmd = require('fs').existsSync(venvPython) ? venvPython : 'python3';

            const pythonProcess = spawn(pythonCmd, [scriptPath, '--names'], {
                cwd: workspaceFolder.uri.fsPath
            });

            let stdout = '';
            let stderr = '';

            pythonProcess.stdout.on('data', (data) => {
                stdout += data.toString();
            });

            pythonProcess.stderr.on('data', (data) => {
                stderr += data.toString();
            });

            pythonProcess.on('close', (code) => {
                try {
                    if (code === 0 && stdout.trim()) {
                        const result = JSON.parse(stdout.trim());
                        if (result.success && result.dod_names) {
                            allDodNames = result.dod_names;
                            dodNamesLoaded = true;
                            console.log(`Loaded ${allDodNames.length} DoD names for autocomplete`);
                            resolve(allDodNames);
                        } else {
                            console.error('Failed to fetch DoD names:', result.error);
                            resolve([]);
                        }
                    } else {
                        console.error('Failed to fetch DoD names:', stderr || `Process exited with code ${code}`);
                        resolve([]);
                    }
                } catch (parseError) {
                    console.error('Failed to parse DoD names response:', parseError);
                    resolve([]);
                }
            });

            pythonProcess.on('error', (error) => {
                console.error('Failed to spawn Python process for DoD names:', error);
                resolve([]);
            });
        });
    }

    // Create decoration type for DoD references with underline
    const dodDecorationType = vscode.window.createTextEditorDecorationType({
        textDecoration: 'underline',
        color: '#0078d4', // VS Code blue color
        cursor: 'pointer'
    });

    // Completion provider for DoD autocomplete
    const completionProvider: vscode.CompletionItemProvider = {
        async provideCompletionItems(document: vscode.TextDocument, position: vscode.Position): Promise<vscode.CompletionItem[]> {
            console.log(`DoD Autocomplete: triggered for ${document.languageId} file`);
            
            // Only provide completions in YAML and Python files
            const languageId = document.languageId;
            if (languageId !== 'yaml' && languageId !== 'python') {
                console.log(`DoD Autocomplete: skipping non-YAML/Python file (${languageId})`);
                return [];
            }

            const line = document.lineAt(position.line);
            const textBeforeCursor = line.text.substring(0, position.character);
            console.log(`DoD Autocomplete: text before cursor: "${textBeforeCursor}"`);

            // Check if we're typing a DoD reference - must have #dod: to trigger
            const dodMatch = textBeforeCursor.match(/#dod:([^)]*)$/);
            if (!dodMatch) {
                console.log(`DoD Autocomplete: no #dod: pattern match`);
                return [];
            }
            
            console.log(`DoD Autocomplete: found #dod pattern match:`, dodMatch);

            // For Python files, ensure we're inside a string
            if (languageId === 'python') {
                const absolutePosition = document.offsetAt(position);
                if (!isInPythonString(document, absolutePosition)) {
                    return [];
                }
            }

            // Get the filter text (what user typed after ":")
            const filterText = dodMatch[1] || '';
            console.log(`DoD Autocomplete: filter text: "${filterText}"`);

            // Fetch all DoD names
            const dodNames = await fetchAllDodNames();
            console.log(`DoD Autocomplete: loaded ${dodNames.length} DoD names`);

            // Filter DoD names based on what user typed
            const filteredNames = dodNames.filter(name =>
                name.toLowerCase().includes(filterText.toLowerCase())
            );
            console.log(`DoD Autocomplete: filtered to ${filteredNames.length} names`);

            // Limit to first 50 items for testing (VS Code might hide too many)
            const limitedNames = filteredNames.slice(0, 50);

            // Create completion items
            const completionItems = limitedNames.map(dodName => {
                // Create a readable title from the DoD name
                const title = dodName
                    .replace(/_/g, ' ')           // Replace underscores with spaces
                    .replace(/-/g, ' ')           // Replace hyphens with spaces
                    .replace(/\b\w/g, l => l.toUpperCase()); // Capitalize first letter of each word

                // Use the title as the label (what shows in dropdown)
                const item = new vscode.CompletionItem(title, vscode.CompletionItemKind.Text);

                // Set the text that will be inserted
                const insertText = `[${title}](#dod:${dodName})`;
                item.insertText = insertText;

                // Set the range to replace from #dod: onwards
                const startOfDod = textBeforeCursor.lastIndexOf('#dod:');
                const range = new vscode.Range(
                    position.line,
                    startOfDod,
                    position.line,
                    position.character
                );
                item.range = range;

                // Set documentation and details
                item.detail = `Insert DoD reference for ${dodName}`;
                
                // Simpler documentation
                item.documentation = `Inserts: ${insertText}`;

                // Set sort text to prioritize exact matches
                if (dodName.toLowerCase().startsWith(filterText.toLowerCase())) {
                    item.sortText = `0_${dodName}`;
                } else {
                    item.sortText = `1_${dodName}`;
                }

                // Make sure it's committed on enter
                item.commitCharacters = ['\t', ' '];

                console.log(`DoD Autocomplete: Created item "${title}" for key "${dodName}"`);
                return item;
            });

            console.log(`DoD Autocomplete: Returning ${completionItems.length} completion items`);
            
            // Log a sample of what we're returning for debugging
            if (completionItems.length > 0) {
                const sample = completionItems[0];
                console.log(`DoD Autocomplete: Sample item:`, {
                    label: sample.label,
                    insertText: sample.insertText,
                    kind: sample.kind,
                    detail: sample.detail,
                    range: sample.range
                });
            }
            
            return completionItems;
        }
    };

    // Hover provider for DOD references
    const hoverProvider: vscode.HoverProvider = {
        async provideHover(document: vscode.TextDocument, position: vscode.Position): Promise<vscode.Hover | undefined> {
            // Only process YAML and Python files
            const languageId = document.languageId;
            if (languageId !== 'yaml' && languageId !== 'python') {
                return undefined;
            }

            // Regex to match [title](#dod:key) pattern
            const dodRegex = /\[([^\]]+)\]\(#dod:([^)]+)\)/g;
            const line = document.lineAt(position.line);
            const lineText = line.text;

            let match: RegExpExecArray | null;
            while ((match = dodRegex.exec(lineText)) !== null) {
                const matchStart = match.index;
                const matchEnd = match.index + match[0].length;

                // Check if the cursor position is within this match
                if (position.character >= matchStart && position.character <= matchEnd) {
                    // For Python files, only show hover if inside strings (raw or regular)
                    if (languageId === 'python') {
                        const absolutePosition = document.offsetAt(new vscode.Position(position.line, matchStart));
                        if (!isInPythonString(document, absolutePosition)) {
                            continue;
                        }
                    }

                    const title = match[1];
                    const key = match[2];

                    // Create range for the entire match
                    const range = new vscode.Range(
                        position.line,
                        matchStart,
                        position.line,
                        matchEnd
                    );

                    // Try to get from cache first
                    const cachedResult = dodCache.get(key);
                    if (cachedResult) {
                        const hoverContent = new vscode.MarkdownString();
                        hoverContent.appendMarkdown(`**${title}**\n\n`);
                        hoverContent.appendMarkdown(`*DoD Key:* \`${key}\`\n\n`);

                        // Handle both old single result format and new batch result format
                        if (cachedResult.success !== false && cachedResult.content) {
                            // New batch format: DOD data directly in the result
                            hoverContent.appendMarkdown(`**Definition:**\n\n${cachedResult.content}\n\n`);
                            if (cachedResult.lastUpdatedBy) {
                                hoverContent.appendMarkdown(`*Last updated by:* ${cachedResult.lastUpdatedBy}\n`);
                            }
                            if (cachedResult.updatedAt) {
                                const updateDate = new Date(cachedResult.updatedAt).toLocaleDateString();
                                hoverContent.appendMarkdown(`*Updated:* ${updateDate}\n`);
                            }
                        } else if (cachedResult.success && cachedResult.dod) {
                            // Old single result format: DOD data in nested 'dod' property
                            const dod = cachedResult.dod;
                            hoverContent.appendMarkdown(`**Definition:**\n\n${dod.content}\n\n`);
                            if (dod.lastUpdatedBy) {
                                hoverContent.appendMarkdown(`*Last updated by:* ${dod.lastUpdatedBy}\n`);
                            }
                            if (dod.updatedAt) {
                                const updateDate = new Date(dod.updatedAt).toLocaleDateString();
                                hoverContent.appendMarkdown(`*Updated:* ${updateDate}\n`);
                            }
                        } else {
                            hoverContent.appendMarkdown(`**Definition not available**\n\n`);
                            hoverContent.appendMarkdown('*Database not accessible. The full definition for this DOD key is stored in the OWID database.*');
                        }

                        return new vscode.Hover(hoverContent, range);
                    } else {
                        // If not in cache, start async fetch and show a simple message
                        const loadingContent = new vscode.MarkdownString();
                        loadingContent.appendMarkdown(`**${title}**\n\n`);
                        loadingContent.appendMarkdown(`*DoD Key:* \`${key}\`\n\n`);
                        loadingContent.appendMarkdown('*Definition not yet loaded. Please try hovering again in a moment.*');

                        // Start async fetch for next time
                        fetchDodDefinition(key).then(result => {
                            if (!result.success) {
                                console.log(`DoD fetch failed for ${key}:`, result.error);
                            }
                        });

                        return new vscode.Hover(loadingContent, range);
                    }
                }
            }

            return undefined;
        }
    };

    // Function to check if a position is within a Python string (raw or regular)
    function isInPythonString(document: vscode.TextDocument, position: number): boolean {
        const text = document.getText();
        const beforeText = text.substring(0, position);

        // Find all string patterns before this position (both raw and regular strings)
        const stringRegex = /r?('''|"""|'|")/g;
        let match: RegExpExecArray | null;
        let inString = false;
        let stringDelimiter = '';

        while ((match = stringRegex.exec(beforeText)) !== null) {
            const delimiter = match[1];

            if (!inString) {
                // Starting a string
                inString = true;
                stringDelimiter = delimiter;
            } else if (delimiter === stringDelimiter) {
                // Ending the current string
                inString = false;
                stringDelimiter = '';
            }
        }

        return inString;
    }

    // Function to update decorations in the active editor
    function updateDecorations(editor: vscode.TextEditor) {
        if (!editor) {
            return;
        }

        const languageId = editor.document.languageId;
        console.log(`DoD Syntax: Processing file with language: ${languageId}`);

        if (languageId !== 'yaml' && languageId !== 'python') {
            console.log(`DoD Syntax: Skipping file with language: ${languageId}`);
            return;
        }

        const decorations: vscode.DecorationOptions[] = [];
        const text = editor.document.getText();
        const dodRegex = /\[([^\]]+)\]\(#dod:([^)]+)\)/g;

        let match: RegExpExecArray | null;
        let matchCount = 0;

        while ((match = dodRegex.exec(text)) !== null) {
            matchCount++;
            console.log(`DoD Syntax: Found match ${matchCount}: ${match[0]} at position ${match.index}`);

            // For Python files, only
            //  if inside strings (raw or regular)
            if (languageId === 'python') {
                const inString = isInPythonString(editor.document, match.index);
                console.log(`DoD Syntax: Match ${matchCount} in string: ${inString}`);
                if (!inString) {
                    console.log(`DoD Syntax: Skipping match ${matchCount} (not in string)`);
                    continue;
                }
            }

            const startPos = editor.document.positionAt(match.index);
            const endPos = editor.document.positionAt(match.index + match[0].length);
            const range = new vscode.Range(startPos, endPos);

            const title = match[1];
            const key = match[2];

            decorations.push({
                range,
                hoverMessage: `DoD Reference: ${title} (${key})`
            });

            console.log(`DoD Syntax: Added decoration for: [${title}](#dod:${key})`);
        }

        console.log(`DoD Syntax: Applied ${decorations.length} decorations`);
        editor.setDecorations(dodDecorationType, decorations);
    }

    // Register hover provider for YAML and Python files
    context.subscriptions.push(
        vscode.languages.registerHoverProvider({ language: 'yaml', scheme: 'file' }, hoverProvider)
    );
    context.subscriptions.push(
        vscode.languages.registerHoverProvider({ language: 'python', scheme: 'file' }, hoverProvider)
    );

    // Register completion provider for DoD autocomplete with ALL possible trigger characters
    context.subscriptions.push(
        vscode.languages.registerCompletionItemProvider(
            { language: 'yaml', scheme: 'file' },
            completionProvider,
            '#', ':', 'd', 'o' // All trigger characters to ensure it triggers
        )
    );
    context.subscriptions.push(
        vscode.languages.registerCompletionItemProvider(
            { language: 'python', scheme: 'file' },
            completionProvider,
            '#', ':', 'd', 'o' // All trigger characters to ensure it triggers
        )
    );

    // Add a manual trigger command for autocomplete
    const triggerAutocompleteCommand = vscode.commands.registerCommand('dod-syntax.triggerAutocomplete', async () => {
        const editor = vscode.window.activeTextEditor;
        if (!editor) {
            vscode.window.showErrorMessage('No active editor');
            return;
        }
        
        // Manually trigger completion at current position
        await vscode.commands.executeCommand('editor.action.triggerSuggest');
    });
    context.subscriptions.push(triggerAutocompleteCommand);

    // Update decorations when the active editor changes
    context.subscriptions.push(
        vscode.window.onDidChangeActiveTextEditor(editor => {
            if (editor) {
                updateDecorations(editor);
                preloadDodDefinitions(editor);
            }
        })
    );

    // Update decorations when document content changes
    context.subscriptions.push(
        vscode.workspace.onDidChangeTextDocument(event => {
            const editor = vscode.window.activeTextEditor;
            if (editor && editor.document === event.document) {
                updateDecorations(editor);
                preloadDodDefinitions(editor);
            }
        })
    );

    // Update decorations for the current active editor
    if (vscode.window.activeTextEditor) {
        updateDecorations(vscode.window.activeTextEditor);
        preloadDodDefinitions(vscode.window.activeTextEditor);
    }

    // Preload DoD names for autocomplete in the background
    fetchAllDodNames().then(names => {
        console.log(`DoD Syntax: Preloaded ${names.length} DoD names for autocomplete`);
    }).catch(error => {
        console.error('DoD Syntax: Failed to preload DoD names:', error);
    });

    // Function to preload DOD definitions found in the current file
    async function preloadDodDefinitions(editor: vscode.TextEditor) {
        const languageId = editor.document.languageId;
        if (languageId !== 'yaml' && languageId !== 'python') {
            return;
        }

        const text = editor.document.getText();
        const dodRegex = /\[([^\]]+)\]\(#dod:([^)]+)\)/g;
        const dodKeys = new Set<string>();

        let match: RegExpExecArray | null;
        while ((match = dodRegex.exec(text)) !== null) {
            // For Python files, only consider matches inside strings
            if (languageId === 'python') {
                const inString = isInPythonString(editor.document, match.index);
                if (!inString) {
                    continue;
                }
            }

            const key = match[2];
            dodKeys.add(key);
        }

        // Preload all DOD definitions found in the file using batch processing
        const keysToPreload = Array.from(dodKeys).filter(key =>
            !ENABLE_CACHE || !dodCache.has(key)
        );

        if (keysToPreload.length > 0) {
            console.log(`Preloading ${keysToPreload.length} DoD definitions: ${keysToPreload.join(', ')}`);
            await fetchDodDefinitions(keysToPreload);
            console.log(`Successfully preloaded ${keysToPreload.length} DoD definitions`);
        } else {
            console.log(`All ${dodKeys.size} DoD definitions already cached`);
        }
    }
}

export function deactivate() { }
