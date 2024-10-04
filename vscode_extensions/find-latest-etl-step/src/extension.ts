import * as vscode from 'vscode';
import * as fs from 'fs';
import * as path from 'path';
import { debounce } from 'lodash'; // Install lodash: npm install lodash
import ignore from 'ignore'; // Install ignore: npm install ignore

export function activate(context: vscode.ExtensionContext) {
    let cachedFiles: { path: string, date: Date | 'latest' }[] = [];

    let disposable = vscode.commands.registerCommand('extension.findLatestETLStep', async () => {
        const workspaceFolder = vscode.workspace.workspaceFolders?.[0].uri.fsPath;
        if (!workspaceFolder) {
            vscode.window.showErrorMessage('No workspace folder found!');
            return;
        }

        // Load .gitignore or other ignore files
        const ig = ignore();
        const gitignorePath = path.join(workspaceFolder, '.gitignore');
        if (fs.existsSync(gitignorePath)) {
            const gitignoreContent = fs.readFileSync(gitignorePath).toString();
            ig.add(gitignoreContent);
        }

        vscode.window.withProgress(
            {
                location: vscode.ProgressLocation.Notification,
                title: "Indexing ETL files...",
                cancellable: false
            },
            async () => {
                cachedFiles = findFiles(workspaceFolder, ig);
            }
        );

        const quickPick = vscode.window.createQuickPick();
        quickPick.placeholder = 'Type to filter files by name...';
        quickPick.matchOnDescription = true;
        quickPick.matchOnDetail = true;

        const debouncedSearch = debounce((filter: string) => {
            if (filter) {
                const filteredFiles = cachedFiles.filter(file =>
                    file.path.includes(filter)
                );

                if (filteredFiles.length > 0) {
                    // Find the latest date and check for 'latest' versions among the filtered files
                    const latestDate = filteredFiles.reduce((maxDate, file) => {
                        if (file.date === 'latest') return maxDate; // Ignore 'latest' for this comparison
                        return file.date > maxDate ? file.date : maxDate;
                    }, new Date(0)); // Start with a very old date

                    // Filter files that either have the latest date or contain 'latest'
                    const latestFiles = filteredFiles.filter(file =>
                        (file.date instanceof Date && file.date.getTime() === latestDate.getTime()) || file.date === 'latest'
                    );

                    // Sort the files by path
                    const sortedFiles = latestFiles.sort((a, b) => a.path.localeCompare(b.path));

                    // Display only files with the latest date or 'latest'
                    quickPick.items = sortedFiles.map(file => ({
                        label: path.relative(workspaceFolder, file.path),  // Show the relative path
                        description: '',  // Empty description to avoid redundancy
                        detail: file.date === 'latest' ? 'Latest Version' : (file.date as Date).toDateString(),  // Date or 'latest' detail
                    }));
                } else {
                    quickPick.items = []; // Clear results if no files match the filter
                }
            } else {
                quickPick.items = []; // Clear results if no input
            }
        }, 300); // 300ms debounce delay

        quickPick.onDidChangeValue((filter) => {
            debouncedSearch(filter);
        });

        quickPick.onDidChangeSelection(async (selection) => {
            if (selection[0] && selection[0].label) {
                const selectedFilePath = path.join(workspaceFolder, selection[0].label);  // Ensure full path
                if (selectedFilePath) {
                    try {
                        const document = await vscode.workspace.openTextDocument(selectedFilePath);
                        vscode.window.showTextDocument(document);
                        quickPick.hide();
                    } catch (err) {
                        vscode.window.showErrorMessage(`Failed to open the file: ${err}`);
                    }
                }
            }
        });

        quickPick.show();
    });

    context.subscriptions.push(disposable);
}

// Find files and respect ignore patterns
function findFiles(dir: string, ig: any): { path: string, date: Date | 'latest' }[] {
    let results: { path: string, date: Date | 'latest' }[] = [];
    const list = fs.readdirSync(dir);

    list.forEach(file => {
        const filePath = path.join(dir, file);
        const stat = fs.statSync(filePath);

        // Exclude 'etl/data' folder explicitly and ignore '__pycache__' and other similar folders
        if (filePath.includes(path.join('etl', 'data')) || filePath.includes('__pycache__')) {
            return;
        }

        if (stat && stat.isDirectory()) {
            results = results.concat(findFiles(filePath, ig));
        } else {
            // Check if the file is ignored based on the ignore rules
            const relativePath = path.relative(dir, filePath);
            if (!ig.ignores(relativePath)) {
                const dateMatch = filePath.match(/(\d{4}-\d{2}-\d{2})/);
                if (dateMatch) {
                    const fileDate = new Date(dateMatch[1]);
                    results.push({ path: filePath, date: fileDate });
                } else if (filePath.includes('latest')) {
                    results.push({ path: filePath, date: 'latest' });
                }
            }
        }
    });

    return results;
}

export function deactivate() {}
