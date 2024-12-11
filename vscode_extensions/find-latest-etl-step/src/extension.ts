import * as vscode from 'vscode';
import * as fs from 'fs';
import * as path from 'path';
import { debounce } from 'lodash'; // Install lodash: npm install lodash
import ignore from 'ignore'; // Install ignore: npm install ignore

// Define a custom QuickPickItem type that includes the originalPath
interface ETLQuickPickItem extends vscode.QuickPickItem {
    originalPath: string;
}

export function activate(context: vscode.ExtensionContext) {
    let cachedFiles: { path: string, date: Date | 'latest', originalPath: string }[] = [];

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

        const quickPick = vscode.window.createQuickPick<ETLQuickPickItem>();
        quickPick.placeholder = 'Type to filter files by name...';
        quickPick.matchOnDescription = true;
        quickPick.matchOnDetail = true;

        const debouncedSearch = debounce((filter: string) => {
            if (filter) {
                const filteredFiles = cachedFiles.filter(file =>
                    file.path.includes(filter)
                );

                if (filteredFiles.length > 0) {
                    const latestDate = filteredFiles.reduce((maxDate, file) => {
                        if (file.date === 'latest') {return maxDate;}
                        return file.date > maxDate ? file.date : maxDate;
                    }, new Date(0));

                    const latestFiles = filteredFiles.filter(file =>
                        (file.date instanceof Date && file.date.getTime() === latestDate.getTime()) || file.date === 'latest'
                    );

                    quickPick.items = latestFiles.map(file => {
                        const relativePath = path.relative(workspaceFolder, file.path);
                        const displayedPath = relativePath
                            .replace(/^etl\/steps\/data\//, '')
                            .replace(/^etl\/steps\/export\//, '');
                        return {
                            label: displayedPath,
                            description: '',
                            originalPath: file.path, // Store the original full path
                        };
                    });
                } else {
                    quickPick.items = [];
                }
            } else {
                quickPick.items = [];
            }
        }, 300);

        quickPick.onDidChangeValue((filter) => {
            debouncedSearch(filter);
        });

        quickPick.onDidChangeSelection(async (selection) => {
            const selectedItem = selection[0] as ETLQuickPickItem;  // Use the custom type
            if (selectedItem && selectedItem.originalPath) {
                const selectedFilePath = selectedItem.originalPath;  // Use the full original path
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

function findFiles(dir: string, ig: any): { path: string, date: Date | 'latest', originalPath: string }[] {
    let results: { path: string, date: Date | 'latest', originalPath: string }[] = [];
    const list = fs.readdirSync(dir);

    list.forEach(file => {
        const filePath = path.join(dir, file);
        const stat = fs.statSync(filePath);

        const excludeFolders = [
            path.join('etl', 'data'),
            path.join('etl', 'export'),
            path.join('snapshots', 'backport')
        ];

        if (excludeFolders.some(excludeFolder => filePath.includes(excludeFolder)) || filePath.includes('__pycache__')) {
            return;
        }

        if (stat && stat.isDirectory()) {
            results = results.concat(findFiles(filePath, ig));
        } else {
            const relativePath = path.relative(dir, filePath);
            if (!ig.ignores(relativePath)) {
                const dateMatch = filePath.match(/(\d{4}-\d{2}-\d{2})/);
                if (dateMatch) {
                    const fileDate = new Date(dateMatch[1]);
                    results.push({ path: filePath, date: fileDate, originalPath: filePath });
                } else if (filePath.includes('latest')) {
                    results.push({ path: filePath, date: 'latest', originalPath: filePath });
                }
            }
        }
    });

    return results;
}

export function deactivate() {}
