import * as vscode from 'vscode';
import * as fs from 'fs';
import * as path from 'path';
import ignore from 'ignore';

export function activate(context: vscode.ExtensionContext) {
    let cachedFiles: { path: string, date: Date | null }[] = [];

    let disposable = vscode.commands.registerCommand('extension.findLatestETLStep', async () => {
        const workspaceFolder = vscode.workspace.workspaceFolders?.[0].uri.fsPath;
        if (!workspaceFolder) {
            vscode.window.showErrorMessage('No workspace folder found!');
            return;
        }

        const ig = ignore();
        const gitignorePath = path.join(workspaceFolder, '.gitignore');
        if (fs.existsSync(gitignorePath)) {
            const gitignoreContent = fs.readFileSync(gitignorePath).toString();
            ig.add(gitignoreContent);
        }

        await vscode.window.withProgress(
            {
                location: vscode.ProgressLocation.Notification,
                title: "Indexing ETL files...",
                cancellable: false
            },
            async () => {
                cachedFiles = findFiles(workspaceFolder, ig, workspaceFolder);
            }
        );

        const quickPick = vscode.window.createQuickPick();
        quickPick.placeholder = 'Type to filter files by name';

        // Sort files by date in descending order based on the extracted date from the file path
        const sortedFiles = cachedFiles
            .filter(file => file.date !== null)
            .sort((a, b) => {
                if (a.date && b.date) {
                    return b.date.getTime() - a.date.getTime(); // Sort by descending order of date
                }
                return 0;
            });

        quickPick.items = sortedFiles.map(file => ({
            label: path.relative(workspaceFolder, file.path),
            description: file.date ? file.date.toISOString().slice(0, 10) : ''
        }));

        quickPick.onDidChangeSelection(selection => {
            if (selection[0]) {
                const fileUri = vscode.Uri.file(path.join(workspaceFolder, selection[0].label));
                vscode.workspace.openTextDocument(fileUri).then(doc => vscode.window.showTextDocument(doc));
            }
        });

        quickPick.show();
    });

    context.subscriptions.push(disposable);
}

function findFiles(dir: string, ig: ReturnType<typeof ignore>, baseDir: string): { path: string, date: Date | null }[] {
    let results: { path: string, date: Date | null }[] = [];

    const files = fs.readdirSync(dir);
    for (const file of files) {
        const fullPath = path.join(dir, file);

        // Check if the file path is ignored by .gitignore
        if (ig.ignores(path.relative(baseDir, fullPath))) {
            continue;
        }

        const stat = fs.lstatSync(fullPath);

        if (stat.isDirectory()) {
            results = results.concat(findFiles(fullPath, ig, baseDir));
        } else {
            const extractedDate = extractDateFromPath(fullPath);
            results.push({ path: fullPath, date: extractedDate });
        }
    }

    return results;
}

function extractDateFromPath(filePath: string): Date | null {
    // Regex to match date in format YYYY-MM-DD in the file path
    const dateRegex = /(\d{4}-\d{2}-\d{2})/;
    const match = filePath.match(dateRegex);

    if (match) {
        return new Date(match[1]); // Convert the matched date string to a Date object
    }

    return null;
}
