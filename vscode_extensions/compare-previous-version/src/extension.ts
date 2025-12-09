import * as vscode from 'vscode';
import * as fs from 'fs';
import * as path from 'path';

// Track diff state per file path
const diffStateMap = new Map<string, { originalUri: vscode.Uri, diffTitle: string }>();

export function activate(context: vscode.ExtensionContext) {
    let disposable = vscode.commands.registerCommand('extension.comparePreviousVersion', async () => {
        const editor = vscode.window.activeTextEditor;
        if (!editor) {
            vscode.window.showErrorMessage('No active editor found!');
            return;
        }

        const currentUri = editor.document.uri;
        const currentFilePath = currentUri.fsPath;

        // Check if this is a diff view we created - if so, toggle back
        if (isDiffViewForOurFile(editor)) {
            // Find the original file from our state map
            const originalData = findOriginalFromDiff(editor);
            if (originalData) {
                // Close current diff and open original file in the same editor group
                const viewColumn = editor.viewColumn;
                await vscode.commands.executeCommand('workbench.action.closeActiveEditor');
                await vscode.window.showTextDocument(originalData.originalUri, { viewColumn });
                
                // Clean up state
                diffStateMap.delete(originalData.originalUri.fsPath);
                return;
            }
        }

        // Not in a diff view, so open diff for current file
        const workspaceFolder = vscode.workspace.workspaceFolders?.[0].uri.fsPath;
        if (!workspaceFolder) {
            vscode.window.showErrorMessage('No workspace folder found!');
            return;
        }

        // Find the previous version of this file
        const previousVersionPath = await vscode.window.withProgress(
            {
                location: vscode.ProgressLocation.Notification,
                title: "Finding previous version...",
                cancellable: false
            },
            async () => {
                return findPreviousVersion(workspaceFolder, currentFilePath);
            }
        );

        if (!previousVersionPath) {
            const fileName = path.basename(currentFilePath);
            vscode.window.showInformationMessage(`No previous version found for "${fileName}".`);
            return;
        }

        // Open diff view with previous version in current editor group
        const diffTitle = await openDiffView(previousVersionPath, currentFilePath, editor.viewColumn);
        
        // Store state for toggle functionality
        diffStateMap.set(currentFilePath, { originalUri: currentUri, diffTitle });
    });

    context.subscriptions.push(disposable);
}

function findPreviousVersion(workspaceDir: string, currentFilePath: string): string | null {
    const relativePath = path.relative(workspaceDir, currentFilePath);
    const pathParts = relativePath.split(path.sep);
    
    // Find the date version in the path (format YYYY-MM-DD)
    let versionIndex = -1;
    let currentVersion = '';
    
    for (let i = 0; i < pathParts.length; i++) {
        const part = pathParts[i];
        // Match date format YYYY-MM-DD
        if (/^\d{4}-\d{2}-\d{2}$/.test(part)) {
            versionIndex = i;
            currentVersion = part;
            break;
        }
    }
    
    if (versionIndex === -1) {
        return null; // No version found in path
    }
    
    // Get the parent directory that contains version folders
    const versionParentParts = pathParts.slice(0, versionIndex);
    const versionParentPath = path.join(workspaceDir, ...versionParentParts);
    
    if (!fs.existsSync(versionParentPath)) {
        return null;
    }
    
    try {
        // Get all subdirectories that are valid date versions
        const entries = fs.readdirSync(versionParentPath);
        const validVersions: string[] = [];
        
        for (const entry of entries) {
            const entryPath = path.join(versionParentPath, entry);
            const stat = fs.statSync(entryPath);
            
            if (stat.isDirectory() && /^\d{4}-\d{2}-\d{2}$/.test(entry)) {
                validVersions.push(entry);
            }
        }
        
        // Sort versions and find the highest one that's lower than current
        validVersions.sort();
        const currentVersionIndex = validVersions.indexOf(currentVersion);
        
        if (currentVersionIndex <= 0) {
            return null; // Current version is the first/only one, or not found
        }
        
        // Look backwards through versions to find one that contains the same file
        for (let i = currentVersionIndex - 1; i >= 0; i--) {
            const candidateVersion = validVersions[i];
            
            // Construct the path to the candidate version file
            const candidateVersionParts = [...versionParentParts, candidateVersion, ...pathParts.slice(versionIndex + 1)];
            const candidateVersionPath = path.join(workspaceDir, ...candidateVersionParts);
            
            // Check if the file actually exists in this version
            if (fs.existsSync(candidateVersionPath)) {
                return candidateVersionPath;
            }
        }
        
        return null; // No previous version contains this file
    } catch (error) {
        // Log error but don't break the extension
        return null;
    }
}

function isDiffViewForOurFile(editor: vscode.TextEditor): boolean {
    // Check if this editor's document title matches any of our tracked diff titles
    const tabTitle = vscode.window.tabGroups.activeTabGroup.activeTab?.label || '';
    
    for (const [_, state] of diffStateMap) {
        if (tabTitle === state.diffTitle) {
            return true;
        }
    }
    
    return false;
}

function findOriginalFromDiff(editor: vscode.TextEditor): { originalUri: vscode.Uri } | null {
    const tabTitle = vscode.window.tabGroups.activeTabGroup.activeTab?.label || '';
    
    for (const [filePath, state] of diffStateMap) {
        if (tabTitle === state.diffTitle) {
            return { originalUri: state.originalUri };
        }
    }
    
    return null;
}

async function openDiffView(previousPath: string, currentPath: string, viewColumn?: vscode.ViewColumn): Promise<string> {
    try {
        const previousUri = vscode.Uri.file(previousPath);
        const currentUri = vscode.Uri.file(currentPath);
        
        const fileName = path.basename(currentPath);
        
        // Extract version info for title
        const previousRelative = vscode.workspace.asRelativePath(previousPath);
        const currentRelative = vscode.workspace.asRelativePath(currentPath);
        
        // Extract versions from paths
        const previousVersion = previousRelative.match(/\d{4}-\d{2}-\d{2}/)?.[0] || 'previous';
        const currentVersion = currentRelative.match(/\d{4}-\d{2}-\d{2}/)?.[0] || 'current';
        
        const title = `${fileName}: ${previousVersion} ↔ ${currentVersion}`;
        
        await vscode.commands.executeCommand(
            'vscode.diff',
            previousUri,
            currentUri,
            title,
            { viewColumn } // Force to open in specified column
        );
        
        return title;
    } catch (error) {
        vscode.window.showErrorMessage(`Failed to open diff view: ${error}`);
        return '';
    }
}

export function deactivate() {}