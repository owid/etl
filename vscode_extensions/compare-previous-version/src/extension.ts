import * as vscode from 'vscode';
import * as fs from 'fs';
import * as path from 'path';

export function activate(context: vscode.ExtensionContext) {
    let disposable = vscode.commands.registerCommand('extension.comparePreviousVersion', async () => {
        const editor = vscode.window.activeTextEditor;
        if (!editor) {
            vscode.window.showErrorMessage('No active editor found!');
            return;
        }

        const currentFilePath = editor.document.uri.fsPath;
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

        // Open diff view with previous version
        await openDiffView(previousVersionPath, currentFilePath);
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

async function openDiffView(previousPath: string, currentPath: string) {
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
            title
        );
    } catch (error) {
        vscode.window.showErrorMessage(`Failed to open diff view: ${error}`);
    }
}

export function deactivate() {}