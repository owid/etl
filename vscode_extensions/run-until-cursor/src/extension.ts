import * as vscode from "vscode";

export function activate(context: vscode.ExtensionContext) {
    let disposable = vscode.commands.registerCommand("extension.runUntilCursor", async () => {
        const editor = vscode.window.activeTextEditor;
        if (!editor) {
            vscode.window.showErrorMessage("No active editor found.");
            return;
        }

        const document = editor.document;
        let cursorLine = editor.selection.active.line;
        let startLine: number | null = null;
        let endLine: number | null = null;

        // Read timeout values from VS Code settings (default: 100ms and 500ms)
        const execDelay = vscode.workspace.getConfiguration("runUntilCursor").get<number>("execDelay", 100);
        const cursorDelay = vscode.workspace.getConfiguration("runUntilCursor").get<number>("cursorDelay", 500);

        // Step 1: Find where `run()` starts
        for (let i = 0; i < document.lineCount; i++) {
            if (/^\s*def run\(/.test(document.lineAt(i).text)) {
                startLine = i + 1; // Start selection after `def run():`
                break;
            }
        }

        if (startLine !== null) {
            // Step 2: Find where `run()` ends (detecting **non-indented** lines)
            for (let i = startLine; i < document.lineCount; i++) {
                const lineText = document.lineAt(i).text;

                // If we find a **non-indented line** (not empty & not a comment), assume `run()` ends here
                if (lineText.trim().length > 0 && !lineText.startsWith(" ") && !lineText.startsWith("\t")) {
                    endLine = i;
                    break;
                }
            }

            // If no non-indented line was found, assume `run()` ends at the last line
            if (endLine === null) {
                endLine = document.lineCount;
            }
        }

        let executeRange: vscode.Range;
        let shouldMoveCursor = false;

        if (startLine !== null && endLine !== null) {
            // If inside `run()`, execute up to the cursor
            if (cursorLine >= startLine && cursorLine < endLine) {
                executeRange = new vscode.Range(startLine, 0, cursorLine + 1, 0);
            }
            // If outside `run()` (either above or below), execute **the entire script** (without `if __name__ == "__main__":`)
            else {
                executeRange = filterOutMainBlock(document);
                shouldMoveCursor = true;
            }
        } else {
            // If `run()` is not found, execute everything (without `if __name__ == "__main__":`)
            executeRange = filterOutMainBlock(document);
        }

        // Step 3: Apply selection and execute immediately (with an adjustable delay)
        editor.selection = new vscode.Selection(executeRange.start, executeRange.end);

        setTimeout(async () => {
            await vscode.commands.executeCommand("jupyter.execSelectionInteractive");
        }, execDelay); // User-configurable execution delay

        // Step 4: Move cursor inside `run()` **only if the cursor was outside**
        if (shouldMoveCursor && startLine !== null) {
            setTimeout(() => {
                const newPosition = new vscode.Position(startLine, 0);
                editor.selection = new vscode.Selection(newPosition, newPosition);
                editor.revealRange(new vscode.Range(newPosition, newPosition));
            }, cursorDelay); // User-configurable cursor movement delay
        }
    });

    context.subscriptions.push(disposable);
}

export function deactivate() {}

/**
 * Filters out `if __name__ == "__main__":` and its indented block.
 */
function filterOutMainBlock(document: vscode.TextDocument): vscode.Range {
    let mainStart: number | null = null;
    let mainEnd: number | null = null;

    // Flexible regex for detecting variations of `if __name__ == "__main__":`
    const mainRegex = /^\s*if\s*["']__main__["']\s*==\s*__name__\s*:|^\s*if\s*__name__\s*==\s*["']__main__["']\s*:/;

    // Step 1: Find `if __name__ == "__main__":`
    for (let i = 0; i < document.lineCount; i++) {
        if (mainRegex.test(document.lineAt(i).text)) {
            mainStart = i;
            break;
        }
    }

    if (mainStart !== null) {
        // Step 2: Find where `if __name__ == "__main__":` block ends (first non-indented line)
        for (let i = mainStart + 1; i < document.lineCount; i++) {
            const lineText = document.lineAt(i).text;

            // A **non-indented line** (not empty & not a comment) signals the end of the block
            if (lineText.trim().length > 0 && !lineText.startsWith(" ") && !lineText.startsWith("\t")) {
                mainEnd = i;
                break;
            }
        }

        // If no non-indented line was found, assume the block goes to the last line
        if (mainEnd === null) {
            mainEnd = document.lineCount;
        }
    }

    // If we found `if __name__ == "__main__":`, remove that block
    if (mainStart !== null && mainEnd !== null) {
        return new vscode.Range(0, 0, mainStart, 0); // Select everything **before** `if __name__ == "__main__"`
    } else {
        return new vscode.Range(0, 0, document.lineCount, 0); // No `main` block, execute everything
    }
}
