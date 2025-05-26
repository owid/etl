import * as vscode from 'vscode';
import * as path from 'path';

export function activate(context: vscode.ExtensionContext) {
	console.log('Clickable DAG Steps extension is active.');

	const linkProvider: vscode.DocumentLinkProvider = {
		provideDocumentLinks(document: vscode.TextDocument) {
			const links: vscode.DocumentLink[] = [];

			// Match URIs like data://... or export://... (stop before colon or whitespace)
			const dagUriRegex = /\b(?:data|export):\/\/[a-zA-Z0-9_/.\-]+/g;

			for (let line = 0; line < document.lineCount; line++) {
				const textLine = document.lineAt(line);
				let match: RegExpExecArray | null;

				while ((match = dagUriRegex.exec(textLine.text)) !== null) {
					const matchedUri = match[0].trim();

					let baseDir: string;
					if (matchedUri.startsWith('data://')) {
						baseDir = 'etl/steps/data/';
					} else if (matchedUri.startsWith('export://')) {
						baseDir = 'etl/steps/export/';
					} else {
						continue;
					}

					const relativePath = matchedUri.replace(/^(data|export):\/\//, '');
					const segments = relativePath.split('/');

					if (segments.length < 2) {
						continue;
					}

					const scriptPath = path.join(baseDir, ...segments) + '.py';

					const workspaceFolder = vscode.workspace.getWorkspaceFolder(document.uri);
					if (!workspaceFolder) {
						continue;
					}

					const fileUri = vscode.Uri.file(path.join(workspaceFolder.uri.fsPath, scriptPath));

					const range = new vscode.Range(
						new vscode.Position(line, match.index!),
						new vscode.Position(line, match.index! + matchedUri.length)
					);

					const link = new vscode.DocumentLink(range, fileUri);
					link.tooltip = `Open script: ${scriptPath}`;
					links.push(link);
				}
			}

			return links;
		}
	};

	const selector: vscode.DocumentSelector = { language: 'yaml', scheme: 'file' };
	context.subscriptions.push(vscode.languages.registerDocumentLinkProvider(selector, linkProvider));
}

export function deactivate() {}
