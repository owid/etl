import * as vscode from 'vscode';
import * as path from 'path';
import * as fs from 'fs';
import { ChildProcess, spawn } from 'child_process';
import { tmpdir } from 'os';

const panels = new Map<string, vscode.WebviewPanel>();
const activeProcesses = new Map<string, ChildProcess>();
const pendingRenders = new Map<string, ReturnType<typeof setTimeout>>();
let outputChannel: vscode.OutputChannel;

export function activate(context: vscode.ExtensionContext) {
	outputChannel = vscode.window.createOutputChannel('Chart Preview');

	const openCmd = vscode.commands.registerCommand('chart-preview.open', () => {
		const editor = vscode.window.activeTextEditor;
		if (!editor) {
			vscode.window.showErrorMessage('No active editor');
			return;
		}
		const filePath = editor.document.uri.fsPath;
		if (!filePath.endsWith('.chart.yml')) {
			vscode.window.showErrorMessage('Not a .chart.yml file');
			return;
		}
		openPreview(filePath);
	});

	const onSave = vscode.workspace.onDidSaveTextDocument((doc) => {
		if (doc.uri.fsPath.endsWith('.chart.yml') && panels.has(doc.uri.fsPath)) {
			renderToPanel(doc.uri.fsPath, panels.get(doc.uri.fsPath)!);
		}
	});

	context.subscriptions.push(openCmd, onSave, outputChannel);
}

export function deactivate() {}

function openPreview(filePath: string) {
	const existing = panels.get(filePath);
	if (existing) {
		existing.reveal(vscode.ViewColumn.Beside);
		return;
	}

	const fileName = path.basename(filePath, '.chart.yml');

	const panel = vscode.window.createWebviewPanel(
		'chartPreview',
		`Preview: ${fileName}`,
		vscode.ViewColumn.Beside,
		{
			enableScripts: true,
			retainContextWhenHidden: true,
		}
	);

	panels.set(filePath, panel);

	panel.onDidDispose(() => {
		panels.delete(filePath);
		// Kill any running render for this file
		const proc = activeProcesses.get(filePath);
		if (proc) {
			proc.kill();
			activeProcesses.delete(filePath);
		}
		const timer = pendingRenders.get(filePath);
		if (timer) {
			clearTimeout(timer);
			pendingRenders.delete(filePath);
		}
	});

	panel.webview.html = getLoadingHtml();
	renderToPanel(filePath, panel);
}

function renderToPanel(filePath: string, panel: vscode.WebviewPanel) {
	const existing = pendingRenders.get(filePath);
	if (existing) {
		clearTimeout(existing);
	}

	const timeout = setTimeout(() => {
		pendingRenders.delete(filePath);
		doRender(filePath, panel);
	}, 500);

	pendingRenders.set(filePath, timeout);
}

function doRender(filePath: string, panel: vscode.WebviewPanel) {
	const workspaceFolder = vscode.workspace.workspaceFolders?.[0];
	if (!workspaceFolder) {
		panel.webview.html = getErrorHtml('No workspace folder found');
		return;
	}

	// Kill any running render for this file
	const existingProc = activeProcesses.get(filePath);
	if (existingProc) {
		existingProc.kill();
		activeProcesses.delete(filePath);
	}

	const wsRoot = workspaceFolder.uri.fsPath;
	const config = vscode.workspace.getConfiguration('chart-preview');
	const pythonRel = config.get<string>('pythonPath', '.venv/bin/python');
	const pythonPath = path.resolve(wsRoot, pythonRel);
	const scriptPath = path.join(wsRoot, 'scripts', 'render_chart.py');
	const tmpFile = path.join(tmpdir(), `chart-preview-${Date.now()}.html`);

	outputChannel.appendLine(`Rendering ${filePath}...`);

	const proc = spawn(pythonPath, [scriptPath, filePath, '-o', tmpFile], {
		cwd: wsRoot,
	});
	activeProcesses.set(filePath, proc);

	let stderr = '';
	proc.stderr.on('data', (data: Buffer) => {
		stderr += data.toString();
	});

	proc.on('close', (code: number | null) => {
		activeProcesses.delete(filePath);

		if (code !== 0) {
			outputChannel.appendLine(`Error (exit ${code}): ${stderr}`);
			panel.webview.html = getErrorHtml(stderr || `Process exited with code ${code}`);
			try { fs.unlinkSync(tmpFile); } catch { /* ignore */ }
			return;
		}

		try {
			const html = fs.readFileSync(tmpFile, 'utf-8');
			panel.webview.html = injectCsp(html);
			outputChannel.appendLine('Rendered successfully.');
		} catch (err: unknown) {
			const msg = err instanceof Error ? err.message : String(err);
			panel.webview.html = getErrorHtml(`Failed to read output: ${msg}`);
		} finally {
			try { fs.unlinkSync(tmpFile); } catch { /* ignore */ }
		}
	});

	proc.on('error', (err: Error) => {
		activeProcesses.delete(filePath);
		outputChannel.appendLine(`Spawn error: ${err.message}`);
		panel.webview.html = getErrorHtml(
			`Failed to run Python:\n${err.message}\n\nEnsure ${pythonRel} exists.`
		);
	});
}

function getLoadingHtml(): string {
	return `<!DOCTYPE html>
<html>
<head>
<style>
body {
	display: flex; align-items: center; justify-content: center;
	height: 100vh; margin: 0; font-family: sans-serif;
	color: var(--vscode-foreground);
	background: var(--vscode-editor-background);
}
.spinner {
	border: 3px solid var(--vscode-editorWidget-border, #ccc);
	border-top: 3px solid var(--vscode-focusBorder, #007acc);
	border-radius: 50%; width: 24px; height: 24px;
	animation: spin 1s linear infinite; margin-right: 12px;
}
@keyframes spin { to { transform: rotate(360deg); } }
</style>
</head>
<body><div class="spinner"></div> Rendering chart...</body>
</html>`;
}

function getErrorHtml(message: string): string {
	const escaped = message
		.replace(/&/g, '&amp;')
		.replace(/</g, '&lt;')
		.replace(/>/g, '&gt;');
	return `<!DOCTYPE html>
<html>
<head>
<style>
body {
	padding: 20px; font-family: sans-serif;
	color: var(--vscode-errorForeground, #f44);
	background: var(--vscode-editor-background);
}
pre {
	white-space: pre-wrap; word-wrap: break-word;
	background: var(--vscode-textBlockQuote-background, #1e1e1e);
	padding: 12px; border-radius: 4px;
	color: var(--vscode-editor-foreground);
}
</style>
</head>
<body>
<h3>Chart Preview Error</h3>
<pre>${escaped}</pre>
</body>
</html>`;
}

function injectCsp(html: string): string {
	const csp = [
		"default-src 'none'",
		"style-src https://fonts.googleapis.com https://expose-grapher-state.owid.pages.dev 'unsafe-inline'",
		"font-src https://fonts.gstatic.com",
		"script-src https://expose-grapher-state.owid.pages.dev 'unsafe-inline'",
		"connect-src https:",
		"img-src https: data:",
	].join('; ');

	const meta = `<meta http-equiv="Content-Security-Policy" content="${csp}">`;
	return html.replace('<head>', `<head>\n    ${meta}`);
}
