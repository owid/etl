import * as vscode from 'vscode';
import * as path from 'path';
import { readFile } from 'fs/promises';
import { ChildProcess, spawn } from 'child_process';

const panels = new Map<string, vscode.WebviewPanel>();
const watchProcesses = new Map<string, ChildProcess>();
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

	context.subscriptions.push(openCmd, outputChannel);
}

export function deactivate() {
	for (const [, proc] of watchProcesses) {
		proc.kill();
	}
	watchProcesses.clear();
}

// --- Helpers ---

function getGitBranch(wsRoot: string): Promise<string> {
	return new Promise((resolve, reject) => {
		const proc = spawn('git', ['branch', '--show-current'], { cwd: wsRoot });
		let stdout = '';
		proc.stdout.on('data', (d: Buffer) => { stdout += d.toString(); });
		proc.on('close', (code) => {
			if (code === 0) { resolve(stdout.trim()); }
			else { reject(new Error(`git exited with code ${code}`)); }
		});
		proc.on('error', reject);
	});
}

/**
 * Port of get_container_name() from etl/config.py lines 50-74.
 * Normalizes a git branch name into a staging container name.
 */
function getContainerName(branch: string): string {
	let normalized = branch.replace(/[\/\._]/g, '-');
	normalized = normalized.replace('staging-site-', '');
	const containerName = `staging-site-${normalized.slice(0, 28)}`;
	return containerName.replace(/-+$/, '');
}

/**
 * Extract graph step URI and slug from a .chart.yml file path.
 * Reads the YAML file to get the actual slug (important for mdim charts
 * where the slug like "covid/covid#covid_cases" differs from the filename).
 * Falls back to the filename if no slug field is found.
 *
 * For mdim charts, also builds the full catalogPath by inserting the version
 * from the step path (matching GraphStep._create_multidim_collection logic):
 *   slug "covid/covid#covid_cases" + version "latest" → "covid/latest/covid#covid_cases"
 */
async function parseChartYml(filePath: string, wsRoot: string): Promise<{ stepUri: string; slug: string; catalogPath?: string }> {
	const graphDir = path.join(wsRoot, 'etl', 'steps', 'graph');
	const rel = path.relative(graphDir, filePath);
	const stepPath = rel.replace('.chart.yml', '');

	// Read YAML and extract top-level slug field
	const content = await readFile(filePath, 'utf8');
	const match = content.match(/^slug:\s*(.+)$/m);
	const slug = match ? match[1].trim() : path.basename(stepPath);

	// For mdim charts (slug contains #), build full catalog path with version.
	// YAML slug "covid/covid#covid_cases" + step version "latest" → "covid/latest/covid#covid_cases"
	let catalogPath: string | undefined;
	if (slug.includes('#') && slug.includes('/')) {
		const parts = stepPath.split('/');  // e.g. ["covid", "latest", "covid-cases"]
		if (parts.length >= 2) {
			const version = parts[1];  // e.g. "latest"
			const slashIdx = slug.indexOf('/');
			const namespace = slug.slice(0, slashIdx);      // "covid"
			const rest = slug.slice(slashIdx + 1);           // "covid#covid_cases"
			catalogPath = `${namespace}/${version}/${rest}`;  // "covid/latest/covid#covid_cases"
		}
	}

	return { stepUri: `graph://${stepPath}`, slug, catalogPath };
}

interface InfoBarData {
	command: string;
	stagingUrl: string;
	stepUri: string;
	status: string;
	latency?: string;
}

function chartIframeDoc(grapherSrc: string, containerName: string): string {
	// Separate HTML document for the chart — loaded inside an iframe so each
	// reload gets a fresh JS context (embedCharts.js uses top-level const).
	// Permissive CSP needed because staging server uses HTTP.
	const csp = `default-src * 'unsafe-inline' 'unsafe-eval' data: blob:;`;
	return `<!DOCTYPE html><html><head><meta charset="utf-8"/><meta http-equiv="Content-Security-Policy" content="${csp}"/><style>html,body{height:100%;margin:0;padding:0}figure{width:100%;height:600px;margin:0;border:0}</style></head><body><figure data-grapher-src="${grapherSrc}"></figure><script src="http://${containerName}/assets/embedCharts.js"><\/script></body></html>`;
}

function generateEmbedHtml(grapherSrc: string, containerName: string, info: InfoBarData, isMdim: boolean): string {
	// For mdim charts, load the page directly in an iframe (embed mechanism doesn't support mdim).
	// For regular charts, use the embed mechanism with data-grapher-src.
	const iframeTag = isMdim
		? `<iframe id="chart-frame" src="${grapherSrc}"></iframe>`
		: `<iframe id="chart-frame" srcdoc="${chartIframeDoc(grapherSrc, containerName).replace(/"/g, '&quot;')}"></iframe>`;

	return `<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <meta http-equiv="Content-Security-Policy" content="default-src * 'unsafe-inline' 'unsafe-eval' data: blob:;" />
    <style>
        html, body { height: 100%; margin: 0; padding: 0; }
        body { display: flex; flex-direction: column; }
        #info-bar {
            flex-shrink: 0;
            padding: 6px 12px;
            font-family: var(--vscode-editor-font-family, monospace);
            font-size: 11px;
            line-height: 1.5;
            color: var(--vscode-descriptionForeground, #888);
            background: var(--vscode-sideBar-background, #1e1e1e);
            border-bottom: 1px solid var(--vscode-panel-border, #333);
            overflow-x: auto;
        }
        iframe { flex: 1; width: 100%; border: none; min-height: 0; }
        #info-bar .row { display: flex; gap: 8px; white-space: nowrap; }
        #info-bar .label { color: var(--vscode-foreground, #ccc); font-weight: 600; min-width: 70px; }
        #info-bar .status-ok { color: var(--vscode-testing-iconPassed, #4ec94e); }
        #info-bar .status-running { color: var(--vscode-progressBar-background, #007acc); }
        #info-bar .status-error { color: var(--vscode-errorForeground, #f44); }
        #info-bar a { color: var(--vscode-textLink-foreground, #3794ff); text-decoration: none; }
    </style>
</head>
<body>
    <div id="info-bar">
        <div class="row"><span class="label">Step</span> <span>${info.stepUri}</span></div>
        <div class="row"><span class="label">Command</span> <span id="cmd">${info.command}</span></div>
        <div class="row"><span class="label">Staging</span> <span><a href="${info.stagingUrl}">${info.stagingUrl}</a></span></div>
        <div class="row"><span class="label">Status</span> <span id="status" class="status-ok">${info.status}</span></div>
        <div class="row"><span class="label">Latency</span> <span id="latency">${info.latency || '-'}</span></div>
    </div>
    ${iframeTag}
    <script>
        const containerName = '${containerName}';
        const isMdim = ${isMdim};
        window.addEventListener('message', (e) => {
            const msg = e.data;
            if (msg.type === 'status') {
                const el = document.getElementById('status');
                el.textContent = msg.text;
                el.className = 'status-' + (msg.cls || 'ok');
            }
            if (msg.type === 'latency') {
                document.getElementById('latency').textContent = msg.text;
            }
            if (msg.type === 'reload') {
                const frame = document.getElementById('chart-frame');
                if (isMdim) {
                    frame.src = msg.src;
                } else {
                    const csp = "default-src * \\'unsafe-inline\\' \\'unsafe-eval\\' data: blob:;";
                    const doc = '<!DOCTYPE html><html><head><meta charset="utf-8"/><meta http-equiv="Content-Security-Policy" content="' + csp + '"/><style>html,body{height:100%;margin:0;padding:0}figure{width:100%;height:600px;margin:0;border:0}</style></head><body><figure data-grapher-src="' + msg.src + '"></figure><script src="http://' + containerName + '/assets/embedCharts.js"><\\/script></body></html>';
                    frame.srcdoc = doc;
                }
            }
        });
    </script>
</body>
</html>`;
}

function getLoadingHtml(command: string): string {
	return `<!DOCTYPE html>
<html>
<head>
<style>
body {
	margin: 0; padding: 16px; font-family: var(--vscode-editor-font-family, monospace);
	font-size: 12px; color: var(--vscode-foreground);
	background: var(--vscode-editor-background);
}
.header {
	display: flex; align-items: center; margin-bottom: 12px;
}
.spinner {
	border: 3px solid var(--vscode-editorWidget-border, #ccc);
	border-top: 3px solid var(--vscode-focusBorder, #007acc);
	border-radius: 50%; width: 16px; height: 16px;
	animation: spin 1s linear infinite; margin-right: 10px; flex-shrink: 0;
}
@keyframes spin { to { transform: rotate(360deg); } }
.cmd {
	color: var(--vscode-descriptionForeground, #888);
	font-size: 11px;
}
#log {
	white-space: pre-wrap; word-wrap: break-word;
	background: var(--vscode-textBlockQuote-background, #1e1e1e);
	padding: 10px; border-radius: 4px;
	color: var(--vscode-editor-foreground);
	font-size: 11px; line-height: 1.4;
	max-height: calc(100vh - 80px); overflow-y: auto;
}
</style>
</head>
<body>
<div class="header">
	<div class="spinner"></div>
	<span class="cmd">$ ${command}</span>
</div>
<pre id="log"></pre>
<script>
	const log = document.getElementById('log');
	window.addEventListener('message', (e) => {
		if (e.data.type === 'log') {
			log.textContent += e.data.text;
			log.scrollTop = log.scrollHeight;
		}
	});
</script>
</body>
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

// --- Core logic ---

async function openPreview(filePath: string) {
	const existing = panels.get(filePath);
	if (existing) {
		existing.reveal(vscode.ViewColumn.Beside);
		return;
	}

	const workspaceFolder = vscode.workspace.workspaceFolders?.[0];
	if (!workspaceFolder) {
		vscode.window.showErrorMessage('No workspace folder found');
		return;
	}

	const wsRoot = workspaceFolder.uri.fsPath;
	const fileName = path.basename(filePath, '.chart.yml');

	try {
		const branch = await getGitBranch(wsRoot);
		const containerName = getContainerName(branch);
		const { stepUri, slug, catalogPath } = await parseChartYml(filePath, wsRoot);
		const isMdim = slug.includes('#');
		// For mdim charts, use the full catalogPath (URL-encoded) at /admin/grapher/
		// For regular charts, use the slug at /grapher/
		const stagingUrl = isMdim && catalogPath
			? `http://${containerName}/admin/grapher/${encodeURIComponent(catalogPath)}`
			: `http://${containerName}/grapher/${slug}`;

		const panel = vscode.window.createWebviewPanel(
			'chartPreview',
			`Preview: ${fileName}`,
			vscode.ViewColumn.Beside,
			{ enableScripts: true, retainContextWhenHidden: true }
		);

		panels.set(filePath, panel);

		panel.onDidDispose(() => {
			panels.delete(filePath);
			killWatchProcess(filePath);
		});

		const config = vscode.workspace.getConfiguration('chart-preview');
		const etlrRel = config.get<string>('etlrPath', '.venv/bin/etlr');
		const args = [stepUri, '--graph', '--graph-push', '--watch', '--private'];
		const command = `${etlrRel} ${args.join(' ')}`;

		panel.webview.html = getLoadingHtml(command);

		startWatchProcess(filePath, panel, wsRoot, stepUri, stagingUrl, containerName, isMdim);
	} catch (err: unknown) {
		const msg = err instanceof Error ? err.message : String(err);
		vscode.window.showErrorMessage(`Chart preview failed: ${msg}`);
	}
}

function startWatchProcess(
	filePath: string,
	panel: vscode.WebviewPanel,
	wsRoot: string,
	stepUri: string,
	stagingUrl: string,
	containerName: string,
	isMdim: boolean,
) {
	killWatchProcess(filePath);

	const config = vscode.workspace.getConfiguration('chart-preview');
	const etlrRel = config.get<string>('etlrPath', '.venv/bin/etlr');
	const etlrPath = path.resolve(wsRoot, etlrRel);

	const args = [stepUri, '--graph', '--graph-push', '--watch', '--private'];
	const command = `${etlrRel} ${args.join(' ')}`;

	outputChannel.appendLine(`Starting watch: ${etlrPath} ${args.join(' ')}`);

	const proc = spawn(etlrPath, args, {
		cwd: wsRoot,
		env: { ...process.env, STAGING: '1' },
	});

	watchProcesses.set(filePath, proc);

	const info: InfoBarData = { command, stagingUrl, stepUri, status: 'Running...' };

	let recentOutput = '';
	let firstRunDone = false;
	let cycleStart = Date.now();
	let stepsRunning = false;  // true after "--- Running", false after step OK
	let detectingDirty = false;  // true after "Detecting which steps", false after OK
	let pendingReloadTimer: ReturnType<typeof setTimeout> | null = null;

	const doReload = () => {
		const elapsed = ((Date.now() - cycleStart) / 1000).toFixed(1);
		info.status = 'OK';
		info.latency = `${elapsed}s`;

		if (!firstRunDone) {
			const bustUrl = `${stagingUrl}?_t=${Date.now()}`;
			panel.webview.html = generateEmbedHtml(bustUrl, containerName, info, isMdim);
			firstRunDone = true;
		} else {
			panel.webview.postMessage({ type: 'status', text: 'OK', cls: 'ok' });
			panel.webview.postMessage({ type: 'latency', text: `${elapsed}s` });
			panel.webview.postMessage({ type: 'reload', src: `${stagingUrl}?_t=${Date.now()}` });
		}
		outputChannel.appendLine('[chart-preview] Chart updated, refreshing preview.');
	};

	const handleOutput = (data: Buffer) => {
		const text = data.toString();
		recentOutput += text;
		// Keep only the last 4000 chars for error display
		if (recentOutput.length > 4000) {
			recentOutput = recentOutput.slice(-4000);
		}
		outputChannel.append(text);

		// Stream logs to loading screen before chart is ready
		if (!firstRunDone) {
			panel.webview.postMessage({ type: 'log', text });
		}

		// Track cycle phases:
		// 1. "--- Detecting which steps need rebuilding..." → dirty check starts
		// 2. "OK (0.6s)" → dirty check done (ignore this OK)
		// 3a. "--- Running N steps..." → steps are executing
		// 3b. "--- All datasets up to date!" → nothing to run
		// 4. "OK (1.5s)" after "--- Running" → step completed

		if (text.includes('Detecting which steps need rebuilding')) {
			cycleStart = Date.now();
			stepsRunning = false;
			detectingDirty = true;
			if (firstRunDone) {
				panel.webview.postMessage({ type: 'status', text: 'Checking...', cls: 'running' });
			}
		}

		if (text.includes('--- Running')) {
			stepsRunning = true;
			detectingDirty = false;
			// Cancel pending reload — steps are about to run
			if (pendingReloadTimer) { clearTimeout(pendingReloadTimer); pendingReloadTimer = null; }
			if (firstRunDone) {
				panel.webview.postMessage({ type: 'status', text: 'Running...', cls: 'running' });
			}
		}

		// "All datasets up to date" — nothing changed
		if (text.includes('All datasets up to date!')) {
			stepsRunning = false;
			detectingDirty = false;
			if (pendingReloadTimer) { clearTimeout(pendingReloadTimer); pendingReloadTimer = null; }
			doReload();
		}

		// OK after dirty detection — might mean "all up to date" (watch mode doesn't
		// always print "All datasets up to date!"). Schedule a reload after a short
		// delay; cancel if "--- Running" arrives before the timer fires.
		if (detectingDirty && !stepsRunning && /OK \(\d/.test(text)) {
			detectingDirty = false;
			if (pendingReloadTimer) { clearTimeout(pendingReloadTimer); }
			pendingReloadTimer = setTimeout(() => { pendingReloadTimer = null; doReload(); }, 500);
		}

		// OK after steps ran — this is the real completion
		if (stepsRunning && /OK \(\d/.test(text)) {
			stepsRunning = false;
			detectingDirty = false;
			if (pendingReloadTimer) { clearTimeout(pendingReloadTimer); pendingReloadTimer = null; }
			doReload();
		}

		// Detect errors
		if (text.includes('FAILED') || text.includes('step_failed')) {
			if (!firstRunDone) {
				panel.webview.html = getErrorHtml(recentOutput);
			} else {
				panel.webview.postMessage({ type: 'status', text: 'FAILED', cls: 'error' });
				vscode.window.showWarningMessage('Chart preview: etlr step failed. See "Chart Preview" output.');
			}
		}

		// Helpful message for missing data dependencies
		if (text.includes('Indicator not found in database') || text.includes('not found in database')) {
			panel.webview.html = getErrorHtml(
				'Data dependencies not found on staging server.\n\n'
				+ 'Run the grapher step for the data dependency first, e.g.:\n'
				+ '  .venv/bin/etlr <data-dependency> --grapher --private\n\n'
				+ 'Then re-open the chart preview.\n\n'
				+ '--- etlr output ---\n' + recentOutput
			);
		}
	};

	proc.stdout.on('data', handleOutput);
	proc.stderr.on('data', handleOutput);

	proc.on('error', (err: Error) => {
		watchProcesses.delete(filePath);
		outputChannel.appendLine(`Watch process error: ${err.message}`);
		panel.webview.html = getErrorHtml(
			`Failed to start etlr:\n${err.message}\n\nEnsure ${etlrRel} exists.`
		);
	});

	proc.on('close', (code: number | null) => {
		watchProcesses.delete(filePath);
		outputChannel.appendLine(`Watch process exited with code ${code}`);
		if (code !== 0 && code !== null && !firstRunDone) {
			panel.webview.html = getErrorHtml(
				`etlr exited with code ${code}.\n\n${recentOutput}`
			);
		}
	});
}

function killWatchProcess(filePath: string) {
	const proc = watchProcesses.get(filePath);
	if (proc) {
		proc.kill();
		watchProcesses.delete(filePath);
	}
}
