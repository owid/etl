import * as vscode from 'vscode';
import * as path from 'path';
import { readFile } from 'fs/promises';
import { ChildProcess, spawn } from 'child_process';

const panels = new Map<string, vscode.WebviewPanel>();
const watchProcesses = new Map<string, ChildProcess>();
let outputChannel: vscode.OutputChannel;
let lastPreviewedFile: string | undefined;

export function activate(context: vscode.ExtensionContext) {
	outputChannel = vscode.window.createOutputChannel('Chart Preview');

	const openCmd = vscode.commands.registerCommand('chart-preview.open', () => {
		const editor = vscode.window.activeTextEditor;
		if (!editor) {
			if (lastPreviewedFile && panels.has(lastPreviewedFile)) {
				panels.get(lastPreviewedFile)!.dispose();
				lastPreviewedFile = undefined;
			}
			return;
		}
		const filePath = editor.document.uri.fsPath;
		if (filePath.includes('/steps/data/') && filePath.endsWith('.py')) {
			openDatasetPreview(filePath);
		} else if (filePath.endsWith('.chart.yml') || (filePath.includes('/export/multidim/') && (filePath.endsWith('.config.yml') || filePath.endsWith('.py')))) {
			openPreview(filePath);
		} else {
			vscode.window.showErrorMessage('Not a previewable file (expected a garden .py step or .chart.yml)');
		}
	});

	// Keep chart-preview.dataset registered as an alias for backwards compatibility
	const datasetCmd = vscode.commands.registerCommand('chart-preview.dataset', () => {
		vscode.commands.executeCommand('chart-preview.open');
	});

	context.subscriptions.push(openCmd, datasetCmd, outputChannel);
}

export function deactivate() {
	for (const [, proc] of watchProcesses) { proc.kill(); }
	watchProcesses.clear();
	for (const [, proc] of previewScriptProcesses) { proc.kill(); }
	previewScriptProcesses.clear();
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

/**
 * Extract export step URI and catalog path from an export/multidim file path.
 * Supports both .config.yml and .py files.
 * Catalog path defaults to namespace/version/name#name (matching PathFinder.create_collection).
 */
function parseExportMultidim(filePath: string, wsRoot: string): { stepUri: string; catalogPath: string } {
	const exportDir = path.join(wsRoot, 'etl', 'steps', 'export', 'multidim');
	const rel = path.relative(exportDir, filePath);
	const stepPath = rel.replace(/\.(config\.yml|py)$/, '');
	const shortName = path.basename(stepPath);
	const catalogPath = `${stepPath}#${shortName}`;
	return { stepUri: `export://multidim/${stepPath}`, catalogPath };
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
        #error-panel {
            display: none; flex: 1; padding: 20px; overflow: auto;
            font-family: var(--vscode-editor-font-family, monospace);
            font-size: 12px; color: var(--vscode-errorForeground, #f44);
            background: var(--vscode-editor-background);
        }
        #error-panel pre {
            white-space: pre-wrap; word-wrap: break-word;
            background: var(--vscode-textBlockQuote-background, #1e1e1e);
            padding: 12px; border-radius: 4px;
            color: var(--vscode-editor-foreground);
        }
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
    <div id="error-panel"><h3>Step Failed</h3><pre id="error-output"></pre></div>
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
            if (msg.type === 'showError') {
                document.getElementById('chart-frame').style.display = 'none';
                const ep = document.getElementById('error-panel');
                ep.style.display = 'block';
                document.getElementById('error-output').textContent = msg.text;
            }
            if (msg.type === 'reload') {
                document.getElementById('error-panel').style.display = 'none';
                const frame = document.getElementById('chart-frame');
                frame.style.display = '';
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

function getErrorHtml(message: string, title = 'Chart Preview Error', retryable = false): string {
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
button {
	margin-top: 12px;
	padding: 6px 16px;
	background: var(--vscode-button-background, #0e639c);
	color: var(--vscode-button-foreground, #fff);
	border: none; border-radius: 3px; cursor: pointer; font-size: 13px;
}
button:hover { background: var(--vscode-button-hoverBackground, #1177bb); }
</style>
</head>
<body>
<h3>${title}</h3>
<pre>${escaped}</pre>
${retryable ? '<button onclick="retry()">Retry</button>' : ''}
<script>
${retryable ? `
const vscode = acquireVsCodeApi();
function retry() { vscode.postMessage({ type: 'retry' }); }
` : ''}
</script>
</body>
</html>`;
}

// --- Core logic ---

async function openPreview(filePath: string) {
	const existing = panels.get(filePath);
	if (existing) {
		existing.dispose();
		lastPreviewedFile = undefined;
		return;
	}
	lastPreviewedFile = filePath;

	const workspaceFolder = vscode.workspace.workspaceFolders?.[0];
	if (!workspaceFolder) {
		vscode.window.showErrorMessage('No workspace folder found');
		return;
	}

	const wsRoot = workspaceFolder.uri.fsPath;
	const fileName = path.basename(filePath).replace(/\.(chart\.yml|config\.yml|py)$/, '');

	try {
		const branch = await getGitBranch(wsRoot);
		const containerName = getContainerName(branch);

		let stepUri: string;
		let stagingUrl: string;
		let isMdim: boolean;
		let etlArgs: string[];

		if (filePath.includes('/export/multidim/')) {
			// Export/multidim step
			const parsed = parseExportMultidim(filePath, wsRoot);
			stepUri = parsed.stepUri;
			stagingUrl = `http://${containerName}/admin/grapher/${encodeURIComponent(parsed.catalogPath)}`;
			isMdim = true;
			etlArgs = [stepUri, '--export', '--watch', '--private'];
		} else {
			// Graph step (.chart.yml)
			const parsed = await parseChartYml(filePath, wsRoot);
			stepUri = parsed.stepUri;
			const slug = parsed.slug;
			const catalogPath = parsed.catalogPath;
			isMdim = slug.includes('#');
			stagingUrl = isMdim && catalogPath
				? `http://${containerName}/admin/grapher/${encodeURIComponent(catalogPath)}`
				: `http://${containerName}/grapher/${slug}`;
			etlArgs = [stepUri, '--graph', '--graph-push', '--watch', '--private'];
		}

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
		const command = `${etlrRel} ${etlArgs.join(' ')}`;

		panel.webview.html = getLoadingHtml(command);

		startWatchProcess(filePath, panel, wsRoot, stepUri, stagingUrl, containerName, isMdim, etlArgs);
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
	etlArgs: string[],
) {
	killWatchProcess(filePath);

	const config = vscode.workspace.getConfiguration('chart-preview');
	const etlrRel = config.get<string>('etlrPath', '.venv/bin/etlr');
	const etlrPath = path.resolve(wsRoot, etlrRel);

	const command = `${etlrRel} ${etlArgs.join(' ')}`;

	outputChannel.appendLine(`Starting watch: ${etlrPath} ${etlArgs.join(' ')}`);

	const proc = spawn(etlrPath, etlArgs, {
		cwd: wsRoot,
		env: { ...process.env, STAGING: '1', PREFER_DOWNLOAD: '1' },
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
				panel.webview.postMessage({ type: 'showError', text: recentOutput });
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
		if (code !== 0 && code !== null) {
			if (!firstRunDone) {
				panel.webview.html = getErrorHtml(
					`etlr exited with code ${code}.\n\n${recentOutput}`
				);
			}
			// Restart watch after failure — etlr crashes on step errors,
			// killing the watch loop. Restart so the next file save triggers
			// a fresh run.
			outputChannel.appendLine('[chart-preview] Restarting watch in 2s...');
			setTimeout(() => {
				if (panels.has(filePath)) {
					startWatchProcess(filePath, panel, wsRoot, stepUri, stagingUrl, containerName, isMdim, etlArgs);
				}
			}, 2000);
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

// --- Dataset Preview ---

/**
 * Parse an ETL data step file path into its components.
 * e.g. /path/to/etl/steps/data/garden/biodiversity/2025-04-07/cherry_blossom.py
 *   → { channel: "garden", namespace: "biodiversity", version: "2025-04-07", shortName: "cherry_blossom" }
 */
function parseDataStepPath(filePath: string, wsRoot: string): {
	stepUri: string;
	channel: string;
	namespace: string;
	version: string;
	shortName: string;
	relStepPath: string;
} {
	const dataDir = path.join(wsRoot, 'etl', 'steps', 'data');
	const rel = path.relative(dataDir, filePath).replace(/\.py$/, '');
	const parts = rel.split(path.sep);
	if (parts.length < 4) {
		throw new Error(`Cannot parse data step path: ${filePath}`);
	}
	const [channel, namespace, version, shortName] = parts;
	return {
		stepUri: `data://${channel}/${namespace}/${version}/${shortName}`,
		channel,
		namespace,
		version,
		shortName,
		relStepPath: `etl/steps/data/${rel}.py`,
	};
}

// Track in-flight preview script processes so we can kill them on panel close / new run
const previewScriptProcesses = new Map<string, ChildProcess>();

/**
 * Run the Python generate_preview.py script and return the JSON payload.
 * Kills any previous in-flight script for the same filePath.
 * Times out after 120s to avoid zombies on huge datasets.
 */
function runPreviewScript(wsRoot: string, stepPath: string, filePath: string): Promise<string> {
	// Kill any previous in-flight script for this panel
	const prev = previewScriptProcesses.get(filePath);
	if (prev) { prev.kill(); previewScriptProcesses.delete(filePath); }

	return new Promise((resolve, reject) => {
		const config = vscode.workspace.getConfiguration('chart-preview');
		const pythonRel = config.get<string>('pythonPath', '.venv/bin/python');
		const pythonPath = path.resolve(wsRoot, pythonRel);
		const scriptPath = path.join(wsRoot, 'ai', 'dataset_preview', 'generate_preview.py');

		const proc = spawn(pythonPath, [scriptPath, stepPath, '--json'], { cwd: wsRoot });
		previewScriptProcesses.set(filePath, proc);

		const timeout = setTimeout(() => {
			proc.kill();
			previewScriptProcesses.delete(filePath);
			reject(new Error(`Preview script timed out after 10 minutes.\n\nThe dataset may be too large to preview quickly.`));
		}, 600_000);

		let stdout = '';
		let stderr = '';
		proc.stdout.on('data', (d: Buffer) => { stdout += d.toString(); });
		proc.stderr.on('data', (d: Buffer) => { stderr += d.toString(); });
		proc.on('close', (code) => {
			clearTimeout(timeout);
			previewScriptProcesses.delete(filePath);
			if (code === 0) {
				resolve(stdout);
			} else if (code !== null) {  // null = killed intentionally, ignore
				reject(new Error(`Preview script failed (code ${code}):\n${stderr}`));
			}
		});
		proc.on('error', (err) => { clearTimeout(timeout); reject(err); });
	});
}

async function openDatasetPreview(filePath: string) {
	const existing = panels.get(filePath);
	if (existing) {
		existing.dispose();
		lastPreviewedFile = undefined;
		return;
	}
	lastPreviewedFile = filePath;

	const workspaceFolder = vscode.workspace.workspaceFolders?.[0];
	if (!workspaceFolder) {
		vscode.window.showErrorMessage('No workspace folder found');
		return;
	}

	const wsRoot = workspaceFolder.uri.fsPath;

	try {
		const parsed = parseDataStepPath(filePath, wsRoot);
		const fileName = parsed.shortName;

		const config = vscode.workspace.getConfiguration('chart-preview');
		const etlrRel = config.get<string>('etlrPath', '.venv/bin/etlr');
		const etlArgs = [parsed.stepUri, '--watch', '--private'];
		const command = `${etlrRel} ${etlArgs.join(' ')}`;

		const panel = vscode.window.createWebviewPanel(
			'datasetPreview',
			`Dataset: ${fileName}`,
			vscode.ViewColumn.Beside,
			{ enableScripts: true, retainContextWhenHidden: true }
		);

		panels.set(filePath, panel);

		panel.onDidDispose(() => {
			panels.delete(filePath);
			killWatchProcess(filePath);
			const scriptProc = previewScriptProcesses.get(filePath);
			if (scriptProc) { scriptProc.kill(); previewScriptProcesses.delete(filePath); }
		});

		panel.webview.onDidReceiveMessage((msg) => {
			if (msg.type === 'retry') {
				panel.webview.html = getLoadingHtml(command);
				startDatasetWatchProcess(filePath, panel, wsRoot, parsed, etlArgs);
			}
		});

		panel.webview.html = getLoadingHtml(command);

		startDatasetWatchProcess(filePath, panel, wsRoot, parsed, etlArgs);
	} catch (err: unknown) {
		const msg = err instanceof Error ? err.message : String(err);
		vscode.window.showErrorMessage(`Dataset preview failed: ${msg}`);
	}
}

function startDatasetWatchProcess(
	filePath: string,
	panel: vscode.WebviewPanel,
	wsRoot: string,
	parsed: ReturnType<typeof parseDataStepPath>,
	etlArgs: string[],
) {
	killWatchProcess(filePath);

	const config = vscode.workspace.getConfiguration('chart-preview');
	const etlrRel = config.get<string>('etlrPath', '.venv/bin/etlr');
	const etlrPath = path.resolve(wsRoot, etlrRel);

	const command = `${etlrRel} ${etlArgs.join(' ')}`;

	outputChannel.appendLine(`Starting dataset watch: ${etlrPath} ${etlArgs.join(' ')}`);

	const proc = spawn(etlrPath, etlArgs, {
		cwd: wsRoot,
		env: { ...process.env, STAGING: '1', PREFER_DOWNLOAD: '1' },
	});

	watchProcesses.set(filePath, proc);

	let recentOutput = '';
	let firstRunDone = false;
	let reloadInProgress = false;
	let cycleStart = Date.now();

	const doReload = async () => {
		if (reloadInProgress) return;
		reloadInProgress = true;
		const elapsed = ((Date.now() - cycleStart) / 1000).toFixed(1);

		try {
			outputChannel.appendLine('[dataset-preview] Rebuild complete, generating preview...');
			const jsonStr = await runPreviewScript(wsRoot, parsed.relStepPath, filePath);

			if (!firstRunDone) {
				panel.webview.html = getDatasetPreviewHtml(jsonStr, command, elapsed);
				firstRunDone = true;
			} else {
				panel.webview.postMessage({ type: 'updateData', json: jsonStr });
				panel.webview.postMessage({ type: 'status', text: 'OK', cls: 'ok' });
				panel.webview.postMessage({ type: 'latency', text: `${elapsed}s` });
			}
			outputChannel.appendLine('[dataset-preview] Preview updated.');
		} catch (err: unknown) {
			const msg = err instanceof Error ? err.message : String(err);
			outputChannel.appendLine(`[dataset-preview] Preview script error: ${msg}`);
			if (!firstRunDone) {
				panel.webview.html = getErrorHtml(`Failed to generate dataset preview:\n\n${msg}`, 'Dataset Preview Error', true);
			} else {
				panel.webview.postMessage({ type: 'status', text: 'FAILED', cls: 'error' });
				panel.webview.postMessage({ type: 'showError', text: msg });
			}
		} finally {
			reloadInProgress = false;
		}
	};

	const handleOutput = (data: Buffer) => {
		const text = data.toString();
		recentOutput += text;
		if (recentOutput.length > 4000) recentOutput = recentOutput.slice(-4000);
		outputChannel.append(text);

		if (!firstRunDone) {
			panel.webview.postMessage({ type: 'log', text });
		}

		if (text.includes('--- Detecting which steps')) {
			cycleStart = Date.now();
			recentOutput = '';  // Reset so errors only show the current cycle's output
			if (firstRunDone) panel.webview.postMessage({ type: 'status', text: 'Checking...', cls: 'running' });
		}

		if (text.includes('--- Running')) {
			if (firstRunDone) panel.webview.postMessage({ type: 'status', text: 'Running...', cls: 'running' });
		}

		// Sentinel printed by etlr after every successful watch cycle (both "up to date" and actual rebuild)
		if (text.includes('--- Dataset rebuild complete')) {
			doReload();
		}

		if (text.includes('FAILED') || text.includes('step_failed')) {
			if (!firstRunDone) {
				panel.webview.html = getErrorHtml(recentOutput, 'Dataset Preview Error', true);
			} else {
				panel.webview.postMessage({ type: 'status', text: 'FAILED', cls: 'error' });
				panel.webview.postMessage({ type: 'showError', text: recentOutput });
			}
		}
	};

	proc.stdout.on('data', handleOutput);
	proc.stderr.on('data', handleOutput);

	proc.on('error', (err: Error) => {
		watchProcesses.delete(filePath);
		outputChannel.appendLine(`Dataset watch process error: ${err.message}`);
		panel.webview.html = getErrorHtml(
			`Failed to start etlr:\n${err.message}\n\nEnsure ${etlrRel} exists.`,
			'Dataset Preview Error'
		);
	});

	proc.on('close', (code: number | null) => {
		watchProcesses.delete(filePath);
		outputChannel.appendLine(`Dataset watch process exited with code ${code}`);
		if (code !== 0 && code !== null) {
			if (!firstRunDone) {
				panel.webview.html = getErrorHtml(
					`etlr exited with code ${code}.\n\n${recentOutput}`,
					'Dataset Preview Error',
					true
				);
			} else {
				panel.webview.postMessage({ type: 'status', text: 'FAILED', cls: 'error' });
			}
		}
	});
}

function getDatasetPreviewHtml(jsonStr: string, command: string, latency: string): string {
	return `<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    html, body { height: 100%; background: var(--vscode-editor-background, #1e1e1e); color: var(--vscode-foreground, #ccc); font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; font-size: 13px; }
    body { display: flex; flex-direction: column; }

    #info-bar {
        flex-shrink: 0;
        padding: 8px 16px;
        font-family: var(--vscode-editor-font-family, 'SF Mono', Menlo, Monaco, 'Courier New', monospace);
        font-size: 11px;
        line-height: 1.6;
        color: var(--vscode-descriptionForeground, #888);
        background: var(--vscode-sideBar-background, #1e1e1e);
        border-bottom: 1px solid var(--vscode-panel-border, #333);
    }
    #info-bar .row { display: flex; gap: 8px; white-space: nowrap; }
    #info-bar .label { color: var(--vscode-foreground, #ccc); font-weight: 600; min-width: 80px; }
    #info-bar .value { color: var(--vscode-descriptionForeground, #aaa); }
    #info-bar .highlight { color: var(--vscode-textLink-foreground, #3794ff); }
    #info-bar .status-ok { color: var(--vscode-testing-iconPassed, #4ec94e); }
    #info-bar .status-running { color: var(--vscode-progressBar-background, #007acc); }
    #info-bar .status-error { color: var(--vscode-errorForeground, #f44); }

    #table-tabs {
        flex-shrink: 0; display: flex; gap: 0;
        background: var(--vscode-editorWidget-background, #252526);
        border-bottom: 1px solid var(--vscode-panel-border, #333);
        padding: 0 16px;
    }
    #table-tabs.hidden { display: none; }
    .tab {
        padding: 6px 16px; font-size: 12px;
        color: var(--vscode-descriptionForeground, #888);
        cursor: pointer; border-bottom: 2px solid transparent;
        transition: color 0.15s, border-color 0.15s;
    }
    .tab:hover { color: var(--vscode-foreground, #ccc); }
    .tab.active { color: var(--vscode-foreground, #e0e0e0); border-bottom-color: var(--vscode-textLink-foreground, #3794ff); }

    #summary {
        flex-shrink: 0; padding: 8px 16px; font-size: 12px;
        color: var(--vscode-descriptionForeground, #888);
        background: var(--vscode-editor-background, #1e1e1e);
        border-bottom: 1px solid var(--vscode-panel-border, #2a2a2a);
        display: flex; gap: 16px; align-items: center; flex-wrap: wrap;
    }
    .quality-dot { display: inline-block; width: 8px; height: 8px; border-radius: 50%; margin-right: 4px; }

    #entity-bar {
        flex-shrink: 0; padding: 6px 16px;
        background: var(--vscode-editor-background, #1e1e1e);
        border-bottom: 1px solid var(--vscode-panel-border, #2a2a2a);
        display: flex; align-items: center; gap: 8px;
        font-size: 11px; color: var(--vscode-descriptionForeground, #888);
    }
    #entity-bar.hidden { display: none; }
    #entity-select {
        background: var(--vscode-input-background, #333);
        color: var(--vscode-input-foreground, #ccc);
        border: 1px solid var(--vscode-input-border, #555);
        border-radius: 3px; padding: 2px 6px;
        font-size: 11px; font-family: inherit; max-width: 250px;
    }
    #entity-select:focus { outline: 1px solid var(--vscode-focusBorder, #3794ff); border-color: var(--vscode-focusBorder, #3794ff); }
    #random-btn {
        background: var(--vscode-input-background, #333);
        color: var(--vscode-descriptionForeground, #aaa);
        border: 1px solid var(--vscode-input-border, #555);
        border-radius: 3px; padding: 2px 8px;
        font-size: 11px; font-family: inherit; cursor: pointer;
    }
    #random-btn:hover { background: var(--vscode-list-hoverBackground, #3c3c3c); color: var(--vscode-foreground, #ccc); }

    #error-panel {
        display: none; flex: 1; padding: 20px; overflow: auto;
        font-family: var(--vscode-editor-font-family, monospace);
        font-size: 12px; color: var(--vscode-errorForeground, #f44);
        background: var(--vscode-editor-background);
    }
    #error-panel pre {
        white-space: pre-wrap; word-wrap: break-word;
        background: var(--vscode-textBlockQuote-background, #1e1e1e);
        padding: 12px; border-radius: 4px;
        color: var(--vscode-editor-foreground);
    }

    #cards {
        flex: 1; overflow-y: auto; padding: 16px;
        display: grid;
        grid-template-columns: repeat(auto-fill, minmax(340px, 1fr));
        gap: 12px; align-content: start;
    }

    .card {
        background: var(--vscode-editorWidget-background, #252526);
        border: 1px solid var(--vscode-editorWidget-border, #3c3c3c);
        border-left: 3px solid var(--vscode-testing-iconPassed, #4ec94e);
        border-radius: 4px; padding: 12px 14px;
        display: flex; flex-direction: column; gap: 8px;
    }
    .card.warn { border-left-color: var(--vscode-editorWarning-foreground, #e8a838); }
    .card.error { border-left-color: var(--vscode-errorForeground, #f44); }

    .card-header { display: flex; justify-content: space-between; align-items: flex-start; gap: 8px; }
    .card-title { font-size: 13px; font-weight: 600; color: var(--vscode-foreground, #e0e0e0); line-height: 1.3; flex: 1; }
    .card-type {
        font-size: 10px; padding: 2px 6px; border-radius: 3px;
        background: var(--vscode-badge-background, #333);
        color: var(--vscode-descriptionForeground, #888);
        white-space: nowrap; flex-shrink: 0;
    }
    .card-type.float { color: var(--vscode-textLink-foreground, #3794ff); }
    .card-type.int { color: #4ec9b0; }
    .card-type.string { color: #c586c0; }
    .card-type.categorical { color: #dcdcaa; }

    .card-unit { font-size: 11px; color: var(--vscode-descriptionForeground, #666); font-style: italic; }
    .pop-badge { font-size: 10px; color: #a0a0a0; white-space: nowrap; flex-shrink: 0; }

    .sparkline-container { padding: 4px 0; }
    .sparkline-placeholder { color: var(--vscode-disabledForeground, #444); font-size: 11px; font-style: italic; height: 40px; display: flex; align-items: center; }

    .stats {
        display: grid; grid-template-columns: 1fr 1fr;
        gap: 2px 12px; font-size: 11px;
        font-family: var(--vscode-editor-font-family, 'SF Mono', Menlo, Monaco, 'Courier New', monospace);
    }
    .stat { display: flex; gap: 6px; }
    .stat-label { color: var(--vscode-descriptionForeground, #666); min-width: 60px; }
    .stat-value { color: var(--vscode-foreground, #aaa); }

    .null-bar-container { display: flex; align-items: center; gap: 6px; }
    .null-bar { width: 50px; height: 4px; background: var(--vscode-input-background, #333); border-radius: 2px; overflow: hidden; }
    .null-bar-fill { height: 100%; border-radius: 2px; }
    .null-bar-fill.low { background: var(--vscode-testing-iconPassed, #4ec94e); }
    .null-bar-fill.mid { background: var(--vscode-editorWarning-foreground, #e8a838); }
    .null-bar-fill.high { background: var(--vscode-errorForeground, #f44); }

    .flags { display: flex; flex-wrap: wrap; gap: 4px; }
    .flag { font-size: 10px; padding: 1px 6px; border-radius: 3px; background: #3a2a00; color: #e8a838; }
    .flag.severe { background: #3a0000; color: #f44; }

    .source { font-size: 11px; color: var(--vscode-descriptionForeground, #555); }
    .value-dist { display: flex; flex-direction: column; gap: 3px; }
    .vd-row { display: flex; align-items: center; gap: 6px; font-size: 10px; font-family: var(--vscode-editor-font-family, 'SF Mono', Menlo, Monaco, 'Courier New', monospace); }
    .vd-label { color: var(--vscode-foreground, #aaa); white-space: nowrap; overflow: hidden; text-overflow: ellipsis; min-width: 0; flex: 1; max-width: 160px; }
    .vd-bar-wrap { flex: 1; height: 6px; background: var(--vscode-input-background, #2a2a2a); border-radius: 3px; overflow: hidden; min-width: 40px; }
    .vd-bar { height: 100%; background: #c586c0; border-radius: 3px; opacity: 0.7; }
    .vd-pct { color: var(--vscode-descriptionForeground, #666); width: 32px; text-align: right; flex-shrink: 0; }
</style>
</head>
<body>

<div id="info-bar"></div>
<div id="table-tabs" class="hidden"></div>
<div id="summary"></div>
<div id="entity-bar" class="hidden">
    <label for="entity-select">Entity:</label>
    <select id="entity-select"></select>
    <button id="random-btn" onclick="randomizeEntity()">Shuffle</button>
</div>
<div id="cards"></div>
<div id="error-panel"><h3>Step Failed</h3><pre id="error-output"></pre></div>

<script>
let DATA = ${jsonStr};
let activeTable = 0;
let selectedEntity = null;
const COMMAND = ${JSON.stringify(command)};
const LATENCY = ${JSON.stringify(latency)};

const vscode = acquireVsCodeApi();

function init() {
    renderInfoBar();
    renderTabs();
    renderTable(0);

    window.addEventListener('message', (e) => {
        const msg = e.data;
        if (msg.type === 'updateData') {
            DATA = JSON.parse(msg.json);
            renderInfoBar();
            renderTabs();
            renderTable(activeTable);
        }
        if (msg.type === 'status') {
            const el = document.getElementById('status');
            if (el) { el.textContent = msg.text; el.className = 'status-' + (msg.cls || 'ok'); }
        }
        if (msg.type === 'latency') {
            const el = document.getElementById('latency');
            if (el) { el.textContent = msg.text; }
        }
        if (msg.type === 'showError') {
            document.getElementById('cards').style.display = 'none';
            const ep = document.getElementById('error-panel');
            ep.style.display = 'block';
            document.getElementById('error-output').textContent = msg.text;
        }
    });
}

function renderInfoBar() {
    const d = DATA;
    const totalRows = d.tables.reduce((s, t) => s + t.n_rows, 0);
    const totalIndicators = d.tables.reduce((s, t) => s + t.n_indicators, 0);
    document.getElementById('info-bar').innerHTML =
        '<div class="row"><span class="label">Step</span> <span class="highlight">' + d.step_uri + '</span></div>' +
        '<div class="row"><span class="label">Dataset</span> <span class="value">' + (d.title || d.short_name) + '</span></div>' +
        '<div class="row"><span class="label">Tables</span> <span class="value">' + d.n_tables + ' table' + (d.n_tables > 1 ? 's' : '') + ' \\u00b7 ' + totalIndicators + ' indicator' + (totalIndicators > 1 ? 's' : '') + ' \\u00b7 ' + totalRows.toLocaleString() + ' rows</span></div>' +
        '<div class="row"><span class="label">Command</span> <span class="value">' + COMMAND + '</span></div>' +
        '<div class="row"><span class="label">Status</span> <span id="status" class="status-ok">OK</span></div>' +
        '<div class="row"><span class="label">Latency</span> <span id="latency">' + LATENCY + 's</span></div>';
}

function renderTabs() {
    const container = document.getElementById('table-tabs');
    if (DATA.tables.length <= 1) { container.classList.add('hidden'); return; }
    container.classList.remove('hidden');
    container.innerHTML = DATA.tables.map(function(t, i) {
        return '<div class="tab ' + (i === activeTable ? 'active' : '') + '" onclick="switchTab(' + i + ')">' + t.table_name + '</div>';
    }).join('');
}

function switchTab(idx) {
    activeTable = idx;
    document.querySelectorAll('.tab').forEach(function(t, i) { t.classList.toggle('active', i === idx); });
    renderTable(idx);
}

function getEntitiesForTable(table) {
    var entities = new Set();
    for (var ind of table.indicators) {
        if (ind.sparkline_by_entity) {
            for (var name of Object.keys(ind.sparkline_by_entity)) { entities.add(name); }
        }
    }
    return Array.from(entities).sort();
}

function pickRandomEntity(table) {
    var entities = getEntitiesForTable(table);
    if (entities.length === 0) return null;
    var arr = new Uint32Array(1);
    crypto.getRandomValues(arr);
    return entities[arr[0] % entities.length];
}

function renderEntitySelector(table) {
    var bar = document.getElementById('entity-bar');
    var select = document.getElementById('entity-select');
    var entities = getEntitiesForTable(table);
    if (entities.length === 0) { bar.classList.add('hidden'); return; }
    bar.classList.remove('hidden');
    if (!selectedEntity) { selectedEntity = pickRandomEntity(table); }
    var options = '';
    for (var e of entities) {
        var sel = e === selectedEntity ? ' selected' : '';
        options += '<option value="' + escHtml(e) + '"' + sel + '>' + escHtml(e) + '</option>';
    }
    select.innerHTML = options;
    select.onchange = function() { selectedEntity = select.value; renderCards(DATA.tables[activeTable]); };
}

function randomizeEntity() {
    var table = DATA.tables[activeTable];
    selectedEntity = pickRandomEntity(table);
    if (!selectedEntity) return;
    document.getElementById('entity-select').value = selectedEntity;
    renderCards(table);
}

function renderTable(idx) {
    var table = DATA.tables[idx];
    // Hide error panel and show cards
    document.getElementById('error-panel').style.display = 'none';
    document.getElementById('cards').style.display = '';

    var flaggedCount = table.indicators.filter(function(ind) { return ind.quality_flags.length > 0; }).length;
    var indLabel = table.truncated
        ? (table.n_indicators_shown + ' of ' + table.n_indicators + ' indicators (capped)')
        : (table.n_indicators + ' indicator' + (table.n_indicators > 1 ? 's' : ''));
    var summaryHtml = '<span>' + indLabel + '</span>';
    summaryHtml += '<span>' + table.n_rows.toLocaleString() + ' rows</span>';
    if (table.dimensions.length) summaryHtml += '<span>Dimensions: ' + table.dimensions.join(', ') + '</span>';
    if (table.year_min != null) summaryHtml += '<span>' + table.year_min + '\\u2013' + table.year_max + '</span>';
    if (table.entity_count != null) summaryHtml += '<span>' + table.entity_count + ' entit' + (table.entity_count === 1 ? 'y' : 'ies') + '</span>';
    if (flaggedCount > 0) {
        summaryHtml += '<span><span class="quality-dot" style="background:#e8a838"></span>' + flaggedCount + ' with quality issues</span>';
    }
    document.getElementById('summary').innerHTML = summaryHtml;
    renderEntitySelector(table);
    renderCards(table);
}

function renderCards(table) {
    document.getElementById('cards').innerHTML = table.indicators.map(function(ind) { return renderCard(ind, table, selectedEntity); }).join('');
}

function renderCard(ind, table, entity) {
    var flagCount = ind.quality_flags.length;
    var cardClass = flagCount >= 3 ? 'error' : flagCount > 0 ? 'warn' : '';
    var typeClass = getTypeClass(ind.type);
    var sparklineData = entity && ind.sparkline_by_entity && ind.sparkline_by_entity[entity] ? ind.sparkline_by_entity[entity] : null;

    var html = '<div class="card ' + cardClass + '">';
    var popBadge = (ind.popularity > 0) ? '<span class="pop-badge" title="Popularity score">\u2605 ' + ind.popularity.toFixed(2) + '</span>' : '';
    html += '<div class="card-header"><span class="card-title">' + escHtml(ind.title) + '</span><span style="display:flex;gap:4px;align-items:center">' + popBadge + '<span class="card-type ' + typeClass + '">' + simplifyType(ind.type) + '</span></span></div>';
    if (ind.unit) html += '<div class="card-unit">' + escHtml(ind.unit) + '</div>';
    if (!ind.is_numeric && ind.value_counts) {
        html += '<div class="sparkline-container">' + renderValueDist(ind.value_counts) + '</div>';
    } else {
        html += '<div class="sparkline-container">' + renderSparkline(sparklineData) + '</div>';
    }
    html += '<div class="stats">';
    if (table.year_min != null) html += '<div class="stat"><span class="stat-label">Years</span><span class="stat-value">' + table.year_min + '\\u2013' + table.year_max + '</span></div>';
    if (table.entity_count != null) html += '<div class="stat"><span class="stat-label">Entities</span><span class="stat-value">' + table.entity_count + '</span></div>';
    if (ind.stats.min != null) {
        html += '<div class="stat"><span class="stat-label">Min</span><span class="stat-value">' + formatNum(ind.stats.min) + '</span></div>';
        html += '<div class="stat"><span class="stat-label">Max</span><span class="stat-value">' + formatNum(ind.stats.max) + '</span></div>';
    }
    html += '<div class="stat null-bar-container"><span class="stat-label">Nulls</span><span class="stat-value">' + ind.null_count.toLocaleString() + ' (' + ind.null_pct + '%)</span><div class="null-bar"><div class="null-bar-fill ' + (ind.null_pct > 30 ? 'high' : ind.null_pct > 10 ? 'mid' : 'low') + '" style="width:' + Math.min(ind.null_pct, 100) + '%"></div></div></div>';
    html += '</div>';
    if (ind.origins_producer) html += '<div class="source">' + escHtml(ind.origins_producer) + (ind.origins_count > 1 ? ' +' + (ind.origins_count - 1) : '') + '</div>';
    if (ind.quality_flags.length > 0) {
        html += '<div class="flags">' + ind.quality_flags.map(function(f) {
            return '<span class="flag ' + (f === 'missing_title' || f === 'missing_unit' ? 'severe' : '') + '">' + formatFlag(f) + '</span>';
        }).join('') + '</div>';
    }
    html += '</div>';
    return html;
}

function renderValueDist(valueCounts) {
    if (!valueCounts || valueCounts.length === 0) return '<div class="sparkline-placeholder">No data</div>';
    var maxPct = valueCounts[0].pct;
    var rows = valueCounts.map(function(vc) {
        return '<div class="vd-row"><span class="vd-label" title="' + escHtml(vc.value) + '">' + escHtml(vc.value) + '</span><div class="vd-bar-wrap"><div class="vd-bar" style="width:' + (vc.pct / maxPct * 100).toFixed(1) + '%"></div></div><span class="vd-pct">' + vc.pct + '%</span></div>';
    }).join('');
    return '<div class="value-dist">' + rows + '</div>';
}

function renderSparkline(data) {
    if (!data || data.length < 2) return '<div class="sparkline-placeholder">No sparkline data</div>';
    var W = 280, H = 40, PAD = 2;
    var values = data.map(function(d) { return d.value; });
    var min = Math.min.apply(null, values);
    var max = Math.max.apply(null, values);
    var range = max - min || 1;
    var points = data.map(function(d, i) {
        var x = PAD + (i / (data.length - 1)) * (W - 2 * PAD);
        var y = H - PAD - ((d.value - min) / range) * (H - 2 * PAD);
        return x.toFixed(1) + ',' + y.toFixed(1);
    });
    var linePoints = points.join(' ');
    var areaPoints = PAD + ',' + H + ' ' + linePoints + ' ' + (W - PAD) + ',' + H;
    return '<svg width="100%" viewBox="0 0 ' + W + ' ' + (H + 12) + '" preserveAspectRatio="none">' +
        '<polygon points="' + areaPoints + '" fill="#3794ff" fill-opacity="0.06"/>' +
        '<polyline points="' + linePoints + '" fill="none" stroke="#3794ff" stroke-width="1.5" stroke-linejoin="round" stroke-linecap="round"/>' +
        '<text x="' + PAD + '" y="' + (H + 10) + '" font-size="8" fill="#555" font-family="monospace">' + data[0].year + '</text>' +
        '<text x="' + (W - PAD) + '" y="' + (H + 10) + '" font-size="8" fill="#555" font-family="monospace" text-anchor="end">' + data[data.length - 1].year + '</text>' +
        '<text x="' + PAD + '" y="8" font-size="8" fill="#555" font-family="monospace">' + formatNum(max) + '</text>' +
        '<text x="' + (W - PAD) + '" y="' + (H - 2) + '" font-size="8" fill="#555" font-family="monospace" text-anchor="end">' + formatNum(min) + '</text>' +
        '</svg>';
}

function getTypeClass(dtype) {
    var d = dtype.toLowerCase();
    if (d.indexOf('float') >= 0) return 'float';
    if (d.indexOf('int') >= 0) return 'int';
    if (d.indexOf('object') >= 0 || d.indexOf('string') >= 0) return 'string';
    if (d.indexOf('categ') >= 0) return 'categorical';
    return '';
}
function simplifyType(dtype) {
    var d = dtype.toLowerCase();
    if (d.indexOf('float') >= 0) return 'float';
    if (d.indexOf('int') >= 0) return 'int';
    if (d.indexOf('object') >= 0 || d.indexOf('string') >= 0) return 'string';
    if (d.indexOf('categ') >= 0) return 'categorical';
    return dtype;
}
function formatNum(n) {
    if (n == null) return '\\u2014';
    if (Math.abs(n) >= 1e9) return (n / 1e9).toFixed(1) + 'B';
    if (Math.abs(n) >= 1e6) return (n / 1e6).toFixed(1) + 'M';
    if (Math.abs(n) >= 1e4) return (n / 1e3).toFixed(1) + 'K';
    if (Math.abs(n) < 0.01 && n !== 0) return n.toExponential(1);
    if (Number.isInteger(n)) return n.toLocaleString();
    return n.toFixed(2);
}
function formatFlag(f) { return f.replace('missing_', 'no ').replace('_', ' '); }
function escHtml(s) {
    if (!s) return '';
    return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

init();
</script>
</body>
</html>`;
}
