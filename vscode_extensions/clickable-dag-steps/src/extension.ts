import * as vscode from 'vscode';
import * as path from 'path';
import * as fs from 'fs';
import * as yaml from 'js-yaml';

function parseStepUri(uri: string): { scheme: string; key: string; version: string, fullKey: string, filePaths: string[] } | null {
  const parts = uri.split('://');
  if (parts.length !== 2) {
    return null;
  }

  const rawScheme = parts[0];
  const scheme = rawScheme.replace('-private', '');
  const remainder = parts[1].replace(/:$/, '');
  const segments = remainder.split('/');

  if (scheme === 'snapshot') {
    if (segments.length < 3) {
      return null;
    }
    const namespace = segments[0];
    const version = segments[1];
    const shortName = segments.slice(2).join('/');
    const key = `snapshot://${namespace}/${shortName}`;
    const fullKey = `snapshot://${namespace}/${version}/${shortName}`;
    const filePath = path.join('snapshots', namespace, version, shortName + '.dvc');
    return { scheme, key, version, fullKey, filePaths: [filePath] };
  }

  if (scheme === 'data' || scheme === 'export') {
    if (segments.length < 4) {
      return null;
    }
    const channel = segments[0];
    const namespace = segments[1];
    const version = segments[2];
    const shortName = segments.slice(3).join('/');
    const key = `${scheme}://${channel}/${namespace}/${shortName}`;
    const fullKey = `${scheme}://${channel}/${namespace}/${version}/${shortName}`;
    const base = scheme === 'data' ? 'etl/steps/data' : 'etl/steps/export';
    const dir = path.join(base, channel, namespace, version);
    const filePaths = [
      path.join(dir, shortName + '.py'),
      path.join(dir, shortName, '__init__.py'),
      path.join(dir, shortName + '.ipynb')
    ];
    return { scheme, key, version, fullKey, filePaths };
  }

  return null;
}

let stepVersionsIndex: Map<string, Set<string>> = new Map();
let stepDefinitionCount: Map<string, number> = new Map();
let archiveDefinedSteps: Set<string> = new Set();

function buildDAGIndex(): void {
  stepVersionsIndex.clear();
  stepDefinitionCount.clear();
  archiveDefinedSteps.clear();

  const workspaceFolder = vscode.workspace.workspaceFolders?.[0];
  if (!workspaceFolder) {
    return;
  }
  const dagDir = path.join(workspaceFolder.uri.fsPath, 'dag');

  const allYmlFiles: string[] = [];
  const walk = (dir: string) => {
    for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
      const fullPath = path.join(dir, entry.name);
      if (entry.isDirectory()) {
        walk(fullPath);
      } else if (entry.isFile() && fullPath.endsWith('.yml')) {
        allYmlFiles.push(fullPath);
      }
    }
  };
  walk(dagDir);

  const uriRegex = /^(?:\s*-\s*|\s*)(data(?:-private)?|export(?:-private)?|snapshot(?:-private)?):\/\/[^\s#]+/;

  for (const filePath of allYmlFiles) {
    const isArchive = filePath.includes(path.join('dag', 'archive'));

    try {
      const lines = fs.readFileSync(filePath, 'utf8').split('\n');
      for (const line of lines) {
        const match = uriRegex.exec(line);
        if (match) {
          const uri = match[0].replace(/^\s*-\s*/, '').trim();
          const parsed = parseStepUri(uri);
          if (parsed) {
            if (!stepVersionsIndex.has(parsed.key)) {
              stepVersionsIndex.set(parsed.key, new Set());
            }
            stepVersionsIndex.get(parsed.key)!.add(parsed.version);

            if (line.trim().endsWith(':')) {
              const count = stepDefinitionCount.get(parsed.fullKey) || 0;
              stepDefinitionCount.set(parsed.fullKey, count + 1);
              if (isArchive) {
                archiveDefinedSteps.add(parsed.fullKey);
              }
            }
          }
        }
      }
    } catch (err) {
      console.error(`Error parsing DAG file ${filePath}:`, err);
    }
  }
}

function getLatestVersion(versions: Set<string>): string {
  if (versions.has('latest')) {
    return 'latest';
  }

  const sorted = Array.from(versions).sort((a, b) => {
    const aDate = Date.parse(a);
    const bDate = Date.parse(b);
    if (!isNaN(aDate) && !isNaN(bDate)) {
      return aDate - bDate;
    }
    return a.localeCompare(b);
  });

  return sorted[sorted.length - 1];
}

export function activate(context: vscode.ExtensionContext) {
  console.log('Clickable DAG Steps with status highlighting activated.');
  buildDAGIndex();

  const emojiDecorations = {
    green: vscode.window.createTextEditorDecorationType({
      after: { contentText: '🟢', margin: '0 0 0 0.25em' }
    }),
    yellow: vscode.window.createTextEditorDecorationType({
      after: { contentText: '🟡', margin: '0 0 0 0.25em' }
    }),
    red: vscode.window.createTextEditorDecorationType({
      after: { contentText: '🔴', margin: '0 0 0 0.25em' }
    }),
    warning: vscode.window.createTextEditorDecorationType({
      after: { contentText: '⚠️', margin: '0 0 0 0.25em' }
    }),
    archive: vscode.window.createTextEditorDecorationType({
      after: { contentText: '❗', margin: '0 0 0 0.25em' }
    }),
    grey: vscode.window.createTextEditorDecorationType({
      after: { contentText: '⚪', margin: '0 0 0 0.25em' }
    }),
    question: vscode.window.createTextEditorDecorationType({
      after: { contentText: '❓', margin: '0 0 0 0.25em' }
    })
  };

  const linkProvider: vscode.DocumentLinkProvider = {
    provideDocumentLinks(document: vscode.TextDocument) {
      const links: vscode.DocumentLink[] = [];
      const regex = /(?:data(?:-private)?|export(?:-private)?|snapshot(?:-private)?):\/\/[^\s"']+/g;
      const text = document.getText();
      let match: RegExpExecArray | null;
      const isArchiveFile = document.uri.fsPath.includes(path.join('dag', 'archive'));

      while ((match = regex.exec(text)) !== null) {
        const uri = match[0];
        const parsed = parseStepUri(uri);
        if (!parsed) {
          continue;
        }

        const workspaceFolder = vscode.workspace.getWorkspaceFolder(document.uri);
        if (!workspaceFolder) {
          continue;
        }

        const fullPath = parsed.filePaths
          .map(p => path.join(workspaceFolder.uri.fsPath, p))
          .find(p => fs.existsSync(p));

        const startPos = document.positionAt(match.index);
        const endPos = document.positionAt(match.index + uri.length);
        const range = new vscode.Range(startPos, endPos);
        const link = new vscode.DocumentLink(range);

        const allVersions = stepVersionsIndex.get(parsed.key);
        let tooltip = '';

        if (fullPath) {
          link.target = vscode.Uri.file(fullPath);
          if (allVersions && allVersions.has(parsed.version)) {
            const latest = getLatestVersion(allVersions);
            tooltip = parsed.version === latest ? '🟢 Open file' : `🟡 Open file (latest: ${latest})`;
          }
        } else {
          tooltip = '🔴 File not found';
        }

        const defCount = stepDefinitionCount.get(parsed.fullKey) || 0;

        if (!isArchiveFile && archiveDefinedSteps.has(parsed.fullKey)) {
          tooltip += (tooltip ? '\n' : '') + '❗ Step defined in the archive';
        }

        if (defCount > 1 && text.substr(match.index, uri.length + 1).trim().endsWith(':')) {
          tooltip += (tooltip ? '\n' : '') + '⚠️ Step defined more than once';
        }

        if (isArchiveFile && archiveDefinedSteps.has(parsed.fullKey)) {
          tooltip += (tooltip ? '\n' : '') + '⚪ Step defined in archive DAG';
        }

        if (!stepDefinitionCount.has(parsed.fullKey) && !parsed.key.startsWith('snapshot://')) {
          tooltip += (tooltip ? '\n' : '') + '❓ Step not defined anywhere';
        }

        link.tooltip = tooltip;
        links.push(link);
      }

      return links;
    }
  };

function updateDecorations(editor: vscode.TextEditor) {
  const filePath = editor.document.uri.fsPath;
  const workspaceFolder = vscode.workspace.workspaceFolders?.[0];
  if (!workspaceFolder || !filePath.includes(path.join(workspaceFolder.uri.fsPath, 'dag'))) {
    return;
  }

  const greenRanges: vscode.DecorationOptions[] = [];
  const yellowRanges: vscode.DecorationOptions[] = [];
  const redRanges: vscode.DecorationOptions[] = [];
  const warningRanges: vscode.DecorationOptions[] = [];
  const archiveRanges: vscode.DecorationOptions[] = [];
  const greyRanges: vscode.DecorationOptions[] = [];
  const questionRanges: vscode.DecorationOptions[] = [];

  const isArchiveFile = filePath.includes(path.join('dag', 'archive'));

  const text = editor.document.getText();
  const regex = /(?:data(?:-private)?|export(?:-private)?|snapshot(?:-private)?):\/\/[^\s"']+/g;
  let match: RegExpExecArray | null;

  while ((match = regex.exec(text)) !== null) {
    const uri = match[0];

    const lineStart = editor.document.positionAt(match.index).line;
    const lineText = editor.document.lineAt(lineStart).text;
    if (lineText.trim().startsWith('#')) {
      continue; // Ignore commented lines
    }

    const parsed = parseStepUri(uri);
    const startPos = editor.document.positionAt(match.index);
    const line = editor.document.lineAt(startPos.line);
    const range = new vscode.Range(line.range.end, line.range.end);

    if (!parsed) {
      redRanges.push({ range });
      continue;
    }

    const key = parsed.key;
    const fullKey = parsed.fullKey;
    const version = parsed.version;
    const allVersions = stepVersionsIndex.get(key);
    const defCount = stepDefinitionCount.get(fullKey) || 0;
    const isDefinition = line.text.trim().endsWith(':');

    const existingPath = parsed.filePaths
      .map(p => path.join(workspaceFolder.uri.fsPath, p))
      .find(p => fs.existsSync(p));

    if (!existingPath) {
      redRanges.push({ range });
      continue;
    }

    // GREY for anything in archive DAG that is also defined in archive
    if (isArchiveFile && archiveDefinedSteps.has(fullKey)) {
      greyRanges.push({ range, hoverMessage: '⚪ Step defined in archive DAG' });
      continue;
    }

    // ❗ for anything outside archive DAG that is defined in archive
    if (!isArchiveFile && archiveDefinedSteps.has(fullKey)) {
      archiveRanges.push({ range, hoverMessage: '❗ Step defined in the archive DAG' });
    }

    // ❓ for anything not defined in any DAG (skip snapshots)
    if (!stepDefinitionCount.has(fullKey) && !key.startsWith('snapshot://')) {
      questionRanges.push({ range, hoverMessage: '❓ Step not defined anywhere' });
    }

    if (defCount > 1 && isDefinition) {
      warningRanges.push({ range, hoverMessage: '⚠️ Step defined more than once' });
      continue;
    }

    if (!allVersions || !allVersions.has(version)) {
      redRanges.push({ range });
    } else {
      const latest = getLatestVersion(allVersions);
      if (version === latest) {
        greenRanges.push({ range });
      } else {
        yellowRanges.push({ range });
      }
    }
  }

  editor.setDecorations(emojiDecorations.green, greenRanges);
  editor.setDecorations(emojiDecorations.yellow, yellowRanges);
  editor.setDecorations(emojiDecorations.red, redRanges);
  editor.setDecorations(emojiDecorations.warning, warningRanges);
  editor.setDecorations(emojiDecorations.archive, archiveRanges);
  editor.setDecorations(emojiDecorations.grey, greyRanges);
  editor.setDecorations(emojiDecorations.question, questionRanges);
}

  context.subscriptions.push(
    vscode.languages.registerDocumentLinkProvider({ language: 'yaml', scheme: 'file' }, linkProvider)
  );

  const updateAllEditors = () => {
    for (const editor of vscode.window.visibleTextEditors) {
      if (editor.document.languageId === 'yaml') {
        updateDecorations(editor);
      }
    }
  };

  const watcher = vscode.workspace.createFileSystemWatcher('**/dag/**/*.yml');
  watcher.onDidChange(() => {
    buildDAGIndex();
    updateAllEditors();
  });
  watcher.onDidCreate(() => {
    buildDAGIndex();
    updateAllEditors();
  });
  watcher.onDidDelete(() => {
    buildDAGIndex();
    updateAllEditors();
  });

  context.subscriptions.push(watcher);

  vscode.workspace.onDidChangeTextDocument(event => {
    const editor = vscode.window.visibleTextEditors.find(e => e.document.uri === event.document.uri);
    if (editor) {
      updateDecorations(editor);
    }
  });

  vscode.window.onDidChangeVisibleTextEditors(() => {
    updateAllEditors();
  });

  updateAllEditors();
}

export function deactivate() {}
