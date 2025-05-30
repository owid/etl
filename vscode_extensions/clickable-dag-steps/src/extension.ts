import * as vscode from 'vscode';
import * as path from 'path';
import * as fs from 'fs';
import * as yaml from 'js-yaml';

// Status symbols configuration (centralized for easy customization)
const symbols = {
  // Primary status indicators
  green: '🟢',      // File exists, latest version, properly defined
  yellow: '🟡',     // File exists, not latest version, properly defined
  red: '🔴',        // Error state (various issues)
  grey: '⚪',       // Archive state (defined in archive)
  
  // Additional error indicators
  noFile: '❌',                      // No file was found
  multipleDefinitions: '⚠️',         // Step defined more than once
  archivedStepUsedInActiveDag: '❗',  // Step defined in archive but used in active DAG
  undefinedStep: '❓'                // Step not defined anywhere
};

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

// Track versions separately for definitions and dependencies
let definedVersionsByKey: Map<string, Set<string>> = new Map(); // For versions from active DAG definitions
let allVersionsByKey: Map<string, Set<string>> = new Map();     // For versions from all references

let stepDefinitionCount: Map<string, number> = new Map();
let archiveDefinedSteps: Set<string> = new Set();

function buildDAGIndex(): void {
  definedVersionsByKey.clear();
  allVersionsByKey.clear();
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
            const isDefinition = line.trim().endsWith(':');
            const isSnapshot = parsed.key.startsWith('snapshot://');
            
            // Only add references from active DAG to the allVersionsByKey
            if (!isArchive) {
              if (!allVersionsByKey.has(parsed.key)) {
                allVersionsByKey.set(parsed.key, new Set());
              }
              allVersionsByKey.get(parsed.key)!.add(parsed.version);
            }
            
            // If it's a definition in active DAG, track it separately
            if (isDefinition && !isArchive) {
              if (!definedVersionsByKey.has(parsed.key)) {
                definedVersionsByKey.set(parsed.key, new Set());
              }
              definedVersionsByKey.get(parsed.key)!.add(parsed.version);
            }

            if (isDefinition) {
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
      return bDate - aDate; // Sort in descending order to get latest first
    }
    return b.localeCompare(a); // Compare in reverse order
  });

  return sorted[0]; // First element is now the latest
}

// For all steps (including snapshots), only consider versions from active DAG
function getLatestVersionForStep(key: string, isSnapshot: boolean): string | undefined {
  // First check active DAG for any versions (both for regular steps and snapshots)
  const activeDagVersions = new Set<string>();
  
  // Collect versions of this key from all files in active DAG
  const allReferences = allVersionsByKey.get(key);
  const definedVersions = definedVersionsByKey.get(key);
  
  // For non-snapshots, prioritize versions defined in the active DAG
  if (!isSnapshot && definedVersions && definedVersions.size > 0) {
    return getLatestVersion(definedVersions);
  }
  
  // For snapshots or as a fallback, get all versions from active DAG references
  // We'll filter out archive versions by checking file paths in buildDAGIndex
  if (allReferences && allReferences.size > 0) {
    return getLatestVersion(allReferences);
  }
  
  return undefined;
}

export function activate(context: vscode.ExtensionContext) {
  console.log('Clickable DAG Steps with status highlighting activated.');
  buildDAGIndex();

  const emojiDecorations = {
    // Primary status indicators - using before for line start placement
    green: vscode.window.createTextEditorDecorationType({
      before: { 
        contentText: symbols.green,
        margin: '0 8px 0 0',
        textDecoration: 'none; pointer-events: none; user-select: none'
      },
      isWholeLine: false,
      rangeBehavior: vscode.DecorationRangeBehavior.ClosedClosed
    }),
    yellow: vscode.window.createTextEditorDecorationType({
      before: { 
        contentText: symbols.yellow,
        margin: '0 8px 0 0',
        textDecoration: 'none; pointer-events: none; user-select: none'
      },
      isWholeLine: false,
      rangeBehavior: vscode.DecorationRangeBehavior.ClosedClosed
    }),
    red: vscode.window.createTextEditorDecorationType({
      before: { 
        contentText: symbols.red,
        margin: '0 8px 0 0',
        textDecoration: 'none; pointer-events: none; user-select: none'
      },
      isWholeLine: false,
      rangeBehavior: vscode.DecorationRangeBehavior.ClosedClosed
    }),
    grey: vscode.window.createTextEditorDecorationType({
      before: { 
        contentText: symbols.grey,
        margin: '0 8px 0 0',
        textDecoration: 'none; pointer-events: none; user-select: none'
      },
      isWholeLine: false,
      rangeBehavior: vscode.DecorationRangeBehavior.ClosedClosed
    }),
    
    // Additional error indicators (shown alongside the main status)
    noFile: vscode.window.createTextEditorDecorationType({
      before: { 
        contentText: symbols.noFile,
        margin: '0 0 0 0',
        textDecoration: 'none; pointer-events: none; user-select: none'
      },
      isWholeLine: false,
      rangeBehavior: vscode.DecorationRangeBehavior.ClosedClosed
    }),
    multipleDefinitions: vscode.window.createTextEditorDecorationType({
      before: { 
        contentText: symbols.multipleDefinitions,
        margin: '0 0 0 0',
        textDecoration: 'none; pointer-events: none; user-select: none'
      },
      isWholeLine: false,
      rangeBehavior: vscode.DecorationRangeBehavior.ClosedClosed
    }),
    archivedStepUsedInActiveDag: vscode.window.createTextEditorDecorationType({
      before: { 
        contentText: symbols.archivedStepUsedInActiveDag,
        margin: '0 0 0 0',
        textDecoration: 'none; pointer-events: none; user-select: none'
      },
      isWholeLine: false,
      rangeBehavior: vscode.DecorationRangeBehavior.ClosedClosed
    }),
    undefinedStep: vscode.window.createTextEditorDecorationType({
      before: { 
        contentText: symbols.undefinedStep,
        margin: '0 0 0 0',
        textDecoration: 'none; pointer-events: none; user-select: none'
      },
      isWholeLine: false,
      rangeBehavior: vscode.DecorationRangeBehavior.ClosedClosed
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
        // Skip commented lines
        const lineStart = document.positionAt(match.index).line;
        const lineText = document.lineAt(lineStart).text;
        if (lineText.trim().startsWith('#')) {
          continue;
        }

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

        if (fullPath) {
          link.target = vscode.Uri.file(fullPath);
        }

        // Build tooltip with status information based on the exact same rules
        // we use for decorations to ensure consistency
        const tooltipParts = [];
        
        // Get basic information
        const key = parsed.key;
        const fullKey = parsed.fullKey;
        const version = parsed.version;
        const isSnapshot = key.startsWith('snapshot://');
        const isDefinition = text.substr(match.index, uri.length + 1).trim().endsWith(':');
        const isDefinedInArchive = archiveDefinedSteps.has(parsed.fullKey);
        const isDefinedSomewhere = stepDefinitionCount.has(fullKey) || isSnapshot;
        const defCount = stepDefinitionCount.get(parsed.fullKey) || 0;
        
        // For all steps, we now consider only versions from active DAG
        // This is set up in buildDAGIndex and getLatestVersionForStep

        // Check if file exists
        const fileExists = !!fullPath;

        // Check version
        let isLatestVersion = false;
        const latest = getLatestVersionForStep(key, isSnapshot);
        if (latest) {
          isLatestVersion = version === latest;
        }

        // CASE: File doesn't exist (always red)
        if (!fileExists) {
          tooltipParts.push(`${symbols.red} ${symbols.noFile} No file was found`);
        } 
        // File exists - apply the same rules as in updateDecorations
        else {
          // CASE: Archive DAG file
          if (isArchiveFile) {
            // File exists and defined in active DAG
            if (isDefinedSomewhere && !isDefinedInArchive) {
              if (isLatestVersion) {
                tooltipParts.push(`${symbols.green} Open file`);
              } else if (latest) {
                tooltipParts.push(`${symbols.yellow} Open file (latest: ${latest})`);
              } else {
                tooltipParts.push(`${symbols.yellow} Open file`);
              }
            }
            // File exists and defined in archive DAG
            else if (isDefinedInArchive) {
              if (latest && version !== latest) {
                tooltipParts.push(`${symbols.grey} Step defined in archive DAG (latest: ${latest})`);
              } else {
                tooltipParts.push(`${symbols.grey} Step defined in archive DAG`);
              }
            }
            // File exists but not defined anywhere (and not a snapshot)
            else {
              tooltipParts.push(`${symbols.red} ${symbols.undefinedStep} Step not defined anywhere`);
            }
          } 
          // CASE: Active DAG file
          else {
            // Check for archive definition misuse in active DAG
            if (isDefinedInArchive) {
              tooltipParts.push(`${symbols.red} ${symbols.archivedStepUsedInActiveDag} Step defined in the archive DAG, but used in the active DAG`);
            }
            // Check if defined anywhere (except for snapshots)
            else if (!isDefinedSomewhere) {
              tooltipParts.push(`${symbols.red} ${symbols.undefinedStep} Step not defined anywhere`);
            }
            // Check if this version exists in the active DAG
            else {
              // Only show green for the latest version
              if (isLatestVersion) {
                tooltipParts.push(`${symbols.green} Open file`);
              }
              // If it's a valid version but not the latest
              else if (latest) {
                tooltipParts.push(`${symbols.yellow} Open file (latest: ${latest})`);
              }
              // Something's wrong with the version
              else {
                tooltipParts.push(`${symbols.red} Version not found in any active DAG file`);
              }
            }
          }

          // Handle steps defined more than once (should override other statuses)
          if (defCount > 1 && isDefinition) {
            tooltipParts.length = 0; // Clear previous tooltip parts
            tooltipParts.push(`${symbols.red} ${symbols.multipleDefinitions} Step defined more than once in the DAG`);
          }
        }

        link.tooltip = tooltipParts.join('\n');
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

  // Decorations for the main indicators
  const greenRanges: vscode.DecorationOptions[] = [];
  const yellowRanges: vscode.DecorationOptions[] = [];
  const redRanges: vscode.DecorationOptions[] = [];
  const greyRanges: vscode.DecorationOptions[] = [];
  
  // Additional error indicators
  const noFileRanges: vscode.DecorationOptions[] = [];
  const multipleDefinitionsRanges: vscode.DecorationOptions[] = [];
  const archivedStepUsedInActiveDagRanges: vscode.DecorationOptions[] = [];
  const undefinedStepRanges: vscode.DecorationOptions[] = [];

  const isArchiveFile = filePath.includes(path.join('dag', 'archive'));

  const text = editor.document.getText();
  const regex = /(?:data(?:-private)?|export(?:-private)?|snapshot(?:-private)?):\/\/[^\s"']+/g;
  let match: RegExpExecArray | null;

  while ((match = regex.exec(text)) !== null) {
    const uri = match[0];

    // Skip commented lines
    const lineStart = editor.document.positionAt(match.index).line;
    const lineText = editor.document.lineAt(lineStart).text;
    if (lineText.trim().startsWith('#')) {
      continue;
    }

    const parsed = parseStepUri(uri);
    // Get the start position of the line for the URI
    const linePos = getLineStart(editor.document, editor.document.positionAt(match.index));
    const range = new vscode.Range(linePos, linePos);

    if (!parsed) {
      redRanges.push({ range, hoverMessage: `${symbols.red} Invalid URI format` });
      continue;
    }

    const key = parsed.key;
    const fullKey = parsed.fullKey;
    const version = parsed.version;
    const isDefinition = lineText.trim().endsWith(':');
    const isSnapshot = key.startsWith('snapshot://');
    const isDefinedSomewhere = stepDefinitionCount.has(fullKey) || isSnapshot;
    const isDefinedInArchive = archiveDefinedSteps.has(fullKey);
    const defCount = stepDefinitionCount.get(fullKey) || 0;
    
    // Check if file exists
    const existingPath = parsed.filePaths
      .map(p => path.join(workspaceFolder.uri.fsPath, p))
      .find(p => fs.existsSync(p));
    
    const fileExists = !!existingPath;

    // Check version
    let isLatestVersion = false;
    const latest = getLatestVersionForStep(key, isSnapshot);
    if (latest) {
      isLatestVersion = version === latest;
    }

    // Apply decoration based on the specific rules
    
    // CASE: File doesn't exist (always red)
    if (!fileExists) {
      redRanges.push({ range });
      noFileRanges.push({ range, hoverMessage: `${symbols.noFile} No file was found` });
      continue;
    }

    // CASE: Step defined more than once (always show red + warning)
    if (defCount > 1 && isDefinition) {
      redRanges.push({ range });
      multipleDefinitionsRanges.push({ range, hoverMessage: `${symbols.multipleDefinitions} Step defined more than once in the DAG` });
      continue; // Skip further processing since we've already marked it as an error
    }
    
    // CASE: Archive DAG file
    if (isArchiveFile) {
      // File exists and defined in active DAG
      if (isDefinedSomewhere && !isDefinedInArchive) {
        if (isLatestVersion) {
          greenRanges.push({ range });
        } else {
          yellowRanges.push({ range });
        }
      }
      // File exists and defined in archive DAG
      else if (isDefinedInArchive) {
        greyRanges.push({ range, hoverMessage: `${symbols.grey} Step defined in archive DAG` });
      }
      // File exists but not defined anywhere (and not a snapshot)
      else {
        redRanges.push({ range });
        undefinedStepRanges.push({ range, hoverMessage: `${symbols.undefinedStep} Step not defined anywhere` });
      }
    }
    // CASE: Active DAG file
    else {
      // Check for archive definition misuse in active DAG
      if (isDefinedInArchive) {
        redRanges.push({ range });
        archivedStepUsedInActiveDagRanges.push({ range, hoverMessage: `${symbols.archivedStepUsedInActiveDag} Step defined in the archive DAG, but used in the active DAG` });
        continue;
      }
      
      // Check if defined anywhere (except for snapshots)
      if (!isDefinedSomewhere) {
        redRanges.push({ range });
        undefinedStepRanges.push({ range, hoverMessage: `${symbols.undefinedStep} Step not defined anywhere` });
        continue;
      }
      
      // Only the latest version should get a green circle
      if (isLatestVersion) {
        greenRanges.push({ range });
      }
      // If it's a valid version but not the latest
      else if (latest && version !== latest) {
        yellowRanges.push({ range });
      }
      // Something's wrong with the version
      else {
        redRanges.push({ range });
      }
    }
  }

  // Apply decorations
  editor.setDecorations(emojiDecorations.green, greenRanges);
  editor.setDecorations(emojiDecorations.yellow, yellowRanges);
  editor.setDecorations(emojiDecorations.red, redRanges);
  editor.setDecorations(emojiDecorations.grey, greyRanges);
  
  // Apply additional error indicators
  editor.setDecorations(emojiDecorations.noFile, noFileRanges);
  editor.setDecorations(emojiDecorations.multipleDefinitions, multipleDefinitionsRanges);
  editor.setDecorations(emojiDecorations.archivedStepUsedInActiveDag, archivedStepUsedInActiveDagRanges);
  editor.setDecorations(emojiDecorations.undefinedStep, undefinedStepRanges);
}

// Helper function to find the line start position (to place decorations at the beginning of the line)
function getLineStart(document: vscode.TextDocument, position: vscode.Position): vscode.Position {
  return new vscode.Position(position.line, 0);
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
