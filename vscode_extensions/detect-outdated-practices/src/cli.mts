/**
 * Command-line runner for the outdated-practice rules, using the exact detector the VS Code extension uses.
 *
 *   node vscode_extensions/detect-outdated-practices/src/cli.mts <file-or-dir> [...] [--json]
 *
 * Paths may be files or directories (searched recursively for .py files), relative to the current
 * directory. Rule scopes are matched against each file's path relative to the repo root. Runs with
 * Node's built-in TypeScript support (Node >= 22.18), so it needs no build step and no npm install.
 */
import fs from 'node:fs';
import path from 'node:path';
import { detect } from './detector.mts';

const repoRoot = path.resolve(import.meta.dirname, '..', '..', '..');
const args = process.argv.slice(2);
const asJson = args.includes('--json');
const targets = args.filter(a => a !== '--json');

if (targets.length === 0) {
    console.error('Usage: node vscode_extensions/detect-outdated-practices/src/cli.mts <file-or-dir> [...] [--json]');
    process.exit(2);
}

function pythonFiles(target: string): string[] {
    const abs = path.resolve(target);
    if (!fs.existsSync(abs)) {
        console.error(`Not found: ${target}`);
        process.exit(2);
    }
    if (fs.statSync(abs).isFile()) {
        return [abs];
    }
    return fs.readdirSync(abs, { recursive: true, withFileTypes: true })
        .filter(entry => entry.isFile() && entry.name.endsWith('.py'))
        .map(entry => path.join(entry.parentPath, entry.name));
}

const files = [...new Set(targets.flatMap(pythonFiles))].sort();
const results = files.flatMap(file => {
    const relativePath = path.relative(repoRoot, file);
    return detect(fs.readFileSync(file, 'utf8'), relativePath).map(finding => ({
        file: relativePath,
        line: finding.line + 1,
        column: finding.start + 1,
        message: finding.message,
    }));
});

if (asJson) {
    console.log(JSON.stringify({ files_checked: files.length, findings: results }, null, 2));
} else {
    for (const r of results) {
        // The first sentence names the problem; --json prints the full fix guidance.
        console.log(`${r.file}:${r.line}:${r.column}: ${r.message.split('. ')[0]}.`);
    }
    console.log(`${results.length} finding(s) in ${files.length} file(s) checked.`);
}
