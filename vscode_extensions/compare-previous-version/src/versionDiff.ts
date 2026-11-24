/**
 * Classify the files of a branch against their previous versions, without any VS Code dependency (so the same code
 * runs in the extension and in the command-line script).
 *
 * A dataset update copies each step folder to a new version folder, so git shows the new files as entirely new. Here,
 * each new file in a version folder is compared with the same file in its previous version folder, and the changes
 * are split into "noise" (the mechanical edits of every update, see NOISE_RULES) and meaningful changes.
 */
import { execFileSync } from 'child_process';
import * as fs from 'fs';
import * as path from 'path';
import { findPreviousVersion, versionInPath } from './previousVersion';

export type Category =
    | 'meaningful'
    | 'noiseOnly'
    | 'identical'
    | 'noPrevious'
    | 'missingInNew'
    | 'dag'
    | 'dagArchive'
    | 'other';

export const CATEGORY_LABELS: Record<Category, string> = {
    meaningful: 'Meaningful changes',
    missingInNew: 'In previous version, missing in new version',
    noPrevious: 'New files, with no previous version',
    dag: 'DAG',
    other: 'Other changed files (diff against base)',
    noiseOnly: 'Only noise changes',
    identical: 'Identical to previous version',
    dagArchive: 'DAG archive',
};

export const CATEGORY_ORDER: Category[] = [
    'meaningful',
    'missingInNew',
    'noPrevious',
    'dag',
    'other',
    'noiseOnly',
    'identical',
    'dagArchive',
];

/** Group of a changed file that is not compared with a previous version (diffed against the base instead). */
function baseDiffCategory(relativePath: string): Category {
    if (relativePath.startsWith('dag/archive/')) {
        return 'dagArchive';
    }
    return relativePath.startsWith('dag/') ? 'dag' : 'other';
}

export interface NoiseRule {
    id: string;
    description: string;
}

// Each rule only ever makes two lines that differ count as equal; a line that is unchanged is never affected.
export const NOISE_RULES: NoiseRule[] = [
    {
        id: 'version-dates',
        description:
            'One version date of this branch replaced by another (any old or new version folder date of the compared files, as a whole token), e.g. data://garden/faostat/2026-02-25/... -> .../2026-11-23/..., or the version in a docstring.',
    },
    { id: 'dvc-outs', description: 'md5, size and nfiles values under "outs:" in a .dvc file.' },
    { id: 'dvc-date-accessed', description: 'The value of date_accessed in a .dvc file.' },
    { id: 'dvc-date-published', description: 'The value of date_published in a .dvc file.' },
    {
        id: 'dvc-citation-dates',
        description:
            'Years, and the date after "accessed", in the attribution or citation_full of a .dvc file, e.g. "(2025)" -> "(2026)", or "(Accessed on 21-August-2026)" -> "(Accessed on 11-November-2026)".',
    },
];

export const ALL_RULE_IDS = NOISE_RULES.map((rule) => rule.id);

export interface FileReport {
    category: Category;
    // Path relative to the repository root (the new file, or the old file for "missingInNew").
    path: string;
    previousPath?: string;
    previousVersion?: string;
    currentVersion?: string;
    // Git status letter, for files diffed against the base ("dag", "dagArchive" and "other") and for "noPrevious".
    gitStatus?: string;
    // Meaningful hunks and lines (for "meaningful"), and lines whose change is noise.
    hunks?: Hunk[];
    added?: number;
    removed?: number;
    noiseLines?: number;
    // The pairs of lines (old, new) that differ only in noise.
    noisePairs?: [string, string][];
}

export interface Hunk {
    // 0-based start and length in the old and new files.
    oldStart: number;
    oldLines: string[];
    newStart: number;
    newLines: string[];
}

export interface ReviewReport {
    root: string;
    baseRef: string;
    mergeBase: string;
    versionDates: string[];
    files: FileReport[];
}

export interface ReviewOptions {
    baseRef?: string;
    rules?: string[];
}

function git(root: string, args: string[]): string {
    // Capture stderr too, so that expected failures (e.g. a file missing in the base) don't print to the log.
    return execFileSync('git', args, {
        cwd: root,
        encoding: 'utf8',
        maxBuffer: 256 * 1024 * 1024,
        stdio: ['ignore', 'pipe', 'pipe'],
    });
}

export function gitRoot(dir: string): string {
    return git(dir, ['rev-parse', '--show-toplevel']).trim();
}

/** Files changed in the working tree (including uncommitted and untracked files) since the merge base with baseRef. */
export function listChangedFiles(root: string, mergeBase: string): { status: string; path: string }[] {
    const changed = git(root, ['diff', '--name-status', '--no-renames', mergeBase])
        .split('\n')
        .filter((line) => line.trim() !== '')
        .map((line) => {
            const [status, ...rest] = line.split('\t');
            return { status: status[0], path: rest.join('\t') };
        });
    const untracked = git(root, ['ls-files', '--others', '--exclude-standard'])
        .split('\n')
        .filter((line) => line.trim() !== '')
        .map((line) => ({ status: 'A', path: line }));
    return [...changed, ...untracked];
}

export function readGitFile(root: string, ref: string, relativePath: string): string {
    try {
        return git(root, ['show', `${ref}:${relativePath}`]);
    } catch {
        return '';
    }
}

export function splitLines(text: string): string[] {
    return text.split(/\r?\n/);
}

function versionDatesRegex(versionDates: string[]): RegExp | null {
    if (versionDates.length === 0) {
        return null;
    }
    return new RegExp(`(?<![\\w-])(${versionDates.join('|')})(?![\\w-])`, 'g');
}

// A date right after "accessed" (as in "Accessed on 11-November-2026", "accessed November 11, 2026" or "accessed
// 2026-11-11"); any other text after "accessed" is left alone.
const ACCESS_DATE =
    /(\b[Aa]ccessed(?: on)?:?\s*)(\d{1,2}[- ][A-Za-z]+[- ,]*\d{4}|[A-Za-z]+\.? \d{1,2},? \d{4}|\d{4}-\d{2}-\d{2}|\d{1,2}\/\d{1,2}\/\d{4})/g;

/** Rewrite the noise parts of each line, so that lines differing only in noise become equal. */
export function normalizeLines(lines: string[], relativePath: string, versionDates: string[], rules: Set<string>): string[] {
    const isDvc = relativePath.endsWith('.dvc');
    const outsStart = isDvc ? lines.findIndex((line) => /^outs:\s*$/.test(line)) : -1;
    const versionRegex = rules.has('version-dates') ? versionDatesRegex(versionDates) : null;
    // Indentation of the attribution or citation_full key whose block (or one-line value) is being read, if any.
    let citationIndent: number | null = null;

    return lines.map((line, index) => {
        let normalized = line;
        if (versionRegex) {
            normalized = normalized.replace(versionRegex, '<VERSION>');
        }
        if (isDvc) {
            if (rules.has('dvc-outs') && outsStart >= 0 && index > outsStart) {
                normalized = normalized.replace(/^(\s*-?\s*)(md5|size|nfiles):\s*\S+\s*$/, '$1$2: <OUTS>');
            }
            if (rules.has('dvc-date-accessed')) {
                normalized = normalized.replace(/^(\s*)date_accessed:.*$/, '$1date_accessed: <DATE>');
            }
            if (rules.has('dvc-date-published')) {
                normalized = normalized.replace(/^(\s*)date_published:.*$/, '$1date_published: <DATE>');
            }
            if (rules.has('dvc-citation-dates')) {
                const indent = line.length - line.trimStart().length;
                const key = line.match(/^\s*(attribution|citation_full):/);
                if (key) {
                    citationIndent = indent;
                } else if (citationIndent !== null && line.trim() !== '' && indent <= citationIndent) {
                    citationIndent = null;
                }
                if (citationIndent !== null) {
                    normalized = normalized
                        .replace(ACCESS_DATE, '$1<DATE>')
                        .replace(/(?<!\d)(1[89]|20)\d{2}(?!\d)/g, '<YEAR>');
                }
            }
        }
        return normalized;
    });
}

/**
 * Myers diff on two sequences of integers. Returns the matched index pairs, or null if the sequences differ in more
 * than maxD places (to bound memory on very different files).
 */
function myersMatches(a: Int32Array, b: Int32Array, maxD: number): [number, number][] | null {
    const n = a.length;
    const m = b.length;
    const max = n + m;
    if (max === 0) {
        return [];
    }
    const offset = max + 1;
    const v = new Int32Array(2 * max + 3);
    const trace: Int32Array[] = [];
    let found = -1;
    for (let d = 0; d <= max && found < 0; d++) {
        if (d > maxD) {
            return null;
        }
        for (let k = -d; k <= d; k += 2) {
            let x: number;
            if (k === -d || (k !== d && v[offset + k - 1] < v[offset + k + 1])) {
                x = v[offset + k + 1];
            } else {
                x = v[offset + k - 1] + 1;
            }
            let y = x - k;
            while (x < n && y < m && a[x] === b[y]) {
                x++;
                y++;
            }
            v[offset + k] = x;
            if (x >= n && y >= m) {
                found = d;
                break;
            }
        }
        trace.push(v.slice(offset - d, offset + d + 1));
    }

    // Walk back through the trace, collecting the diagonal moves (matches).
    const pairs: [number, number][] = [];
    let x = n;
    let y = m;
    for (let d = found; d > 0; d--) {
        const previous = trace[d - 1];
        const get = (k: number) => previous[k + d - 1];
        const k = x - y;
        const previousK = k === -d || (k !== d && get(k - 1) < get(k + 1)) ? k + 1 : k - 1;
        const previousX = get(previousK);
        const previousY = previousX - previousK;
        const snakeStartX = previousK === k + 1 ? previousX : previousX + 1;
        while (x > snakeStartX) {
            x--;
            y--;
            pairs.push([x, y]);
        }
        x = previousX;
        y = previousY;
    }
    while (x > 0 && y > 0) {
        x--;
        y--;
        pairs.push([x, y]);
    }
    return pairs.reverse();
}

/** Matched line pairs between two line arrays (common prefix and suffix are matched directly). */
export function matchLines(a: string[], b: string[], maxD = 3000): [number, number][] {
    let start = 0;
    while (start < a.length && start < b.length && a[start] === b[start]) {
        start++;
    }
    let endA = a.length;
    let endB = b.length;
    while (endA > start && endB > start && a[endA - 1] === b[endB - 1]) {
        endA--;
        endB--;
    }

    const ids = new Map<string, number>();
    const toIds = (lines: string[]) =>
        Int32Array.from(lines, (line) => {
            let id = ids.get(line);
            if (id === undefined) {
                id = ids.size;
                ids.set(line, id);
            }
            return id;
        });
    // If the middle is too different, match nothing in it: everything there counts as a meaningful change.
    const middle = myersMatches(toIds(a.slice(start, endA)), toIds(b.slice(start, endB)), maxD) ?? [];

    const pairs: [number, number][] = [];
    for (let i = 0; i < start; i++) {
        pairs.push([i, i]);
    }
    for (const [i, j] of middle) {
        pairs.push([i + start, j + start]);
    }
    for (let k = 0; k < a.length - endA; k++) {
        pairs.push([endA + k, endB + k]);
    }
    return pairs;
}

export interface ComparedLines {
    hunks: Hunk[];
    noiseLines: number;
    noisePairs: [string, string][];
    // The old file with its noise changes replaced by the new lines, so a diff against the new file shows only the
    // meaningful hunks.
    patchedOldText: string;
}

export function compareTexts(
    oldText: string,
    newText: string,
    relativePath: string,
    versionDates: string[],
    rules: Set<string>
): ComparedLines {
    const oldLines = splitLines(oldText);
    const newLines = splitLines(newText);
    const pairs = matchLines(
        normalizeLines(oldLines, relativePath, versionDates, rules),
        normalizeLines(newLines, relativePath, versionDates, rules)
    );

    const hunks: Hunk[] = [];
    const patched: string[] = [];
    const noisePairs: [string, string][] = [];
    let i = 0;
    let j = 0;
    for (const [pi, pj] of [...pairs, [oldLines.length, newLines.length] as [number, number]]) {
        if (pi > i || pj > j) {
            hunks.push({ oldStart: i, oldLines: oldLines.slice(i, pi), newStart: j, newLines: newLines.slice(j, pj) });
            patched.push(...oldLines.slice(i, pi));
        }
        if (pi < oldLines.length) {
            if (oldLines[pi] !== newLines[pj]) {
                noisePairs.push([oldLines[pi], newLines[pj]]);
            }
            patched.push(newLines[pj]);
        }
        i = pi + 1;
        j = pj + 1;
    }
    const eol = newText.includes('\r\n') ? '\r\n' : '\n';
    return { hunks, noiseLines: noisePairs.length, noisePairs, patchedOldText: patched.join(eol) };
}

/** Classify every file changed in the branch against its previous version. */
export function buildReport(dir: string, options: ReviewOptions = {}): ReviewReport {
    const root = gitRoot(dir);
    const baseRef = options.baseRef ?? 'origin/master';
    const rules = new Set(options.rules ?? ALL_RULE_IDS);
    const mergeBase = git(root, ['merge-base', baseRef, 'HEAD']).trim();
    const changed = listChangedFiles(root, mergeBase);

    const files: FileReport[] = [];
    const pairs: { path: string; previousPath: string }[] = [];
    for (const { status, path: relativePath } of changed) {
        const absolutePath = path.join(root, relativePath);
        // Only files added in a version folder are compared with a previous version; files edited in place are
        // diffed against the base instead.
        if (status === 'A' && versionInPath(relativePath) && fs.existsSync(absolutePath)) {
            const previous = findPreviousVersion(root, absolutePath);
            if (previous) {
                pairs.push({ path: relativePath, previousPath: path.relative(root, previous) });
            } else {
                files.push({ category: 'noPrevious', path: relativePath, gitStatus: status });
            }
        } else {
            files.push({ category: baseDiffCategory(relativePath), path: relativePath, gitStatus: status });
        }
    }

    const versionDates = [
        ...new Set(pairs.flatMap((pair) => [versionInPath(pair.path)!, versionInPath(pair.previousPath)!])),
    ].sort();

    for (const pair of pairs) {
        const report: FileReport = {
            category: 'identical',
            path: pair.path,
            previousPath: pair.previousPath,
            previousVersion: versionInPath(pair.previousPath)!,
            currentVersion: versionInPath(pair.path)!,
        };
        const oldBuffer = fs.readFileSync(path.join(root, pair.previousPath));
        const newBuffer = fs.readFileSync(path.join(root, pair.path));
        if (!oldBuffer.equals(newBuffer)) {
            const compared = compareTexts(oldBuffer.toString('utf8'), newBuffer.toString('utf8'), pair.path, versionDates, rules);
            report.hunks = compared.hunks;
            report.noiseLines = compared.noiseLines;
            report.noisePairs = compared.noisePairs;
            report.added = compared.hunks.reduce((total, hunk) => total + hunk.newLines.length, 0);
            report.removed = compared.hunks.reduce((total, hunk) => total + hunk.oldLines.length, 0);
            report.category = compared.hunks.length > 0 ? 'meaningful' : 'noiseOnly';
        }
        files.push(report);
    }

    files.push(...findMissingInNew(root, pairs));
    files.sort((a, b) => pipelineRank(a.path) - pipelineRank(b.path) || a.path.localeCompare(b.path));
    return { root, baseRef, mergeBase, versionDates, files };
}

/** Order files as the pipeline runs: snapshot, meadow, garden, grapher, then everything else. */
function pipelineRank(relativePath: string): number {
    const prefixes = ['snapshots/', 'etl/steps/data/meadow/', 'etl/steps/data/garden/', 'etl/steps/data/grapher/', 'etl/steps/'];
    const index = prefixes.findIndex((prefix) => relativePath.startsWith(prefix));
    return index === -1 ? prefixes.length : index;
}

/**
 * Files of an updated step that exist in its previous version folder but not in the new one (e.g. a dropped
 * .countries.json or .corrections.yml). A file counts as belonging to an updated step when its name starts with the
 * same step name (the part before the first dot) as a file in the new folder.
 */
function findMissingInNew(root: string, pairs: { path: string; previousPath: string }[]): FileReport[] {
    const folderPairs = new Map<string, string>();
    for (const pair of pairs) {
        folderPairs.set(`${path.dirname(pair.previousPath)}\t${path.dirname(pair.path)}`, '');
    }
    const missing: FileReport[] = [];
    for (const key of folderPairs.keys()) {
        const [previousFolder, currentFolder] = key.split('\t');
        const listFiles = (folder: string) =>
            fs.readdirSync(path.join(root, folder)).filter((name) => fs.statSync(path.join(root, folder, name)).isFile());
        const currentFiles = new Set(listFiles(currentFolder));
        const currentSteps = new Set([...currentFiles].map((name) => name.split('.')[0]));
        for (const name of listFiles(previousFolder)) {
            if (currentFiles.has(name) || !currentSteps.has(name.split('.')[0])) {
                continue;
            }
            // Skip it if a more recent version folder than this one also has the file.
            const previous = findPreviousVersion(root, path.join(root, currentFolder, name));
            if (previous && path.relative(root, previous) === path.join(previousFolder, name)) {
                missing.push({
                    category: 'missingInNew',
                    path: path.join(previousFolder, name),
                    previousVersion: versionInPath(previousFolder)!,
                    currentVersion: versionInPath(currentFolder)!,
                });
            }
        }
    }
    return missing;
}
