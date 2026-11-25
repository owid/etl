/**
 * "ETL PR Review" view (its own Activity Bar container): lists the files changed in the branch, grouped
 * by how they differ from their previous version, and opens each one as a diff against that version. The
 * "reviewed" checkboxes are synced with GitHub's "Viewed" state on the branch's pull request, when there is one.
 */
import * as crypto from 'crypto';
import * as fs from 'fs';
import * as path from 'path';
import * as vscode from 'vscode';
import {
    fetchViewedStates,
    findPullRequest,
    ghRunner,
    PullRequest,
    SetViewedResult,
    ViewedState,
    ViewedSync,
} from './githubSync';
import {
    ALL_RULE_IDS,
    buildReport,
    Category,
    CATEGORY_LABELS,
    CATEGORY_ORDER,
    FileReport,
    readGitFile,
    ReviewReport,
} from './versionDiff';

const SCHEME = 'owid-version-diff';
const VIEW_ID = 'owidVersionDiff';
const REVIEWED_KEY = 'owidVersionDiff.reviewed';
const GITHUB_KEY = 'owidVersionDiff.github';
// Categories stepped through by "next" and "previous", in this order.
const TO_REVIEW: Category[] = ['meaningful', 'missingInNew', 'noPrevious', 'dag', 'other'];
const COLLAPSED: Category[] = ['noiseOnly', 'identical', 'dagArchive'];
// Categories diffed against the branch's merge base instead of a previous version.
const BASE_DIFF: Category[] = ['dag', 'dagArchive', 'other'];

// What this workspace knows about the pull request's Viewed state, kept between sessions.
interface GitHubRecord {
    prId: string;
    // The last GitHub state read or set for each file.
    seen: Record<string, ViewedState>;
    // Ticks and unticks that have not reached GitHub yet (failed, or made while the state was being read).
    pending: Record<string, boolean>;
}

interface GroupNode {
    kind: 'group';
    category: Category;
    children: FileNode[];
}

interface FileNode {
    kind: 'file';
    file: FileReport;
    parent: GroupNode;
}

type Node = GroupNode | FileNode;

function config() {
    const settings = vscode.workspace.getConfiguration('owidVersionDiff');
    return {
        baseRef: settings.get<string>('baseRef', 'origin/master'),
        rules: settings.get<string[]>('noiseRules', ALL_RULE_IDS),
        githubSync: settings.get<boolean>('githubSync', true),
        ghPath: settings.get<string>('ghPath', 'gh'),
    };
}

/** The gh executable: the configured one, or Homebrew's when "gh" is not on the PATH of VS Code. */
function resolveGh(configured: string): string {
    if (configured !== 'gh') {
        return configured;
    }
    const onPath = (process.env.PATH ?? '').split(path.delimiter).some((dir) => fs.existsSync(path.join(dir, 'gh')));
    if (onPath) {
        return 'gh';
    }
    return ['/opt/homebrew/bin/gh', '/usr/local/bin/gh'].find((candidate) => fs.existsSync(candidate)) ?? 'gh';
}

class ReviewTree implements vscode.TreeDataProvider<Node>, vscode.TextDocumentContentProvider {
    report: ReviewReport | null = null;
    groups: GroupNode[] = [];
    view: vscode.TreeView<Node> | undefined;
    lastOpened: FileNode | undefined;
    private scanned = false;
    private fingerprints = new Map<string, string>();
    // Local "reviewed" records (file path -> fingerprint of its content when it was ticked).
    private local: Record<string, string>;
    // GitHub sync.
    private generation = 0;
    private githubState: 'loading' | 'on' | 'off' = 'off';
    private githubReason = '';
    private notifiedOff = false;
    private pullRequest: PullRequest | undefined;
    private sync: ViewedSync | undefined;
    private remote = new Map<string, ViewedState>();
    private lastWrite = new Map<string, number>();
    private record: GitHubRecord | undefined;
    // Files ticked here before GitHub knew about them, until the user says whether to send those ticks to GitHub.
    private undecided = new Set<string>();
    private readonly treeChanged = new vscode.EventEmitter<Node | undefined>();
    readonly onDidChangeTreeData = this.treeChanged.event;
    private readonly contentChanged = new vscode.EventEmitter<vscode.Uri>();
    readonly onDidChange = this.contentChanged.event;

    constructor(private readonly context: vscode.ExtensionContext) {
        this.local = { ...context.workspaceState.get<Record<string, string>>(REVIEWED_KEY, {}) };
        this.record = context.workspaceState.get<GitHubRecord>(GITHUB_KEY);
    }

    refresh(fire = true): void {
        const folder = vscode.workspace.workspaceFolders?.[0];
        const generation = ++this.generation;
        this.scanned = true;
        this.fingerprints.clear();
        this.report = null;
        this.groups = [];
        this.lastOpened = undefined;
        if (!folder) {
            this.setMessage('Open the ETL repository to compare its changes with previous versions.');
        } else {
            const { baseRef, rules } = config();
            try {
                this.report = buildReport(folder.uri.fsPath, { baseRef, rules });
                this.groups = CATEGORY_ORDER.map((category) => {
                    const group: GroupNode = { kind: 'group', category, children: [] };
                    group.children = this.report!.files
                        .filter((file) => file.category === category)
                        .map((file) => ({ kind: 'file', file, parent: group }));
                    return group;
                }).filter((group) => group.children.length > 0);
                void this.loadGitHub(generation);
                this.updateMessage();
            } catch (error) {
                this.setMessage(`Could not compare with ${baseRef}: ${error instanceof Error ? error.message : error}`);
            }
        }
        if (fire) {
            this.treeChanged.fire(undefined);
        }
        // Diffs already open against the merge base depend on the scan.
        this.refreshOpenDocuments();
    }

    fileSaved(): void {
        this.fingerprints.clear();
        this.treeChanged.fire(undefined);
    }

    refreshOpenDocuments(): void {
        for (const document of vscode.workspace.textDocuments) {
            if (document.uri.scheme === SCHEME) {
                this.contentChanged.fire(document.uri);
            }
        }
    }

    private setMessage(message: string): void {
        if (this.view) {
            this.view.message = message;
        }
    }

    updateMessage(): void {
        if (!this.report) {
            return;
        }
        const count = (category: Category) => this.report!.files.filter((file) => file.category === category).length;
        const compared = count('meaningful') + count('noiseOnly') + count('identical');
        const pending = Object.keys(this.record?.pending ?? {}).length;
        const github =
            this.githubState === 'loading'
                ? 'Reading the Viewed state from GitHub...'
                : this.githubState === 'on'
                  ? `Checkboxes are synced with "Viewed" on PR #${this.pullRequest!.number}` +
                    (pending > 0 ? ` (${plural(pending, 'change')} not on GitHub yet).` : '.')
                  : `Checkboxes are kept only in VS Code: ${this.githubReason}.`;
        this.setMessage(
            `${compared} files compared with their previous version: ${count('meaningful')} with meaningful changes, ` +
                `${count('noiseOnly')} with only date and hash changes, ${count('identical')} identical. ` +
                `Base: ${this.report.baseRef}. ` +
                github
        );
    }

    // GitHub sync.

    private async loadGitHub(generation: number): Promise<void> {
        const settings = config();
        if (!settings.githubSync) {
            this.setGitHubOff('GitHub sync is turned off in the settings', false);
            return;
        }
        this.githubState = 'loading';
        const run = ghRunner(this.report!.root, resolveGh(settings.ghPath));
        const found = await findPullRequest(run);
        if (generation !== this.generation) {
            return;
        }
        if (!found.pr) {
            this.setGitHubOff(found.reason!, true);
            return;
        }
        const pr = found.pr;
        const startedAt = Date.now();
        let states: Map<string, ViewedState>;
        try {
            states = await fetchViewedStates(run, pr.id);
        } catch (error) {
            if (generation === this.generation) {
                this.setGitHubOff(`could not read the Viewed state from GitHub (${errorMessage(error)})`, true);
            }
            return;
        }
        if (generation !== this.generation || !this.report) {
            return;
        }

        // Keep what this session sent to GitHub after the read started, since the read may not include it.
        const samePullRequest = this.pullRequest?.id === pr.id;
        if (samePullRequest) {
            for (const [filePath, time] of this.lastWrite) {
                const known = this.remote.get(filePath);
                if (time > startedAt && known !== undefined) {
                    states.set(filePath, known);
                }
            }
        } else {
            this.sync = new ViewedSync(
                run,
                pr,
                (result) => this.onBatch(pr, result),
                (failed) => this.onFlushEnd(pr, failed),
                (work) =>
                    Promise.resolve(
                        vscode.window.withProgress(
                            { location: vscode.ProgressLocation.Window, title: 'Syncing "Viewed" with GitHub' },
                            work
                        )
                    )
            );
            this.lastWrite.clear();
        }
        this.pullRequest = pr;
        this.remote = states;
        const record: GitHubRecord =
            this.record?.prId === pr.id ? this.record : { prId: pr.id, seen: {}, pending: {} };
        this.record = record;

        this.undecided.clear();
        for (const file of this.report.files) {
            const state = states.get(file.path);
            if (state === undefined) {
                // Not in the pull request (not pushed yet, or an old file): the local state is all there is.
                delete record.pending[file.path];
                continue;
            }
            const pending = record.pending[file.path];
            if (pending !== undefined) {
                if ((state === 'VIEWED') === pending) {
                    delete record.pending[file.path];
                    record.seen[file.path] = state;
                } else {
                    this.sync!.enqueue(file.path, pending);
                }
                continue;
            }
            // A tick made here that never reached GitHub (made before the pull request existed, or while GitHub
            // was unreachable): ask before sending it.
            if (state !== 'VIEWED' && record.seen[file.path] !== 'VIEWED' && this.isLocallyReviewed(file)) {
                this.undecided.add(file.path);
                continue;
            }
            record.seen[file.path] = state;
            this.setLocal(file, state === 'VIEWED');
        }
        this.githubState = 'on';
        await this.persist();
        this.updateMessage();
        this.treeChanged.fire(undefined);
        if (this.undecided.size > 0) {
            void this.askAboutUndecided(generation, pr);
        }
    }

    private setGitHubOff(reason: string, notify: boolean): void {
        this.githubState = 'off';
        this.githubReason = reason;
        this.updateMessage();
        this.treeChanged.fire(undefined);
        if (notify && !this.notifiedOff) {
            this.notifiedOff = true;
            vscode.window.showInformationMessage(`Reviewed checkboxes are kept only in VS Code: ${reason}.`);
        }
    }

    private async askAboutUndecided(generation: number, pr: PullRequest): Promise<void> {
        const mark = 'Mark them as viewed on GitHub';
        const untick = 'Untick them here';
        const choice = await vscode.window.showInformationMessage(
            `${plural(this.undecided.size, 'file')} ticked as reviewed in VS Code ${this.undecided.size === 1 ? 'is' : 'are'} ` +
                `not marked as viewed on PR #${pr.number}.`,
            mark,
            untick
        );
        if (generation !== this.generation || !choice || !this.report || !this.record) {
            return;
        }
        for (const filePath of this.undecided) {
            const file = this.report.files.find((candidate) => candidate.path === filePath);
            if (!file) {
                continue;
            }
            if (choice === mark) {
                this.record.pending[filePath] = true;
                this.sync!.enqueue(filePath, true);
            } else {
                this.record.seen[filePath] = this.remote.get(filePath)!;
                this.setLocal(file, false);
            }
        }
        this.undecided.clear();
        await this.persist();
        this.updateMessage();
        this.treeChanged.fire(undefined);
    }

    private onBatch(pr: PullRequest, result: SetViewedResult): void {
        if (this.pullRequest?.id !== pr.id || !this.record) {
            return;
        }
        const now = Date.now();
        for (const { path: filePath, viewed } of result.succeeded) {
            const state: ViewedState = viewed ? 'VIEWED' : 'UNVIEWED';
            this.remote.set(filePath, state);
            this.lastWrite.set(filePath, now);
            this.record.seen[filePath] = state;
            if (this.record.pending[filePath] === viewed) {
                delete this.record.pending[filePath];
            }
        }
        void this.persist();
        this.updateMessage();
        this.treeChanged.fire(undefined);
    }

    private onFlushEnd(pr: PullRequest, failed: SetViewedResult['failed']): void {
        if (failed.length === 0 || this.pullRequest?.id !== pr.id) {
            return;
        }
        const retry = 'Retry now';
        void vscode.window
            .showWarningMessage(
                `Could not update "Viewed" on GitHub for ${plural(failed.length, 'file')} (${failed[0].message}). ` +
                    'They keep their state here, and are sent again at the next refresh.',
                retry
            )
            .then((choice) => {
                if (choice === retry && this.record && this.pullRequest?.id === pr.id) {
                    for (const { path: filePath } of failed) {
                        const pending = this.record.pending[filePath];
                        if (pending !== undefined) {
                            this.sync!.enqueue(filePath, pending);
                        }
                    }
                    void this.sync!.flushNow();
                }
            });
    }

    private async persist(): Promise<void> {
        await this.context.workspaceState.update(REVIEWED_KEY, this.local);
        await this.context.workspaceState.update(GITHUB_KEY, this.record);
    }

    // Tree.

    getChildren(node?: Node): Node[] {
        if (!node) {
            if (!this.scanned) {
                this.refresh(false);
            }
            return this.groups;
        }
        return node.kind === 'group' ? node.children : [];
    }

    getParent(node: Node): Node | undefined {
        return node.kind === 'file' ? node.parent : undefined;
    }

    getTreeItem(node: Node): vscode.TreeItem {
        if (node.kind === 'group') {
            const item = new vscode.TreeItem(
                CATEGORY_LABELS[node.category],
                COLLAPSED.includes(node.category)
                    ? vscode.TreeItemCollapsibleState.Collapsed
                    : vscode.TreeItemCollapsibleState.Expanded
            );
            const reviewed = node.children.filter((child) => this.isReviewed(child.file)).length;
            const all = reviewed === node.children.length;
            item.description = `${node.children.length} (${reviewed} reviewed)`;
            item.contextValue = `group-${node.category}`;
            item.checkboxState = {
                state: all ? vscode.TreeItemCheckboxState.Checked : vscode.TreeItemCheckboxState.Unchecked,
                tooltip: all ? 'Untick all files of this group' : 'Tick all files of this group as reviewed',
            };
            return item;
        }

        const file = node.file;
        const absolutePath = path.join(this.report!.root, file.path);
        const item = new vscode.TreeItem(path.basename(file.path));
        item.resourceUri = vscode.Uri.file(absolutePath);
        item.description = describe(file);
        item.tooltip = tooltip(file);
        item.contextValue = `file-${file.category}`;
        item.command = { command: 'owidVersionDiff.openDiff', title: 'Open Diff', arguments: [node] };
        item.checkboxState = this.isReviewed(file)
            ? vscode.TreeItemCheckboxState.Checked
            : vscode.TreeItemCheckboxState.Unchecked;
        return item;
    }

    // Reviewed state. Locally, a file stays reviewed until its content (or its previous version's) changes. For a file
    // of the pull request, GitHub's Viewed state wins once it has been read (see the README).

    private fingerprint(file: FileReport): string {
        const cached = this.fingerprints.get(file.path);
        if (cached !== undefined) {
            return cached;
        }
        const root = this.report!.root;
        const read = (relativePath: string) => {
            const absolutePath = path.join(root, relativePath);
            return fs.existsSync(absolutePath) ? fs.readFileSync(absolutePath) : Buffer.alloc(0);
        };
        const hash = crypto.createHash('sha1');
        hash.update(read(file.path));
        hash.update('\0');
        if (file.previousPath) {
            hash.update(read(file.previousPath));
        } else if (BASE_DIFF.includes(file.category)) {
            hash.update(readGitFile(root, this.report!.mergeBase, file.path));
        }
        const fingerprint = hash.digest('hex');
        this.fingerprints.set(file.path, fingerprint);
        return fingerprint;
    }

    private isLocallyReviewed(file: FileReport): boolean {
        const stored = this.local[file.path];
        return stored !== undefined && stored === this.fingerprint(file);
    }

    private setLocal(file: FileReport, reviewed: boolean): void {
        if (reviewed) {
            this.local[file.path] = this.fingerprint(file);
        } else {
            delete this.local[file.path];
        }
    }

    isReviewed(file: FileReport): boolean {
        if (this.githubState === 'on' && this.remote.has(file.path) && !this.undecided.has(file.path)) {
            return this.record?.pending[file.path] ?? this.remote.get(file.path) === 'VIEWED';
        }
        return this.isLocallyReviewed(file);
    }

    async setReviewed(nodes: FileNode[], reviewed: boolean): Promise<void> {
        for (const { file } of nodes) {
            this.setLocal(file, reviewed);
            this.undecided.delete(file.path);
            const remote = this.remote.get(file.path);
            if (this.githubState === 'on' && remote !== undefined && this.record && this.sync) {
                if ((remote === 'VIEWED') === reviewed && !this.sync.isBusyWith(file.path)) {
                    delete this.record.pending[file.path];
                    this.record.seen[file.path] = remote;
                } else {
                    this.record.pending[file.path] = reviewed;
                    this.sync.enqueue(file.path, reviewed);
                }
            } else if (this.githubState === 'loading' && this.record) {
                // Sent once the state is read (and dropped if the file turns out not to be in the pull request).
                this.record.pending[file.path] = reviewed;
            }
        }
        await this.persist();
        this.updateMessage();
        this.treeChanged.fire(undefined);
    }

    // Content of the left side of the diffs.

    provideTextDocumentContent(uri: vscode.Uri): string {
        const query = decodeQuery(uri.query);
        const report = this.report;
        if (!report) {
            return '';
        }
        switch (query.kind) {
            case 'git':
                return readGitFile(report.root, query.ref, query.path);
            default:
                return '';
        }
    }

    // Opening files.

    async open(node: FileNode): Promise<void> {
        const report = this.report!;
        const file = node.file;
        const absolutePath = path.join(report.root, file.path);
        const name = path.basename(file.path);
        this.lastOpened = node;

        if (file.category === 'noPrevious' || file.category === 'missingInNew') {
            await vscode.commands.executeCommand('vscode.open', vscode.Uri.file(absolutePath));
            return;
        }
        if (BASE_DIFF.includes(file.category)) {
            const left = vscode.Uri.from({
                scheme: SCHEME,
                path: absolutePath,
                query: encodeQuery({ kind: 'git', ref: report.mergeBase, path: file.path }),
            });
            const right = fs.existsSync(absolutePath)
                ? vscode.Uri.file(absolutePath)
                : vscode.Uri.from({ scheme: SCHEME, path: absolutePath, query: encodeQuery({ kind: 'empty' }) });
            const title = `${name}: ${report.baseRef} ↔ working tree`;
            await vscode.commands.executeCommand('vscode.diff', left, right, title);
            return;
        }

        const left = vscode.Uri.file(path.join(report.root, file.previousPath!));
        const suffix = file.category === 'noiseOnly' ? ' (only date and hash changes)' : '';
        const title = `${name}: ${file.previousVersion} ↔ ${file.currentVersion}${suffix}`;
        await vscode.commands.executeCommand('vscode.diff', left, vscode.Uri.file(absolutePath), title);
    }

    async step(direction: 1 | -1): Promise<void> {
        if (!this.scanned) {
            this.refresh();
        }
        const nodes = this.groups.filter((group) => TO_REVIEW.includes(group.category)).flatMap((group) => group.children);
        if (nodes.length === 0) {
            vscode.window.showInformationMessage('No changed files to review.');
            return;
        }
        const current = this.lastOpened ? nodes.indexOf(this.lastOpened) : -1;
        const next = current < 0 ? (direction === 1 ? 0 : nodes.length - 1) : (current + direction + nodes.length) % nodes.length;
        await this.view?.reveal(nodes[next], { select: true, focus: false });
        await this.open(nodes[next]);
    }
}

// The query of the virtual documents, in base64url so that it survives any URI encoding and decoding.
function encodeQuery(query: Record<string, string>): string {
    return Buffer.from(JSON.stringify(query)).toString('base64url');
}

function decodeQuery(query: string): Record<string, string> {
    return JSON.parse(Buffer.from(query, 'base64url').toString('utf8'));
}

function shortFolder(relativePath: string): string {
    return path
        .dirname(relativePath)
        .replace(/^etl\/steps\/data\//, '')
        .replace(/^etl\/steps\//, '')
        .split('/')
        .filter((part) => !/^\d{4}-\d{2}-\d{2}$/.test(part))
        .join('/');
}

function describe(file: FileReport): string {
    const folder = shortFolder(file.path);
    switch (file.category) {
        case 'meaningful':
            return `${folder} · +${file.added} −${file.removed} · from ${file.previousVersion}`;
        case 'noiseOnly':
            return `${folder} · ${file.noiseLines} date and hash lines · from ${file.previousVersion}`;
        case 'identical':
            return `${folder} · from ${file.previousVersion}`;
        case 'missingInNew':
            return `${folder} · ${file.previousVersion}, not in ${file.currentVersion}`;
        default:
            return `${file.gitStatus} · ${path.dirname(file.path)}`;
    }
}

function tooltip(file: FileReport): string {
    const lines = [file.path];
    if (file.previousPath) {
        lines.push(`Previous version: ${file.previousPath}`);
    }
    if (file.category === 'meaningful') {
        lines.push(
            `${file.hunks!.length} meaningful hunks (+${file.added} −${file.removed}); ${file.noiseLines} date and hash lines.`
        );
    }
    if (file.category === 'missingInNew') {
        lines.push(`This file is in version ${file.previousVersion} but not in ${file.currentVersion}.`);
    }
    return lines.join('\n');
}

export function registerReviewView(context: vscode.ExtensionContext): void {
    const tree = new ReviewTree(context);
    const view = vscode.window.createTreeView(VIEW_ID, {
        treeDataProvider: tree,
        showCollapseAll: true,
        // Group checkboxes tick their files here, and are ticked when all their files are.
        manageCheckboxStateManually: true,
    });
    tree.view = view;

    context.subscriptions.push(
        view,
        vscode.workspace.registerTextDocumentContentProvider(SCHEME, tree),
        view.onDidChangeCheckboxState(async (event) => {
            for (const [node, state] of event.items) {
                const checked = state === vscode.TreeItemCheckboxState.Checked;
                await tree.setReviewed(node.kind === 'group' ? node.children : [node], checked);
            }
        }),
        vscode.commands.registerCommand('owidVersionDiff.refresh', () => tree.refresh()),
        vscode.commands.registerCommand('owidVersionDiff.openDiff', (node: FileNode) => tree.open(node)),
        vscode.commands.registerCommand('owidVersionDiff.next', () => tree.step(1)),
        vscode.commands.registerCommand('owidVersionDiff.previous', () => tree.step(-1)),
        vscode.workspace.onDidChangeConfiguration((event) => {
            const rescan = ['baseRef', 'noiseRules', 'githubSync', 'ghPath'];
            if (rescan.some((name) => event.affectsConfiguration(`owidVersionDiff.${name}`))) {
                tree.refresh();
            }
        }),
        // An edited file changes its reviewed state.
        vscode.workspace.onDidSaveTextDocument(() => tree.fileSaved())
    );
}

function errorMessage(error: unknown): string {
    return error instanceof Error ? error.message : String(error);
}

function plural(count: number, noun: string): string {
    return `${count} ${noun}${count === 1 ? '' : 's'}`;
}
