/**
 * Read and set GitHub's "Viewed" state of the files of the branch's pull request, through the GitHub CLI (gh) and
 * the user's own gh login. No VS Code dependency, so that it can be tested on its own.
 */
import { spawn } from 'child_process';

export type ViewedState = 'VIEWED' | 'UNVIEWED' | 'DISMISSED';

export interface PullRequest {
    id: string;
    number: number;
    url: string;
}

export interface GhResult {
    code: number;
    stdout: string;
    stderr: string;
}

export type GhRunner = (args: string[], input?: string) => Promise<GhResult>;

export function ghRunner(cwd: string, ghPath: string): GhRunner {
    return (args, input) =>
        new Promise((resolve, reject) => {
            const child = spawn(ghPath, args, { cwd, env: { ...process.env, GH_PROMPT_DISABLED: '1', NO_COLOR: '1' } });
            let stdout = '';
            let stderr = '';
            child.stdout.on('data', (chunk) => (stdout += chunk));
            child.stderr.on('data', (chunk) => (stderr += chunk));
            child.on('error', reject);
            child.on('close', (code) => resolve({ code: code ?? 1, stdout, stderr }));
            child.stdin.end(input ?? '');
        });
}

/** The open pull request of the current branch, or the reason there is none (to show to the user). */
export async function findPullRequest(run: GhRunner): Promise<{ pr?: PullRequest; reason?: string }> {
    let result: GhResult;
    try {
        result = await run(['pr', 'view', '--json', 'id,number,url,state']);
    } catch (error) {
        const code = (error as NodeJS.ErrnoException).code;
        return { reason: code === 'ENOENT' ? 'the GitHub CLI (gh) was not found' : `gh failed (${error})` };
    }
    if (result.code !== 0) {
        const message = result.stderr.trim().split('\n')[0];
        if (/no pull requests found/i.test(message)) {
            return { reason: 'this branch has no pull request' };
        }
        if (/auth|log ?in|token/i.test(message)) {
            return { reason: `gh is not logged in (${message})` };
        }
        return { reason: `gh could not find the pull request (${message})` };
    }
    const data = JSON.parse(result.stdout);
    if (data.state !== 'OPEN') {
        return { reason: `the pull request of this branch (#${data.number}) is ${String(data.state).toLowerCase()}` };
    }
    return { pr: { id: data.id, number: data.number, url: data.url } };
}

interface GraphqlResponse {
    data?: Record<string, unknown> | null;
    errors?: { message: string; path?: (string | number)[] }[];
}

async function graphql(run: GhRunner, query: string, variables: Record<string, unknown>): Promise<GraphqlResponse> {
    const result = await run(['api', 'graphql', '--input', '-'], JSON.stringify({ query, variables }));
    try {
        return JSON.parse(result.stdout) as GraphqlResponse;
    } catch {
        throw new Error(result.stderr.trim() || `gh exited with code ${result.code}`);
    }
}

/** The Viewed state of every file of the pull request, for the logged-in user. */
export async function fetchViewedStates(run: GhRunner, pullRequestId: string): Promise<Map<string, ViewedState>> {
    const query = `query($id: ID!, $after: String) {
        node(id: $id) { ... on PullRequest {
            files(first: 100, after: $after) { pageInfo { hasNextPage endCursor } nodes { path viewerViewedState } }
        } }
    }`;
    const states = new Map<string, ViewedState>();
    let after: string | null = null;
    for (;;) {
        const response = await graphql(run, query, { id: pullRequestId, after });
        if (response.errors?.length || !response.data) {
            throw new Error(response.errors?.[0]?.message ?? 'empty response from GitHub');
        }
        const files = (response.data.node as { files: FilesPage }).files;
        for (const node of files.nodes) {
            states.set(node.path, node.viewerViewedState);
        }
        if (!files.pageInfo.hasNextPage) {
            return states;
        }
        after = files.pageInfo.endCursor;
    }
}

interface FilesPage {
    pageInfo: { hasNextPage: boolean; endCursor: string };
    nodes: { path: string; viewerViewedState: ViewedState }[];
}

export interface SetViewedResult {
    succeeded: { path: string; viewed: boolean }[];
    failed: { path: string; viewed: boolean; message: string }[];
}

/** Mark (or unmark) several files as viewed, in a single GraphQL request with one aliased mutation per file. */
export async function setViewed(
    run: GhRunner,
    pullRequestId: string,
    changes: { path: string; viewed: boolean }[]
): Promise<SetViewedResult> {
    const variables: Record<string, string> = { pr: pullRequestId };
    const declarations = ['$pr: ID!'];
    const fields: string[] = [];
    changes.forEach((change, index) => {
        variables[`p${index}`] = change.path;
        declarations.push(`$p${index}: String!`);
        const mutation = change.viewed ? 'markFileAsViewed' : 'unmarkFileAsViewed';
        fields.push(`f${index}: ${mutation}(input: {pullRequestId: $pr, path: $p${index}}) { clientMutationId }`);
    });
    const query = `mutation(${declarations.join(', ')}) { ${fields.join(' ')} }`;

    let response: GraphqlResponse;
    try {
        response = await graphql(run, query, variables);
    } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        return { succeeded: [], failed: changes.map((change) => ({ ...change, message })) };
    }
    const errorByAlias = new Map<string, string>();
    let generalError: string | undefined;
    for (const error of response.errors ?? []) {
        const alias = error.path?.[0];
        if (typeof alias === 'string' && /^f\d+$/.test(alias)) {
            errorByAlias.set(alias, error.message);
        } else {
            generalError = error.message;
        }
    }
    const result: SetViewedResult = { succeeded: [], failed: [] };
    changes.forEach((change, index) => {
        const alias = `f${index}`;
        const done = response.data?.[alias] !== undefined && response.data?.[alias] !== null;
        if (done && !errorByAlias.has(alias)) {
            result.succeeded.push(change);
        } else {
            result.failed.push({ ...change, message: errorByAlias.get(alias) ?? generalError ?? 'no result from GitHub' });
        }
    });
    return result;
}

export interface SyncOptions {
    // Files per GraphQL request, pause between requests, and wait before sending (to group quick clicks).
    batchSize: number;
    pauseMs: number;
    debounceMs: number;
}

// At most 20 files every 4 seconds (300 a minute), well below GitHub's limits for mutations.
const DEFAULT_SYNC_OPTIONS: SyncOptions = { batchSize: 20, pauseMs: 4000, debounceMs: 400 };

/**
 * Queue of Viewed changes for one pull request. Changes to the same file are merged (the last one wins), and they are
 * sent in batches, one request at a time, with a pause between requests.
 */
export class ViewedSync {
    private readonly queue = new Map<string, boolean>();
    private readonly inFlight = new Set<string>();
    private timer: NodeJS.Timeout | undefined;
    private flushing: Promise<void> | undefined;
    private readonly options: SyncOptions;

    constructor(
        private readonly run: GhRunner,
        readonly pullRequest: PullRequest,
        // Called after each request, and once at the end of each flush with all the failures of that flush.
        private readonly onBatch: (result: SetViewedResult) => void,
        private readonly onFlushEnd: (failed: SetViewedResult['failed']) => void,
        private readonly wrapFlush: (work: () => Promise<void>) => Promise<void> = (work) => work(),
        options: Partial<SyncOptions> = {}
    ) {
        this.options = { ...DEFAULT_SYNC_OPTIONS, ...options };
    }

    enqueue(path: string, viewed: boolean): void {
        this.queue.set(path, viewed);
        this.schedule();
    }

    dequeue(path: string): void {
        this.queue.delete(path);
    }

    isBusyWith(path: string): boolean {
        return this.queue.has(path) || this.inFlight.has(path);
    }

    /** Send everything queued now, and resolve when the queue is empty. */
    flushNow(): Promise<void> {
        if (this.timer) {
            clearTimeout(this.timer);
            this.timer = undefined;
        }
        if (!this.flushing) {
            this.flushing = this.wrapFlush(() => this.drain()).finally(() => {
                this.flushing = undefined;
                if (this.queue.size > 0) {
                    this.schedule();
                }
            });
        }
        return this.flushing;
    }

    private schedule(): void {
        if (this.flushing || this.timer) {
            return;
        }
        this.timer = setTimeout(() => {
            this.timer = undefined;
            void this.flushNow();
        }, this.options.debounceMs);
    }

    private async drain(): Promise<void> {
        const failed: SetViewedResult['failed'] = [];
        let first = true;
        while (this.queue.size > 0) {
            if (!first) {
                await new Promise((resolve) => setTimeout(resolve, this.options.pauseMs));
            }
            first = false;
            const batch = [...this.queue.entries()].slice(0, this.options.batchSize).map(([path, viewed]) => ({ path, viewed }));
            for (const change of batch) {
                this.queue.delete(change.path);
                this.inFlight.add(change.path);
            }
            const result = await setViewed(this.run, this.pullRequest.id, batch);
            for (const change of batch) {
                this.inFlight.delete(change.path);
            }
            failed.push(...result.failed);
            this.onBatch(result);
        }
        this.onFlushEnd(failed);
    }
}
