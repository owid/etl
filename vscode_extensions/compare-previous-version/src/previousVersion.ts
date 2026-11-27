import * as fs from 'fs';
import * as path from 'path';

export const VERSION_PATTERN = /^\d{4}-\d{2}-\d{2}$/;

/** Return the first YYYY-MM-DD folder in a path relative to the workspace, or null. */
export function versionInPath(relativePath: string): string | null {
    for (const part of relativePath.split(/[\\/]/)) {
        if (VERSION_PATTERN.test(part)) {
            return part;
        }
    }
    return null;
}

/**
 * Find the previous version of a file: the same file, at the same path, in the most recent date folder that is older
 * than the file's own date folder and that contains that file. Returns an absolute path, or null.
 */
export function findPreviousVersion(workspaceDir: string, currentFilePath: string): string | null {
    const relativePath = path.relative(workspaceDir, currentFilePath);
    const pathParts = relativePath.split(path.sep);

    const versionIndex = pathParts.findIndex((part) => VERSION_PATTERN.test(part));
    if (versionIndex === -1) {
        return null;
    }
    const currentVersion = pathParts[versionIndex];

    const versionParentParts = pathParts.slice(0, versionIndex);
    const versionParentPath = path.join(workspaceDir, ...versionParentParts);
    if (!fs.existsSync(versionParentPath)) {
        return null;
    }

    try {
        const validVersions = fs
            .readdirSync(versionParentPath)
            .filter((entry) => VERSION_PATTERN.test(entry) && fs.statSync(path.join(versionParentPath, entry)).isDirectory())
            .sort();
        const currentVersionIndex = validVersions.indexOf(currentVersion);
        if (currentVersionIndex <= 0) {
            return null;
        }

        // Look backwards through versions to find one that contains the same file.
        for (let i = currentVersionIndex - 1; i >= 0; i--) {
            const candidatePath = path.join(
                workspaceDir,
                ...versionParentParts,
                validVersions[i],
                ...pathParts.slice(versionIndex + 1)
            );
            if (fs.existsSync(candidatePath)) {
                return candidatePath;
            }
        }
        return null;
    } catch {
        return null;
    }
}
