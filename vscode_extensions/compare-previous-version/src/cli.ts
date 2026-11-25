/**
 * Print which files of a branch changed meaningfully against their previous versions.
 *
 * Usage: node dist/cli.js [repo dir] [--base origin/master] [--hunks] [--noise] [--all] [--json] [--rules dvc-outs,dvc-date-accessed,...]
 *
 * --hunks prints the meaningful hunks, --noise prints the line pairs whose only change is a date or hash, --all also lists identical files.
 */
import { ALL_RULE_IDS, buildReport, CATEGORY_LABELS, CATEGORY_ORDER, FileReport, NOISE_RULES } from './versionDiff';

function main() {
    const args = process.argv.slice(2);
    const option = (name: string) => {
        const index = args.indexOf(name);
        return index >= 0 ? args.splice(index, 2)[1] : undefined;
    };
    const flag = (name: string) => {
        const index = args.indexOf(name);
        return index >= 0 ? args.splice(index, 1).length > 0 : false;
    };
    const baseRef = option('--base') ?? 'origin/master';
    const rules = option('--rules')?.split(',') ?? ALL_RULE_IDS;
    const showHunks = flag('--hunks');
    const showNoise = flag('--noise');
    const showAll = flag('--all');
    const asJson = flag('--json');
    const dir = args[0] ?? process.cwd();

    const report = buildReport(dir, { baseRef, rules });
    if (asJson) {
        console.log(JSON.stringify(report, null, 2));
        return;
    }

    console.log(`Base: ${baseRef} (merge base ${report.mergeBase.slice(0, 10)})`);
    console.log(`Date and hash rules: ${NOISE_RULES.filter((rule) => rules.includes(rule.id)).map((rule) => rule.id).join(', ')}`);
    console.log('');
    for (const category of CATEGORY_ORDER) {
        const files = report.files.filter((file) => file.category === category);
        console.log(`${CATEGORY_LABELS[category]}: ${files.length}`);
    }
    for (const category of CATEGORY_ORDER) {
        const files = report.files.filter((file) => file.category === category);
        if (files.length === 0 || (category === 'identical' && !showAll)) {
            continue;
        }
        console.log(`\n== ${CATEGORY_LABELS[category]} (${files.length})`);
        for (const file of files) {
            console.log(`  ${describe(file)}`);
            if (showHunks && file.hunks) {
                for (const hunk of file.hunks) {
                    console.log(`    @@ -${hunk.oldStart + 1},${hunk.oldLines.length} +${hunk.newStart + 1},${hunk.newLines.length} @@`);
                    hunk.oldLines.forEach((line) => console.log(`    -${line}`));
                    hunk.newLines.forEach((line) => console.log(`    +${line}`));
                }
            }
            if (showNoise && file.noisePairs) {
                for (const [oldLine, newLine] of file.noisePairs) {
                    console.log(`    date/hash -${oldLine}`);
                    console.log(`    date/hash +${newLine}`);
                }
            }
        }
    }
}

function describe(file: FileReport): string {
    switch (file.category) {
        case 'meaningful':
            return `${file.path}  (from ${file.previousVersion}; ${file.hunks!.length} hunks, +${file.added} -${file.removed}, ${file.noiseLines} date and hash lines)`;
        case 'noiseOnly':
            return `${file.path}  (from ${file.previousVersion}; ${file.noiseLines} date and hash lines)`;
        case 'missingInNew':
            return `${file.path}  (no ${file.currentVersion} counterpart)`;
        default:
            return `${file.gitStatus} ${file.path}`;
    }
}

main();
