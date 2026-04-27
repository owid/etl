import * as assert from 'assert';

import * as vscode from 'vscode';

import { classifyDagLine } from '../extension';

suite('classifyDagLine', () => {
	vscode.window.showInformationMessage('Start all tests.');

	test('top-level flat definition', () => {
		const result = classifyDagLine('  data://garden/un/2022-07-11/un_wpp:');
		assert.deepStrictEqual(result, {
			uri: 'data://garden/un/2022-07-11/un_wpp',
			isDefinition: true,
		});
	});

	test('flat dependency reference', () => {
		const result = classifyDagLine('    - data://garden/un/2022-07-11/un_wpp');
		assert.deepStrictEqual(result, {
			uri: 'data://garden/un/2022-07-11/un_wpp',
			isDefinition: false,
		});
	});

	test('nested declaration (list item ending with colon)', () => {
		const result = classifyDagLine('    - data://garden/un/2022-07-11/un_wpp:');
		assert.deepStrictEqual(result, {
			uri: 'data://garden/un/2022-07-11/un_wpp',
			isDefinition: true,
		});
	});

	test('deeply-nested declaration', () => {
		const result = classifyDagLine('      - data://meadow/un/2022-07-11/un_wpp:');
		assert.deepStrictEqual(result, {
			uri: 'data://meadow/un/2022-07-11/un_wpp',
			isDefinition: true,
		});
	});

	test('leaf snapshot reference inside a nested chain', () => {
		const result = classifyDagLine('        - snapshot://un/2022-07-11/un_wpp.zip');
		assert.deepStrictEqual(result, {
			uri: 'snapshot://un/2022-07-11/un_wpp.zip',
			isDefinition: false,
		});
	});

	test('comment-only line returns null', () => {
		assert.strictEqual(classifyDagLine('  # UN WPP (2022)'), null);
		assert.strictEqual(classifyDagLine(''), null);
		assert.strictEqual(classifyDagLine('steps:'), null);
	});

	test('data-private scheme is recognised', () => {
		const result = classifyDagLine('  data-private://garden/wip/latest/foo:');
		assert.deepStrictEqual(result, {
			uri: 'data-private://garden/wip/latest/foo',
			isDefinition: true,
		});
	});

	test('export scheme is recognised', () => {
		const result = classifyDagLine('    - export://explorers/un/latest/un_wpp');
		assert.deepStrictEqual(result, {
			uri: 'export://explorers/un/latest/un_wpp',
			isDefinition: false,
		});
	});

	test('a compacted chain parses as exactly one definition per step', () => {
		// Regression test for the compact nested DAG syntax (issue #5881):
		// every step that appears with a trailing ``:`` must be classified as a
		// definition exactly once, whether it lives at the top level or is
		// tucked inside a dep list.
		const chain = [
			'  data://grapher/un/2022-07-11/un_wpp:',
			'    - data://garden/un/2022-07-11/un_wpp:',
			'      - data://meadow/un/2022-07-11/un_wpp:',
			'        - snapshot://un/2022-07-11/un_wpp.zip',
		];
		const defs = new Set<string>();
		const refs = new Set<string>();
		for (const line of chain) {
			const c = classifyDagLine(line);
			if (!c) {
				continue;
			}
			(c.isDefinition ? defs : refs).add(c.uri);
		}
		assert.deepStrictEqual(defs, new Set([
			'data://grapher/un/2022-07-11/un_wpp',
			'data://garden/un/2022-07-11/un_wpp',
			'data://meadow/un/2022-07-11/un_wpp',
		]));
		assert.deepStrictEqual(refs, new Set(['snapshot://un/2022-07-11/un_wpp.zip']));
	});
});
