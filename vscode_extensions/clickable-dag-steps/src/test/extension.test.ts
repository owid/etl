import * as assert from 'assert';

import * as vscode from 'vscode';

import { classifyDagLine } from '../extension';

suite('classifyDagLine', () => {
	vscode.window.showInformationMessage('Start all tests.');

	test('top-level step declaration', () => {
		const result = classifyDagLine('  data://garden/un/2022-07-11/un_wpp:');
		assert.deepStrictEqual(result, {
			uri: 'data://garden/un/2022-07-11/un_wpp',
			isDefinition: true,
		});
	});

	test('plain dependency reference', () => {
		const result = classifyDagLine('    - data://garden/un/2022-07-11/un_wpp');
		assert.deepStrictEqual(result, {
			uri: 'data://garden/un/2022-07-11/un_wpp',
			isDefinition: false,
		});
	});

	test('snapshot dependency', () => {
		const result = classifyDagLine('    - snapshot://un/2022-07-11/un_wpp.zip');
		assert.deepStrictEqual(result, {
			uri: 'snapshot://un/2022-07-11/un_wpp.zip',
			isDefinition: false,
		});
	});

	test('lines with no URI return null', () => {
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
});
