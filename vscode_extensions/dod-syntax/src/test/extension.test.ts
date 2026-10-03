import * as assert from 'assert';
import * as vscode from 'vscode';

suite('DOD Syntax Extension Test Suite', () => {
    vscode.window.showInformationMessage('Start all tests.');

    test('Extension should be present', () => {
        assert.ok(vscode.extensions.getExtension('owid.dod-syntax'));
    });

    test('Extension should activate', async () => {
        const extension = vscode.extensions.getExtension('owid.dod-syntax');
        if (extension) {
            await extension.activate();
            assert.strictEqual(extension.isActive, true);
        }
    });

    test('DOD regex pattern matching', () => {
        const dodRegex = /\[([^\]]+)\]\(#dod:([^)]+)\)/g;

        // Test valid patterns
        const validText = 'The [Gini index](#dod:gini) measures inequality.';
        const match = dodRegex.exec(validText);

        assert.ok(match, 'Should match valid DOD pattern');
        assert.strictEqual(match[1], 'Gini index', 'Should extract title correctly');
        assert.strictEqual(match[2], 'gini', 'Should extract key correctly');

        // Test invalid patterns
        const invalidText = 'The [Gini index](#not-dod:gini) should not match.';
        dodRegex.lastIndex = 0; // Reset regex
        const noMatch = dodRegex.exec(invalidText);

        assert.strictEqual(noMatch, null, 'Should not match invalid pattern');
    });

    test('Python string detection', () => {
        // Mock a simple Python string checker (both raw and regular)
        function isInPythonString(text: string, position: number): boolean {
            const beforeText = text.substring(0, position);
            const stringRegex = /r?('''|"""|'|")/g;
            let match: RegExpExecArray | null;
            let inString = false;
            let stringDelimiter = '';

            while ((match = stringRegex.exec(beforeText)) !== null) {
                const delimiter = match[1];

                if (!inString) {
                    inString = true;
                    stringDelimiter = delimiter;
                } else if (delimiter === stringDelimiter) {
                    inString = false;
                    stringDelimiter = '';
                }
            }

            return inString;
        }

        // Test cases for Python strings (both raw and regular)
        const pythonCode1 = 'description = r"The [Gini index](#dod:gini) measures inequality."';
        const dodPosition1 = pythonCode1.indexOf('[Gini index]');
        assert.ok(isInPythonString(pythonCode1, dodPosition1), 'Should detect DOD inside raw string');

        const pythonCode2 = 'description = "The [Gini index](#dod:gini) measures inequality."';
        const dodPosition2 = pythonCode2.indexOf('[Gini index]');
        assert.ok(isInPythonString(pythonCode2, dodPosition2), 'Should detect DOD in regular string');

        const pythonCode3 = 'description = r"""The [Gini index](#dod:gini) measures inequality."""';
        const dodPosition3 = pythonCode3.indexOf('[Gini index]');
        assert.ok(isInPythonString(pythonCode3, dodPosition3), 'Should detect DOD inside triple-quoted raw string');

        const pythonCode4 = 'print("Regular string with [DOD ref](#dod:test)")';
        const dodPosition4 = pythonCode4.indexOf('[DOD ref]');
        assert.ok(isInPythonString(pythonCode4, dodPosition4), 'Should detect DOD inside regular double-quoted string');

        const pythonCode5 = 'description = [Gini index](#dod:gini)  # Not in quotes';
        const dodPosition5 = pythonCode5.indexOf('[Gini index]');
        assert.ok(!isInPythonString(pythonCode5, dodPosition5), 'Should not detect DOD outside of strings');
    });
});
