"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function (o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
        desc = { enumerable: true, get: function () { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function (o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function (o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function (o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function (o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
Object.defineProperty(exports, "__esModule", { value: true });
const assert = __importStar(require("assert"));
const vscode = __importStar(require("vscode"));
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
    test('Python raw string detection', () => {
        // Mock a simple Python raw string checker
        function isInPythonRawString(text, position) {
            const beforeText = text.substring(0, position);
            const rawStringRegex = /r('''|"""|'|")/g;
            let match;
            let inRawString = false;
            let stringDelimiter = '';
            while ((match = rawStringRegex.exec(beforeText)) !== null) {
                const delimiter = match[1];
                if (!inRawString) {
                    inRawString = true;
                    stringDelimiter = delimiter;
                }
                else if (delimiter === stringDelimiter) {
                    inRawString = false;
                    stringDelimiter = '';
                }
            }
            return inRawString;
        }
        // Test cases for Python raw strings
        const pythonCode1 = 'description = r"The [Gini index](#dod:gini) measures inequality."';
        const dodPosition1 = pythonCode1.indexOf('[Gini index]');
        assert.ok(isInPythonRawString(pythonCode1, dodPosition1), 'Should detect DOD inside raw string');
        const pythonCode2 = 'description = "The [Gini index](#dod:gini) measures inequality."';
        const dodPosition2 = pythonCode2.indexOf('[Gini index]');
        assert.ok(!isInPythonRawString(pythonCode2, dodPosition2), 'Should not detect DOD in regular string');
        const pythonCode3 = 'description = r"""The [Gini index](#dod:gini) measures inequality."""';
        const dodPosition3 = pythonCode3.indexOf('[Gini index]');
        assert.ok(isInPythonRawString(pythonCode3, dodPosition3), 'Should detect DOD inside triple-quoted raw string');
    });
});
//# sourceMappingURL=extension.test.js.map
