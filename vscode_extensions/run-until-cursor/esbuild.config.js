require("esbuild").build({
    entryPoints: ["src/extension.ts"],
    bundle: true,
    outfile: "dist/extension.js",
    external: ["vscode"],  // Ensure vscode is NOT bundled
    format: "cjs",         // Change to CommonJS format
    platform: "node"
}).catch(() => process.exit(1));

