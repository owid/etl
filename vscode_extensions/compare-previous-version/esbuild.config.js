const esbuild = require('esbuild');

// Common build options: the extension, and the command-line script that shares its comparison code.
const buildOptions = {
  entryPoints: ['src/extension.ts', 'src/cli.ts'],
  bundle: true,
  outdir: 'dist',
  external: ['vscode'],
  platform: 'node',
  format: 'cjs',
  sourcemap: true,
  target: 'node18'
};

// Start the build
async function startBuild() {
  try {
    // If watch mode is enabled, create a context and start watching for changes
    if (process.argv.includes('--watch')) {
      const ctx = await esbuild.context(buildOptions);
      console.log('Entering watch mode...');
      await ctx.watch();
    } else {
      // Otherwise, just run the build once
      await esbuild.build(buildOptions);
      console.log('Build complete.');
    }
  } catch (error) {
    console.error('Build failed:', error);
    process.exit(1);
  }
}

startBuild();
