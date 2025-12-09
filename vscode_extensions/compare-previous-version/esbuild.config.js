const esbuild = require('esbuild');

// Common build options
const buildOptions = {
  entryPoints: ['src/extension.ts'],
  bundle: true,
  outfile: 'dist/extension.js',
  external: ['vscode'],
  platform: 'node',
  format: 'cjs',
  sourcemap: true,
  target: 'node12'
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