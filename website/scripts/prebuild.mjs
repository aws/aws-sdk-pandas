// Runs all content generators before start/build.
// - Tutorials are always regenerated from ../tutorials/*.ipynb.
// - API docs need an environment where awswrangler is importable; we try the
//   repo .venv first, then python3. If neither works and docs/api doesn't
//   exist yet, the build fails loudly rather than shipping without API docs.
import { execFileSync } from 'node:child_process';
import { existsSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = dirname(dirname(fileURLToPath(import.meta.url)));

execFileSync('node', [join(root, 'scripts', 'convert-notebooks.mjs')], { stdio: 'inherit' });

const candidates = [join(root, '..', '.venv', 'bin', 'python'), 'python3'];
let generated = false;
for (const python of candidates) {
  if (python !== 'python3' && !existsSync(python)) continue;
  try {
    execFileSync(python, [join(root, 'scripts', 'generate-api-docs.py')], { stdio: 'inherit' });
    generated = true;
    break;
  } catch {
    // try next candidate
  }
}

if (!generated) {
  if (existsSync(join(root, 'docs', 'api', 'index.md'))) {
    console.warn('prebuild: awswrangler not importable; reusing existing docs/api');
  } else {
    console.error(
      'prebuild: could not generate docs/api — no python with awswrangler installed.\n' +
        'Install the package (e.g. `pip install -e ..[all]` or use the repo .venv) and retry.',
    );
    process.exit(1);
  }
}
