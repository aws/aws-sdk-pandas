// Converts ../tutorials/*.ipynb into docs/tutorials/*.md so the notebooks
// remain the single source of truth. Runs automatically via npm pre-scripts.
//
// Output is plain CommonMark (.md, not .mdx) — combined with `markdown.format:
// 'detect'` in docusaurus.config.ts, this keeps raw notebook content (HTML
// snippets, curly braces) from being parsed as MDX/JSX.

import {
  readdirSync,
  readFileSync,
  writeFileSync,
  mkdirSync,
  rmSync,
  copyFileSync,
} from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = dirname(dirname(fileURLToPath(import.meta.url)));
const notebooksDir = join(root, '..', 'tutorials');
const outDir = join(root, 'docs', 'tutorials');
const staticSrcDir = join(notebooksDir, '_static');
const staticOutDir = join(root, 'static', 'img', 'tutorials');

const MAX_OUTPUT_LINES = 40;

function slugify(name) {
  return name
    .toLowerCase()
    .replace(/&/g, 'and')
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '');
}

function textOf(source) {
  return Array.isArray(source) ? source.join('') : String(source ?? '');
}

function truncate(text) {
  const lines = text.split('\n');
  if (lines.length <= MAX_OUTPUT_LINES) return text;
  return (
    lines.slice(0, MAX_OUTPUT_LINES).join('\n') +
    `\n… (output truncated, ${lines.length - MAX_OUTPUT_LINES} more lines)`
  );
}

function renderOutputs(outputs) {
  const parts = [];
  for (const output of outputs ?? []) {
    if (output.output_type === 'stream') {
      parts.push('```text\n' + truncate(textOf(output.text)).trimEnd() + '\n```');
    } else if (
      output.output_type === 'execute_result' ||
      output.output_type === 'display_data'
    ) {
      const data = output.data ?? {};
      if (data['image/png']) {
        const b64 = textOf(data['image/png']).replace(/\n/g, '');
        parts.push(`![output](data:image/png;base64,${b64})`);
      } else if (data['text/plain']) {
        parts.push('```text\n' + truncate(textOf(data['text/plain'])).trimEnd() + '\n```');
      }
    } else if (output.output_type === 'error') {
      const trace = (output.traceback ?? [])
        .join('\n')
        // Strip ANSI escape codes from tracebacks
        .replace(/\[[0-9;]*m/g, '');
      parts.push('```text\n' + truncate(trace).trimEnd() + '\n```');
    }
  }
  return parts;
}

function convert(filename) {
  const nb = JSON.parse(readFileSync(join(notebooksDir, filename), 'utf8'));
  const base = filename.replace(/\.ipynb$/, '');
  const match = base.match(/^(\d+)\s*-\s*(.*)$/);
  const position = match ? parseInt(match[1], 10) : 999;
  const title = match ? match[2].trim() : base;
  const slug = slugify(base);

  const blocks = [];
  for (const cell of nb.cells ?? []) {
    const source = textOf(cell.source).trimEnd();
    if (!source && cell.cell_type !== 'code') continue;
    if (cell.cell_type === 'markdown') {
      // Drop the logo banner every notebook opens with; the site has its own chrome.
      const cleaned = source
        .replace(/\[!\[[^\]]*\]\(_static\/logo\.png[^)]*\)\]\([^)]*\)\n?/g, '')
        .replace(/(\]\()_static\//g, '$1/img/tutorials/')
        .trim();
      if (cleaned) blocks.push(cleaned);
    } else if (cell.cell_type === 'code') {
      if (source) blocks.push('```python\n' + source + '\n```');
      blocks.push(...renderOutputs(cell.outputs));
    }
  }

  // Notebooks start with an H1 title; the frontmatter title would duplicate it,
  // so drop the first markdown H1 if present.
  if (blocks.length && /^#\s/.test(blocks[0].split('\n')[0])) {
    const lines = blocks[0].split('\n');
    blocks[0] = lines.slice(1).join('\n').trim();
    if (!blocks[0]) blocks.shift();
  }

  const frontmatter = [
    '---',
    `id: ${slug}`,
    `title: "${title.replace(/"/g, '\\"')}"`,
    `sidebar_position: ${position}`,
    `sidebar_label: "${position} - ${title.replace(/"/g, '\\"')}"`,
    'custom_edit_url: ' +
      `https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/${encodeURIComponent(filename)}`,
    '---',
    '',
    `# ${title}`,
    '',
    `> This page is generated from [\`tutorials/${filename}\`](https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/${encodeURIComponent(filename)}). Open it in Jupyter to run it yourself.`,
    '',
  ];

  writeFileSync(join(outDir, `${slug}.md`), frontmatter.join('\n') + blocks.join('\n\n') + '\n');
  return { slug, title, position };
}

rmSync(outDir, { recursive: true, force: true });
mkdirSync(outDir, { recursive: true });

rmSync(staticOutDir, { recursive: true, force: true });
mkdirSync(staticOutDir, { recursive: true });
for (const asset of readdirSync(staticSrcDir)) {
  copyFileSync(join(staticSrcDir, asset), join(staticOutDir, asset));
}


const notebooks = readdirSync(notebooksDir)
  .filter((f) => f.endsWith('.ipynb'))
  .sort();

const converted = notebooks.map(convert);
console.log(`convert-notebooks: converted ${converted.length} notebooks to ${outDir}`);
