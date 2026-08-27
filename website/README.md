# Documentation Site

Docusaurus 3 site for AWS SDK for pandas, deployed to GitHub Pages.

Structure mirrors the old ReadTheDocs site (docs at the site root, no landing
page); visual theme modeled on the hermes-agent docs (dark-first, amber accent).

## Development

```bash
npm install
npm start       # http://localhost:3000/aws-sdk-pandas/
```

`npm start` / `npm run build` first run `scripts/prebuild.mjs`, which:

- regenerates `docs/tutorials/` from the Jupyter notebooks in `../tutorials/`
- regenerates `docs/api/` from awswrangler docstrings (parses
  `../docs/source/api.rst` for the module/function listing; needs a Python
  with awswrangler importable, e.g. the repo `.venv`)

Both output directories are gitignored — the notebooks and docstrings are
the single sources of truth.

## Content layout

- `docs/index.md` — site root: quick start + navigation (mirrors old index.rst)
- `docs/about.md`, `docs/install.md`, `docs/scale.md`, `docs/lambda-layers.md`
- `docs/tutorials/`, `docs/api/` — generated, do not edit

## Versioning

ReadTheDocs-style: the frozen release snapshot (`versioned_docs/`) is served
at the site root as "stable"; `docs/` (main) is served at `/latest` with an
"unreleased" banner. The navbar has a version dropdown.

On release:

```bash
npm run docusaurus -- docs:version <x.y.z>
```

then update `lastVersion`/`versions` in `docusaurus.config.ts` and drop the
previous entry from `versions.json` (we keep one stable snapshot; older
versions remain on ReadTheDocs). Unlike `docs/`, the snapshot is committed —
it must not change as notebooks/docstrings evolve on main.

## Deployment

`.github/workflows/docs-site.yml` builds on every PR touching `website/`,
`tutorials/` or `awswrangler/`, and deploys to GitHub Pages on push to `main`.
