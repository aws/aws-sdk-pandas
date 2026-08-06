"""Generate the API reference under docs/api/ from awswrangler docstrings.

Single source of truth is docs/source/api.rst (the same listing Sphinx
autosummary uses): each section there becomes one page here, and the
functions are documented from their live signatures and numpydoc docstrings.

Requires an environment where awswrangler is importable (e.g. the repo
.venv). scripts/prebuild.mjs calls this and falls back to a stub page
linking to ReadTheDocs when no such environment exists.
"""

from __future__ import annotations

import importlib
import inspect
import re
import shutil
import sys
from pathlib import Path

WEBSITE_DIR = Path(__file__).resolve().parent.parent
REPO_DIR = WEBSITE_DIR.parent
API_RST = REPO_DIR / "docs" / "source" / "api.rst"
OUT_DIR = WEBSITE_DIR / "docs" / "api"


def slugify(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")


def parse_api_rst(text: str) -> list[dict]:
    """Parse api.rst into [{title, module, names}] sections."""
    lines = text.splitlines()
    sections: list[dict] = []
    current: dict | None = None
    in_autosummary = False

    for i, line in enumerate(lines):
        nxt = lines[i + 1] if i + 1 < len(lines) else ""
        if line.strip() and set(nxt.strip()) == {"-"} and len(nxt.strip()) >= 3:
            current = {"title": line.strip(), "module": None, "names": []}
            sections.append(current)
            in_autosummary = False
            continue
        if current is None:
            continue
        m = re.match(r"\.\.\s+currentmodule::\s+(\S+)", line.strip())
        if m:
            current["module"] = m.group(1)
            continue
        if line.strip().startswith(".. autosummary::"):
            in_autosummary = True
            continue
        if in_autosummary:
            stripped = line.strip()
            if not stripped or stripped.startswith(":"):
                continue
            if line.startswith((" ", "\t")):
                current["names"].append(stripped)
            else:
                in_autosummary = False
    return [s for s in sections if s["module"] and s["names"]]


def doctest_blocks_to_md(text: str) -> str:
    """Convert `>>>` doctest runs in `text` into fenced python code blocks.

    Non-blank lines directly following a doctest run are its expected output
    and are kept inside the fence, per doctest convention.
    """
    out: list[str] = []
    block: list[str] = []
    in_output = False
    for line in text.splitlines():
        if line.lstrip().startswith((">>>", "...")):
            block.append(line.strip())
            in_output = True
        elif block and in_output and line.strip():
            block.append(line.strip())
        else:
            if block:
                out.append("```python\n" + "\n".join(block) + "\n```")
                block = []
            in_output = False
            out.append(line)
    if block:
        out.append("```python\n" + "\n".join(block) + "\n```")
    return "\n".join(out)


def _is_rubric(lines: list[str], i: int) -> str | None:
    """Return the rubric title if lines[i] starts a `Note\\n----` style rubric."""
    title = lines[i].strip()
    nxt = lines[i + 1].strip() if i + 1 < len(lines) else ""
    if title in ("Note", "Notes", "Warning", "Warnings") and nxt and set(nxt) == {"-"}:
        return title
    return None


def notes_to_admonitions(doc: str) -> str:
    """Convert numpydoc `Note\\n----` style rubrics into Docusaurus admonitions.

    A rubric body may span single blank lines (e.g. a paragraph followed by a
    list), but ends at the next rubric or numpydoc section — otherwise the
    unconverted `Note\\n----` would render as a markdown setext heading and
    pollute the page TOC.
    """
    lines = doc.splitlines()
    out: list[str] = []
    i = 0
    while i < len(lines):
        title = _is_rubric(lines, i)
        if title:
            kind = "warning" if title.startswith("Warning") else "note"
            body: list[str] = []
            i += 2
            while i < len(lines):
                if _is_rubric(lines, i) or lines[i].strip() in NUMPYDOC_SECTIONS:
                    break
                if not lines[i].strip() and (not body or not body[-1].strip()):
                    break  # two consecutive blanks: note is over
                body.append(lines[i])
                i += 1
            out.append(f":::{kind}\n" + "\n".join(body).strip() + "\n:::")
            continue
        out.append(lines[i])
        i += 1
    return "\n".join(out)


NUMPYDOC_SECTIONS = (
    "Parameters",
    "Returns",
    "Yields",
    "Raises",
    "Warnings",
    "See Also",
    "Notes",
    "References",
    "Examples",
)


def split_numpydoc(doc: str) -> list[tuple[str, str]]:
    """Split a numpydoc docstring into [('', summary), (section, body), ...]."""
    lines = doc.splitlines()
    parts: list[tuple[str, list[str]]] = [("", [])]
    i = 0
    while i < len(lines):
        line = lines[i]
        nxt = lines[i + 1] if i + 1 < len(lines) else ""
        if line.strip() in NUMPYDOC_SECTIONS and set(nxt.strip()) == {"-"}:
            parts.append((line.strip(), []))
            i += 2
            continue
        parts[-1][1].append(line)
        i += 1
    return [(name, "\n".join(body).strip()) for name, body in parts]


def render_params(body: str, named: bool = True) -> str:
    """Render a Parameters/Returns/Raises body as a bullet list.

    With named=False (Returns/Yields), first lines are types or prose, not
    parameter names, so they are not bold-coded.
    """
    items: list[tuple[str, list[str]]] = []
    for line in body.splitlines():
        if line and not line.startswith((" ", "\t")):
            items.append((line.strip(), []))
        elif items:
            items[-1][1].append(line.strip())
        elif line.strip():
            items.append(("", [line.strip()]))
    out = []
    for name, desc in items:
        text = " ".join(d for d in desc if d)
        if name and named:
            out.append(f"- **`{name}`** — {text}" if text else f"- **`{name}`**")
        else:
            combined = f"{name} {text}".strip()
            if combined:
                out.append(f"- {combined}")
    return "\n".join(out)


def render_docstring(doc: str) -> str:
    doc = re.sub(r"``([^`]+)``", r"`\1`", doc)  # RST literals -> md code
    parts: list[str] = []
    for section, body in split_numpydoc(notes_to_admonitions(doc)):
        if not body:
            continue
        if section == "":
            parts.append(doctest_blocks_to_md(body))
        elif section in ("Parameters", "Raises"):
            parts.append(f"**{section}**\n\n{render_params(body)}")
        elif section in ("Returns", "Yields"):
            parts.append(f"**{section}**\n\n{render_params(body, named=False)}")
        else:
            parts.append(f"**{section}**\n\n{doctest_blocks_to_md(body)}")
    return "\n\n".join(parts)


def render_function(module, module_name: str, name: str) -> str | None:
    try:
        obj = getattr(module, name)
    except AttributeError:
        print(f"  warning: {module_name}.{name} not found, skipped", file=sys.stderr)
        return None

    try:
        sig = str(inspect.signature(obj))
    except (ValueError, TypeError):
        sig = "(...)"
    # Long signatures: put each parameter on its own line for readability
    if len(name + sig) > 88:
        params = re.sub(r"^\((.*)\)( -> .*)?$", r"\1", sig, flags=re.S)
        ret = re.search(r" -> (.*)$", sig)
        sig_lines = re.split(r", (?![^\[\(]*[\]\)])", params)
        sig = "(\n    " + ",\n    ".join(sig_lines) + "\n)" + (f" -> {ret.group(1)}" if ret else "")

    doc = inspect.getdoc(obj) or "*No documentation available.*"
    short_module = module_name.replace("awswrangler", "wr")
    parts = [
        f"### {name}",
        "",
        f"```python\n{short_module}.{name}{sig}\n```",
        "",
        render_docstring(doc),
        "",
        "---",
        "",
    ]
    return "\n".join(parts)


def main() -> int:
    try:
        import awswrangler  # noqa: F401
    except ImportError as exc:
        print(f"generate-api-docs: awswrangler not importable ({exc})", file=sys.stderr)
        return 1

    sections = parse_api_rst(API_RST.read_text())

    shutil.rmtree(OUT_DIR, ignore_errors=True)
    OUT_DIR.mkdir(parents=True)

    index_rows: list[str] = []
    for position, section in enumerate(sections, start=1):
        title, module_name, names = section["title"], section["module"], section["names"]
        slug = slugify(title)
        try:
            module = importlib.import_module(module_name)
        except ModuleNotFoundError:
            # Some currentmodule targets (e.g. awswrangler.config) are objects
            # inside the package, not importable modules.
            parent, _, attr = module_name.rpartition(".")
            module = getattr(importlib.import_module(parent), attr)
        short_module = module_name.replace("awswrangler", "wr")

        rendered = [r for name in names if (r := render_function(module, module_name, name))]
        page = [
            "---",
            f"id: {slug}",
            f'title: "{title}"',
            f"sidebar_position: {position}",
            "---",
            "",
            f"# {title}",
            "",
            f"Module: `{short_module}`",
            "",
            *rendered,
        ]
        (OUT_DIR / f"{slug}.md").write_text("\n".join(page))
        index_rows.append(f"- [{title}]({slug}.md)")

    version = awswrangler.__version__
    index = [
        "---",
        "id: index",
        "title: API Reference",
        "sidebar_position: 0",
        "---",
        "",
        "# API Reference",
        "",
        f"Generated from `awswrangler` {version} docstrings.",
        "",
        *index_rows,
        "",
    ]
    (OUT_DIR / "index.md").write_text("\n".join(index))
    print(f"generate-api-docs: wrote {len(sections)} pages to {OUT_DIR}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
