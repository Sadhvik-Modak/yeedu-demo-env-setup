#!/usr/bin/env python3
"""Convert `# %%`-delimited Python sources in src/ into Jupyter notebooks.

Two cell markers are understood:

    # %%                 -> a code cell, the lines following it verbatim
    # %% [markdown]      -> a prose cell; following lines are comments and get
                            their leading "# " stripped. `{{IMG:name}}` in one
                            expands to an embedded assets/name.png data URI.

Keeping the sources as plain .py means they stay diffable, greppable and
`ast.parse`-able; the .ipynb is a build artifact.

Two output flavours, because Yeedu treats prose differently in its two runners:

    python3 build.py <dir>              native markdown cells   <- ship this
    python3 build.py --runnable <dir>   display(Markdown(...))  <- CLI runs only

**Native markdown cells are what the Yeedu notebook UI writes and renders.**
They are the right thing to open in front of an audience: real headings, tables
and images, collapsible, no Python noise. Cells are emitted in Yeedu's own
on-disk shape (`cell_uuid`, string `source`, `metadata.order`) so they round-trip
through the UI unchanged.

**But `yeedu notebook start` feeds every cell to the Python kernel regardless of
cell_type.** A native markdown cell therefore kills a headless CLI run on its
first prose line with a SyntaxError — verified twice on platform 2.10.1, once in
canonical nbformat shape and once in Yeedu's own UI shape (run 1810202, run
1810204: `Cells: 1/3 completed`). `--runnable` exists purely for that path: it
wraps the same prose in `display(Markdown(...))` code cells so the pipeline can
be verified end-to-end from the CLI. The two builds differ only in prose cells;
every code cell is byte-identical.
"""
import base64
import json
import os
import re
import sys
import uuid

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(HERE, "src")
ASSETS = os.path.join(HERE, "assets")

_IMG_CACHE = {}


def inline_images(md):
    """Replace `{{IMG:name}}` with a base64 data: URI for assets/name.png.

    The notebooks embed their diagrams rather than hot-linking them, so a demo
    works with no network access and nothing to 404 later. Sources keep the
    short token so they stay readable and diffable; regenerate the PNGs from
    the .mmd sources with `python3 assets/render.py`.
    """
    def sub(m):
        name = m.group(1)
        if name not in _IMG_CACHE:
            path = os.path.join(ASSETS, name + ".png")
            if not os.path.exists(path):
                raise SystemExit("missing asset %s (run assets/render.py)" % path)
            with open(path, "rb") as fh:
                _IMG_CACHE[name] = ("data:image/png;base64,"
                                    + base64.b64encode(fh.read()).decode())
        return _IMG_CACHE[name]
    return re.sub(r"\{\{IMG:([A-Za-z0-9_]+)\}\}", sub, md)


def to_cells(text):
    """Split a source file into (kind, text) cells on the `# %%` markers."""
    cells, cur, kind = [], [], "code"

    def flush():
        body = "".join(cur).strip("\n")
        if body.strip():
            cells.append((kind, body + "\n"))

    for line in text.splitlines(keepends=True):
        if line.startswith("# %%"):
            flush()
            cur = []
            kind = "markdown" if "[markdown]" in line else "code"
            continue
        cur.append(line)
    flush()
    return cells


def undent_markdown(body):
    """Turn a block of `# `-prefixed comment lines back into markdown."""
    out = []
    for line in body.splitlines():
        if line.startswith("# "):
            out.append(line[2:])
        elif line.strip() == "#":
            out.append("")
        else:
            out.append(line)
    return "\n".join(out).strip("\n") + "\n"


def md_cell(text, order):
    """A prose cell in Yeedu's own notebook shape (see module docstring)."""
    return {
        "cell_uuid": str(uuid.uuid4()),
        "cell_type": "markdown",
        "source": text,
        "outputs": [],
        "execution_count": None,
        "metadata": {"runStatus": False, "isOutputError": False,
                     "isOutputHidden": False, "collapsed": False, "scrolled": True,
                     "deletable": True, "editable": True, "isCodeModified": False,
                     "isNewlyCreated": False, "order": order},
    }


def code_cell(text, order):
    return {
        "cell_uuid": str(uuid.uuid4()),
        "cell_type": "code",
        "source": text,
        "outputs": [],
        "execution_count": None,
        "metadata": {"order": order, "runStatus": False, "collapsed": False,
                     "scrolled": True, "isCodeModified": False, "deletable": True,
                     "editable": True, "execution": {}, "jupyter": {}, "tags": [],
                     "isOutputError": False, "isOutputHidden": False,
                     "isNewlyCreated": False},
    }


def build(py_path, out_path, runnable=False):
    with open(py_path) as fh:
        source = fh.read()
    cells, n_prose = [], 0
    for order, (kind, body) in enumerate(to_cells(source)):
        if kind == "markdown":
            md = inline_images(undent_markdown(body))
            n_prose += 1
            if runnable:
                if '"""' in md:
                    raise SystemExit("prose cell contains a triple quote: " + md[:60])
                cells.append(code_cell(
                    'from IPython.display import Markdown, display\n'
                    'display(Markdown(r"""\n' + md + '"""))\n', order))
            else:
                cells.append(md_cell(md, order))
        else:
            cells.append(code_cell(body, order))
    nb = {
        "nbformat": 4,
        "nbformat_minor": 5,
        "metadata": {
            "kernelspec": {"display_name": "Python 3", "language": "python",
                           "name": "python3"},
            "language_info": {"codemirror_mode": {"name": "ipython", "version": 3},
                              "file_extension": ".py", "mimetype": "text/x-python",
                              "name": "python", "nbconvert_exporter": "python",
                              "pygments_lexer": "ipython3", "version": "3.8.10"},
            "application/vnd.yeedu.v1+notebook": {"widgets": {}},
        },
        "cells": cells,
    }
    with open(out_path, "w") as fh:
        json.dump(nb, fh, indent=1)
    return len(cells) - n_prose, n_prose


def main():
    args = sys.argv[1:]
    runnable = "--runnable" in args
    args = [a for a in args if a != "--runnable"]
    out_dir = args[0] if args else HERE
    os.makedirs(out_dir, exist_ok=True)
    for name in sorted(os.listdir(SRC)):
        if not name.endswith(".py"):
            continue
        out = os.path.join(out_dir, name[:-3] + ".ipynb")
        n_code, n_md = build(os.path.join(SRC, name), out, runnable)
        print("%-40s %2d code + %2d prose (%s) -> %s" % (
            name, n_code, n_md, "runnable" if runnable else "markdown", out))


if __name__ == "__main__":
    main()
