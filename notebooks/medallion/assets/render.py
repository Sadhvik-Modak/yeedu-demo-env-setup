#!/usr/bin/env python3
"""Render every assets/*.mmd to a palette-quantised PNG beside it.

The notebooks embed these as base64 `data:` URIs so they need no network at
view time; this script is the only thing that ever talks to mermaid.ink, and
only when a diagram source actually changes.

    python3 assets/render.py           # render all
    python3 assets/render.py flow      # render matching names only
"""
import base64, glob, io, json, os, subprocess, sys

HERE = os.path.dirname(os.path.abspath(__file__))
# mermaid.ink sits behind Cloudflare and 403s a default urllib user-agent, so
# shell out to curl with a browser UA rather than fighting it.
UA = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0 Safari/537.36")


def render(mmd_path, scale=2):
    graph = open(mmd_path).read()
    payload = {"code": graph, "mermaid": {"theme": "default"}}
    enc = base64.urlsafe_b64encode(json.dumps(payload).encode()).decode()
    url = "https://mermaid.ink/img/%s?type=png&scale=%d" % (enc, scale)
    raw = subprocess.run(["curl", "-sS", "--fail", "-A", UA, url],
                         capture_output=True, check=True).stdout
    if not raw.startswith(b"\x89PNG"):
        raise SystemExit("not a PNG for %s: %r" % (mmd_path, raw[:120]))

    # Flat-colour diagrams quantise to a small palette with no visible loss,
    # which matters because every byte here is base64-inflated into 5 notebooks.
    from PIL import Image
    im = Image.open(io.BytesIO(raw)).convert("RGB")
    best, buf = None, None
    for colors in (32, 64, 128):
        b = io.BytesIO()
        im.quantize(colors=colors, method=Image.MEDIANCUT).save(b, "PNG", optimize=True)
        if best is None or b.tell() < best:
            best, buf = b.tell(), b.getvalue()
    out = mmd_path[:-4] + ".png"
    open(out, "wb").write(buf)
    print("%-24s %5d x %-4d  %6.1f KB raw -> %6.1f KB  (%.0f%% saved)" % (
        os.path.basename(out), im.width, im.height,
        len(raw) / 1024, len(buf) / 1024, 100 * (1 - len(buf) / len(raw))))


if __name__ == "__main__":
    pats = sys.argv[1:] or [""]
    for f in sorted(glob.glob(os.path.join(HERE, "*.mmd"))):
        if any(p in os.path.basename(f) for p in pats):
            render(f)
