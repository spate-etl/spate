#!/usr/bin/env python3
"""Generate the Spate brand assets.

Text is shaped with HarfBuzz and baked to outlines, so the emitted SVGs carry no
font dependency. All layout is driven by measured ink bounds rather than the
nominal canvas, so the mark, the rule and the tagline share one optical left
edge and the lockup never clips an ascender or descender.
"""
import hashlib
import io
import pathlib
import sys
import urllib.request

import uharfbuzz as hb
from fontTools.misc.transform import Transform
from fontTools.pens.boundsPen import BoundsPen
from fontTools.pens.svgPathPen import SVGPathPen
from fontTools.pens.transformPen import TransformPen
from fontTools.ttLib import TTFont
from fontTools.varLib import instancer

HERE = pathlib.Path(__file__).parent
STATIC = (HERE / ".." / ".." / "static" / "img").resolve()
OUT = STATIC / "brand"

SRC_FONT = HERE / ".cache" / "IBMPlexSans.ttf"
UPEM = 1000

# The wordmark is set in IBM Plex Sans (SIL Open Font License 1.1). The font is
# fetched on demand rather than vendored: the glyphs ship as baked outlines, so
# the repository never redistributes font software and the license surface is
# unchanged. Pinned by digest. A mismatch means the upstream file moved, and
# the wordmark is re-cut deliberately.
FONT_URL = (
    "https://github.com/google/fonts/raw/main/ofl/ibmplexsans/"
    "IBMPlexSans%5Bwdth,wght%5D.ttf"
)
FONT_SHA256 = "3b031aa4216174205bd8471f88a49b91f093169e9e87bd5262242bc5967fe2e3"


def ensure_font():
    if SRC_FONT.exists():
        digest = hashlib.sha256(SRC_FONT.read_bytes()).hexdigest()
        if digest == FONT_SHA256:
            return
        print(f"cached font digest mismatch, refetching ({digest[:12]}…)")

    print(f"fetching {FONT_URL}")
    SRC_FONT.parent.mkdir(parents=True, exist_ok=True)
    with urllib.request.urlopen(FONT_URL) as r:  # noqa: S310 - pinned https URL
        blob = r.read()

    digest = hashlib.sha256(blob).hexdigest()
    if digest != FONT_SHA256:
        sys.exit(
            f"font digest mismatch\n  expected {FONT_SHA256}\n  got      {digest}"
        )
    SRC_FONT.write_bytes(blob)

# ---------------------------------------------------------------- palette
#
# The one place a colour is chosen. `gen_tokens` writes the whole table to
# website/src/css/brand.css, which the site's stylesheet maps onto Infima, so
# an asset and the page it sits on cannot disagree.
#
# The mark's colours and the UI's accent differ on the light ground on
# purpose: the mark is a graphic and reads at 3:1, while the accent carries
# link text and has to clear 4.5:1 on paper. `make check-brand` holds both.

DARK_BASE = "#16181d"
DARK_NODE = "#ff8c4a"
DARK_CORE = "#ffc9a8"

LIGHT_NODE = "#d1500b"
LIGHT_EDGE = "#e65a0f"
LIGHT_CORE = "#f16413"
LIGHT_ACCENT = "#c8480a"

BANNER_TEXT = "#f4f5f6"
BANNER_MUTED = "#9aa1a9"

TOKENS = {
    "light": {
        "bg": "#fbfaf8",
        "surface": "#ffffff",
        "surface-2": "#f3f1ed",
        "ink": "#17181c",
        "muted": "#5f646c",
        "border": "#e2ded7",
        "accent": LIGHT_ACCENT,
        "accent-ink": "#ffffff",
        "accent-soft": "rgba(200, 72, 10, 0.08)",
        "mark-node": LIGHT_NODE,
        "mark-edge": LIGHT_EDGE,
        "mark-core": LIGHT_CORE,
        "code-bg": "#17181c",
        "code-ink": "#e8e9ec",
        "danger": "#b8382c",
        "warning": "#8a5a00",
    },
    "dark": {
        "bg": DARK_BASE,
        "surface": "#1c1f26",
        "surface-2": "#22262f",
        "ink": "#f4f5f6",
        "muted": "#9aa1a9",
        "border": "#2b303a",
        "accent": DARK_NODE,
        "accent-ink": DARK_BASE,
        "accent-soft": "rgba(255, 140, 74, 0.10)",
        "mark-node": DARK_NODE,
        "mark-edge": DARK_NODE,
        "mark-core": DARK_CORE,
        "code-bg": "#111318",
        "code-ink": "#e8e9ec",
        "danger": "#ff8a7a",
        "warning": "#f2c46d",
    },
}

# Infima's primary ramp, darkest to lightest, per ground. Hand-picked steps of
# the accent rather than a computed ramp, so hover and active states stay
# inside the same hue.
RAMP = {
    "light": ["#8f3307", "#a33a08", "#b64109", LIGHT_ACCENT, "#d95612", "#e6631f", LIGHT_CORE],
    "dark": ["#e15200", "#ff6b18", "#ff7629", DARK_NODE, "#ffa26b", "#ffad7c", DARK_CORE],
}

WORD_WEIGHT, WORD_TRACK = 600, -0.022
SUB_WEIGHT, SUB_TRACK = 400, -0.012

# ---------------------------------------------------------------- the mark

# The confluence mark: 3 sources, 1 core, 1 sink, on a 32-unit canvas.
# The ink occupies only part of that canvas; every layout below aligns to INK,
# not to the canvas.
#   left   6 - 2.4 = 3.6      right  26 + 2.9 = 28.9
#   top    7.5 - 2.4 = 5.1    bottom 24.5 + 2.4 = 26.9
MARK_INK = (3.6, 5.1, 28.9, 26.9)
MARK_INK_W = MARK_INK[2] - MARK_INK[0]   # 25.3
MARK_INK_H = MARK_INK[3] - MARK_INK[1]   # 21.8


def mark(node, edge, core, scale=1.0, dx=0.0, dy=0.0):
    return f"""<g transform="translate({dx:.3f} {dy:.3f}) scale({scale:.6f})">
    <g stroke="{edge}" stroke-width="2.2" stroke-linecap="round" fill="none">
      <path d="M7.7 9.3 L13.6 15.6"/>
      <path d="M8.4 16 L13.6 16"/>
      <path d="M7.7 22.7 L13.6 16.4"/>
      <path d="M19.6 16 L23.0 16"/>
    </g>
    <circle cx="6" cy="7.5" r="2.4" fill="{node}"/>
    <circle cx="6" cy="16" r="2.4" fill="{node}"/>
    <circle cx="6" cy="24.5" r="2.4" fill="{node}"/>
    <rect x="12.6" y="12.6" width="6.8" height="6.8" rx="1.6" fill="{core}"/>
    <circle cx="26" cy="16" r="2.9" fill="{node}"/>
  </g>"""


def mark_by_ink(node, edge, core, ink_h, left, mid_y):
    """Place the mark so its INK is `ink_h` tall, its left ink edge at `left`,
    and its vertical ink center at `mid_y`."""
    s = ink_h / MARK_INK_H
    dx = left - MARK_INK[0] * s
    dy = mid_y - (MARK_INK[1] + MARK_INK_H / 2) * s
    return mark(node, edge, core, s, dx, dy), MARK_INK_W * s


# ---------------------------------------------------------------- type

_instances = {}


def instance(weight):
    if weight not in _instances:
        f = instancer.instantiateVariableFont(
            TTFont(SRC_FONT), {"wght": weight, "wdth": 100}, inplace=False
        )
        buf = io.BytesIO()
        f.save(buf)
        _instances[weight] = (f, buf.getvalue())
    return _instances[weight]


def shape(text, weight, tracking=0.0):
    """Shape `text` at upem scale, baseline at y=0, y-down.

    Returns (path_d, advance, ink_bounds). `ink_bounds` is (x0, y0, x1, y1) with
    y0 above the baseline (negative). `tracking` is in em, between glyphs only.
    """
    font, raw = instance(weight)
    hbfont = hb.Font(hb.Face(raw))
    hbfont.scale = (UPEM, UPEM)

    buf = hb.Buffer()
    buf.add_str(text)
    buf.guess_segment_properties()
    hb.shape(hbfont, buf, {"kern": True, "liga": True})

    glyphset = font.getGlyphSet()
    order = font.getGlyphOrder()
    spen, bpen = SVGPathPen(glyphset), BoundsPen(glyphset)

    cursor = 0.0
    infos, positions = buf.glyph_infos, buf.glyph_positions
    for i, (info, pos) in enumerate(zip(infos, positions)):
        name = order[info.codepoint]
        t = Transform(1, 0, 0, -1, cursor + pos.x_offset, -pos.y_offset)
        glyphset[name].draw(TransformPen(spen, t))
        glyphset[name].draw(TransformPen(bpen, t))
        cursor += pos.x_advance
        if i != len(infos) - 1:
            cursor += tracking * UPEM

    return spen.getCommands(), cursor, bpen.bounds


class Run:
    """A shaped run of text, measured, ready to place at a baseline origin."""

    def __init__(self, text, size, weight, tracking=0.0):
        d, adv, ink = shape(text, weight, tracking)
        k = size / UPEM
        self.d = d
        self.k = k
        self.advance = adv * k
        self.ink = tuple(v * k for v in ink)

    @property
    def ink_left(self):
        return self.ink[0]

    @property
    def ink_w(self):
        return self.ink[2] - self.ink[0]

    def at(self, x, baseline, fill):
        """Emit the run with its baseline at `baseline` and pen origin at `x`."""
        return (
            f'<path transform="translate({x:.2f} {baseline:.2f}) '
            f'scale({self.k:.6f})" fill="{fill}" d="{self.d}"/>'
        )

    def at_ink_left(self, x, baseline, fill):
        """Emit with the run's left INK edge at `x`, for optical alignment."""
        return self.at(x - self.ink_left, baseline, fill)


def metric(char, size, weight):
    """Height of `char` above the baseline, in the same units as `size`."""
    _, _, ink = shape(char, weight)
    return -ink[1] * size / UPEM


# ---------------------------------------------------------------- lockup

def lockup(size, node, edge, core, word_fill, sub_fill, sub=None):
    """Mark + 'spate' (+ optional lighter second word), laid out on ink.

    Origin is (0, 0) at the top-left of the composed ink box.
    Returns (svg_body, width, height).
    """
    cap = metric("S", size, WORD_WEIGHT)
    xh = metric("x", size, WORD_WEIGHT)

    word = Run("spate", size, WORD_WEIGHT, WORD_TRACK)
    runs = [(word, word_fill, WORD_WEIGHT)]
    if sub:
        runs.append((Run(sub, size, SUB_WEIGHT, SUB_TRACK), sub_fill, SUB_WEIGHT))

    mark_h = cap * 1.15
    gap = cap * 0.34
    word_gap = size * 0.24

    # Vertical: the mark's ink center sits on the x-height band center, which is
    # where a lowercase wordmark carries its visual mass.
    baseline = 0.0
    mark_mid = baseline - xh / 2

    # Compose left to right in a temporary frame, then normalize to (0, 0).
    body, mark_w = mark_by_ink(node, edge, core, mark_h, 0.0, mark_mid)
    parts = [body]
    x = mark_w + gap
    for i, (run, fill, _) in enumerate(runs):
        if i:
            x += word_gap
        parts.append(run.at_ink_left(x, baseline, fill))
        x += run.ink_w

    top = min(mark_mid - mark_h / 2, min(r.ink[1] for r, _, _ in runs))
    bottom = max(mark_mid + mark_h / 2, max(r.ink[3] for r, _, _ in runs))

    shifted = f'<g transform="translate(0 {-top:.2f})">\n' + "\n".join(parts) + "\n</g>"
    return shifted, x, bottom - top


def write(name, body, dest=None):
    dest = OUT if dest is None else dest
    dest.mkdir(parents=True, exist_ok=True)
    (dest / name).write_text(body.rstrip() + "\n")
    print(f"  {(dest / name).relative_to(STATIC.parent.parent)}")


# ---------------------------------------------------------------- assets

def gen_marks():
    # The navbar logo ships in two grounds; docusaurus.config.ts picks between
    # them with `logo.srcDark`.
    for name, (n, e, c) in {
        "logo.svg": (LIGHT_NODE, LIGHT_EDGE, LIGHT_CORE),
        "logo-dark.svg": (DARK_NODE, DARK_NODE, DARK_CORE),
    }.items():
        write(
            name,
            '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 32 32" '
            f'fill="none" role="img" aria-label="Spate">\n{mark(n, e, c)}\n</svg>',
            STATIC,
        )

    write(
        "favicon.svg",
        '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 32 32" '
        'role="img" aria-label="Spate">\n'
        f'  <rect width="32" height="32" rx="7" fill="{DARK_BASE}"/>\n'
        f"{mark(DARK_NODE, DARK_NODE, DARK_CORE)}\n</svg>",
        STATIC,
    )

    gen_square_icon(512, "avatar.svg")


def gen_tokens():
    """Write the palette as CSS custom properties the site maps onto Infima."""
    steps = ["darkest", "darker", "dark", "", "light", "lighter", "lightest"]
    lines = [
        "/* Generated by website/tools/brand/brandgen.py. Do not edit; change the",
        " * palette there and run website/tools/brand/generate.sh. */",
        "",
    ]
    # The light block also matches an explicit light element, so a light
    # swatch inside a dark page (the brand page) takes the light tokens.
    for ground, selector in (("light", ":root,\n[data-theme='light']"), ("dark", "[data-theme='dark']")):
        lines.append(f"{selector} {{")
        for k, v in TOKENS[ground].items():
            lines.append(f"  --spate-{k}: {v};")
        for step, v in zip(steps, RAMP[ground]):
            name = "--spate-primary" + (f"-{step}" if step else "")
            lines.append(f"  {name}: {v};")
        lines.append("}")
        lines.append("")
    write("brand.css", "\n".join(lines), STATIC.parent.parent / "src" / "css")


def gen_square_icon(size, name):
    # Full-bleed square, no corner radius: the platform applies its own mask
    # (GitHub for the avatar, iOS for the touch icon). Ink fills 62% of the
    # width, which keeps it clear of that mask.
    ink_w = size * 0.62
    body, _ = mark_by_ink(
        DARK_NODE, DARK_NODE, DARK_CORE,
        ink_w * MARK_INK_H / MARK_INK_W, (size - ink_w) / 2, size / 2,
    )
    write(
        name,
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {size} {size}" '
        f'width="{size}" height="{size}" role="img" aria-label="Spate">\n'
        f'  <rect width="{size}" height="{size}" fill="{DARK_BASE}"/>\n{body}\n</svg>',
    )


def gen_touch_icon():
    gen_square_icon(180, "apple-touch-icon.svg")


def gen_lockups():
    for name, (n, e, c, wf, sf) in {
        "lockup-light.svg": (LIGHT_NODE, LIGHT_EDGE, LIGHT_CORE, "#16181d", "#676d74"),
        "lockup-dark.svg": (DARK_NODE, DARK_NODE, DARK_CORE, "#f4f5f6", "#9aa1a9"),
    }.items():
        body, w, h = lockup(120, n, e, c, wf, sf)
        pad = 10
        write(
            name,
            '<svg xmlns="http://www.w3.org/2000/svg" '
            f'viewBox="{-pad} {-pad} {w + 2 * pad:.2f} {h + 2 * pad:.2f}" '
            f'width="{w + 2 * pad:.0f}" height="{h + 2 * pad:.0f}" '
            f'role="img" aria-label="Spate">\n{body}\n</svg>',
        )


BANNER_W, BANNER_H = 1280, 640


def banner(sub, tagline, filename):
    margin = 128
    body, lw, lh = lockup(
        108, DARK_NODE, DARK_NODE, DARK_CORE, BANNER_TEXT, BANNER_MUTED, sub=sub
    )

    rule_h, rule_gap, tag_size, tag_gap = 3, 46, 32, 42
    tag = Run(tagline, tag_size, 400)
    tag_h = tag.ink[3] - tag.ink[1]

    block_h = lh + rule_gap + rule_h + tag_gap + tag_h
    top = (BANNER_H - block_h) / 2

    rule_y = top + lh + rule_gap
    tag_baseline = rule_y + rule_h + tag_gap - tag.ink[1]

    write(
        filename,
        '<svg xmlns="http://www.w3.org/2000/svg" '
        f'viewBox="0 0 {BANNER_W} {BANNER_H}" width="{BANNER_W}" '
        f'height="{BANNER_H}" role="img" aria-label="Spate">\n'
        f'  <rect width="{BANNER_W}" height="{BANNER_H}" fill="{DARK_BASE}"/>\n'
        f'  <g transform="translate({margin} {top:.2f})">\n{body}\n  </g>\n'
        f'  <rect x="{margin}" y="{rule_y:.2f}" width="64" height="{rule_h}" '
        f'rx="1.5" fill="{DARK_NODE}"/>\n'
        f"  {tag.at_ink_left(margin, tag_baseline, BANNER_MUTED)}\n"
        "</svg>",
    )


def gen_banners():
    banner(
        None,
        "At-least-once streaming ETL for Rust.",
        "social-spate.svg",
    )
    banner(
        "benchmark",
        "Streaming ETL systems on one fixed pipeline: Kafka → Avro → ClickHouse.",
        "social-benchmark.svg",
    )


if __name__ == "__main__":
    ensure_font()
    gen_tokens()
    gen_marks()
    gen_touch_icon()
    gen_lockups()
    gen_banners()
    print("done")
