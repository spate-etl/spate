#!/usr/bin/env python3
"""Generate the Spate brand assets.

Text is shaped with HarfBuzz and baked to outlines, so the emitted SVGs carry no
font dependency. All layout is driven by measured ink bounds rather than the
nominal canvas, so the wordmark, the rule and the tagline share one optical left
edge. The mark itself comes from `waterline.py`.
"""
import hashlib
import pathlib
import sys
import urllib.request

import uharfbuzz as hb
from fontTools.misc.transform import Transform
from fontTools.pens.boundsPen import BoundsPen
from fontTools.pens.svgPathPen import SVGPathPen
from fontTools.pens.transformPen import TransformPen

import waterline
from waterline import SRC_FONT, UPEM, instance

HERE = pathlib.Path(__file__).parent
STATIC = (HERE / ".." / ".." / "static" / "img").resolve()
OUT = STATIC / "brand"

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
# link text and has to clear 4.5:1 on paper.

DARK_BASE = "#16181d"
DARK_NODE = "#ff8c4a"
DARK_CORE = "#ffc9a8"

LIGHT_NODE = "#d1500b"
LIGHT_EDGE = "#e65a0f"
LIGHT_CORE = "#f16413"
LIGHT_ACCENT = "#c04409"

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
        "dim": "#6b7280",
        "grid": "rgba(23, 24, 28, 0.035)",
        "accent": LIGHT_ACCENT,
        "accent-ink": "#ffffff",
        "accent-soft": "rgba(192, 68, 9, 0.08)",
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
        "dim": "#7d848f",
        "grid": "rgba(244, 245, 246, 0.028)",
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

SUB_WEIGHT, SUB_TRACK = 400, -0.012

# ---------------------------------------------------------------- type

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


def write(name, body, dest=None):
    dest = OUT if dest is None else dest
    dest.mkdir(parents=True, exist_ok=True)
    (dest / name).write_text(body.rstrip() + "\n")
    print(f"  {(dest / name).relative_to(STATIC.parent.parent)}")


# ---------------------------------------------------------------- the mark
#
# One artwork. The wordmark is every glyph's pieces; the icon is the `s` alone,
# framed by `waterline.crop()`. Ink fills the pieces above the water and the
# accent fills those below; the grayscale version fills both with ink.

WORDMARK_K = 0.1  # wordmark SVG units per upem
WORDMARK_PAD = 0.12 * UPEM


def two_tone(ground):
    return TOKENS[ground]["ink"], TOKENS[ground]["accent"]


def grayscale(ground):
    return TOKENS[ground]["ink"], TOKENS[ground]["ink"]


def pieces(aboves, belows, t, top, bottom, attr="fill"):
    """`<path>` elements for the pieces above and below the water, mapped by `t`.

    Equal `top` and `bottom` values give one path holding every piece."""
    if top == bottom or not belows:
        return [f'<path {attr}="{top}" d="{waterline.svg_d(aboves + belows, t)}"/>']
    return [
        f'<path {attr}="{top}" d="{waterline.svg_d(aboves, t)}"/>',
        f'<path {attr}="{bottom}" d="{waterline.svg_d(belows, t)}"/>',
    ]


def icon_paths(art, c, top, bottom, attr="fill"):
    """The icon's paths on the 32-unit canvas, under the crop `c`."""
    k, dx, dy = c
    above, below = art["pieces"][0]
    return pieces([above], [below], Transform(k, 0, 0, k, dx, dy), top, bottom, attr)


def finished_ink(art):
    """Ink bounds of the cut and blunted word, in upem."""
    bs = [piece.bounds for pair in art["pieces"] for piece in pair if piece.bounds]
    return (min(b[0] for b in bs), min(b[1] for b in bs),
            max(b[2] for b in bs), max(b[3] for b in bs))


def word_paths(art, t, top, bottom):
    aboves = [a for a, _ in art["pieces"] if a.bounds]
    belows = [b for _, b in art["pieces"] if b.bounds]
    return pieces(aboves, belows, t, top, bottom)


def svg(view_box, body, size=None):
    dims = f' width="{size[0]:.0f}" height="{size[1]:.0f}"' if size else ""
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="{view_box}"{dims} '
        'role="img" aria-label="Spate">\n'
        + "".join(f"  {line}\n" for line in body)
        + "</svg>"
    )


def gen_marks(art, c):
    # The icon and the wordmark ship in two grounds; docusaurus.config.ts picks
    # between them with `logo.srcDark`.
    for name, dest, colors in (
        ("logo.svg", STATIC, two_tone("light")),
        ("logo-dark.svg", STATIC, two_tone("dark")),
        ("logo-mono.svg", OUT, grayscale("light")),
        ("logo-mono-dark.svg", OUT, grayscale("dark")),
    ):
        write(name, svg("0 0 32 32", icon_paths(art, c, *colors)), dest)

    # The paths keep the master's frame, the uncut word's ink plus WORDMARK_PAD
    # on every side; the viewBox crops that frame to the finished ink.
    x0, y0, _, _ = art["ink"]
    k = WORDMARK_K
    t = Transform(k, 0, 0, k, -(x0 - WORDMARK_PAD) * k, -(y0 - WORDMARK_PAD) * k)
    fx0, fy0, fx1, fy1 = finished_ink(art)
    vx, vy = (fx0 - x0 + WORDMARK_PAD) * k, (fy0 - y0 + WORDMARK_PAD) * k
    w, h = (fx1 - fx0) * k, (fy1 - fy0) * k
    for name, colors in (
        ("wordmark.svg", two_tone("light")),
        ("wordmark-dark.svg", two_tone("dark")),
        ("wordmark-mono.svg", grayscale("light")),
        ("wordmark-mono-dark.svg", grayscale("dark")),
    ):
        write(name, svg(f"{vx:.2f} {vy:.2f} {w:.2f} {h:.2f}", word_paths(art, t, *colors), (w, h)))

    light, dark = two_tone("light"), two_tone("dark")
    style = (
        f"<style>.above{{fill:{light[0]}}}.below{{fill:{light[1]}}}"
        "@media (prefers-color-scheme:dark){"
        f".above{{fill:{dark[0]}}}.below{{fill:{dark[1]}}}}}</style>"
    )
    write("favicon.svg", svg("0 0 32 32", [style, *icon_paths(art, c, "above", "below", "class")]), STATIC)

    gen_square_icon(art, c, 180, "apple-touch-icon.svg")
    gen_square_icon(art, c, 512, "avatar.svg")


def gen_square_icon(art, c, size, name):
    # Full-bleed square, no corner radius: the platform applies its own mask
    # (GitHub for the avatar, iOS for the touch icon).
    body = "".join(icon_paths(art, c, *two_tone("dark")))
    write(
        name,
        svg(
            f"0 0 {size} {size}",
            [
                f'<rect width="{size}" height="{size}" fill="{DARK_BASE}"/>',
                f'<g transform="scale({size / 32:g})">{body}</g>',
            ],
            (size, size),
        ),
    )


# ---------------------------------------------------------------- assets

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


BANNER_W, BANNER_H = 1280, 640


def banner(art, sub, tagline, filename):
    margin, size = 128, 108
    s = size / UPEM
    x0, y0, x1, y1 = finished_ink(art)
    runs = [Run(sub, size, SUB_WEIGHT, SUB_TRACK)] if sub else []
    top = min([y0 * s] + [r.ink[1] for r in runs])
    bottom = max([y1 * s] + [r.ink[3] for r in runs])

    rule_h, rule_gap, tag_size, tag_gap = 3, 46, 32, 42
    tag = Run(tagline, tag_size, 400)
    tag_h = tag.ink[3] - tag.ink[1]

    block_h = (bottom - top) + rule_gap + rule_h + tag_gap + tag_h
    block_top = (BANNER_H - block_h) / 2
    baseline = block_top - top

    ink, accent = two_tone("dark")
    word = word_paths(art, Transform(s, 0, 0, s, margin - x0 * s, baseline), ink, accent)
    word += [r.at_ink_left(margin + (x1 - x0) * s + size * 0.24, baseline, BANNER_MUTED) for r in runs]

    rule_y = block_top + (bottom - top) + rule_gap
    tag_baseline = rule_y + rule_h + tag_gap - tag.ink[1]

    write(
        filename,
        svg(
            f"0 0 {BANNER_W} {BANNER_H}",
            [
                f'<rect width="{BANNER_W}" height="{BANNER_H}" fill="{DARK_BASE}"/>',
                *word,
                f'<rect x="{margin}" y="{rule_y:.2f}" width="64" height="{rule_h}" '
                f'rx="1.5" fill="{DARK_NODE}"/>',
                tag.at_ink_left(margin, tag_baseline, BANNER_MUTED),
            ],
            (BANNER_W, BANNER_H),
        ),
    )


def gen_banners(art):
    banner(
        art,
        None,
        "At-least-once streaming ETL for Rust.",
        "social-spate.svg",
    )
    banner(
        art,
        "benchmark",
        "Streaming ETL systems on one fixed pipeline: Kafka → Avro → ClickHouse.",
        "social-benchmark.svg",
    )


if __name__ == "__main__":
    ensure_font()
    gen_tokens()
    art = waterline.artwork(waterline.P)
    gen_marks(art, waterline.crop(art))
    gen_banners(art)
    print("done")
