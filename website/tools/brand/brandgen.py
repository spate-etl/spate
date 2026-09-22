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
from waterline import MONO_FONT, SRC_FONT, UPEM, instance

HERE = pathlib.Path(__file__).parent
STATIC = (HERE / ".." / ".." / "static" / "img").resolve()
OUT = STATIC / "brand"

# The wordmark is set in IBM Plex Sans and the carriers in Overpass Mono (both
# SIL Open Font License 1.1). The fonts are fetched on demand rather than
# vendored: the glyphs ship as baked outlines, so the repository never
# redistributes font software and the license surface is unchanged. Pinned by
# digest. A mismatch means the upstream file moved, and the artwork is re-cut
# deliberately.
FONTS = {
    SRC_FONT: (
        "https://github.com/google/fonts/raw/main/ofl/ibmplexsans/"
        "IBMPlexSans%5Bwdth,wght%5D.ttf",
        "3b031aa4216174205bd8471f88a49b91f093169e9e87bd5262242bc5967fe2e3",
    ),
    MONO_FONT: (
        "https://github.com/google/fonts/raw/main/ofl/overpassmono/"
        "OverpassMono%5Bwght%5D.ttf",
        "49f230e10251608f0ae1a2ce46be768d7b9ddcbe5cdca2e9f6b762fcbce1ae4f",
    ),
}


def ensure_fonts():
    for path, (url, sha256) in FONTS.items():
        if path.exists():
            digest = hashlib.sha256(path.read_bytes()).hexdigest()
            if digest == sha256:
                continue
            print(f"cached font digest mismatch, refetching ({digest[:12]}…)")

        print(f"fetching {url}")
        path.parent.mkdir(parents=True, exist_ok=True)
        with urllib.request.urlopen(url) as r:  # noqa: S310 - pinned https URL
            blob = r.read()

        digest = hashlib.sha256(blob).hexdigest()
        if digest != sha256:
            sys.exit(
                f"font digest mismatch for {path.name}\n  expected {sha256}\n  got      {digest}"
            )
        path.write_bytes(blob)

# ---------------------------------------------------------------- palette
#
# The one place a colour is chosen. `gen_tokens` writes the whole table to
# website/src/css/brand.css, which the site's stylesheet maps onto Infima, so
# an asset and the page it sits on cannot disagree.
#
# The diagram colours and the UI's accent differ on the light ground on
# purpose: a diagram is a graphic and reads at 3:1, while the accent carries
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
        "diagram-node": LIGHT_NODE,
        "diagram-edge": LIGHT_EDGE,
        "diagram-core": LIGHT_CORE,
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
        "diagram-node": DARK_NODE,
        "diagram-edge": DARK_NODE,
        "diagram-core": DARK_CORE,
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

def shape(text, weight, tracking=0.0, src=SRC_FONT):
    """Shape `text` in the font's own units, baseline at y=0, y-down.

    Returns (path_d, advance, ink_bounds, upem). `ink_bounds` is (x0, y0, x1, y1)
    with y0 above the baseline (negative). `tracking` is in em, between glyphs
    only.
    """
    font, raw = instance(weight, src)
    upem = font["head"].unitsPerEm
    hbfont = hb.Font(hb.Face(raw))
    hbfont.scale = (upem, upem)

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
            cursor += tracking * upem

    return spen.getCommands(), cursor, bpen.bounds, upem


class Run:
    """A shaped run of text, measured, ready to place at a baseline origin."""

    def __init__(self, text, size, weight, tracking=0.0, src=SRC_FONT):
        d, adv, ink, upem = shape(text, weight, tracking, src)
        k = size / upem
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


def svg(view_box, body, size=None, label="Spate"):
    dims = f' width="{size[0]:.0f}" height="{size[1]:.0f}"' if size else ""
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="{view_box}"{dims} '
        f'role="img" aria-label="{label}">\n'
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


def gen_social_card(art):
    """The site's Open Graph card on the dark ground, carrying the `04` of the
    four stages. The layout is drawn for 1200×630; at 1280×640, items anchored
    to the right move by the extra width and the bottom row by the extra height."""
    c = TOKENS["dark"]
    dx, dy = BANNER_W - 1200, BANNER_H - 630
    margin = 64
    fx0, fy0, fx1, _ = finished_ink(art)
    s = 142 / (fx1 - fx0)
    body = [f'<rect width="{BANNER_W}" height="{BANNER_H}" fill="{c["bg"]}"/>']
    body += word_paths(art, Transform(s, 0, 0, s, margin - fx0 * s, 48 - fy0 * s), c["ink"], c["accent"])
    body += [
        Run("RUST / STREAMING ETL", 17, 500, src=MONO_FONT).at(64, 182, c["accent"]),
        Run("One pipeline.", 74, 500, -0.035).at(60, 273, c["ink"]),
        Run("Four stages.", 74, 500, -0.035).at(60, 351, c["ink"]),
        Run("At-least-once delivery.", 29, 400).at(64, 421, c["muted"]),
    ]
    above, below = waterline.carrier("04", (744 + dx, 158, 392, 303))
    body += pieces([above], [below], Transform(), c["ink"], c["accent"])
    body.append(
        f'<rect x="{margin}" y="{510 + dy}" width="{BANNER_W - 2 * margin}" height="1" fill="{c["border"]}"/>'
    )
    col = 232
    for i, stage in enumerate(["Extract", "Transform", "Load", "Observe"]):
        body.append(Run(f"0{i + 1}", 13, 500, src=MONO_FONT).at(64 + i * col, 562 + dy, c["accent"]))
        body.append(Run(stage, 20, 400).at(94 + i * col, 562 + dy, c["ink"]))
    body.append(Run("spate.kainth.dev", 14, 400, src=MONO_FONT).at(939 + dx, 606 + dy, c["muted"]))
    write("social-spate.svg", svg(f"0 0 {BANNER_W} {BANNER_H}", body, (BANNER_W, BANNER_H)))


def gen_not_found():
    """The 404 page's `404` carrier on a transparent ground, in the study's 697×345 box."""
    w, h = 697, 345
    above, below = waterline.carrier("404", (0, 0, w, h))
    for name, colors in (("not-found.svg", two_tone("light")), ("not-found-dark.svg", two_tone("dark"))):
        write(name, svg(f"0 0 {w} {h}", pieces([above], [below], Transform(), *colors), (w, h), "404"))


def gen_misuse(art):
    """The brand page's misuse gallery: the wordmark done wrong, each on a
    transparent 300×140 canvas. The page carries the captions."""
    w, h, ink_w = 300, 140, 220
    ink, accent = two_tone("light")

    def placed(art_, rotate=0.0):
        fx0, fy0, fx1, fy1 = finished_ink(art_)
        k = ink_w / (fx1 - fx0)
        t = Transform(k, 0, 0, k, (w - ink_w) / 2 - fx0 * k, (h - (fy1 - fy0) * k) / 2 - fy0 * k)
        if rotate:
            t = Transform().translate(w / 2, h / 2).rotate(rotate).translate(-w / 2, -h / 2).transform(t)
        return t

    t = placed(art)
    uncut = waterline.shape("spate", waterline.P["weight"], waterline.P["track"])
    whole = [g for g, _ in uncut]
    swell = " ".join(f"{x:.2f},{y:.2f}" for x, y in (t.transformPoint(pt) for pt in art["swell"]))
    fx0, fy0, fx1, fy1 = finished_ink(art)
    fh = (fy1 - fy0) * t[0]
    mono = waterline.carrier("spate", ((w - ink_w) / 2, (h - fh) / 2, ink_w, fh))

    cases = {
        "recolored": word_paths(art, t, "#2457c5", "#2f9e44"),
        "stroke": [
            f'<path fill="{ink}" d="{waterline.svg_d(whole, t)}"/>',
            f'<polyline fill="none" stroke="{accent}" stroke-width="{waterline.P["gap"] * t[0]:.2f}" '
            f'stroke-linecap="round" points="{swell}"/>',
        ],
        "moved": word_paths(waterline.artwork({**waterline.P, "cut": 0.25}), t, ink, accent),
        "outlined": [
            line.replace('fill="', 'fill="none" stroke-width="1.5" stroke="')
            for line in word_paths(art, t, ink, accent)
        ],
        "typeface": pieces([mono[0]], [mono[1]], Transform(), ink, accent),
        "rotated": word_paths(art, placed(art, -0.2), ink, accent),
    }
    for name, body in cases.items():
        write(f"misuse-{name}.svg", svg(f"0 0 {w} {h}", body, (w, h), f"Misuse: {name}"))


if __name__ == "__main__":
    ensure_fonts()
    gen_tokens()
    art = waterline.artwork(waterline.P)
    gen_marks(art, waterline.crop(art))
    gen_social_card(art)
    gen_not_found()
    gen_misuse(art)
    banner(
        art,
        "benchmark",
        "Streaming ETL systems on one fixed pipeline: Kafka → Avro → ClickHouse.",
        "social-benchmark.svg",
    )
    print("done")
