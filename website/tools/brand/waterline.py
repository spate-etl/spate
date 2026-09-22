"""The waterline mark: `spate` set in IBM Plex Sans and cut once by a swell.

The word is shaped with HarfBuzz, instanced from the variable font and baked to
outlines. A sine swell runs across it, and a channel `gap` wide, measured across
the swell, is knocked out of every glyph. `artwork()` returns the pieces above
and below the channel for each glyph; the icon is glyph 0 framed by `crop()`.
Coordinates are upem, y down, baseline at 0.

`brandgen.py` fetches and verifies the font before anything here reads it.
"""
import io
import math
import pathlib

import pathops
import skia
import uharfbuzz as hb
from fontTools.misc.transform import Transform
from fontTools.pens.basePen import decomposeQuadraticSegment
from fontTools.pens.boundsPen import BoundsPen
from fontTools.pens.recordingPen import RecordingPen
from fontTools.pens.svgPathPen import SVGPathPen
from fontTools.pens.transformPen import TransformPen
from fontTools.ttLib import TTFont
from fontTools.varLib import instancer

HERE = pathlib.Path(__file__).parent
SRC_FONT = HERE / ".cache" / "IBMPlexSans.ttf"
UPEM = 1000
ICON_S = 0.76 * 32  # the s's ink height on the 32-unit icon canvas; its ink is centred on (16, 16)

P = dict(
    weight=600,     # wght instance for the whole artwork
    track=-0.022,   # em, between glyphs
    cut=0.62,       # swell centre, fraction down the s's ink height
    gap=45.0,       # channel width, upem, perpendicular to the swell
    amp=50.0,       # swell amplitude, upem
    periods=2.5,    # sine periods across the swell span (word ink width + 100 upem each side)
    phase=0.35,     # fraction of a period; the surface falls to the right through the s
    blunt=1.9,      # minimum ink feature diameter after the cut, icon canvas units
    corner=25.0,    # a glyph joint turning more than this many degrees is a corner the blunting keeps
    tol=0.5,        # swell polyline decimation tolerance, upem
    n=512,          # swell samples before decimation
)

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
    """Shape `text` at upem scale, baseline y=0, y-down.

    Returns a list of (pathops.Path, ink bounds) per glyph, in run order."""
    font, raw = instance(weight)
    hbfont = hb.Font(hb.Face(raw))
    hbfont.scale = (UPEM, UPEM)
    buf = hb.Buffer()
    buf.add_str(text)
    buf.guess_segment_properties()
    hb.shape(hbfont, buf, {"kern": True, "liga": True})

    glyphset = font.getGlyphSet()
    order = font.getGlyphOrder()
    out = []
    cursor = 0.0
    infos, positions = buf.glyph_infos, buf.glyph_positions
    for i, (info, pos) in enumerate(zip(infos, positions)):
        name = order[info.codepoint]
        t = Transform(1, 0, 0, -1, cursor + pos.x_offset, -pos.y_offset)
        path = pathops.Path()
        glyphset[name].draw(TransformPen(path.getPen(), t))
        bpen = BoundsPen(glyphset)
        glyphset[name].draw(TransformPen(bpen, t))
        path.simplify()
        out.append((path, bpen.bounds))
        cursor += pos.x_advance
        if i != len(infos) - 1:
            cursor += tracking * UPEM
    return out


# ---------------------------------------------------------------- geometry

def decimate(pts, tol):
    """Douglas-Peucker on an open polyline."""
    if tol <= 0 or len(pts) < 3:
        return pts
    (x0, y0), (x1, y1) = pts[0], pts[-1]
    dx, dy = x1 - x0, y1 - y0
    n = math.hypot(dx, dy) or 1.0
    best, bi = 0.0, 0
    for i in range(1, len(pts) - 1):
        d = abs(dy * (pts[i][0] - x0) - dx * (pts[i][1] - y0)) / n
        if d > best:
            best, bi = d, i
    if best <= tol:
        return [pts[0], pts[-1]]
    return decimate(pts[:bi + 1], tol)[:-1] + decimate(pts[bi:], tol)


def swell(x0, x1, yc, p):
    """The water surface from x0 to x1 about yc, as a decimated polyline."""
    pts = []
    for i in range(p["n"] + 1):
        u = i / p["n"]
        x = x0 + (x1 - x0) * u
        y = yc - p["amp"] * math.sin(2 * math.pi * (p["periods"] * u + p["phase"]))
        pts.append((x, y))
    return decimate(pts, p["tol"])


def half_plane(pts, offset, above, far=1e5):
    """Region on one side of the polyline, offset by `offset` along the local normal."""
    path = pathops.Path()
    pen = path.getPen()
    shifted = []
    for i, (x, y) in enumerate(pts):
        (xa, ya), (xb, yb) = pts[max(i - 1, 0)], pts[min(i + 1, len(pts) - 1)]
        cos = (xb - xa) / math.hypot(xb - xa, yb - ya)
        shifted.append((x, y + offset / cos))
    if above:
        pen.moveTo((shifted[0][0] - far, -far))
        pen.lineTo((shifted[-1][0] + far, -far))
        pen.lineTo((shifted[-1][0] + far, shifted[-1][1]))
        for x, y in reversed(shifted):
            pen.lineTo((x, y))
        pen.lineTo((shifted[0][0] - far, shifted[0][1]))
    else:
        pen.moveTo((shifted[0][0] - far, far))
        pen.lineTo((shifted[0][0] - far, shifted[0][1]))
        for x, y in shifted:
            pen.lineTo((x, y))
        pen.lineTo((shifted[-1][0] + far, shifted[-1][1]))
        pen.lineTo((shifted[-1][0] + far, far))
    pen.closePath()
    return path


def cut(glyph, pts, gap):
    above = pathops.op(glyph, half_plane(pts, -gap / 2, True), pathops.PathOp.INTERSECTION)
    below = pathops.op(glyph, half_plane(pts, +gap / 2, False), pathops.PathOp.INTERSECTION)
    return above, below


# ---------------------------------------------------------------- blunting
#
# A cut across a stroke leaves a tip that tapers to nothing. Each piece is
# opened with a disk (eroded then dilated by r) so nothing thinner than 2r
# survives and every tip ends in a semicircle. The opening would also round
# the glyph's own corners (Plex's flat terminals), so the piece within 2r of
# each corner is put back unchanged.

class _SkPen:
    def __init__(self):
        self.p = skia.Path()

    def moveTo(self, pt):
        self.p.moveTo(*pt)

    def lineTo(self, pt):
        self.p.lineTo(*pt)

    def qCurveTo(self, *pts):
        for c, e in decomposeQuadraticSegment(pts):
            self.p.quadTo(*c, *e)

    def curveTo(self, a, b, c):
        self.p.cubicTo(*a, *b, *c)

    def closePath(self):
        self.p.close()

    def endPath(self):
        pass


def to_skia(path):
    pen = _SkPen()
    path.draw(pen)
    return pen.p


def to_pathops(sp):
    out = pathops.Path()
    pen = out.getPen()
    it = skia.Path.Iter(sp, False)
    while True:
        verb, pts = it.next()
        if verb == skia.Path.kDone_Verb:
            break
        if verb == skia.Path.kMove_Verb:
            pen.moveTo((pts[0].x(), pts[0].y()))
        elif verb == skia.Path.kLine_Verb:
            pen.lineTo((pts[1].x(), pts[1].y()))
        elif verb == skia.Path.kQuad_Verb:
            pen.qCurveTo((pts[1].x(), pts[1].y()), (pts[2].x(), pts[2].y()))
        elif verb == skia.Path.kCubic_Verb:
            pen.curveTo((pts[1].x(), pts[1].y()), (pts[2].x(), pts[2].y()), (pts[3].x(), pts[3].y()))
        elif verb == skia.Path.kConic_Verb:
            qs = skia.Path.ConvertConicToQuads(pts[0], pts[1], pts[2], it.conicWeight(), 3)
            for i in range(0, len(qs) - 1, 2):
                pen.qCurveTo((qs[i + 1].x(), qs[i + 1].y()), (qs[i + 2].x(), qs[i + 2].y()))
        elif verb == skia.Path.kClose_Verb:
            pen.closePath()
    return out


def stroke_ring(sp, r):
    paint = skia.Paint()
    paint.setStyle(skia.Paint.kStroke_Style)
    paint.setStrokeWidth(2 * r)
    paint.setStrokeJoin(skia.Paint.kRound_Join)
    paint.setStrokeCap(skia.Paint.kRound_Cap)
    ring = skia.Path()
    paint.getFillPath(sp, ring)
    return ring


def opening(piece, r):
    sp = to_skia(piece)
    eroded = skia.Op(sp, stroke_ring(sp, r), skia.PathOp.kDifference_PathOp)
    dilated = skia.Op(eroded, stroke_ring(eroded, r), skia.PathOp.kUnion_PathOp)
    out = to_pathops(dilated)
    out.simplify()
    return out


def corners(glyph, min_deg):
    """Corners of `glyph`, grouped: on-curve points where the outline turns by
    more than min_deg, and two corners joined by a straight segment (a flat
    terminal) form one group."""
    rec = RecordingPen()
    glyph.draw(rec)
    groups = []
    contour = []  # [point, tangent_in, tangent_out, joined_to_previous_by_line]
    start = None

    def flush():
        pts = []
        for i, (pt, tin, tout, line) in enumerate(contour):
            if tin is None or tout is None:
                continue
            a = math.atan2(tin[1], tin[0])
            b = math.atan2(tout[1], tout[0])
            turn = abs((b - a + math.pi) % (2 * math.pi) - math.pi)
            if math.degrees(turn) > min_deg:
                pts.append(i)
        used = set()
        n = len(contour)
        for i in pts:
            if i in used:
                continue
            group = [contour[i][0]]
            used.add(i)
            j = (i + 1) % n
            if j in pts and j not in used and contour[j][3]:
                group.append(contour[j][0])
                used.add(j)
            groups.append(group)

    for op, args in rec.value:
        if op == "moveTo":
            contour = [[args[0], None, None, False]]
            start = args[0]
        elif op == "lineTo":
            prev = contour[-1][0]
            d = (args[0][0] - prev[0], args[0][1] - prev[1])
            contour[-1][2] = contour[-1][2] or d
            contour.append([args[0], d, None, True])
        elif op in ("qCurveTo", "curveTo"):
            prev = contour[-1][0]
            d_out = (args[0][0] - prev[0], args[0][1] - prev[1])
            d_in = (args[-1][0] - args[-2][0], args[-1][1] - args[-2][1])
            contour[-1][2] = contour[-1][2] or d_out
            contour.append([args[-1], d_in, None, False])
        elif op in ("closePath", "endPath"):
            if contour and contour[-1][0] == start:
                contour[0][1] = contour[-1][1]
                contour[0][3] = contour[-1][3]
                contour.pop()
            elif contour:
                prev = contour[-1][0]
                d = (start[0] - prev[0], start[1] - prev[1])
                contour[-1][2] = contour[-1][2] or d
                contour[0][1] = d
                contour[0][3] = True
            flush()
            contour = []
    return groups


def disk_path(c, r):
    p = pathops.Path()
    pen = p.getPen()
    k = 0.5522847498
    x, y = c
    pen.moveTo((x + r, y))
    pen.curveTo((x + r, y + k * r), (x + k * r, y + r), (x, y + r))
    pen.curveTo((x - k * r, y + r), (x - r, y + k * r), (x - r, y))
    pen.curveTo((x - r, y - k * r), (x - k * r, y - r), (x, y - r))
    pen.curveTo((x + k * r, y - r), (x + r, y - k * r), (x + r, y))
    pen.closePath()
    return p


def dist_to_polyline(c, pts):
    x, y = c
    best = float("inf")
    for (ax, ay), (bx, by) in zip(pts, pts[1:]):
        dx, dy = bx - ax, by - ay
        t = max(0.0, min(1.0, ((x - ax) * dx + (y - ay) * dy) / (dx * dx + dy * dy)))
        best = min(best, math.hypot(x - (ax + t * dx), y - (ay + t * dy)))
    return best


def blunt(piece, r, keep, clear):
    """Open `piece` with a disk of radius r, keeping it unchanged within 2r of
    each point in `keep`. A restored disk is clipped to `clear`, the region the
    opening cannot have changed near a cut, so it never puts back part of a tip."""
    if r <= 0:
        return piece
    out = opening(piece, r)
    for c in keep:
        restored = pathops.op(piece, disk_path(c, 2 * r), pathops.PathOp.INTERSECTION)
        restored = pathops.op(restored, clear, pathops.PathOp.INTERSECTION)
        out = pathops.op(out, restored, pathops.PathOp.UNION)
    out.simplify()
    return out


def artwork(p):
    """The cut word. Returns dict with per-glyph (above, below) pieces, the
    swell polyline, the word ink bounds and the s ink bounds, all in upem."""
    glyphs = shape("spate", p["weight"], p["track"])
    x0 = min(b[0] for _, b in glyphs)
    x1 = max(b[2] for _, b in glyphs)
    y0 = min(b[1] for _, b in glyphs)
    y1 = max(b[3] for _, b in glyphs)
    s_ink = glyphs[0][1]
    yc = s_ink[1] + p["cut"] * (s_ink[3] - s_ink[1])
    pts = swell(x0 - 100, x1 + 100, yc, p)
    k = ICON_S / (s_ink[3] - s_ink[1])   # canvas units per upem
    r = p["blunt"] / 2 / k
    # The opening rounds a cut tip within about r of the channel edge. Outside
    # 2r of it the piece is unchanged, so a restored corner is clipped there.
    clear = pathops.op(half_plane(pts, -(p["gap"] / 2 + 2 * r), True),
                       half_plane(pts, +(p["gap"] / 2 + 2 * r), False), pathops.PathOp.UNION)
    pieces = []
    for g, _ in glyphs:
        # Restoring a corner puts back the piece within 2r of it. A corner
        # closer than gap/2 + 2r to the swell would put part of a cut tip back,
        # so its whole group (the terminal it belongs to) is rounded with the tip.
        keep = [c for group in corners(g, p["corner"])
                if all(dist_to_polyline(c, pts) > p["gap"] / 2 + 2 * r for c in group)
                for c in group]
        above, below = cut(g, pts, p["gap"])
        pieces.append((blunt(above, r, keep, clear), blunt(below, r, keep, clear)))
    return dict(pieces=pieces, swell=pts, ink=(x0, y0, x1, y1), s_ink=s_ink, yc=yc, blunt_r_upem=r)


def crop(art):
    """Affine mapping wordmark upem coordinates onto the 32-unit icon canvas:
    the s's ink is ICON_S tall and centred. Returns (k, dx, dy) with
    canvas = upem * k + (dx, dy). The scale comes from the uncut glyph, which
    blunting needs before the cut exists; the centre comes from the finished
    pieces, so rounded terminals do not pull the letter off centre."""
    sx0, sy0, sx1, sy1 = art["s_ink"]
    k = ICON_S / (sy1 - sy0)
    bs = [piece.bounds for piece in art["pieces"][0] if piece.bounds]
    fx0, fy0 = min(b[0] for b in bs), min(b[1] for b in bs)
    fx1, fy1 = max(b[2] for b in bs), max(b[3] for b in bs)
    dx = 16 - (fx0 + fx1) / 2 * k
    dy = 16 - (fy0 + fy1) / 2 * k
    return k, dx, dy


# ---------------------------------------------------------------- output

def fmt(v):
    return f"{v:.2f}".rstrip("0").rstrip(".") or "0"


def svg_d(paths, t):
    pen = SVGPathPen(None, ntos=fmt)
    for path in paths:
        path.draw(TransformPen(pen, t))
    return pen.getCommands()
