#!/usr/bin/env bash
#
# Regenerate the Spate brand assets: SVG sources, then the PNGs that get
# uploaded to GitHub by hand.
#
# Everything here is derived. Do not hand-edit anything under
# `website/static/img/brand/`, `logo.svg`, `logo-dark.svg` or `favicon.svg`.
# Change `brandgen.py` and re-run this.
#
# Prerequisites: python3, and resvg + oxipng for the raster step
# (`brew install resvg oxipng`). Without them the SVGs are still written, the
# script reports which PNGs it could not refresh, and it exits non-zero.

set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
img="$(cd "$here/../../static/img" && pwd)"
brand="$img/brand"
venv="$here/.venv"

# --- SVG sources -----------------------------------------------------------

if [[ ! -x "$venv/bin/python" ]]; then
    echo "creating venv at $venv"
    python3 -m venv "$venv"
fi
"$venv/bin/pip" install --quiet --disable-pip-version-check -r "$here/requirements.txt"
"$venv/bin/python" "$here/brandgen.py"

# --- raster ----------------------------------------------------------------

missing=()
command -v resvg  >/dev/null 2>&1 || missing+=(resvg)
command -v oxipng >/dev/null 2>&1 || missing+=(oxipng)

if (( ${#missing[@]} )); then
    echo
    echo "SVG sources written, but the PNGs were NOT refreshed."
    echo "missing: ${missing[*]}  (brew install ${missing[*]})"
    exit 1
fi

# width is the intrinsic size of each source; resvg honours the viewBox
rendered=()
render() {
    local src="$1" width="$2" out="$3"
    resvg --width "$width" "$src" "$out"
    rendered+=("$out")
    echo "  ${out#"$(dirname "$(dirname "$img")")/"}"
}

echo "rendering:"
render "$brand/avatar.svg"           512  "$brand/avatar.png"
render "$brand/social-spate.svg"     1280 "$brand/social-spate.png"
render "$brand/social-benchmark.svg" 1280 "$brand/social-benchmark.png"
render "$brand/wordmark.svg"         880  "$brand/wordmark.png"
render "$brand/wordmark-dark.svg"    880  "$brand/wordmark-dark.png"
render "$brand/apple-touch-icon.svg" 180  "$img/apple-touch-icon.png"

oxipng --quiet --opt 4 --strip safe "${rendered[@]}"
echo "optimized ${#rendered[@]} PNGs"

# The starter deck places resvg renders of the wordmark and the carriers.
"$venv/bin/python" "$here/slides.py"

# favicon.ico at the site root, which browsers request without being told.
# One PNG entry per size, all from the dark-ground touch icon source.
ico_sizes=(16 32 48 64 128 256)
ico_dir="$(mktemp -d "${TMPDIR:-/tmp}/favicon.XXXXXX")"
ico_pngs=()
for size in "${ico_sizes[@]}"; do
    resvg --width "$size" "$brand/apple-touch-icon.svg" "$ico_dir/$size.png"
    ico_pngs+=("$ico_dir/$size.png")
done
oxipng --quiet --opt 4 --strip safe "${ico_pngs[@]}"
"$venv/bin/python" - "$img/../favicon.ico" "${ico_pngs[@]}" <<'PY'
import struct, sys
out, *srcs = sys.argv[1:]
pngs = [open(src, "rb").read() for src in srcs]
entries, offset = b"", 6 + 16 * len(pngs)
for png in pngs:
    w, h = struct.unpack(">II", png[16:24])
    # A dimension of 256 is stored as 0.
    entries += struct.pack("<BBBBHHII", w % 256, h % 256, 0, 0, 1, 32, len(png), offset)
    offset += len(png)
open(out, "wb").write(struct.pack("<HHH", 0, 1, len(pngs)) + entries + b"".join(pngs))
PY
rm -rf "$ico_dir"
echo "  static/favicon.ico"
