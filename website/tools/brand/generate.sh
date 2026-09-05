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
"$venv/bin/pip" install --quiet --disable-pip-version-check fonttools brotli uharfbuzz
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
    echo "  ${out#"$(dirname "$img")/"}"
}

echo "rendering:"
render "$brand/avatar.svg"           512  "$brand/avatar.png"
render "$brand/social-spate.svg"     1280 "$brand/social-spate.png"
render "$brand/social-benchmark.svg" 1280 "$brand/social-benchmark.png"
render "$brand/lockup-light.svg"     880  "$brand/lockup-light.png"
render "$brand/lockup-dark.svg"      880  "$brand/lockup-dark.png"
render "$brand/apple-touch-icon.svg" 180  "$img/apple-touch-icon.png"

oxipng --quiet --opt 4 --strip safe "${rendered[@]}"
echo "optimized ${#rendered[@]} PNGs"

# favicon.ico at the site root, which browsers request without being told.
# One 32px PNG inside an ICO container; every current browser reads it.
favicon_png="$(mktemp -t favicon).png"
resvg --width 32 "$img/favicon.svg" "$favicon_png"
oxipng --quiet --opt 4 --strip safe "$favicon_png"
"$venv/bin/python" - "$favicon_png" "$img/../favicon.ico" <<'PY'
import struct, sys
png = open(sys.argv[1], "rb").read()
header = struct.pack("<HHH", 0, 1, 1)
entry = struct.pack("<BBBBHHII", 32, 32, 0, 0, 1, 32, len(png), 6 + 16)
open(sys.argv[2], "wb").write(header + entry + png)
PY
rm -f "$favicon_png"
echo "  static/favicon.ico"
