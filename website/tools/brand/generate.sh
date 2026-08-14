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
    resvg --width "$width" "$brand/$src" "$brand/$out"
    rendered+=("$brand/$out")
    echo "  static/img/brand/$out"
}

echo "rendering:"
render avatar.svg           512  avatar.png
render social-spate.svg     1280 social-spate.png
render social-benchmark.svg 1280 social-benchmark.png
render lockup-light.svg     880  lockup-light.png
render lockup-dark.svg      880  lockup-dark.png

oxipng --quiet --opt 4 --strip safe "${rendered[@]}"
echo "optimised ${#rendered[@]} PNGs"
