# Brand assets

The Spate mark, the wordmark lockups, and the images uploaded to GitHub by hand.

Everything in `website/static/img/brand/`, plus `logo.svg`, `logo-dark.svg`,
`favicon.svg` and `apple-touch-icon.png` in `website/static/img/`,
`website/static/favicon.ico`, and the colour tokens in
`website/src/css/brand.css`, is **generated**. Change
[`brandgen.py`](brandgen.py) and re-run, rather than editing an asset:

```sh
./website/tools/brand/generate.sh
```

The script creates its own virtualenv, fetches the typeface, writes the SVG
sources, then rasterises the PNGs. It needs `python3`, plus `resvg` and `oxipng`
for the raster step (`brew install resvg oxipng`). Without those it writes the
SVGs, names what it could not refresh, and exits non-zero.

## The mark

Three sources converge on one core and leave through one sink: many partitions
feeding a single monomorphized loop. Layout is driven by the mark's measured ink
bounds, never by its 32-unit canvas. The ink occupies `x 3.6→28.9`,
`y 5.1→26.9`, so aligning to the canvas leaves it visually inset and floating
above whatever sits beneath it.

The palette is the `TOKENS` and `RAMP` tables in `brandgen.py`, written to
[`src/css/brand.css`](../../src/css/brand.css) as `--spate-*` custom properties
and mapped onto Infima by [`src/css/custom.css`](../../src/css/custom.css). The
mark's colours and the UI accent differ on the light ground: the mark is a
graphic and reads at 3:1, while the accent carries link text and clears 4.5:1.
`make check-brand` holds every pair to those floors.

## Typeface

The wordmark is IBM Plex Sans, SemiBold at −0.022 em for `spate` and Regular at
−0.012 em for a second word. Glyphs are shaped with HarfBuzz and baked to
outlines, so no asset carries a font dependency and the repository never
redistributes font software. The site sets its text in the same family, served
from the `@fontsource/ibm-plex-sans` and `@fontsource/ibm-plex-mono` packages
(SIL Open Font License 1.1) at build time; those files are a dependency, not
part of the repository.

`brandgen.py` pins the upstream file by SHA-256. A digest mismatch stops the
run, since the wordmark would otherwise be re-cut from a different source.

## What goes where

The site picks these up from the config. Nothing to do by hand:

| Asset | Used by |
| --- | --- |
| `img/brand/lockup-light.svg`, `img/brand/lockup-dark.svg` | Navbar, via `logo.src` / `logo.srcDark` |
| `img/logo.svg`, `img/logo-dark.svg` | The mark alone, for pages that set it beside their own text |
| `img/favicon.svg`, `favicon.ico`, `img/apple-touch-icon.png` | Browser tab, search-result thumbnails and the iOS home screen, via `favicon` and `headTags` |
| `img/brand/social-spate.png` | Open Graph card, via `themeConfig.image` |
| `src/css/brand.css` | Every colour on the site, through `custom.css` |

These three are uploaded by hand. GitHub exposes no REST endpoint for either
kind, so there is nothing to script:

| Asset | Where it goes |
| --- | --- |
| `avatar.png` (512×512) | `github.com/organizations/spate-etl/settings/profile` → Profile picture |
| `social-spate.png` (1280×640) | `spate-etl/spate` → Settings → Social preview |
| `social-benchmark.png` (1280×640) | `spate-etl/benchmark` → Settings → Social preview |

The avatar is a full-bleed square with no corner radius of its own: GitHub
rounds it, and baking in a second radius double-rounds the corners. Its ink
fills 62% of the width, which keeps it clear of that rounding.

`lockup-light.png` and `lockup-dark.png` are for READMEs and slides. Pair them
behind a `<picture>` element so each theme gets the right one.
