# Brand assets

The Spate wordmark and icon, and the images uploaded to GitHub by hand.

Everything in `website/static/img/brand/`, plus `logo.svg`, `logo-dark.svg`,
`favicon.svg` and `apple-touch-icon.png` in `website/static/img/`,
`website/static/favicon.ico`, the slide deck in `website/static/brand/`, and
the colour tokens in
`website/src/css/brand.css`, is **generated**. Change
[`brandgen.py`](brandgen.py) and re-run, rather than editing an asset:

```sh
./website/tools/brand/generate.sh
```

The script creates its own virtualenv, installs the versions pinned in
[`requirements.txt`](requirements.txt), fetches the typeface, writes the SVG
sources, then rasterises the PNGs. It needs `python3`, plus `resvg` and `oxipng`
for the raster step (`brew install resvg oxipng`). Without those it writes the
SVGs, names what it could not refresh, and exits non-zero.

## The mark

The mark is the word `spate` in IBM Plex Sans SemiBold, cut once by a swell.
The letters above the water are ink, the letters below it take the accent, and
the channel between them is ground. [`waterline.py`](waterline.py) builds the
artwork. The wordmark is every glyph's pieces; the icon is the `s` alone,
framed on a 32-unit canvas with its ink 24.32 units tall and centred.

The wordmark's viewBox is cropped to its ink, so a CSS height sets the height of
the ink. Use it at 96 px wide or more, which a 35 px height gives with 2 px to
spare; below that, use the icon.

The palette is the `TOKENS` and `RAMP` tables in `brandgen.py`, written to
[`src/css/brand.css`](../../src/css/brand.css) as `--spate-*` custom properties
and mapped onto Infima by [`src/css/custom.css`](../../src/css/custom.css).
A change to a colour is measured by hand against its floor, 4.5:1 for text and
3:1 for graphics; the site's accessibility sweep covers the text pairs on the
routes it visits and no graphic pair anywhere.

## Typeface

The wordmark is IBM Plex Sans, SemiBold at −0.022 em for `spate` and Regular at
−0.012 em for a second word. The large numerals, or carriers, on the social card
are Overpass Mono SemiBold at −0.055 em, cut by a channel of two cubics at 0.72
of their ink height, with no blunting. Glyphs are shaped with HarfBuzz and baked to
outlines, so no asset carries a font dependency and the repository never
redistributes font software. The site sets its text in the same family and its
code in Overpass Mono, served from the `@fontsource/ibm-plex-sans` and
`@fontsource-variable/overpass-mono` packages (SIL Open Font License 1.1) at
build time; those files are a dependency, not part of the repository.

`brandgen.py` fetches both upstream files from `google/fonts` and pins each by
SHA-256. A digest mismatch stops the run, since the artwork would otherwise be
re-cut from a different source.

## What goes where

The site picks these up from the config. Nothing to do by hand:

| Asset | Used by |
| --- | --- |
| `img/brand/wordmark.svg`, `img/brand/wordmark-dark.svg` | Navbar, via `logo.src` / `logo.srcDark`, and the footer |
| `img/logo.svg`, `img/logo-dark.svg` | The icon, for pages that set it beside their own text |
| `img/brand/*-mono.svg`, `img/brand/*-mono-dark.svg` | Grayscale output, with both portions in ink |
| `img/favicon.svg`, `favicon.ico`, `img/apple-touch-icon.png` | Browser tab, search-result thumbnails and the iOS home screen, via `favicon` and `headTags` |
| `img/brand/social-spate.png` | Open Graph card, via `themeConfig.image` |
| `img/brand/not-found.svg`, `img/brand/not-found-dark.svg` | The 404 page's `404` carrier, via `src/theme/NotFound/Content` |
| `img/brand/misuse-*.svg` | The brand page's misuse gallery, captioned on the page |
| `src/css/brand.css` | Every colour on the site, through `custom.css` |

These three are uploaded by hand. GitHub exposes no REST endpoint for either
kind, so there is nothing to script:

| Asset | Where it goes |
| --- | --- |
| `avatar.png` (512×512) | `github.com/organizations/spate-etl/settings/profile` → Profile picture |
| `social-spate.png` (1280×640) | `spate-etl/spate` → Settings → Social preview |
| `social-benchmark.png` (1280×640) | `spate-etl/benchmark` → Settings → Social preview |

The avatar and the touch icon are full-bleed squares on the dark ground with no
corner radius of their own: the platform rounds them, and a baked-in radius
double-rounds the corners.

`brand/spate-slides.pptx` is the starter deck [`slides.py`](slides.py) writes:
title, section, content, code and closing slides on both grounds. Its text
boxes name IBM Plex Sans and Overpass Mono, Regular and Medium, as the static
cuts from IBM's `IBM/plex` release and from Google Fonts install them. Reruns
write the same bytes.

`wordmark.png` and `wordmark-dark.png` are for READMEs and anywhere else that
takes no SVG. Pair them behind a `<picture>` element so each theme gets the
right one.
