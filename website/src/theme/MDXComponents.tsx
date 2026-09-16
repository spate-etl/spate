import React from 'react';

import useBrokenLinks from '@docusaurus/useBrokenLinks';
import {useAnchorTargetClassName} from '@docusaurus/theme-common';
import MDXComponents from '@theme-original/MDXComponents';

import MarkdownTable from '../components/MarkdownTable';
import {useToolchain} from '../toolchain';

/**
 * A link target on something other than a heading, carrying the theme's offset
 * for the sticky navbar. A literal `<a id>` in Markdown compiles to an
 * intrinsic element, which the build's broken-anchor check never collects.
 */
function Anchor({id}: {id: string}): React.JSX.Element {
  useBrokenLinks().collectAnchor(id);
  return <a id={id} className={useAnchorTargetClassName(id)} />;
}

/** The MSRV `Cargo.toml` declares. */
function Msrv(): React.JSX.Element {
  return <>{useToolchain().msrv}</>;
}

/** The edition `Cargo.toml` declares. */
function Edition(): React.JSX.Element {
  return <>{useToolchain().edition}</>;
}

export default {...MDXComponents, Anchor, Msrv, Edition, table: MarkdownTable};
