import React from 'react';

import {useDoc} from '@docusaurus/plugin-content-docs/client';
import useBrokenLinks from '@docusaurus/useBrokenLinks';
import {useAnchorTargetClassName} from '@docusaurus/theme-common';
import MDXComponents from '@theme-original/MDXComponents';

import Carrier from '../components/Carrier';
import {pageOrdinal, sectionOrdinal} from '../components/Carrier/ordinal';
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

/**
 * The ordinal of the H2 `heading`, set on its own line directly above that
 * heading. Docs pages only; the build fails on an id the page has no H2 for.
 */
function SectionCarrier({heading}: {heading: string}): React.JSX.Element {
  const {metadata, toc} = useDoc();
  const ordinal = sectionOrdinal(toc, heading, pageOrdinal(metadata) !== null);
  return <Carrier ordinal={ordinal} className="section-carrier" />;
}

/** The MSRV `Cargo.toml` declares. */
function Msrv(): React.JSX.Element {
  return <>{useToolchain().msrv}</>;
}

/** The edition `Cargo.toml` declares. */
function Edition(): React.JSX.Element {
  return <>{useToolchain().edition}</>;
}

export default {...MDXComponents, Anchor, SectionCarrier, Msrv, Edition, table: MarkdownTable};
