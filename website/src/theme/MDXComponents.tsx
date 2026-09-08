import React from 'react';

import useBrokenLinks from '@docusaurus/useBrokenLinks';
import {useAnchorTargetClassName} from '@docusaurus/theme-common';
import MDXComponents from '@theme-original/MDXComponents';

/**
 * A link target on something other than a heading, carrying the theme's offset
 * for the sticky navbar. A literal `<a id>` in Markdown compiles to an
 * intrinsic element, which the build's broken-anchor check never collects.
 */
function Anchor({id}: {id: string}): React.JSX.Element {
  useBrokenLinks().collectAnchor(id);
  return <a id={id} className={useAnchorTargetClassName(id)} />;
}

export default {...MDXComponents, Anchor};
