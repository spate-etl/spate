import React from 'react';

import SiteFooter from '@site/src/components/SiteFooter';

/** The docs pages take the site footer rather than the theme's configured one. */
export default function FooterWrapper(): React.JSX.Element {
  return <SiteFooter />;
}
