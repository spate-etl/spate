import MDXContent from '@theme/MDXContent';
import React from 'react';

import SiteLayout from '../components/SiteLayout';
import Body from './_brand/body.mdx';

export default function BrandPage(): React.JSX.Element {
  return (
    <SiteLayout
      title="Brand"
      description="The Spate mark, wordmark lockups, color tokens and typefaces, with the files to download and the rules for using them.">
      <div className="site-container site-prose">
        <h1>Brand</h1>
        <MDXContent>
          <Body />
        </MDXContent>
      </div>
    </SiteLayout>
  );
}
