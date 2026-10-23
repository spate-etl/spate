import MDXContent from '@theme/MDXContent';
import React from 'react';

import SiteLayout from '../components/SiteLayout';
import Body from './_privacy/body.mdx';

export default function PrivacyPage(): React.JSX.Element {
  return (
    <SiteLayout
      title="Privacy"
      description="What the site's analytics counts, what it does not, and how to opt out with Global Privacy Control or Do Not Track.">
      <div className="site-container site-prose">
        <h1>Privacy</h1>
        <MDXContent>
          <Body />
        </MDXContent>
      </div>
    </SiteLayout>
  );
}
