import Link from '@docusaurus/Link';
import useBaseUrl from '@docusaurus/useBaseUrl';
import type {Props} from '@theme/NotFound/Content';
import ThemedImage from '@theme/ThemedImage';
import clsx from 'clsx';
import React from 'react';

/** The 404 page: the `404` carrier beside the status, one line of explanation and two ways back. */
export default function NotFoundContent({className}: Props): React.JSX.Element {
  const light = useBaseUrl('/img/brand/not-found.svg');
  const dark = useBaseUrl('/img/brand/not-found-dark.svg');
  return (
    <main className={clsx('site-main', className)}>
      <div className="site-container not-found">
        <div className="not-found__art" aria-hidden="true">
          <ThemedImage sources={{light, dark}} alt="" width={697} height={345} />
        </div>
        <div className="not-found__copy">
          <p className="not-found__eyebrow">Page not found</p>
          <h1 className="not-found__title">No page at this address.</h1>
          <p className="home-lead">Check the URL, or return to the guide.</p>
          <div className="not-found__actions">
            <Link className="home-btn home-btn--primary" to="/docs/user-guide">
              Open documentation
            </Link>
            <Link className="not-found__home" to="/">
              Go to homepage
            </Link>
          </div>
        </div>
      </div>
    </main>
  );
}
