import Link from '@docusaurus/Link';
import useBaseUrl from '@docusaurus/useBaseUrl';
import ThemedImage from '@theme/ThemedImage';
import React from 'react';

import {FOOTER_COLUMNS} from '../../data/nav';

/** The site's footer: the brand lockup and tagline beside the columns from `FOOTER_COLUMNS`. */
export default function SiteFooter(): React.JSX.Element {
  const light = useBaseUrl('/img/brand/lockup-light.svg');
  const dark = useBaseUrl('/img/brand/lockup-dark.svg');
  return (
    <footer className="site-footer">
      <div className="site-container site-footer__grid">
        <div className="site-footer__brand">
          <Link to="/" className="site-footer__lockup">
            <ThemedImage sources={{light, dark}} alt="Spate" height={30} />
          </Link>
          <p className="site-footer__tagline">
            /speɪt/ · a river in sudden flood. At-least-once streaming ETL for Rust.
          </p>
        </div>
        {FOOTER_COLUMNS.map((col) => (
          <nav key={col.title} className="site-footer__col" aria-label={col.title}>
            <h2 className="site-footer__title">{col.title}</h2>
            <ul>
              {col.items.map((item) => (
                <li key={item.label}>
                  {item.href ? (
                    <Link href={item.href}>{item.label}</Link>
                  ) : (
                    <Link to={item.to}>{item.label}</Link>
                  )}
                </li>
              ))}
            </ul>
          </nav>
        ))}
      </div>
      <div className="site-container site-footer__legal">
        <span>Copyright © {new Date().getFullYear()} Marcus Kainth. Spate is licensed under Apache-2.0.</span>
      </div>
    </footer>
  );
}
