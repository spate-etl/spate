import Link from '@docusaurus/Link';
import useBaseUrl from '@docusaurus/useBaseUrl';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import {usePluginData} from '@docusaurus/useGlobalData';
import ThemedImage from '@theme/ThemedImage';
import React from 'react';

import {FOOTER_COLUMNS} from '../../data/nav';

type Proof = {downloads?: number; version?: string; asOf?: string};

/** The row of project figures: the release falls back to `0.x`, and the invariant and download counts appear only when their source published one. */
function Facts(): React.JSX.Element {
  const proof = (usePluginData('social-proof') as Proof | undefined) ?? {};
  const {siteConfig} = useDocusaurusContext();
  const invariants = siteConfig.customFields?.invariants;
  const facts: Array<[string, string]> = [
    [proof.version ?? '0.x', 'latest release'],
    ['Apache-2.0', 'license, no CLA'],
    ['1.94', 'MSRV, edition 2024'],
    ...(typeof invariants === 'number' ? [[String(invariants), 'numbered invariants'] as [string, string]] : []),
    ...(typeof proof.downloads === 'number'
      ? [[proof.downloads.toLocaleString('en-US'), 'crates.io downloads'] as [string, string]]
      : []),
  ];
  return (
    <div className="site-container site-footer__facts">
      <ul className="site-footer__facts-row" aria-label="Project facts">
        {facts.map(([n, l]) => (
          <li key={l}>
            <span className="site-footer__fact-n">{n}</span>
            <span className="site-footer__fact-l">{l}</span>
          </li>
        ))}
      </ul>
      {proof.asOf && <p className="site-footer__asof">Figures as of {proof.asOf}.</p>}
    </div>
  );
}

/** The site's footer: the brand lockup and tagline beside the columns from `FOOTER_COLUMNS`, above the project figures. */
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
      <Facts />
      <div className="site-container site-footer__legal">
        <span>Copyright © {new Date().getFullYear()} Marcus Kainth. Spate is licensed under Apache-2.0.</span>
      </div>
    </footer>
  );
}
