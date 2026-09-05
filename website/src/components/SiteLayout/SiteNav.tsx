import Link from '@docusaurus/Link';
import useBaseUrl from '@docusaurus/useBaseUrl';
import {usePluginData} from '@docusaurus/useGlobalData';
import NavbarColorModeToggle from '@theme/Navbar/ColorModeToggle';
import SearchBar from '@theme/SearchBar';
import ThemedImage from '@theme/ThemedImage';
import clsx from 'clsx';
import React, {useEffect, useRef, useState} from 'react';

import {NAV_ITEMS} from '../../data/nav';
import {githubUrl} from '../../repoUrl';

type SocialProof = {stars?: number};

const GitHubIcon = () => (
  <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" aria-hidden="true">
    <path d="M9 19c-4.3 1.4-4.3-2.5-6-3m12 5v-3.5c0-1 .1-1.4-.5-2 2.8-.3 5.5-1.4 5.5-6a4.6 4.6 0 0 0-1.3-3.2 4.2 4.2 0 0 0-.1-3.2s-1.1-.3-3.5 1.3a12.3 12.3 0 0 0-6.2 0C6.5 2.8 5.4 3.1 5.4 3.1a4.2 4.2 0 0 0-.1 3.2A4.6 4.6 0 0 0 4 9.5c0 4.6 2.7 5.7 5.5 6-.6.6-.6 1.2-.5 2V21" />
  </svg>
);

const StarIcon = () => (
  <svg width="13" height="13" viewBox="0 0 24 24" fill="currentColor" aria-hidden="true">
    <path d="M12 2.5l2.9 6 6.6.9-4.8 4.6 1.2 6.5L12 17.4 6.1 20.5l1.2-6.5L2.5 9.4l6.6-.9z" />
  </svg>
);

const MenuIcon = () => (
  <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" aria-hidden="true">
    <path d="M4 7h16M4 12h16M4 17h16" />
  </svg>
);

/**
 * The marketing navbar: translucent over the hero, solid once the page has
 * scrolled, and a native dialog for the phone menu, which traps focus and
 * closes on Escape without script of its own.
 */
export default function SiteNav(): React.JSX.Element {
  const [solid, setSolid] = useState(false);
  const sheet = useRef<HTMLDialogElement>(null);
  const proof = usePluginData('social-proof') as SocialProof | undefined;
  const light = useBaseUrl('/img/brand/lockup-light.svg');
  const dark = useBaseUrl('/img/brand/lockup-dark.svg');

  useEffect(() => {
    const onScroll = () => setSolid(window.scrollY > 8);
    onScroll();
    window.addEventListener('scroll', onScroll, {passive: true});
    return () => window.removeEventListener('scroll', onScroll);
  }, []);

  const github = (
    <Link href={githubUrl} className="site-nav__github">
      <GitHubIcon />
      <span>GitHub</span>
      {typeof proof?.stars === 'number' && (
        <span className="site-nav__stars">
          <StarIcon />
          {proof.stars.toLocaleString('en-US')}
        </span>
      )}
    </Link>
  );

  return (
    <header className={clsx('site-nav', solid && 'site-nav--solid')}>
      <nav className="site-container site-nav__inner" aria-label="Main">
        <Link to="/" className="site-nav__brand">
          <ThemedImage sources={{light, dark}} alt="Spate" height={28} />
        </Link>
        <ul className="site-nav__links">
          {NAV_ITEMS.map((item) => (
            <li key={item.to}>
              <Link to={item.to}>{item.label}</Link>
            </li>
          ))}
        </ul>
        <div className="site-nav__tools">
          <SearchBar />
          {github}
          <NavbarColorModeToggle className="site-nav__mode" />
          <button
            type="button"
            className="site-nav__menu"
            aria-label="Open menu"
            onClick={() => sheet.current?.showModal()}>
            <MenuIcon />
          </button>
        </div>
      </nav>
      <dialog ref={sheet} className="site-nav__sheet" aria-label="Menu">
        <div className="site-nav__sheet-head">
          <ThemedImage sources={{light, dark}} alt="Spate" height={26} />
          <button type="button" className="site-nav__close" aria-label="Close menu" onClick={() => sheet.current?.close()}>
            ×
          </button>
        </div>
        <ul className="site-nav__sheet-links">
          {NAV_ITEMS.map((item) => (
            <li key={item.to}>
              <Link to={item.to}>{item.label}</Link>
            </li>
          ))}
          <li>
            <Link href={githubUrl}>GitHub</Link>
          </li>
        </ul>
        <NavbarColorModeToggle className="site-nav__mode" />
      </dialog>
    </header>
  );
}
