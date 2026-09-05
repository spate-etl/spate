import Link from '@docusaurus/Link';
import {useNavbarMobileSidebar} from '@docusaurus/theme-common/internal';
import {usePluginData} from '@docusaurus/useGlobalData';
import NavbarColorModeToggle from '@theme/Navbar/ColorModeToggle';
import NavbarLogo from '@theme/Navbar/Logo';
import NavbarMobileSidebarToggle from '@theme/Navbar/MobileSidebar/Toggle';
import NavbarSearch from '@theme/Navbar/Search';
import SearchBar from '@theme/SearchBar';
import React, {useEffect} from 'react';

import {NAV_ITEMS} from '@site/src/data/nav';
import {githubUrl} from '@site/src/repoUrl';

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

/**
 * The one navbar, on the docs pages and the marketing pages alike: the
 * lockup, the primary links from src/data/nav.ts, search, GitHub with the
 * star count, and the colour-mode toggle. The theme's layout around it keeps
 * the mobile sidebar, which is where the docs tree lives on a phone.
 *
 * The swizzle CLI marks this component unsafe, so a theme upgrade that
 * changes the mobile sidebar toggle or the logo component needs a manual
 * check here rather than an automatic merge.
 */
export default function NavbarContent(): React.JSX.Element {
  const mobileSidebar = useNavbarMobileSidebar();
  const proof = usePluginData('social-proof') as SocialProof | undefined;

  // Translucent over the top of a page, solid once scrolled.
  useEffect(() => {
    const nav = document.querySelector('.navbar');
    if (!nav) return undefined;
    const onScroll = () => nav.toggleAttribute('data-scrolled', window.scrollY > 8);
    onScroll();
    window.addEventListener('scroll', onScroll, {passive: true});
    return () => window.removeEventListener('scroll', onScroll);
  }, []);

  return (
    <div className="navbar__inner">
      <div className="navbar__items">
        {!mobileSidebar.disabled && <NavbarMobileSidebarToggle />}
        <NavbarLogo />
        <ul className="site-nav__links">
          {NAV_ITEMS.map((item) => (
            <li key={item.to}>
              <Link to={item.to} className="site-nav__link" activeClassName="site-nav__link--active">
                {item.label}
              </Link>
            </li>
          ))}
        </ul>
      </div>
      <div className="navbar__items navbar__items--right">
        <NavbarSearch>
          <SearchBar />
        </NavbarSearch>
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
        <NavbarColorModeToggle className="site-nav__mode" />
      </div>
    </div>
  );
}
