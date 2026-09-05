import ErrorBoundary from '@docusaurus/ErrorBoundary';
import Head from '@docusaurus/Head';
import {PageMetadata, SkipToContentFallbackId, ThemeClassNames} from '@docusaurus/theme-common';
import ErrorPageContent from '@theme/ErrorPageContent';
import LayoutProvider from '@theme/Layout/Provider';
import Navbar from '@theme/Navbar';
import SkipToContent from '@theme/SkipToContent';
import clsx from 'clsx';
import React from 'react';

import SiteFooter from '../SiteFooter';
import MotionRoot from '../motion/MotionRoot';

type Props = {
  /** The page title. Formatted with the site name unless `exactTitle` is set. */
  title: string;
  description: string;
  /** Emit `title` as the whole document title and as `og:title`. */
  exactTitle?: boolean;
  className?: string;
  children: React.ReactNode;
};

/**
 * The layout of the marketing pages: the theme's navbar and the site footer
 * around a full-width main, inside the theme's providers so colour mode,
 * search and code blocks work as they do on a docs page. One navbar serves
 * every page; src/theme/Navbar/Content gives it the design's content.
 */
export default function SiteLayout({title, description, exactTitle, className, children}: Props): React.JSX.Element {
  return (
    <LayoutProvider>
      <PageMetadata title={exactTitle ? undefined : title} description={description} />
      {exactTitle && (
        <Head>
          <title>{title}</title>
          <meta property="og:title" content={title} />
        </Head>
      )}
      <MotionRoot />
      <SkipToContent />
      <Navbar />
      <div
        id={SkipToContentFallbackId}
        className={clsx(ThemeClassNames.layout.main.container, ThemeClassNames.wrapper.main, 'site-main', className)}>
        <ErrorBoundary fallback={(params) => <ErrorPageContent {...params} />}>
          <main>{children}</main>
        </ErrorBoundary>
      </div>
      <SiteFooter />
    </LayoutProvider>
  );
}
