import useBaseUrl from '@docusaurus/useBaseUrl';
import React from 'react';

type SpecimenProps = {
  src: string;
  alt: string;
  ground: 'light' | 'dark';
  caption: string;
  /** Largest image height in CSS pixels; a narrow frame scales the image down with its ratio kept. */
  height?: number;
};

/**
 * A brand asset on a fixed ground with its caption. `data-theme` pins the
 * ground's tokens, so a light asset sits on the light ground inside a dark page.
 */
export function Specimen({src, alt, ground, caption, height = 96}: SpecimenProps): React.JSX.Element {
  return (
    <figure data-theme={ground} className="brand-specimen">
      <img src={useBaseUrl(src)} alt={alt} style={{maxHeight: height}} />
      <figcaption>{caption}</figcaption>
    </figure>
  );
}

/** A wrapping row of specimens. */
export function Specimens({children}: {children: React.ReactNode}): React.JSX.Element {
  return <div className="brand-specimens">{children}</div>;
}
