import React from 'react';

/** The token table the brand page renders, one row per colour per ground. */
const TOKENS: Array<{name: string; role: string}> = [
  {name: 'bg', role: 'Page ground'},
  {name: 'surface', role: 'Cards and code'},
  {name: 'surface-2', role: 'Raised surface'},
  {name: 'ink', role: 'Text'},
  {name: 'muted', role: 'Secondary text'},
  {name: 'border', role: 'Rules and borders'},
  {name: 'accent', role: 'Links and controls'},
  {name: 'mark-node', role: 'Mark: sources and sink'},
  {name: 'mark-core', role: 'Mark: core'},
  {name: 'danger', role: 'Loses data'},
  {name: 'warning', role: 'Costs time'},
];

function Ground({ground}: {ground: 'light' | 'dark'}) {
  return (
    <div data-theme={ground} style={{background: 'var(--spate-bg)', color: 'var(--spate-ink)', border: '1px solid var(--spate-border)', borderRadius: 8, padding: '1rem'}}>
      <div style={{fontFamily: 'var(--ifm-font-family-monospace)', fontSize: '0.8rem', color: 'var(--spate-muted)', marginBottom: '0.75rem'}}>
        {ground} ground
      </div>
      <div style={{display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(140px, 1fr))', gap: '0.75rem'}}>
        {TOKENS.map((t) => (
          <div key={t.name} style={{display: 'flex', flexDirection: 'column', gap: '0.35rem'}}>
            <div aria-hidden="true" style={{height: 44, borderRadius: 6, background: `var(--spate-${t.name})`, border: '1px solid var(--spate-border)'}} />
            <div style={{fontFamily: 'var(--ifm-font-family-monospace)', fontSize: '0.75rem'}}>--spate-{t.name}</div>
            <div style={{fontSize: '0.8rem', color: 'var(--spate-muted)'}}>{t.role}</div>
          </div>
        ))}
      </div>
    </div>
  );
}

export default function BrandPalette(): React.JSX.Element {
  return (
    <div style={{display: 'grid', gap: '1rem'}}>
      <Ground ground="light" />
      <Ground ground="dark" />
    </div>
  );
}
