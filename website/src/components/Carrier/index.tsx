import useBaseUrl from '@docusaurus/useBaseUrl';
import ThemedImage from '@theme/ThemedImage';
import clsx from 'clsx';
import React from 'react';

import {numeral} from './ordinal';

/** A documentation ordinal cut by the water, hidden from assistive technology; the class sets its size. */
export default function Carrier({ordinal, className}: {ordinal: number; className: string}): React.JSX.Element {
  const value = numeral(ordinal);
  const light = useBaseUrl(`/img/brand/carrier-${value}.svg`);
  const dark = useBaseUrl(`/img/brand/carrier-${value}-dark.svg`);
  return (
    <div className={clsx('carrier', className)} aria-hidden="true">
      <ThemedImage sources={{light, dark}} alt="" width={181} height={166} />
    </div>
  );
}
