import {useDoc} from '@docusaurus/plugin-content-docs/client';
import type {Props} from '@theme/DocItem/Content';
import Content from '@theme-original/DocItem/Content';
import React from 'react';

import Carrier from '../../../components/Carrier';
import {pageOrdinal} from '../../../components/Carrier/ordinal';

/** A doc's content, preceded by its page carrier when the page has an authored place in its category. */
export default function ContentWrapper(props: Props): React.JSX.Element {
  const ordinal = pageOrdinal(useDoc().metadata);
  return (
    <>
      {ordinal !== null && <Carrier ordinal={ordinal} className="page-carrier" />}
      <Content {...props} />
    </>
  );
}
