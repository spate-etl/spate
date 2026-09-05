import Head from '@docusaurus/Head';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import {usePluginData} from '@docusaurus/useGlobalData';
import React from 'react';

import SiteLayout from '../components/SiteLayout';
import {
  Benchmarks,
  Code,
  Connectors,
  Deploy,
  Facts,
  Faq,
  Hero,
  HowItWorks,
  Install,
  ProofStrip,
  Shapes,
  SUBLINE,
} from '../components/home/sections';
import {FAQ} from '../data/faq';
import {githubUrl} from '../repoUrl';

const TITLE = 'Spate: at-least-once streaming ETL framework for Rust';

export default function Home(): React.JSX.Element {
  const {siteConfig} = useDocusaurusContext();
  const proof = (usePluginData('social-proof') as {version?: string} | undefined) ?? {};
  const ld = [
    {
      '@context': 'https://schema.org',
      '@type': 'SoftwareSourceCode',
      name: 'Spate',
      description: SUBLINE,
      url: siteConfig.url,
      codeRepository: githubUrl,
      programmingLanguage: 'Rust',
      license: 'https://www.apache.org/licenses/LICENSE-2.0',
      ...(proof.version ? {version: proof.version} : {}),
    },
    {'@context': 'https://schema.org', '@type': 'WebSite', name: 'Spate', url: siteConfig.url},
    {
      '@context': 'https://schema.org',
      '@type': 'FAQPage',
      mainEntity: FAQ.map((f) => ({
        '@type': 'Question',
        name: f.q,
        acceptedAnswer: {'@type': 'Answer', text: f.a},
      })),
    },
  ];
  return (
    <SiteLayout title={TITLE} description={SUBLINE} exactTitle className="home">
      <Head>
        <script type="application/ld+json">{JSON.stringify(ld)}</script>
      </Head>
      <Hero />
      <ProofStrip />
      <Shapes />
      <HowItWorks />
      <Code />
      <Connectors />
      <Benchmarks />
      <Deploy />
      <Install />
      <Facts />
      <Faq />
    </SiteLayout>
  );
}
