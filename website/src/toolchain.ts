import useDocusaurusContext from '@docusaurus/useDocusaurusContext';

/** The MSRV and edition the site config reads off the workspace manifest. */
export function useToolchain(): {msrv: string; edition: string} {
  const {siteConfig} = useDocusaurusContext();
  return {
    msrv: String(siteConfig.customFields?.msrv),
    edition: String(siteConfig.customFields?.edition),
  };
}
