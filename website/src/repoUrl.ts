/**
 * Where this repository lives, and the ref its source links point at.
 *
 * One home for the four values, because two of them are consumed by parts of
 * the site that never see each other: `docusaurus.config.ts` builds the navbar,
 * footer and `editUrl` from them, and `remark/repoLinks.ts` builds every
 * `repo:` link from them. A page offering to edit a file at one ref while
 * linking its source at another is the failure this file exists to make
 * impossible.
 *
 * `SOURCE_REF` is a branch rather than a tag or a commit: the site is built
 * from the tree it documents and published from the same commit, so a reader
 * following a source link should land on what the page describes. It is also
 * the only value here that cannot be checked off disk — a path is verified to
 * exist, the ref is taken on trust.
 */
export const organizationName = 'spate-etl';
export const projectName = 'spate';
export const githubUrl = `https://github.com/${organizationName}/${projectName}`;
export const SOURCE_REF = 'main';
