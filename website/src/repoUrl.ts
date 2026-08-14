/**
 * Where this repository lives, and the ref its source links point at.
 *
 * Both consumers read from here. `docusaurus.config.ts` builds the navbar,
 * footer and `editUrl` from these values, and `remark/repoLinks.ts` builds
 * every `repo:` link from them.
 *
 * `SOURCE_REF` is a branch rather than a tag or a commit, and it is the one
 * value here that is not checked off disk. A path is verified to exist; the
 * ref is taken on trust.
 */
export const organizationName = 'spate-etl';
export const projectName = 'spate';
export const githubUrl = `https://github.com/${organizationName}/${projectName}`;
export const SOURCE_REF = 'main';
