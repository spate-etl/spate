//! The documentation site, which is a Node toolchain under `website/`.

use std::path::Path;

use crate::run::{self, Outcome, Step};

pub(crate) fn dispatch(root: &Path, explain: bool, serve: bool) -> Outcome {
    if serve {
        return run::run(root, explain, &Step::new("npm", ["start"]).dir("website"));
    }
    run::steps(root, explain, &docs_steps())
}

/// The site's checks and build, then the check of what the build wrote.
fn docs_steps() -> Vec<Step<'static>> {
    vec![
        Step::new("npm", ["ci"]).dir("website"),
        Step::new("npm", ["run", "typecheck"]).dir("website"),
        Step::new("npm", ["test"]).dir("website"),
        // The client-redirects plugin only registers under `CI`, so a build
        // without it skips redirect validation and a broken redirect reaches
        // the deployed site.
        Step::new("npm", ["run", "build"])
            .env("CI", "true")
            .dir("website"),
        Step::new("cargo", ["xtask", "site-check"]),
    ]
}

#[cfg(test)]
mod tests {
    /// The site check reads what the build wrote, so it runs last.
    #[test]
    fn the_site_check_runs_after_the_build() {
        let steps: Vec<String> = super::docs_steps()
            .iter()
            .map(super::Step::display)
            .collect();
        assert_eq!(
            steps[steps.len() - 2..],
            [
                "(cd website && CI=\"true\" npm run build)",
                "cargo xtask site-check"
            ]
        );
    }
}
