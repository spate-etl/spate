//! The documentation site, which is a Node toolchain under `website/`.

use std::path::Path;

use crate::run::{self, Outcome, Step};

pub(crate) fn dispatch(root: &Path, explain: bool, serve: bool) -> Outcome {
    if serve {
        return run::run(root, explain, &Step::new("npm", ["start"]).dir("website"));
    }
    run::steps(
        root,
        explain,
        &[
            Step::new("npm", ["ci"]).dir("website"),
            Step::new("npm", ["run", "typecheck"]).dir("website"),
            Step::new("npm", ["test"]).dir("website"),
            // The client-redirects plugin only registers under `CI`, so a build
            // without it skips redirect validation and a broken redirect reaches
            // the deployed site.
            Step::new("npm", ["run", "build"])
                .env("CI", "true")
                .dir("website"),
        ],
    )
}
