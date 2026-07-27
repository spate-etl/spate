//! Configuration errors.

use std::path::PathBuf;

/// Why a pipeline configuration could not be loaded or is invalid.
///
/// Every variant carries enough context to act on without opening the
/// source code: file paths, YAML paths (`source.kafka.brokers`), or
/// line/column positions.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ConfigError {
    /// The configuration file could not be read.
    #[error("failed to read config file `{path}`: {source}")]
    Io {
        /// File that could not be read.
        path: PathBuf,
        /// Underlying I/O error.
        #[source]
        source: std::io::Error,
    },

    /// `${VAR}` environment interpolation failed before parsing.
    #[error("environment interpolation failed at line {line}, column {column}: {reason}")]
    Interpolation {
        /// 1-based line of the offending `$`.
        line: usize,
        /// 1-based byte column of the offending `$`.
        column: usize,
        /// What went wrong (undefined variable, malformed syntax, ...).
        reason: String,
    },

    /// The YAML did not match the expected schema.
    #[error("invalid configuration at `{path}`: {source}")]
    Parse {
        /// Dotted YAML path to the offending value (best effort; `.` when
        /// the error is not attributable to a specific field).
        path: String,
        /// Underlying YAML/serde error.
        #[source]
        source: serde_yaml::Error,
    },

    /// A component section (`source:`, `sink:`, `deserializer:`) is not a
    /// single-key mapping, or its opaque body failed to deserialize into
    /// the component's typed config.
    #[error("{context}: {message}")]
    Component {
        /// Where in the config the problem is, e.g. `source.kafka.brokers`.
        context: String,
        /// What went wrong.
        message: String,
    },

    /// The YAML parsed but violates a cross-field rule.
    #[error("invalid configuration: {0}")]
    Validation(String),
}
