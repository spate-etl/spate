//! Amazon SQS work-feed backend for Spate.
//!
//! [`SqsFeed`] is a [`Feed`](spate_core::feed::Feed) over one SQS standard
//! queue. A pipeline names it in its `feed:` section, `main()` builds it from
//! that section with [`SqsFeed::connect`], and passes it to
//! [`Pipeline::feed`](spate_core::pipeline::Pipeline::feed):
//!
//! ```yaml
//! feed:
//!   sqs:
//!     queue_url: "https://sqs.eu-west-2.amazonaws.com/111122223333/jobs"
//!     visibility: 60s
//! ```
//!
//! ```no_run
//! use spate_core::pipeline::Pipeline;
//! use spate_core::config::PipelineConfig;
//! use spate_sqs::SqsFeed;
//! use std::sync::Arc;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let pipeline = Pipeline::from_config(PipelineConfig::from_path("pipeline.yaml".as_ref())?)?;
//! let section = pipeline.config().feed.clone().expect("the file names a feed");
//! let feed = SqsFeed::connect(&section, &pipeline.io_handle())?;
//! let pipeline = pipeline.feed(Arc::new(feed));
//! # Ok(()) }
//! ```
//!
//! # Delivery
//!
//! - A received unit stays hidden from other readers for the visibility
//!   period, and the feed extends it every third of that period while the
//!   reader renews it. A unit the feed has received and not yet handed out
//!   is extended while the reader keeps calling the feed. A reader that stops
//!   renewing loses its units at most one visibility period after their last
//!   extension, and SQS hands them to another reader.
//! - A unit is deleted only when the reader acknowledges it. A failed unit is
//!   made visible at once, and the queue's redrive policy moves it to the
//!   dead-letter queue after `maxReceiveCount` receives. The feed never sends
//!   to the dead-letter queue itself.
//! - SQS keeps a message hidden for at most 12 hours from the receive that
//!   issued its receipt handle, and the feed clamps every extension to that
//!   hold.
//!
//! No AWS SDK type appears in this crate's public API.

mod client;
mod config;
mod error;
mod feed;
mod queue;
#[cfg(test)]
mod scripted;
mod startup;

pub use error::SqsError;
pub use feed::SqsFeed;
