//! Allocation snapshot of the sink writer's batch preparation.
//!
//! Each case prepares the same batch the counted tier in
//! `prepare_gungraun.rs` measures, under the same case names, and prints its
//! allocation, reallocation and byte counts for `cargo xtask bench alloc` to
//! compare with `benches/alloc/`. Harness-free, so every case runs on the main
//! thread and nothing else allocates while one is counted. Each case is
//! counted once after one uncounted pass.
//!
//! librdkafka allocates each header list through C `malloc`, which the
//! counting allocator never sees; the counted tier's DHAT rows include it.

use spate_test_support::alloc::{Counting, main_with};

#[expect(dead_code, reason = "only the rigs that frame a batch are used here")]
#[path = "support/encode_rig.rs"]
mod encode_rig;
#[path = "support/messages.rs"]
mod messages;
#[path = "support/prepare_rig.rs"]
mod prepare_rig;

use prepare_rig::{keyed, keyless, prepare_run, stamped};

#[global_allocator]
static ALLOC: Counting = Counting;

fn main() {
    main_with(|report| {
        for (name, batch) in [
            ("keyless", keyless()),
            ("keyed", keyed()),
            ("stamped_headers", stamped()),
        ] {
            report.case(name, || {
                assert_eq!(prepare_run(&batch), batch.expected, "counts moved");
            });
        }
    });
}
