//! [`ScriptedQueue`]: a [`Queue`] the unit tests script, which records every
//! call the feed makes.

use crate::error::CallError;
use crate::queue::{
    BatchOutcome, BoxFuture, DeleteEntry, EntryFailure, Queue, ReceiveRequest, Received, SendEntry,
    VisibilityEntry,
};
use std::collections::VecDeque;
use std::sync::{Mutex, MutexGuard};
use std::time::Duration;

/// A message a receive hands out.
#[derive(Clone, Debug)]
pub(crate) struct Message {
    pub(crate) body: String,
    pub(crate) receive_count: u32,
}

impl Message {
    pub(crate) fn new(body: &str) -> Message {
        Message {
            body: body.to_owned(),
            receive_count: 1,
        }
    }

    pub(crate) fn redelivered(mut self, receive_count: u32) -> Message {
        self.receive_count = receive_count;
        self
    }
}

/// One scripted entry failure: the entry's index and how it failed.
#[derive(Clone, Debug)]
pub(crate) struct Failure {
    pub(crate) index: usize,
    pub(crate) code: &'static str,
    pub(crate) sender_fault: bool,
}

#[derive(Debug, Default)]
struct Script {
    ready: VecDeque<Message>,
    /// Answer each receive only once its wait has passed.
    end_of_poll: bool,
    receive_error: Option<CallError>,
    send_failures: VecDeque<Vec<Failure>>,
    visibility_failures: VecDeque<Vec<Failure>>,
    delete_failures: VecDeque<Vec<Failure>>,
    receives: Vec<ReceiveRequest>,
    sends: Vec<Vec<SendEntry>>,
    visibility: Vec<Vec<VisibilityEntry>>,
    deletes: Vec<Vec<DeleteEntry>>,
    next: u64,
}

/// A queue that answers from a script and records every call.
///
/// With nothing ready, a receive waits out its long poll in tokio time and
/// returns nothing. A batch applies every entry the script does not fail.
#[derive(Debug, Default)]
pub(crate) struct ScriptedQueue {
    script: Mutex<Script>,
}

impl ScriptedQueue {
    fn lock(&self) -> MutexGuard<'_, Script> {
        self.script.lock().unwrap()
    }

    pub(crate) fn ready(&self, message: Message) {
        self.lock().ready.push_back(message);
    }

    pub(crate) fn answer_at_end_of_poll(&self) {
        self.lock().end_of_poll = true;
    }

    pub(crate) fn fail_next_receive(&self, e: CallError) {
        self.lock().receive_error = Some(e);
    }

    pub(crate) fn fail_next_send(&self, failures: Vec<Failure>) {
        self.lock().send_failures.push_back(failures);
    }

    pub(crate) fn fail_next_visibility(&self, failures: Vec<Failure>) {
        self.lock().visibility_failures.push_back(failures);
    }

    pub(crate) fn fail_next_delete(&self, failures: Vec<Failure>) {
        self.lock().delete_failures.push_back(failures);
    }

    pub(crate) fn receives(&self) -> Vec<ReceiveRequest> {
        self.lock().receives.clone()
    }

    pub(crate) fn sends(&self) -> Vec<Vec<SendEntry>> {
        self.lock().sends.clone()
    }

    pub(crate) fn visibility(&self) -> Vec<Vec<VisibilityEntry>> {
        self.lock().visibility.clone()
    }

    pub(crate) fn deletes(&self) -> Vec<Vec<DeleteEntry>> {
        self.lock().deletes.clone()
    }

    /// Up to `max` messages.
    fn take(&self, max: usize) -> Vec<Received> {
        let mut script = self.lock();
        let mut out = Vec::new();
        while out.len() < max {
            let Some(message) = script.ready.pop_front() else {
                break;
            };
            let n = script.next;
            script.next += 1;
            out.push(Received {
                message_id: format!("m{n}"),
                handle: format!("h{n}"),
                body: message.body,
                unit_id: Some(format!("u{n}")),
                receive_count: Some(message.receive_count.to_string()),
            });
        }
        out
    }
}

/// Apply every entry but the scripted failures.
fn outcome(ids: impl Iterator<Item = String>, failures: Vec<Failure>) -> BatchOutcome {
    let mut out = BatchOutcome::default();
    for (i, id) in ids.enumerate() {
        match failures.iter().find(|f| f.index == i) {
            Some(f) => out.failed.push(EntryFailure {
                id,
                sender_fault: f.sender_fault,
                code: f.code.to_owned(),
                message: "scripted".to_owned(),
            }),
            None => out.successful.push(id),
        }
    }
    out
}

impl Queue for ScriptedQueue {
    fn receive(&self, request: ReceiveRequest) -> BoxFuture<'_, Result<Vec<Received>, CallError>> {
        Box::pin(async move {
            let (end_of_poll, error) = {
                let mut script = self.lock();
                script.receives.push(request);
                (script.end_of_poll, script.receive_error.take())
            };
            if let Some(e) = error {
                return Err(e);
            }
            let wait = Duration::from_secs(u64::try_from(request.wait).unwrap());
            if end_of_poll {
                tokio::time::sleep(wait).await;
            }
            let messages = self.take(usize::try_from(request.max).unwrap());
            if messages.is_empty() && !end_of_poll {
                tokio::time::sleep(wait).await;
            }
            Ok(messages)
        })
    }

    fn send_batch(
        &self,
        entries: Vec<SendEntry>,
    ) -> BoxFuture<'_, Result<BatchOutcome, CallError>> {
        Box::pin(async move {
            let mut script = self.lock();
            let failures = script.send_failures.pop_front().unwrap_or_default();
            let ids: Vec<String> = entries.iter().map(|e| e.id.clone()).collect();
            script.sends.push(entries);
            Ok(outcome(ids.into_iter(), failures))
        })
    }

    fn change_visibility_batch(
        &self,
        entries: Vec<VisibilityEntry>,
    ) -> BoxFuture<'_, Result<BatchOutcome, CallError>> {
        Box::pin(async move {
            let mut script = self.lock();
            let failures = script.visibility_failures.pop_front().unwrap_or_default();
            let ids: Vec<String> = entries.iter().map(|e| e.id.clone()).collect();
            script.visibility.push(entries);
            Ok(outcome(ids.into_iter(), failures))
        })
    }

    fn delete_batch(
        &self,
        entries: Vec<DeleteEntry>,
    ) -> BoxFuture<'_, Result<BatchOutcome, CallError>> {
        Box::pin(async move {
            let mut script = self.lock();
            let failures = script.delete_failures.pop_front().unwrap_or_default();
            let ids: Vec<String> = entries.iter().map(|e| e.id.clone()).collect();
            script.deletes.push(entries);
            Ok(outcome(ids.into_iter(), failures))
        })
    }
}
