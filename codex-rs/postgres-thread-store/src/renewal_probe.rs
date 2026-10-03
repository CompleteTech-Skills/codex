//! Per-original-task test observation; never a production closure capability.

use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use tokio::sync::Notify;

#[derive(Default)]
pub(super) struct RenewalProbe {
    ready: AtomicBool,
    ready_event: Notify,
    dropping: AtomicBool,
    dropping_event: Notify,
    released: Mutex<bool>,
    release_event: Condvar,
    returned: AtomicBool,
}

impl RenewalProbe {
    pub(super) async fn ready(&self) {
        loop {
            let notified = self.ready_event.notified();
            if self.ready.load(Ordering::SeqCst) {
                return;
            }
            notified.await;
        }
    }

    pub(super) async fn dropping(&self) {
        loop {
            let notified = self.dropping_event.notified();
            if self.dropping.load(Ordering::SeqCst) {
                return;
            }
            notified.await;
        }
    }

    pub(super) fn release(&self) {
        *self
            .released
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = true;
        self.release_event.notify_all();
    }

    pub(super) fn returned(&self) -> bool {
        self.returned.load(Ordering::SeqCst)
    }
}

pub(super) struct OriginalRenewalExitHold(Option<Arc<RenewalProbe>>);

impl OriginalRenewalExitHold {
    pub(super) fn enter(probe: Option<Arc<RenewalProbe>>) -> Self {
        if let Some(probe) = probe.as_ref() {
            probe.ready.store(true, Ordering::SeqCst);
            probe.ready_event.notify_waiters();
        }
        Self(probe)
    }
}

impl Drop for OriginalRenewalExitHold {
    fn drop(&mut self) {
        if let Some(probe) = self.0.as_ref() {
            probe.dropping.store(true, Ordering::SeqCst);
            probe.dropping_event.notify_waiters();
            let mut released = probe
                .released
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            while !*released {
                released = probe
                    .release_event
                    .wait(released)
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
            }
            probe.returned.store(true, Ordering::SeqCst);
        }
    }
}
