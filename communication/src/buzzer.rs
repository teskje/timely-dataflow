//! A type that can unpark specific threads.

use std::sync::Arc;

use tokio::sync::Notify;

use crate::current_task_notify;

/// Can unpark a specific thread.
#[derive(Clone)]
pub struct Buzzer {
    notify: Arc<Notify>,
}

impl Default for Buzzer {
    fn default() -> Self { Self { notify: current_task_notify() } }
}

impl Buzzer {
    /// Unparks the target thread.
    pub fn buzz(&self) {
        self.notify.notify_one()
    }
}
