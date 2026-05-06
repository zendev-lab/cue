use std::sync::Arc;

use anyhow::Result;

type RestartFn = dyn Fn() -> Result<()> + Send + Sync + 'static;

/// Cloneable transport-level restart capability for frontends.
#[derive(Clone)]
pub struct RestartHandle {
    restart: Arc<RestartFn>,
}

impl RestartHandle {
    pub fn new<F>(restart: F) -> Self
    where
        F: Fn() -> Result<()> + Send + Sync + 'static,
    {
        Self {
            restart: Arc::new(restart),
        }
    }

    pub fn restart(&self) -> Result<()> {
        (self.restart)()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    #[test]
    fn restart_handle_invokes_wrapped_action() {
        let calls = Arc::new(AtomicUsize::new(0));
        let shared = Arc::clone(&calls);
        let handle = RestartHandle::new(move || {
            shared.fetch_add(1, Ordering::SeqCst);
            Ok(())
        });

        handle.restart().expect("restart should succeed");
        handle.restart().expect("restart should succeed");

        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }
}
