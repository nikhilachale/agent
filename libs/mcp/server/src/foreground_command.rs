use std::io;
use std::process::ExitStatus;
use tokio::process::Child;

/// Owns a foreground child's group until the child is reaped. Commands that
/// deliberately leave this group are outside this cancellation boundary.
pub(crate) struct ForegroundCommand {
    child: Child,
}

impl ForegroundCommand {
    pub(crate) fn new(child: Child) -> Self {
        Self { child }
    }

    pub(crate) fn child_mut(&mut self) -> &mut Child {
        &mut self.child
    }

    #[cfg(unix)]
    fn kill_group(&mut self) -> io::Result<()> {
        if let Some(pid) = self.child.id() {
            let pid = libc::pid_t::try_from(pid)
                .map_err(|_| io::Error::other("invalid foreground process ID"))?;
            if pid <= 1 {
                return Err(io::Error::other("invalid foreground process group"));
            }
            // SAFETY: process_group(0) made this owned child the group leader.
            // Child::id() becomes None after wait/try_wait successfully reaps it;
            // while unreaped (even a zombie), its PID cannot be reused.
            if unsafe { libc::kill(-pid, libc::SIGKILL) } == -1 {
                let error = io::Error::last_os_error();
                if error.raw_os_error() != Some(libc::ESRCH) {
                    return Err(error);
                }
            }
        }
        Ok(())
    }

    pub(crate) async fn terminate(&mut self) -> io::Result<ExitStatus> {
        #[cfg(unix)]
        self.kill_group()?;
        #[cfg(not(unix))]
        self.child.start_kill()?;
        tokio::time::timeout(std::time::Duration::from_secs(5), self.child.wait())
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "foreground cleanup timed out"))?
    }
}

impl Drop for ForegroundCommand {
    fn drop(&mut self) {
        #[cfg(unix)]
        if let Err(error) = self.kill_group() {
            tracing::error!("Failed to kill foreground command group: {error}");
        }
        // The child was spawned with kill_on_drop(true). Tokio handles direct
        // child fallback/reaping; group signalling must happen synchronously
        // first, including when a request future is aborted or dropped.
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use tokio::process::Command;

    fn successful_child() -> ForegroundCommand {
        ForegroundCommand::new(Command::new("sh")
            .arg("-c").arg("exit 7").process_group(0)
            .kill_on_drop(true).spawn().unwrap())
    }

    #[tokio::test]
    async fn foreground_wait_disarms_group_after_reap() {
        let mut owned = successful_child();
        assert_eq!(owned.child_mut().wait().await.unwrap().code(), Some(7));
        assert!(owned.child_mut().id().is_none());
        // No numeric authority remains even if Drop is delayed.
        owned.kill_group().unwrap();
        drop(owned);
    }

    #[tokio::test]
    async fn foreground_try_wait_disarms_group_after_reap() {
        let mut owned = successful_child();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if let Some(status) = owned.child_mut().try_wait().unwrap() {
                    assert_eq!(status.code(), Some(7));
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        }).await.unwrap();
        assert!(owned.child_mut().id().is_none());
        owned.kill_group().unwrap();
        drop(owned);
    }
}
