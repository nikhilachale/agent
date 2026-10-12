use std::io;
use std::process::ExitStatus;
use tokio::process::Child;

/// Owns one foreground command, never the persistent MCP server's group.
/// On Unix the unreaped leader pins the PGID until the last group signal.
/// Descendants that deliberately leave this group are outside this boundary.
pub(crate) struct ForegroundCommand {
    child: Option<Child>,
}

impl ForegroundCommand {
    pub(crate) fn new(child: Child) -> Self {
        Self { child: Some(child) }
    }

    pub(crate) fn child_mut(&mut self) -> &mut Child {
        // The child is taken only by Drop, after all methods have returned.
        #[allow(clippy::expect_used)]
        self.child.as_mut().expect("foreground child is owned")
    }

    #[cfg(unix)]
    fn kill_group(&mut self) -> io::Result<()> {
        if let Some(pid) = self.child_mut().id() {
            // SAFETY: process_group(0) made the owned, unreaped child its group
            // leader. No wait/try_wait may reap it before this signal.
            if unsafe { libc::kill(-(pid as libc::pid_t), libc::SIGKILL) } == -1 {
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
        self.child_mut().start_kill()?;
        self.child_mut().wait().await
    }

    #[cfg(unix)]
    pub(crate) fn has_exited(&mut self) -> io::Result<bool> {
        let Some(pid) = self.child_mut().id() else {
            return Ok(true);
        };
        let mut info = std::mem::MaybeUninit::<libc::siginfo_t>::zeroed();
        // SAFETY: info is writable. WNOWAIT preserves the owned leader's PID
        // until kill_group finishes, including when it has become a zombie.
        let rc = unsafe {
            libc::waitid(
                libc::P_PID,
                pid as libc::id_t,
                info.as_mut_ptr(),
                libc::WEXITED | libc::WNOHANG | libc::WNOWAIT,
            )
        };
        if rc == -1 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: successful waitid initializes info; si_pid is zero when
        // WNOHANG finds no waitable child.
        Ok(unsafe { info.assume_init().si_pid() } != 0)
    }

    pub(crate) async fn wait(&mut self) -> io::Result<ExitStatus> {
        #[cfg(unix)]
        {
            while !self.has_exited()? {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
            self.kill_group()?;
        }
        self.child_mut().wait().await
    }

}

impl Drop for ForegroundCommand {
    fn drop(&mut self) {
        #[cfg(unix)]
        let _ = self.kill_group();
        if let Some(mut child) = self.child.take() {
            let _ = child.start_kill();
            // Reaping survives an aborted request future. Runtime shutdown is
            // still responsible for driving Tokio's process reaper.
            if let Ok(runtime) = tokio::runtime::Handle::try_current() {
                runtime.spawn(async move {
                    let _ = child.wait().await;
                });
            }
        }
    }
}
