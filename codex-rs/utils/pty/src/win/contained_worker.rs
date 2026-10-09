use super::JobObject;
use std::io;
use std::os::windows::io::AsRawHandle;
use std::sync::Mutex;
use tokio::process::Child;
use tokio::process::Command;
use winapi::shared::ntdef::NT_SUCCESS;
use winapi::shared::ntdef::NTSTATUS;
use winapi::um::jobapi2::QueryInformationJobObject;
use winapi::um::winnt::HANDLE;
use winapi::um::winnt::JOBOBJECT_BASIC_ACCOUNTING_INFORMATION;
use winapi::um::winnt::JobObjectBasicAccountingInformation;

#[link(name = "ntdll")]
unsafe extern "system" {
    fn NtResumeProcess(process_handle: HANDLE) -> NTSTATUS;
}

struct Admission {
    closed: bool,
    // Failed assignment can leave a suspended root outside the job. Retain its
    // exact native owner until wait confirms exit, independently of job members.
    pending_launch: Option<Child>,
}

/// A private worker's retained, non-breakaway process-tree owner.
///
/// This deliberately exposes neither the native handle nor descendant preservation.
/// Default interactive jobs retain their existing compatibility policy.
pub struct ContainedWorkerJob {
    job: JobObject,
    admission: Mutex<Admission>,
}

/// Native zero-member evidence for an admission-closed retained worker job.
///
/// This does not prove output drain, thread shutdown, or termination of a process
/// launched outside this owner. Consumers must additionally establish those fences.
pub struct WorkerJobTerminal<'a> {
    _owner: &'a ContainedWorkerJob,
}

impl ContainedWorkerJob {
    pub fn create() -> io::Result<Self> {
        Ok(Self {
            job: JobObject::create_without_breakaway()?,
            admission: Mutex::new(Admission {
                closed: false,
                pending_launch: None,
            }),
        })
    }

    /// Assigns the captured child handle while suspended, then resumes it.
    /// Assignment failure never retries uncontained or resumes the child.
    pub fn spawn(&self, command: &mut Command) -> io::Result<Child> {
        let mut admission = self
            .admission
            .lock()
            .map_err(|_| io::Error::other("private worker job admission lock poisoned"))?;
        if admission.closed {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "private worker job admission is closed",
            ));
        }
        self.job.prepare_suspended_spawn(command);
        let child = command.spawn()?;
        admission.pending_launch = Some(child);
        let launched = (|| {
            let child = admission
                .pending_launch
                .as_ref()
                .ok_or_else(|| io::Error::other("missing retained suspended child"))?;
            let handle = child
                .raw_handle()
                .ok_or_else(|| io::Error::other("missing child process handle"))?;
            self.job.assign_process(handle)?;
            let status = unsafe { NtResumeProcess(handle.cast()) };
            if !NT_SUCCESS(status) {
                return Err(io::Error::other(format!(
                    "failed to resume owned worker child: NTSTATUS {status:#x}"
                )));
            }
            Ok(())
        })();
        if let Err(error) = launched {
            admission.closed = true;
            // Caller retains this owner; drop or kill is not terminal evidence.
            return Err(error);
        }
        admission
            .pending_launch
            .take()
            .ok_or_else(|| io::Error::other("missing retained resumed child"))
    }

    /// Permanently closes admission before requesting native tree termination.
    /// A successful request alone is never terminal evidence.
    pub fn request_shutdown(&self) -> io::Result<()> {
        let mut admission = self
            .admission
            .lock()
            .map_err(|_| io::Error::other("private worker job admission lock poisoned"))?;
        admission.closed = true;
        let root_request = match admission.pending_launch.as_mut() {
            Some(child) => child.start_kill(),
            None => Ok(()),
        };
        // Attempt both owned cleanup requests even if the unassigned root fails.
        let job_request = self.job.terminate();
        root_request.and(job_request)
    }

    /// Permanently closes admission and observes the retained native job.
    /// On a live member or query error, callers must retain this owner and retry.
    pub fn observe_terminal(&self) -> io::Result<Option<WorkerJobTerminal<'_>>> {
        let mut admission = self
            .admission
            .lock()
            .map_err(|_| io::Error::other("private worker job admission lock poisoned"))?;
        admission.closed = true;
        if let Some(child) = admission.pending_launch.as_mut() {
            if child.try_wait()?.is_none() {
                return Ok(None);
            }
            admission.pending_launch.take();
        }
        let mut accounting: JOBOBJECT_BASIC_ACCOUNTING_INFORMATION = unsafe { std::mem::zeroed() };
        let queried = unsafe {
            QueryInformationJobObject(
                self.job.as_raw_handle().cast(),
                JobObjectBasicAccountingInformation,
                std::ptr::addr_of_mut!(accounting).cast(),
                std::mem::size_of::<JOBOBJECT_BASIC_ACCOUNTING_INFORMATION>() as u32,
                std::ptr::null_mut(),
            )
        };
        if queried == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok((accounting.ActiveProcesses == 0).then_some(WorkerJobTerminal { _owner: self }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use filedescriptor::OwnedHandle;
    use std::os::windows::io::FromRawHandle;
    use std::os::windows::process::CommandExt;
    use std::process::Stdio;
    use std::sync::Arc;
    use std::sync::OnceLock;
    use std::time::Duration;
    use winapi::shared::winerror::ERROR_ACCESS_DENIED;
    use winapi::um::handleapi::DuplicateHandle;
    use winapi::um::jobapi::IsProcessInJob;
    use winapi::um::processthreadsapi::GetCurrentProcess;
    use winapi::um::processthreadsapi::OpenProcess;
    use winapi::um::winnt::HANDLE;
    use winapi::um::winnt::JOB_OBJECT_QUERY;
    use winapi::um::winnt::PROCESS_DUP_HANDLE;

    const PHASE: &str = "CODEX_TEST_CONTAINED_WORKER_PHASE";
    const JOB_OWNER_PID: &str = "CODEX_TEST_CONTAINED_WORKER_JOB_OWNER_PID";
    const JOB_HANDLE: &str = "CODEX_TEST_CONTAINED_WORKER_JOB_HANDLE";
    const DESCENDANT_TEST: &str =
        "win::contained_worker::tests::root_exit_does_not_acknowledge_live_descendant";

    struct UncertainRoot {
        _owner: Arc<ContainedWorkerJob>,
        _root: Child,
    }

    static UNCERTAIN: OnceLock<Mutex<Vec<Arc<ContainedWorkerJob>>>> = OnceLock::new();
    static UNRESOLVED_CHILDREN: OnceLock<Mutex<Vec<std::process::Child>>> = OnceLock::new();

    fn duplicate_owner_job_for_query() -> anyhow::Result<OwnedHandle> {
        let owner_pid: u32 = std::env::var(JOB_OWNER_PID)?.parse()?;
        let owner_job: usize = std::env::var(JOB_HANDLE)?.parse()?;
        let owner_process = unsafe { OpenProcess(PROCESS_DUP_HANDLE, 0, owner_pid) };
        anyhow::ensure!(
            !owner_process.is_null(),
            "open job owner: {}",
            io::Error::last_os_error()
        );
        let owner_process = unsafe { OwnedHandle::from_raw_handle(owner_process.cast()) };
        let mut duplicate: HANDLE = std::ptr::null_mut();
        let copied = unsafe {
            DuplicateHandle(
                owner_process.as_raw_handle().cast(),
                owner_job as HANDLE,
                GetCurrentProcess(),
                &mut duplicate,
                JOB_OBJECT_QUERY,
                0,
                0,
            )
        };
        anyhow::ensure!(
            copied != 0 && !duplicate.is_null(),
            "duplicate job query handle: {}",
            io::Error::last_os_error()
        );
        Ok(unsafe { OwnedHandle::from_raw_handle(duplicate.cast()) })
    }

    fn child_in_private_job(
        child: &std::process::Child,
        owner_job: &OwnedHandle,
    ) -> anyhow::Result<bool> {
        let mut in_job = 0;
        let queried = unsafe {
            IsProcessInJob(
                child.as_raw_handle().cast(),
                owner_job.as_raw_handle().cast(),
                &mut in_job,
            )
        };
        anyhow::ensure!(
            queried != 0,
            "query child membership: {}",
            io::Error::last_os_error()
        );
        Ok(in_job != 0)
    }

    async fn terminate_and_reap(child: &mut std::process::Child) -> io::Result<()> {
        if let Err(kill_error) = child.kill() {
            if child.try_wait()?.is_some() {
                return Ok(());
            }
            return Err(kill_error);
        }
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if child.try_wait()?.is_some() {
                    return Ok::<(), io::Error>(());
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "exact child did not terminate"))?
    }

    async fn retain_unresolved_child(
        child: std::process::Child,
        primary: &str,
        cleanup: &io::Error,
    ) -> anyhow::Result<()> {
        let pid = child.id();
        let owner_pid = std::env::var(JOB_OWNER_PID).unwrap_or_else(|_| "unknown".to_string());
        let owner_job = std::env::var(JOB_HANDLE).unwrap_or_else(|_| "unknown".to_string());
        UNRESOLVED_CHILDREN
            .get_or_init(|| Mutex::new(Vec::new()))
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(child);
        eprintln!(
            "WORKER_FOUNDATION_CHILD_UNCERTAIN_RETAINED owner_host_pid={owner_pid} owner_job_handle={owner_job} root_pid={} child_pid={pid} primary={primary:?} cleanup={cleanup}; supervisor must reconcile exact host",
            std::process::id()
        );
        std::future::pending::<anyhow::Result<()>>().await
    }

    async fn require_owned_descendant(
        mut child: std::process::Child,
        owner_job: &OwnedHandle,
        launch_kind: &str,
    ) -> anyhow::Result<()> {
        let pid = child.id();
        let primary = match child_in_private_job(&child, owner_job) {
            Ok(true) => {
                eprintln!(
                    "WORKER_FOUNDATION_DESCENDANT membership=private-job launch={launch_kind} child_pid={pid}"
                );
                // The outer owner must see this live member after root exit.
                return Ok(());
            }
            Ok(false) => "child is outside the exact private job".to_string(),
            Err(error) => format!("private-job membership query failed: {error}"),
        };
        if let Err(cleanup) = terminate_and_reap(&mut child).await {
            return retain_unresolved_child(child, &primary, &cleanup).await;
        }
        anyhow::bail!(
            "descendant fixture rejected launch={launch_kind} child_pid={pid}: {primary}; exact child reached terminal state"
        );
    }

    async fn finish_native(
        owner: Arc<ContainedWorkerJob>,
        original: anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        let request = owner.request_shutdown();
        let terminal = wait_for_terminal(&owner).await;
        eprintln!(
            "WORKER_FOUNDATION_CLEANUP host_pid={} job_handle={:#x} original={original:?} request={request:?} terminal={terminal:?}",
            std::process::id(),
            owner.job.as_raw_handle() as usize
        );
        if terminal.is_err() {
            UNCERTAIN
                .get_or_init(|| Mutex::new(Vec::new()))
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(owner);
            eprintln!(
                "WORKER_FOUNDATION_UNCERTAIN_HOST_RETAINED: supervisor reconciliation required"
            );
            return std::future::pending::<anyhow::Result<()>>().await;
        }
        original?;
        request?;
        Ok(())
    }

    async fn wait_for_terminal(owner: &ContainedWorkerJob) -> anyhow::Result<()> {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if owner.observe_terminal()?.is_some() {
                    return Ok::<(), anyhow::Error>(());
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await??;
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires explicitly admitted native Windows child-process fixture"]
    async fn assignment_failure_retains_unassigned_suspended_child() -> anyhow::Result<()> {
        anyhow::ensure!(
            std::env::var("CODEX_TEST_CONTAINED_WORKER_NATIVE").as_deref() == Ok("1"),
            "selected native fixture requires explicit admission",
        );
        let owner = Arc::new(ContainedWorkerJob::create()?);
        let original = async {
            let mut limits: winapi::um::winnt::JOBOBJECT_EXTENDED_LIMIT_INFORMATION =
                unsafe { std::mem::zeroed() };
            limits.BasicLimitInformation.LimitFlags =
                winapi::um::winnt::JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
                    | winapi::um::winnt::JOB_OBJECT_LIMIT_ACTIVE_PROCESS;
            limits.BasicLimitInformation.ActiveProcessLimit = 1;
            let configured = unsafe {
                winapi::um::jobapi2::SetInformationJobObject(
                    owner.job.as_raw_handle().cast(),
                    winapi::um::winnt::JobObjectExtendedLimitInformation,
                    std::ptr::addr_of_mut!(limits).cast(),
                    std::mem::size_of_val(&limits) as u32,
                )
            };
            anyhow::ensure!(configured != 0, "{}", io::Error::last_os_error());
            let mut command = Command::new(std::env::current_exe()?);
            command
                .args(["--ignored", "--exact", DESCENDANT_TEST])
                .env(PHASE, "descendant")
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null());
            let mut admitted = owner.spawn(&mut command)?;
            anyhow::ensure!(owner.spawn(&mut command).is_err());
            {
                let admission = owner
                    .admission
                    .lock()
                    .map_err(|_| io::Error::other("poisoned"))?;
                anyhow::ensure!(admission.closed);
                anyhow::ensure!(
                    admission
                        .pending_launch
                        .as_ref()
                        .and_then(Child::id)
                        .is_some(),
                    "failed native assignment lost the exact suspended child"
                );
            }
            anyhow::ensure!(owner.observe_terminal()?.is_none());
            owner.request_shutdown()?;
            tokio::time::timeout(Duration::from_secs(/*secs*/ 5), admitted.wait()).await??;
            wait_for_terminal(&owner).await?;
            let admission = owner
                .admission
                .lock()
                .map_err(|_| io::Error::other("poisoned"))?;
            anyhow::ensure!(
                admission.pending_launch.is_none(),
                "terminal evidence retained root"
            );
            Ok(())
        }
        .await;
        finish_native(owner, original).await
    }

    #[tokio::test]
    #[ignore = "requires explicitly admitted native Windows child-process fixture"]
    async fn root_exit_does_not_acknowledge_live_descendant() -> anyhow::Result<()> {
        const NAME: &str = DESCENDANT_TEST;
        let executable = std::env::current_exe()?;
        match std::env::var(PHASE).as_deref() {
            Ok("descendant") => {
                std::thread::sleep(Duration::from_secs(/*secs*/ 30));
                return Ok(());
            }
            Ok("root") => {
                let owner_job = duplicate_owner_job_for_query()?;
                let mut breakaway = std::process::Command::new(&executable);
                breakaway
                    .args(["--ignored", "--exact", NAME])
                    .env(PHASE, "descendant")
                    .stdin(Stdio::null())
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .creation_flags(winapi::um::winbase::CREATE_BREAKAWAY_FROM_JOB);
                match breakaway.spawn() {
                    Ok(child) => {
                        // Successful CreateProcess is not proof of breakaway: a nested job may
                        // keep the child inside this exact private job.
                        require_owned_descendant(child, &owner_job, "CREATE_BREAKAWAY_FROM_JOB")
                            .await?;
                    }
                    Err(error) if error.raw_os_error() == Some(ERROR_ACCESS_DENIED as i32) => {
                        eprintln!(
                            "WORKER_FOUNDATION_BREAKAWAY_DENIED error={error}; probing inherited child"
                        );
                        breakaway.creation_flags(0);
                        require_owned_descendant(
                            breakaway.spawn()?,
                            &owner_job,
                            "ordinary inherited launch after access-denied breakaway",
                        )
                        .await?;
                    }
                    Err(error) => {
                        return Err(anyhow::anyhow!(
                            "unexpected breakaway fixture spawn failure: {error}"
                        ));
                    }
                }
                return Ok(());
            }
            _ => {}
        }
        anyhow::ensure!(
            std::env::var("CODEX_TEST_CONTAINED_WORKER_NATIVE").as_deref() == Ok("1"),
            "selected native fixture requires explicit admission",
        );
        let owner = Arc::new(ContainedWorkerJob::create()?);
        let original = async {
            let mut command = Command::new(executable);
            command
                .args(["--ignored", "--exact", NAME])
                .env(PHASE, "root")
                .env(JOB_OWNER_PID, std::process::id().to_string())
                .env(JOB_HANDLE, (owner.job.as_raw_handle() as usize).to_string())
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::inherit());
            let mut root = owner.spawn(&mut command)?;
            let root_pid = root.id().ok_or_else(|| anyhow::anyhow!("missing exact root process ID"))?;
            let root_status = match tokio::time::timeout(Duration::from_secs(5), root.wait()).await {
                Ok(Ok(status)) => status,
                uncertain => {
                    // A root may itself retain an unexpected outside-job child.
                    // Never kill its host before an actual root terminal observation.
                    static ROOTS: OnceLock<Mutex<Vec<UncertainRoot>>> = OnceLock::new();
                    ROOTS.get_or_init(|| Mutex::new(Vec::new())).lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .push(UncertainRoot { _owner: Arc::clone(&owner), _root: root });
                    eprintln!(
                        "WORKER_FOUNDATION_ROOT_UNCERTAIN_RETAINED host_pid={} root_pid={root_pid} job_handle={:#x} result={uncertain:?}; no shutdown request; supervisor must reconcile exact host",
                        std::process::id(),
                        owner.job.as_raw_handle() as usize
                    );
                    return std::future::pending::<anyhow::Result<()>>().await;
                }
            };
            anyhow::ensure!(root_status.success());
            anyhow::ensure!(owner.observe_terminal()?.is_none(), "root exit falsely acknowledged descendant");
            anyhow::ensure!(owner.spawn(&mut command).is_err(), "terminal observation reopened admission");
            owner.request_shutdown()?;
            wait_for_terminal(&owner).await?;
            Ok(())
        }
        .await;
        finish_native(owner, original).await
    }
}
