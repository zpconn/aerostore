#![cfg(target_os = "linux")]

use std::io;
use std::sync::Arc;
use std::time::{Duration, Instant};

use aerostore_core::{spawn_wal_writer_daemon, SharedWalRing, ShmArena};

const RING_SLOTS: usize = 64;
const RING_SLOT_BYTES: usize = 256;

#[test]
fn wal_writer_daemon_exits_when_parent_process_exits() {
    // SAFETY:
    // raw libc process primitives are used intentionally to model parent death semantics.
    unsafe {
        let mut pipefd = [0_i32; 2];
        assert_eq!(libc::pipe(pipefd.as_mut_ptr()), 0, "pipe() failed");

        let parent_worker = libc::fork();
        assert!(parent_worker >= 0, "fork() failed");

        if parent_worker == 0 {
            libc::close(pipefd[0]);

            let shm = Arc::new(ShmArena::new(4 << 20).expect("child failed to allocate shm"));
            let ring = SharedWalRing::<RING_SLOTS, RING_SLOT_BYTES>::create(Arc::clone(&shm))
                .expect("child failed to create shared wal ring");

            let wal_path =
                std::env::temp_dir().join(format!("aerostore_lifecycle_{}.wal", libc::getpid()));
            let daemon = spawn_wal_writer_daemon(ring, &wal_path)
                .expect("child failed to spawn wal writer daemon");
            let daemon_pid = daemon.pid();
            let pid_bytes = daemon_pid.to_ne_bytes();

            let mut written = 0;
            while written < pid_bytes.len() {
                let rc = libc::write(
                    pipefd[1],
                    pid_bytes[written..].as_ptr() as *const libc::c_void,
                    pid_bytes.len() - written,
                );
                if rc < 0 {
                    libc::_exit(2);
                }
                written += rc as usize;
            }
            libc::close(pipefd[1]);

            // Exit without explicit daemon shutdown. Regression target: no orphan remains.
            libc::_exit(0);
        }

        libc::close(pipefd[1]);

        let mut daemon_pid_bytes = [0_u8; std::mem::size_of::<libc::pid_t>()];
        let mut read = 0;
        while read < daemon_pid_bytes.len() {
            let rc = libc::read(
                pipefd[0],
                daemon_pid_bytes[read..].as_mut_ptr() as *mut libc::c_void,
                daemon_pid_bytes.len() - read,
            );
            assert!(rc > 0, "failed to read daemon pid from child");
            read += rc as usize;
        }
        libc::close(pipefd[0]);

        let daemon_pid = libc::pid_t::from_ne_bytes(daemon_pid_bytes);

        let mut status = 0_i32;
        let waited = libc::waitpid(parent_worker, &mut status as *mut i32, 0);
        assert_eq!(waited, parent_worker, "waitpid failed for parent worker");
        assert!(
            libc::WIFEXITED(status) && libc::WEXITSTATUS(status) == 0,
            "parent worker exited unexpectedly (status={status})"
        );

        let deadline = Instant::now() + Duration::from_secs(3);
        loop {
            let kill_rc = libc::kill(daemon_pid, 0);
            if kill_rc != 0 {
                let err = io::Error::last_os_error();
                if err.raw_os_error() == Some(libc::ESRCH) {
                    break;
                }
            }

            if Instant::now() >= deadline {
                let _ = libc::kill(daemon_pid, libc::SIGKILL);
                panic!(
                    "wal writer daemon pid {} survived parent exit and became orphaned",
                    daemon_pid
                );
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}
