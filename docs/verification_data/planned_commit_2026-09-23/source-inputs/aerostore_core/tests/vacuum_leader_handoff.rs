use std::sync::Arc;
use std::time::{Duration, Instant};

use aerostore_core::{spawn_vacuum_daemon, OccTable, ShmArena};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct TinyRow {
    value: u64,
}

#[test]
fn second_spawn_in_same_process_becomes_follower() {
    let shm = Arc::new(ShmArena::new(8 << 20).expect("failed to allocate shared memory"));
    let table = Arc::new(OccTable::<TinyRow>::new(Arc::clone(&shm), 1).expect("create table"));
    table
        .seed_row(0, TinyRow { value: 0 })
        .expect("seed row failed");

    let leader = spawn_vacuum_daemon(Arc::clone(&table)).expect("spawn leader");
    assert!(
        leader.is_leader(),
        "first vacuum daemon should acquire leadership"
    );

    let follower = spawn_vacuum_daemon(Arc::clone(&table)).expect("spawn follower");
    assert!(
        !follower.is_leader(),
        "second vacuum daemon in same process should not become leader"
    );
    assert_eq!(
        follower.leader_pid(),
        leader.leader_pid(),
        "follower must report current leader pid"
    );
    assert_eq!(
        shm.vacuum_daemon_pid(),
        leader.leader_pid(),
        "shared leader pid slot diverged from leader handle"
    );
}

#[test]
fn stale_pid_is_taken_over_by_new_daemon() {
    let shm = Arc::new(ShmArena::new(8 << 20).expect("failed to allocate shared memory"));
    let table = Arc::new(OccTable::<TinyRow>::new(Arc::clone(&shm), 1).expect("create table"));
    table
        .seed_row(0, TinyRow { value: 0 })
        .expect("seed row failed");

    let stale_pid = i32::MAX;
    shm.set_vacuum_daemon_pid(stale_pid);
    assert_eq!(shm.vacuum_daemon_pid(), stale_pid);

    let daemon = spawn_vacuum_daemon(Arc::clone(&table)).expect("spawn daemon");
    assert!(
        daemon.is_leader(),
        "daemon should reclaim leadership from dead pid"
    );
    assert_ne!(
        daemon.leader_pid(),
        stale_pid,
        "leader pid should no longer match stale pid"
    );
    assert_eq!(
        shm.vacuum_daemon_pid(),
        daemon.leader_pid(),
        "shared leader pid slot should reflect new leader"
    );
}

#[test]
fn leader_stop_clears_slot_and_next_spawn_reacquires() {
    let shm = Arc::new(ShmArena::new(8 << 20).expect("failed to allocate shared memory"));
    let table = Arc::new(OccTable::<TinyRow>::new(Arc::clone(&shm), 1).expect("create table"));
    table
        .seed_row(0, TinyRow { value: 0 })
        .expect("seed row failed");

    let leader = spawn_vacuum_daemon(Arc::clone(&table)).expect("spawn leader");
    assert!(leader.is_leader());
    assert_ne!(shm.vacuum_daemon_pid(), 0);
    leader.stop().expect("stop leader daemon");
    wait_until(
        Duration::from_secs(2),
        || shm.vacuum_daemon_pid() == 0,
        "leader pid slot did not clear after stop",
    );

    let next = spawn_vacuum_daemon(Arc::clone(&table)).expect("spawn next leader");
    assert!(
        next.is_leader(),
        "fresh daemon should reacquire leadership once slot is clear"
    );
    assert_eq!(shm.vacuum_daemon_pid(), next.leader_pid());
}

fn wait_until(mut timeout: Duration, mut condition: impl FnMut() -> bool, message: &str) {
    let start = Instant::now();
    while timeout > Duration::ZERO {
        if condition() {
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
        timeout = timeout.saturating_sub(Duration::from_millis(10));
    }
    panic!("{} (elapsed={:?})", message, start.elapsed());
}
