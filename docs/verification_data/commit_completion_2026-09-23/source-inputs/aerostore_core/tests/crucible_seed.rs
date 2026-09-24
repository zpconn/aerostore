#[path = "../benches/support/crucible_seed.rs"]
mod crucible_seed;

use std::collections::HashSet;
use std::ffi::OsStr;

use crucible_seed::{fixed_worker_seed, next_u64, parse_seed, FIXED_SEED_ALGORITHM, SEED_ENV};

#[test]
fn absence_is_distinct_from_explicit_zero_and_both_u64_boundaries_are_accepted() {
    assert_eq!(SEED_ENV, "AEROSTORE_CRUCIBLE_SEED");
    assert_eq!(FIXED_SEED_ALGORITHM, "worker_add_xorshift64_v1");
    assert_eq!(parse_seed(None), Ok(None));
    for (text, value) in [
        ("0", 0),
        ("00000", 0),
        ("1", 1),
        ("2026092301", 2026092301),
        ("18446744073709551615", u64::MAX),
    ] {
        assert_eq!(parse_seed(Some(OsStr::new(text))), Ok(Some(value)));
    }
}

#[test]
fn invalid_provided_seeds_fail_instead_of_using_entropy() {
    for text in [
        "",
        " ",
        " 1",
        "1 ",
        "1\n",
        "+1",
        "-1",
        "1.0",
        "1e2",
        "0x12",
        "18446744073709551616",
        "99999999999999999999999999999999999999999999",
        "١",
        "none",
    ] {
        assert!(
            parse_seed(Some(OsStr::new(text))).is_err(),
            "accepted {text:?}"
        );
    }
}

#[cfg(unix)]
#[test]
fn non_unicode_provided_seed_is_rejected_without_environment_mutation() {
    use std::os::unix::ffi::OsStrExt;
    assert!(parse_seed(Some(OsStr::from_bytes(&[0xff]))).is_err());
}

#[test]
fn seed_worker_and_existing_engine_salt_each_affect_the_worker_state() {
    let mut states = HashSet::new();
    for seed in [0, 1, u64::MAX] {
        for worker in 0..16 {
            for salt in [0xA3E0_52D1_9911_AA11, 0xCC77_AA22_1958_3321] {
                let state = fixed_worker_seed(seed, worker, salt);
                assert_ne!(state, 0);
                assert_eq!(state, fixed_worker_seed(seed, worker, salt));
                assert!(states.insert(state), "unexpected fixture-state collision");
            }
        }
    }
    assert_eq!(states.len(), 96);
}

#[test]
fn wrapping_zero_state_is_normalized_without_rejecting_seed_zero() {
    assert_eq!(fixed_worker_seed(0, 0, 0), 1);
    assert_eq!(fixed_worker_seed(u64::MAX, 0, 1), 1);
    let mut state = fixed_worker_seed(0, 0, 0);
    assert_ne!(next_u64(&mut state), 0);
}

#[test]
fn fixed_seeds_reproduce_the_initial_retry_draw_and_following_workload_draws() {
    let cases = [
        (
            0,
            0,
            0xA3E0_52D1_9911_AA11,
            11808529283135154705,
            [
                9406193496257277189,
                9547888588708302367,
                3214740728031298627,
                1150336892143707659,
                14645058260259751303,
                15671131939985430988,
            ],
        ),
        (
            2026092301,
            7,
            0xA3E0_52D1_9911_AA11,
            17826556725585429937,
            [
                17103565902084060282,
                204318839596778130,
                16501298228645644823,
                7259158590259512899,
                15552639714370158351,
                1385428799333271601,
            ],
        ),
        (
            u64::MAX,
            15,
            0xCC77_AA22_1958_3321,
            1276713322149148763,
            [
                10177195725116105835,
                11772064569553537123,
                1651743996599424563,
                3934310641067546719,
                7425754279215141423,
                13866168508752072907,
            ],
        ),
    ];
    for (seed, worker, salt, initial, expected) in cases {
        let mut state = fixed_worker_seed(seed, worker, salt);
        assert_eq!(state, initial);
        // The worker uses the first draw to seed its independent RetryBackoff;
        // later draws select rows. Keep that existing draw sequence intact.
        for draw in expected {
            assert_eq!(next_u64(&mut state), draw);
        }
        let mut replay = fixed_worker_seed(seed, worker, salt);
        for draw in expected {
            assert_eq!(next_u64(&mut replay), draw);
        }
    }
}
