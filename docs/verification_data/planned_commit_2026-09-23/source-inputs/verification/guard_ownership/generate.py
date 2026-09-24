#!/usr/bin/env python3
"""Restricted native CAS/guard construction/Drop and index-lock adaptation.

Ghost grant/revoke updates are checked proof bookkeeping at the corresponding
atomic events. This is not an implementation of the raw atomic primitive.
"""
from pathlib import Path
import argparse
import importlib.util
import re

ROOT = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent
SOURCE = ROOT / 'aerostore_core/src/shm_lock.rs'
INDEX = ROOT / 'aerostore_core/src/shm_index.rs'
OCC = ROOT / 'aerostore_core/src/occ_partitioned.rs'
CONTRACTS = HERE / 'contracts.rs'
OUTPUT = HERE / 'ownership.verus.rs'
spec = importlib.util.spec_from_file_location('ownership_guard_adapter', ROOT / 'verification/guards/generate.py')
guards = importlib.util.module_from_spec(spec)
spec.loader.exec_module(guards)
adapter = guards.adapter
adapter.TOKEN = re.compile(adapter.TOKEN.pattern + r"|@")


def representation(source):
    # The native guard has an immutable lifetime-bound mutex reference, private
    # fields and no Copy/Clone constructor. Source changes require review.
    expected = """pub(crate) struct ShmMutexGuard<'a> {
    mutex: &'a ShmMutex,
    _not_send: std::marker::PhantomData<*mut ()>,
}"""
    match = re.search(r"pub\(crate\) struct ShmMutexGuard<'a>\s*\{[^}]*\}", source)
    if not match or re.sub(r'\s+', '', match[0]) != re.sub(r'\s+', '', expected):
        raise ValueError('native guard representation/lifetime changed')
    if re.search(r'(?:impl[^\n]*(?:Clone|Copy)[^\n]*ShmMutexGuard|derive[^\n]*(?:Clone|Copy))', source):
        raise ValueError('native guard can be duplicated')
    if source.count('ShmMutexGuard {') != 1:
        raise ValueError('unreviewed native guard constructor')


def render(source=None, index=None, template=None, occ=None):
    source = SOURCE.read_text() if source is None else source
    index = INDEX.read_text() if index is None else index
    template = CONTRACTS.read_text() if template is None else template
    occ = OCC.read_text() if occ is None else occ
    representation(source)
    body = guards.method(source, 'try_acquire', "fn try_acquire(&self) -> Option<ShmMutexGuard<'_>>")
    prefix = adapter.tokenize('self.state.compare_exchange(')
    if body[:len(prefix)] != prefix:
        raise ValueError('unsupported native CAS receiver')
    end = adapter.balanced_end(body, len(prefix) - 1)
    arguments = adapter.show_tokens(body[len(prefix):end - 1])
    tail = body[end:]
    constructor = adapter.tokenize(".ok().map(|_| ShmMutexGuard { mutex: self, _not_send: std::marker::PhantomData, })")
    failed_constructor = adapter.tokenize(".err().map(|_| ShmMutexGuard { mutex: self, _not_send: std::marker::PhantomData, })")
    if tail not in (constructor, failed_constructor):
        raise ValueError('unsupported native CAS result/guard construction')
    branch = 'Ok' if tail == constructor else 'Err'
    other = 'Err' if branch == 'Ok' else 'Ok'
    acquire = f'''let ghost before = *authority;
    match driver.compare_exchange(key, {arguments}, Tracked(&mut *authority)) {{
        {branch}(_) => {{
            let tracked token = grant(&mut *authority, key);
            proof {{
                assert(cells(&before)[key].bit == 0);
                assert(cells(&before)[key].owner.is_none());
                assert(cells(authority) =~= cells(&before).insert(key, Cell {{ bit: 1, owner: Some(token.serial) }}));
                assert(step(&before, &*authority, Step::Acquire {{ key, serial: token.serial }}));
            }}
            Some(ReadLease {{ physical: key, token: Tracked(token) }})
        }}
        {other}(_) => {{
            proof {{ assert(cells(authority) =~= cells(&before)); }}
            None
        }},
    }}'''
    drop_start = source.index("impl Drop for ShmMutexGuard<'_>")
    body = guards.method(source[drop_start:], 'drop', 'fn drop(&mut self)')
    body = adapter.replace(body, 'self.mutex.state.store', 'driver.store', 1)
    # Arguments/order remain actual source data, including semantic mutants.
    prefix = adapter.tokenize('driver.store(')
    if body[:len(prefix)] != prefix or body[-2:] != [')', ';']:
        raise ValueError('unsupported native guard destructor')
    drop_arguments = adapter.show_tokens(body[len(prefix):-2])
    drop = f'''let ghost before = *authority;
    let ghost old_key = lease_key(&lease);
    let ghost old_serial = lease_serial(&lease);
    let ReadLease {{ physical, token: Tracked(token) }} = lease;
    proof {{ revoke(&mut *authority, token); }}
    driver.store(physical, {drop_arguments}, Tracked(&mut *authority));
    proof {{
        assert(cells(authority) =~= cells(&before).insert(old_key, Cell {{ bit: 0, owner: None }}));
        assert(step(&before, &*authority, Step::Release {{ key: old_key, serial: old_serial }}));
    }}'''

    body = guards.method(source, 'try_lock', "fn try_lock(&self) -> Option<ShmMutexGuard<'_>>")
    body = adapter.replace(body, 'self.priority_waiters.load(Ordering::Acquire)',
        'driver.priority_waiters(key, Ordering::Acquire)', 2)
    body = adapter.replace(body, 'self.try_acquire()', 'acquire_preserving_borrows(driver, key, held, Tracked(&mut *authority))', 1)
    drop_call = adapter.tokenize('drop(guard);')
    if any(body[i:i+len(drop_call)] == drop_call for i in range(len(body))):
        body = adapter.replace(body, 'drop(guard);', 'drop_preserving_borrows(driver, guard, held, Tracked(&mut *authority));', 1)
    else:
        body = adapter.replace(body, 'std::mem::forget(guard);', 'let _forgotten = guard;', 1)
    # An omitted explicit drop would still invoke Rust's implicit destructor on
    # return. It is deliberately not accepted as an ownership-negative mutant.
    guards.reject(body)
    # Preserve native control flow, with explicit rely-trace cuts before each
    # relevant atomic observation/CAS and the consuming release operation.
    body = adapter.replace(body, 'let guard = acquire_preserving_borrows(driver, key, held, Tracked(&mut *authority))?;',
        """environment(driver, Ghost(protected), Tracked(&mut *authority));
        proof { protected_borrows_remain_authorized(&entry, &*authority, held@); }
        let guard = acquire_preserving_borrows(driver, key, held, Tracked(&mut *authority))?;
        let ghost with_local = protected.insert((lease_key(&guard), lease_serial(&guard)));
        let ghost acquired = *authority;
        let ghost local_serial = lease_serial(&guard);
        environment(driver, Ghost(with_local), Tracked(&mut *authority));
        proof {
            protection_can_shrink(&*authority, with_local, protected);
            protected_borrows_remain_authorized(&entry, &*authority, held@);
            protected_key_authorizes(&acquired, &*authority, &guard, with_local);
        }""", 1)
    for release in ('drop_preserving_borrows(driver, guard, held, Tracked(&mut *authority));', 'let _forgotten = guard;'):
        needle = adapter.tokenize(release)
        if any(body[i:i + len(needle)] == needle for i in range(len(body))):
            body = adapter.replace(body, release, release +
                ' proof { assert(cells(authority)[key].owner != Some(local_serial)); }', 1)
    # Source has exactly two returns here; on either one the caller's borrowed
    # lease set must still be live. No unchanged foreign-cell frame is asserted.
    body = adapter.replace(body, 'return None;',
        'proof { protected_borrows_remain_authorized(&entry, &*authority, held@); } return None;', 2)
    lock = 'environment(driver, Ghost(protected), Tracked(&mut *authority));\n' + adapter.show_tokens(body)

    body = guards.method(index, 'transactional_try_lock_bucket', """fn transactional_try_lock_bucket(
        &self, bucket: usize,
    ) -> Result<Option<ShmMutexGuard<'_>>, ShmIndexError>""")
    body = adapter.replace(body, 'self.publication_header()', 'driver.publication_header(binding)', 1)
    body = adapter.replace(body, 'header.poisoned.load(AtomicOrdering::Acquire)', 'driver.poisoned(header)', 1)
    prefix = adapter.tokenize('header.buckets.get(')
    positions = [i for i in range(len(body)) if body[i:i + len(prefix)] == prefix]
    if len(positions) != 1:
        raise ValueError('unsupported bucket resolution expression')
    begin = positions[0]
    end = adapter.balanced_end(body, begin + len(prefix) - 1)
    argument = body[begin + len(prefix):end - 1]
    guards.reject(argument)
    body = body[:begin] + adapter.tokenize('driver.bucket(header, ' + adapter.show_tokens(argument)
        + ', Tracked(&*authority))') + body[end:]
    body = adapter.replace(body, 'bucket.lock.try_lock()', 'try_lock(driver, bucket, held, Tracked(&mut *authority))', 1)
    body = ['IndexError' if token == 'ShmIndexError' else token for token in body]
    guards.reject(body)
    selected_index = adapter.show_tokens(body)
    body = guards.method(occ, 'acquire_index_bucket', """fn acquire_index_bucket(
        index: &SecondaryIndex<usize>, bucket: usize,
    ) -> Result<ShmMutexGuard<'_>, Error>""")
    body = adapter.replace(body, """#[cfg(test)] if attempt == 0 {
        INDEX_BUCKET_CONTENDED_HOOK.with(|hook| {
            if let Some(hook) = hook.borrow_mut().take() { hook(); }
        });
    }""", '', 1)
    body = adapter.replace(body, 'index.transactional_try_lock_bucket(bucket)',
        'transactional_try_lock_bucket(driver, binding, bucket, held, Tracked(&mut *authority))', 1)
    body = adapter.replace(body, 'std::thread::yield_now()', 'driver.yield_now()', 1)
    body = adapter.replace(body, 'std::hint::spin_loop()', 'driver.spin_loop()', 1)
    body = ['IndexError' if token == 'Error' else token for token in body]
    prefix = adapter.tokenize('for attempt in 0..INDEX_LOCK_SPIN_LIMIT {')
    if body[:len(prefix)] != prefix:
        raise ValueError('unsupported native bounded acquisition loop')
    end = adapter.balanced_end(body, len(prefix) - 1)
    inside, tail = body[len(prefix):end - 1], body[end:]
    guards.reject(inside + tail)
    limits = re.findall(r'const INDEX_LOCK_SPIN_LIMIT: u32 = (\d+);', occ)
    if len(limits) != 1 or int(limits[0]) <= 0:
        raise ValueError('missing native bounded retry limit')
    for delay in ('yield_now', 'spin_loop'):
        inside = adapter.replace(inside, f'driver.{delay}();',
            f'environment_for_borrows(driver, held, Tracked(&mut *authority)); driver.{delay}();', 1)
    bounded = f"""let ghost initial = *authority;
    let mut attempt: u32 = 0;
    while attempt < {limits[0]}
        invariant attempt <= {limits[0]}, binding < driver.registry().len(),
            valid(authority), leases_authorized(authority, held@), physical_domain(driver, authority),
            cells(authority).dom() == cells(&initial).dom(),
            cells(&initial).dom() == cells(old(authority)).dom(),
        decreases {limits[0]} - attempt,
    {{
""" + adapter.show_tokens(inside) + "\nattempt += 1;\n}\n" + adapter.show_tokens(tail)
    for marker, replacement in [('/* NATIVE_TRY_ACQUIRE */', acquire), ('/* NATIVE_DROP */', drop),
        ('/* NATIVE_TRY_LOCK */', lock), ('/* NATIVE_INDEX_TRY_LOCK */', selected_index), ('/* NATIVE_ACQUIRE_BUCKET */', bounded)]:
        if template.count(marker) != 1:
            raise ValueError('missing/duplicate native placeholder: ' + marker)
        template = template.replace(marker, replacement)
    return '// Generated from native ShmMutex CAS/Drop and index selection; see generate.py.\n' + template


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--check', action='store_true')
    args = parser.parse_args()
    rendered = render()
    if args.check:
        if OUTPUT.read_text() != rendered:
            raise SystemExit('stale native guard ownership proof')
    else:
        OUTPUT.write_text(rendered)


if __name__ == '__main__':
    main()
