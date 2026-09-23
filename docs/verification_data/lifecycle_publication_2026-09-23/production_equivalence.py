from pathlib import Path
import hashlib
import importlib.util
import json
import re
import subprocess

ROOT = Path('/home/zpconn/code/aerostore')
BASE = 'c5d27d280630d901b3221f836bbacccaf8c11349'
OUT = ROOT / 'target/verification-lifecycle/production-equivalence.json'
spec = importlib.util.spec_from_file_location('equivalence_tokens', ROOT / 'verification/concurrent/generate.py')
adapter = importlib.util.module_from_spec(spec)
spec.loader.exec_module(adapter)
adapter.TOKEN = re.compile(adapter.TOKEN.pattern.replace('|[0-9]+|', "|'(?:\\\\.|[^'\\\\])'|'[A-Za-z_][A-Za-z_0-9]*|0x[0-9a-fA-F]+|[0-9]+|")
                           .replace('::|->', '<=|>=|::|->').replace('[{}()', '[%{}()'))


def without_test_modules(source):
    marker = '\n#[cfg(test)]\nmod tests {'
    assert source.count(marker) == 1
    prefix, tail = source.split(marker, 1)
    tail = marker + tail
    tokens = adapter.tokenize(tail)
    kept = []
    while tokens:
        if tokens[:8] == ['#', '[', 'cfg', '(', 'test', ')', ']', 'mod']:
            assert re.fullmatch('[A-Za-z_][A-Za-z_0-9]*', tokens[8])
            assert tokens[9] == '{'
            end = adapter.balanced_end(tokens, 9)
            tokens = tokens[end:]
        else:
            kept.append(tokens.pop(0))
    return prefix + '\n' + ' '.join(kept)


def normalize(name, source):
    if name not in ('aerostore_core/src/occ_partitioned.rs', 'aerostore_core/src/procarray.rs'):
        return source
    source = without_test_modules(source)
    if name.endswith('occ_partitioned.rs'):
        assert '#[cfg(test)]\nthread_local! {' in source
        for hook in ('TRANSACTION_FINISHING_HOOK', 'TRANSACTION_FINISHED_HOOK'):
            declaration = f'    static {hook}: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =\n        std::cell::RefCell::new(None);\n'
            if declaration in source:
                start = source.index('#[cfg(test)]\nthread_local! {')
                end = source.index('\n}', start)
                assert declaration in source[start:end + 1]
                source = source.replace(declaration, '', 1)
            block = f'''        #[cfg(test)]
        {hook}.with(|hook| {{
            if let Some(hook) = hook.borrow_mut().take() {{
                hook();
            }}
        }});
'''
            if block in source:
                assert source.count(block) == 1
                source = source.replace(block, '', 1)
    return source


roots = ['Cargo.toml', 'Cargo.lock', 'aerostore_core', 'aerostore_verified', 'aerostore_macros', 'aerostore_tcl']
tracked = subprocess.check_output(['git', 'ls-tree', '-r', '--name-only', BASE, *roots], cwd=ROOT, text=True).splitlines()
files = [n for n in tracked if n.endswith(('.rs', '.toml')) or n == 'Cargo.lock']
untracked = subprocess.check_output(['git', 'ls-files', '--others', '--exclude-standard', '--', *roots], cwd=ROOT, text=True).splitlines()
assert not untracked, untracked
changed = []
hashes = {}
for name in files:
    before = subprocess.check_output(['git', 'show', f'{BASE}:{name}'], cwd=ROOT).decode()
    after = (ROOT / name).read_text()
    if before != after:
        changed.append(name)
    clean_before, clean_after = normalize(name, before), normalize(name, after)
    assert clean_before == clean_after, name
    hashes[name] = {'baseline_sha256': hashlib.sha256(before.encode()).hexdigest(),
                    'current_sha256': hashlib.sha256(after.encode()).hexdigest(),
                    'production_projection_sha256': hashlib.sha256(clean_after.encode()).hexdigest()}
result = {'baseline': BASE, 'passed': True, 'checked_files': len(files),
          'changed_native_files': changed,
          'comparison': 'Native Rust/Cargo inputs unchanged after excluding complete balanced cfg(test) modules and the exact two new cfg(test) hook declarations/calls. Production items after test modules retain identical tokens; other production regions compare byte-for-byte. No production synchronization, allocator, atomic or algorithm change.',
          'files': hashes}
OUT.parent.mkdir(parents=True, exist_ok=True)
OUT.write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps({'passed': True, 'checked_files': len(files), 'changed_test_files': changed, 'receipt': str(OUT)}))
