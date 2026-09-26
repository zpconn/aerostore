"""Erase only reviewed retry-diagnostic statements for feature-disabled proofs.

This is not a proof of the instrumented feature. Unknown conditional syntax,
changed arguments and unguarded observations must remain errors. The separate
site checker pins placement and the complete erased native token stream.
"""
import re

PREFIX = ['#', '[', 'cfg', '(', 'feature', '=', '"retry-diagnostics"', ')', ']']
CALL = ['crate', '::', 'retry_diagnostics', '::', 'record']
CAUSE = ['crate', '::', 'retry_diagnostics', '::', 'Cause', '::']
ARGS = {
    'IndexBucketBusy': ('Some(index.header_offset())', 'None'),
    'LookupPostSnapshotStamp': ('Some(index.header_offset())', 'None'),
    'LookupChangedCapturedStamp': ('Some(index.header_offset())', 'None'),
    'StickyIndexConflict': ('None', 'None'),
    'PredicateValidationStamp': ('Some(read.index_offset)', 'None'),
    'LockForUpdateHeld': ('None', 'Some(row_id)'),
    'LockForUpdateRace': ('None', 'Some(row_id)'),
    'ReadRowLocked': ('None', 'Some(row_id)'),
    'WriteRowLocked': ('None', 'Some(row_id)'),
    'WriteDirtyRowLocked': ('None', 'Some(row_id)'),
    'CommitRowLocked': ('None', 'Some(*row_id)'),
    'ReadVersionIdentityChanged': ('None', 'Some(read.row_id)'),
    'ReadVersionDeletedAfterSnapshot': ('None', 'Some(read.row_id)'),
    'WriteBaseHeadChanged': ('None', 'Some(write.row_id)'),
    'WriteBaseXmaxSet': ('None', 'Some(write.row_id)'),
    'VisibleChainLimit': ('None', 'Some(row_id)'),
    'PartitionLockBusy': ('None', 'None'),
    'WalWriterEpochChanged': ('None', 'None'),
}

def argument_tokens(text):
    # Arguments come exclusively from the finite literal table above. This
    # tokenizer never sees source or user input, and has no external dependency.
    tokens = re.findall(r"::|[A-Za-z_][A-Za-z_0-9]*|[().,*]", text)
    if "".join(tokens) != text:
        raise ValueError("invalid reviewed diagnostic argument")
    return tokens


def pattern(cause):
    index, row = ARGS[cause]
    return PREFIX + CALL + ['('] + CAUSE + [cause, ','] + argument_tokens(index) + [','] + argument_tokens(row) + [')', ';']


def erase(tokens):
    """Return erased tokens and indexed occurrences; tolerate Rust trailing ',' only."""
    result, sites, position = [], [], 0
    while position < len(tokens):
        if tokens[position:position + len(PREFIX)] == PREFIX:
            matched = None
            for cause in ARGS:
                expected = pattern(cause)
                choices = [expected, expected[:-2] + [','] + expected[-2:]]
                for candidate in choices:
                    if tokens[position:position + len(candidate)] == candidate:
                        matched = cause, len(candidate)
                        break
                if matched:
                    break
            if matched is None:
                raise ValueError('unreviewed retry-diagnostics statement or arguments')
            cause, length = matched
            sites.append({'cause': cause, 'position': len(result)})
            position += length
            continue
        if tokens[position:position + len(CALL)] == CALL:
            raise ValueError('retry diagnostic call lacks the exact feature guard')
        result.append(tokens[position])
        position += 1
    return result, sites


def normalize(tokens):
    return erase(tokens)[0]
