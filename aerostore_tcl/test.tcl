# Demo script for the aerostore Tcl extension.
# Usage example:
#   tclsh aerostore_tcl/test.tcl

if {[catch {package require aerostore} version]} {
    set libpath [file normalize ./target/debug/libaerostore_tcl.so]
    puts "package require failed; loading extension directly from $libpath"
    load $libpath Aerostore
    set version [package require aerostore]
}

puts "Loaded aerostore package version: $version"
set data_dir ./tmp/aerostore_tcl_demo
puts [aerostore::init $data_dir]

set batch [join [list \
    "UAL123\t37.618805\t-122.375416\t35000\t451\t1709000000" \
    "AAL456\t33.942500\t-118.408100\t12000\t390\t1709000001" \
    "DAL789\t40.641300\t-73.778100\t9000\t310\t1709000002" \
    "SWA321\t32.733800\t-117.193300\t14500\t365\t1709000003" \
] "\n"]

set ingest_result [FlightState ingest_tsv $batch 128]
puts "ingest: $ingest_result"

set count [FlightState search -compare {{> altitude 10000}} -sort lat -limit 50]
puts "search altitude>10000 count: $count"
if {$count != 3} {
    error "expected altitude>10000 count to be 3, got $count"
}

set complex_count [FlightState search \
    -compare {{> altitude 10000} {< altitude 36000} {in flight {UAL123 AAL456 SWA321}}} \
    -sort altitude \
    -limit 10]
puts "complex STAPI-style search count: $complex_count"
if {$complex_count != 3} {
    error "expected complex STAPI-style count to be 3, got $complex_count"
}

set raw_compare "{> altitude 10000} {< altitude 36000} {in flight {UAL123 AAL456 SWA321}}"
set raw_count [FlightState search -compare $raw_compare -sort altitude -limit 10]
puts "raw compare literal count: $raw_count"
if {$raw_count != $complex_count} {
    error "expected raw compare literal count $complex_count, got $raw_count"
}

set match_count [FlightState search -compare {{match flight UAL*}} -limit 10]
puts "match flight UAL* count: $match_count"
if {$match_count != 1} {
    error "expected match count 1 after first ingest, got $match_count"
}

set second_batch [join [list \
    "UAL123\t37.618805\t-122.375416\t35800\t459\t1709001000" \
    "UAL555\t41.974200\t-87.907300\t41000\t472\t1709001001" \
] "\n"]
set second_ingest [FlightState ingest_tsv $second_batch 64]
puts "second ingest: $second_ingest"

set post_upsert_match [FlightState search -compare {{match ident UAL*}} -limit 10]
puts "post-upsert match ident UAL* count: $post_upsert_match"
if {$post_upsert_match != 2} {
    error "expected UAL* count 2 after upsert batch, got $post_upsert_match"
}

set paged_desc_count [FlightState search \
    -compare {{> alt 30000}} \
    -sort alt \
    -desc \
    -offset 1 \
    -limit 1]
puts "paged descending altitude count: $paged_desc_count"
if {$paged_desc_count != 1} {
    error "expected paged descending count 1, got $paged_desc_count"
}

set high_alt_count [FlightState search -compare {{> altitude 35000}} -limit 10]
puts "high altitude count (>35000): $high_alt_count"
if {$high_alt_count != 2} {
    error "expected high altitude count 2 after upsert batch, got $high_alt_count"
}

if {[catch {FlightState search -compare {{> altitude}} -limit 5} malformed_err]} {
    puts "malformed query rejected as expected: $malformed_err"
    if {![string match "TCL_ERROR:*" $malformed_err]} {
        error "expected malformed query error to start with TCL_ERROR:, got '$malformed_err'"
    }
} else {
    error "expected malformed query to return TCL_ERROR, but it succeeded"
}

set malformed_raw "{> altitude 10000"
if {[catch {FlightState search -compare $malformed_raw -limit 5} malformed_raw_err]} {
    puts "malformed raw compare rejected as expected: $malformed_raw_err"
    if {![string match "TCL_ERROR:*" $malformed_raw_err]} {
        error "expected malformed raw error to start with TCL_ERROR:, got '$malformed_raw_err'"
    }
} else {
    error "expected malformed raw compare to return TCL_ERROR, but it succeeded"
}

if {[catch {FlightState search -bogus value -limit 5} malformed_opt_err]} {
    puts "unknown search option rejected as expected: $malformed_opt_err"
    if {![string match "TCL_ERROR:*" $malformed_opt_err]} {
        error "expected unknown option error to start with TCL_ERROR:, got '$malformed_opt_err'"
    }
} else {
    error "expected unknown search option to return TCL_ERROR, but it succeeded"
}

# Re-init with the same directory to validate idempotent initialization semantics.
set init_again [aerostore::init $data_dir]
puts "reinit: $init_again"

puts "coverage: toggling aerostore.synchronous_commit on/off"
set initial_mode [aerostore::get_config aerostore.synchronous_commit]
puts "coverage: initial synchronous_commit mode = $initial_mode"

aerostore::set_config aerostore.synchronous_commit off
set mode [aerostore::get_config aerostore.synchronous_commit]
puts "coverage: synchronous_commit now $mode"
if {$mode ne "off"} {
    error "expected synchronous_commit=off after toggle, got $mode"
}

if {[catch {aerostore::checkpoint_now} checkpoint_err]} {
    puts "coverage: checkpoint blocked while async as expected: $checkpoint_err"
    if {![string match "TCL_ERROR:*" $checkpoint_err]} {
        error "expected checkpoint async error to start with TCL_ERROR:, got '$checkpoint_err'"
    }
} else {
    error "expected checkpoint to fail while synchronous_commit=off"
}

aerostore::set_config aerostore.synchronous_commit on
set mode [aerostore::get_config aerostore.synchronous_commit]
puts "coverage: synchronous_commit restored to $mode"
if {$mode ne "on"} {
    error "expected synchronous_commit=on after restore, got $mode"
}

aerostore::set_config aerostore.checkpoint_interval_secs 1
set interval [aerostore::get_config aerostore.checkpoint_interval_secs]
puts "coverage: checkpoint interval now $interval seconds"
if {$interval != 1} {
    error "expected checkpoint interval 1, got $interval"
}

set checkpoint_result [aerostore::checkpoint_now]
puts "coverage: checkpoint result = $checkpoint_result"
