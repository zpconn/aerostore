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
puts [aerostore::init ./tmp/aerostore_tcl_demo]

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

if {[catch {FlightState search -compare {{> altitude}} -limit 5} malformed_err]} {
    puts "malformed query rejected as expected: $malformed_err"
    if {![string match "TCL_ERROR:*" $malformed_err]} {
        error "expected malformed query error to start with TCL_ERROR:, got '$malformed_err'"
    }
} else {
    error "expected malformed query to return TCL_ERROR, but it succeeded"
}

# Coverage hooks for upcoming synchronous_commit and retry-loop integration.
# These are optional so the script remains compatible with current builds.

if {[llength [info commands aerostore::set_config]] > 0 && [llength [info commands aerostore::get_config]] > 0} {
    puts "coverage: toggling aerostore.synchronous_commit on/off"
    aerostore::set_config aerostore.synchronous_commit off
    set mode [aerostore::get_config aerostore.synchronous_commit]
    puts "coverage: synchronous_commit now $mode"
    aerostore::set_config aerostore.synchronous_commit on
    set mode [aerostore::get_config aerostore.synchronous_commit]
    puts "coverage: synchronous_commit restored to $mode"
} else {
    puts "coverage: skip synchronous_commit hook (aerostore::set_config/get_config not exported yet)"
}

if {[llength [info commands aerostore::simulate_serialization_failure]] > 0} {
    puts "coverage: checking retry-loop serialization failure signal"
    if {[catch {aerostore::simulate_serialization_failure} err]} {
        puts "coverage: serialization failure observed: $err"
    } else {
        puts "coverage: expected serialization failure but command succeeded"
    }
} else {
    puts "coverage: skip retry-loop hook (simulate_serialization_failure not exported yet)"
}
