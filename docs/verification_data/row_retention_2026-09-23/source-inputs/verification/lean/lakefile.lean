import Lake
open Lake DSL

require aeneas from "../../target/verification-tools/aeneas/backends/lean"

package aerostoreProofs where
  moreLeanArgs := #["-DwarningAsError=true"]

@[default_target] lean_lib AerostoreProofs
