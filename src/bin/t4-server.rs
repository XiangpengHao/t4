//! The server of a store in shared mode (`doc/02-multi-process.md`). A
//! client starts it when the store has none running; it is not meant to
//! be run by hand.

fn main() -> std::process::ExitCode {
    t4::__server::main()
}
