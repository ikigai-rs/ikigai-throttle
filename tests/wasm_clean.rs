//! The library reads no platform time outside its native-only modules
//! (ledger #1507).
//!
//! On wasm32-unknown-unknown, `Instant::now()`, `SystemTime::now()`, a spawned
//! thread and futures-timer's native `Delay` all COMPILE and then fail at runtime.
//! So CI's `wasm32 lib clippy` job cannot see one of them creep back into a
//! governor: it would build green and panic on the first governed call in a
//! browser. This guard is the check that can. Time belongs to a core `Clock`
//! (`with_clock`); the platform is read only in `native_clock` (the native
//! default clock), `native_timer` (the native default timer a `Retry` waits a
//! `RetryAfter` on) and `timeout` (a timer, which a clock cannot be), and all three
//! are declared behind `cfg(not(target_arch = "wasm32"))`.
//!
//! Comment lines are skipped, so prose and doctests (which run natively) may name
//! these; the `#[cfg(test)]` module at the end of a file is skipped for the same
//! reason.

use std::path::Path;

/// Modules that may read the platform, each declared native-only in `lib.rs`.
const NATIVE_ONLY: [&str; 3] = ["native_clock", "native_timer", "timeout"];

/// What compiles on wasm32 and fails there at runtime.
const PLATFORM_TIME: [&str; 5] = [
    "Instant",
    "SystemTime",
    "futures_timer",
    "thread::spawn",
    "thread::sleep",
];

const NATIVE_CFG: &str = "#[cfg(not(target_arch = \"wasm32\"))]";

fn src() -> std::path::PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("src")
}

/// The file's code lines before its `#[cfg(test)]` module, comments dropped.
fn code(text: &str) -> Vec<(usize, &str)> {
    text.lines()
        .enumerate()
        .take_while(|(_, line)| line.trim() != "#[cfg(test)]")
        .filter(|(_, line)| !line.trim_start().starts_with("//"))
        .map(|(n, line)| (n + 1, line))
        .collect()
}

#[test]
fn only_the_native_modules_read_platform_time() {
    let mut offenders = Vec::new();
    for entry in std::fs::read_dir(src()).expect("src/") {
        let file = entry.expect("entry").path();
        let stem = file.file_stem().and_then(|s| s.to_str()).unwrap_or("");
        if file.extension().and_then(|e| e.to_str()) != Some("rs") || NATIVE_ONLY.contains(&stem) {
            continue;
        }
        let text = std::fs::read_to_string(&file).expect("read");
        for (n, line) in code(&text) {
            for token in PLATFORM_TIME {
                if line.contains(token) {
                    offenders.push(format!("src/{stem}.rs:{n}: `{token}` in `{}`", line.trim()));
                }
            }
        }
    }
    assert!(
        offenders.is_empty(),
        "platform time read outside the native-only modules; take it from the \
         governor's core Clock instead:\n{}",
        offenders.join("\n")
    );
}

#[test]
fn the_native_modules_are_left_out_of_a_wasm32_build() {
    let lib = std::fs::read_to_string(src().join("lib.rs")).expect("lib.rs");
    let lines = code(&lib);
    for module in NATIVE_ONLY {
        let declaration = format!("mod {module};");
        let at = lines
            .iter()
            .position(|(_, line)| line.trim() == declaration)
            .unwrap_or_else(|| panic!("lib.rs declares no `{declaration}`"));
        assert!(
            at > 0 && lines[at - 1].1.trim() == NATIVE_CFG,
            "`{declaration}` must sit directly under `{NATIVE_CFG}`, or wasm32 compiles it"
        );
    }
}
