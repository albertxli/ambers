//! Opens an address in the user's browser, with the system's own command.
//! No crate; failing is not an error, since the address is printed too.

use std::process::{Command, Stdio};

/// Ask the system to open `url`. Returns whether a command was started.
pub fn open(url: &str) -> bool {
    let mut command = if cfg!(windows) {
        // Not `cmd /C start`: `&` in the address would need quoting there.
        let mut command = Command::new("rundll32");
        command.args(["url.dll,FileProtocolHandler", url]);
        command
    } else if cfg!(target_os = "macos") {
        let mut command = Command::new("open");
        command.arg(url);
        command
    } else {
        let mut command = Command::new("xdg-open");
        command.arg(url);
        command
    };
    command
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .is_ok()
}
