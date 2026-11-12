//! Putting text on the system clipboard without a clipboard library.
//!
//! The operating system's own command is tried first (`clip` on Windows,
//! `pbcopy` on macOS, `wl-copy` / `xclip` / `xsel` on Linux). When none
//! works, the caller falls back to the terminal's clipboard escape
//! sequence (OSC 52), which some terminals honour and others ignore.

use std::io::Write;
use std::process::{Command, Stdio};

/// Copy `text` with the system's clipboard command. Returns whether one
/// of them accepted it.
pub fn copy(text: &str) -> bool {
    commands()
        .iter()
        .any(|(program, args)| pipe_to(program, args, &encode(text)))
}

/// The clipboard commands to try, in order.
fn commands() -> &'static [(&'static str, &'static [&'static str])] {
    if cfg!(windows) {
        &[("clip", &[])]
    } else if cfg!(target_os = "macos") {
        &[("pbcopy", &[])]
    } else {
        &[
            ("wl-copy", &[]),
            ("xclip", &["-selection", "clipboard"]),
            ("xsel", &["--clipboard", "--input"]),
        ]
    }
}

/// The bytes a clipboard command expects. Windows' `clip` reads UTF-16
/// without a byte-order mark (a mark would be pasted as a character) and
/// Windows programs expect CRLF line ends; the others read UTF-8.
fn encode(text: &str) -> Vec<u8> {
    if cfg!(windows) {
        (text.replace('\n', "\r\n").encode_utf16())
            .flat_map(u16::to_le_bytes)
            .collect()
    } else {
        text.as_bytes().to_vec()
    }
}

fn pipe_to(program: &str, args: &[&str], bytes: &[u8]) -> bool {
    let Ok(mut child) = Command::new(program)
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
    else {
        return false;
    };
    // Dropping the pipe after writing lets the command see the end of its input.
    let written = child
        .stdin
        .take()
        .is_some_and(|mut stdin| stdin.write_all(bytes).is_ok());
    child.wait().is_ok_and(|status| status.success()) && written
}

/// The escape sequence that asks the terminal to put `text` on the
/// clipboard (OSC 52).
pub fn escape_sequence(text: &str) -> String {
    format!("\x1b]52;c;{}\x07", base64(text.as_bytes()))
}

/// Standard base64 with padding.
fn base64(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut encoded = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let group = (u32::from(chunk[0]) << 16)
            | (u32::from(*chunk.get(1).unwrap_or(&0)) << 8)
            | u32::from(*chunk.get(2).unwrap_or(&0));
        for position in 0..4 {
            if position <= chunk.len() {
                let index = (group >> (18 - 6 * position)) & 0x3f;
                encoded.push(ALPHABET[index as usize] as char);
            } else {
                encoded.push('=');
            }
        }
    }
    encoded
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base64_matches_the_standard_vectors() {
        // RFC 4648, section 10.
        for (plain, encoded) in [
            ("", ""),
            ("f", "Zg=="),
            ("fo", "Zm8="),
            ("foo", "Zm9v"),
            ("foob", "Zm9vYg=="),
            ("fooba", "Zm9vYmE="),
            ("foobar", "Zm9vYmFy"),
        ] {
            assert_eq!(base64(plain.as_bytes()), encoded, "{plain:?}");
        }
    }

    #[test]
    fn the_escape_sequence_carries_utf8() {
        assert_eq!(escape_sequence("Gender"), "\x1b]52;c;R2VuZGVy\x07");
        // "é" is two bytes in UTF-8.
        assert_eq!(escape_sequence("é"), "\x1b]52;c;w6k=\x07");
    }

    #[cfg(windows)]
    #[test]
    fn windows_gets_utf16_with_crlf_and_no_byte_order_mark() {
        assert_eq!(encode("a\nb"), [b'a', 0, b'\r', 0, b'\n', 0, b'b', 0]);
        // U+6E80, little-endian.
        assert_eq!(encode("満"), [0x80, 0x6e]);
    }

    #[test]
    fn a_missing_command_is_not_a_copy() {
        assert!(!pipe_to("ambers-no-such-clipboard-command", &[], b"x"));
    }
}
