//! One running server per ambers version.
//!
//! A server writes a small record (port, token, version) into the user's
//! state folder. A second `am open` reads it and, when the versions match,
//! asks that server to open the file instead of starting another one. The
//! record holds the access token, so it is as private as the folder it is
//! in: the user's own.
//!
//! Also here: the few lines of HTTP a second `am open` (and the tests)
//! need to talk to a server on this machine. No HTTP client crate.

use std::io::{self, Read, Write};
use std::net::{Ipv4Addr, SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::time::Duration;
use std::{env, fs};

use serde::{Deserialize, Serialize};

/// The name of the record inside the state folder.
const RECORD: &str = "server.json";

/// What a running server says about itself.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Record {
    pub port: u16,
    pub token: String,
    /// The ambers version of the server. A different version is never
    /// joined: its API may differ.
    pub version: String,
}

/// Where ambers keeps what must outlive one run: the server record now,
/// recent and starred files later. `AMBERS_STATE_DIR` overrides it.
pub fn state_dir() -> Option<PathBuf> {
    let var = |name: &str| env::var_os(name).filter(|value| !value.is_empty());
    if let Some(dir) = var("AMBERS_STATE_DIR") {
        return Some(PathBuf::from(dir));
    }
    if cfg!(windows) {
        var("LOCALAPPDATA").map(|dir| PathBuf::from(dir).join("ambers"))
    } else if cfg!(target_os = "macos") {
        var("HOME").map(|home| PathBuf::from(home).join("Library/Application Support/ambers"))
    } else {
        var("XDG_STATE_HOME")
            .map(|dir| PathBuf::from(dir).join("ambers"))
            .or_else(|| var("HOME").map(|home| PathBuf::from(home).join(".local/state/ambers")))
    }
}

/// The record in `dir`, if there is a readable one.
pub fn read(dir: &Path) -> Option<Record> {
    let text = fs::read_to_string(dir.join(RECORD)).ok()?;
    serde_json::from_str(&text).ok()
}

pub fn write(dir: &Path, record: &Record) -> io::Result<()> {
    fs::create_dir_all(dir)?;
    let text = serde_json::to_string_pretty(record).map_err(io::Error::other)?;
    fs::write(dir.join(RECORD), text)
}

/// Remove the record, but only if it is still `record`: a newer server
/// may have written its own.
pub fn remove(dir: &Path, record: &Record) {
    if read(dir).as_ref() == Some(record) {
        let _ = fs::remove_file(dir.join(RECORD));
    }
}

/// An HTTP answer from a local server.
#[derive(Debug, Clone)]
pub struct Answer {
    pub status: u16,
    /// Header names are lower-case.
    pub headers: Vec<(String, String)>,
    pub body: String,
}

impl Answer {
    pub fn header(&self, name: &str) -> Option<&str> {
        (self.headers.iter())
            .find(|(header, _)| header == name)
            .map(|(_, value)| value.as_str())
    }
}

/// Send one request to a server on this machine and read the answer.
/// `headers` are sent as given; `Host` defaults to the server's own.
pub fn request(
    port: u16,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: Option<&str>,
    timeout: Duration,
) -> io::Result<Answer> {
    let address = SocketAddr::from((Ipv4Addr::LOCALHOST, port));
    let mut stream = TcpStream::connect_timeout(&address, timeout)?;
    stream.set_read_timeout(Some(timeout))?;
    stream.set_write_timeout(Some(timeout))?;

    let mut text = format!("{method} {path} HTTP/1.1\r\nConnection: close\r\n");
    if !headers
        .iter()
        .any(|(name, _)| name.eq_ignore_ascii_case("host"))
    {
        text.push_str(&format!("Host: 127.0.0.1:{port}\r\n"));
    }
    for (name, value) in headers {
        text.push_str(&format!("{name}: {value}\r\n"));
    }
    if let Some(body) = body {
        text.push_str("Content-Type: application/json\r\n");
        text.push_str(&format!("Content-Length: {}\r\n", body.len()));
    }
    text.push_str("\r\n");
    text.push_str(body.unwrap_or(""));
    stream.write_all(text.as_bytes())?;

    let mut raw = Vec::new();
    stream.read_to_end(&mut raw)?;
    let raw = String::from_utf8_lossy(&raw);
    let bad = || io::Error::new(io::ErrorKind::InvalidData, "not an HTTP answer");
    let (head, body) = raw.split_once("\r\n\r\n").ok_or_else(bad)?;
    let mut lines = head.split("\r\n");
    let status = (lines.next().and_then(|line| line.split(' ').nth(1)))
        .and_then(|code| code.parse().ok())
        .ok_or_else(bad)?;
    let headers = lines
        .filter_map(|line| line.split_once(':'))
        .map(|(name, value)| (name.trim().to_ascii_lowercase(), value.trim().to_string()))
        .collect();
    Ok(Answer {
        status,
        headers,
        body: body.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_record_is_written_read_and_removed_only_by_its_owner() {
        let dir = tempfile::tempdir().unwrap();
        let folder = dir.path().join("state");
        assert_eq!(read(&folder), None);
        let mine = Record {
            port: 4000,
            token: "abc".to_string(),
            version: "1.2.3".to_string(),
        };
        write(&folder, &mine).unwrap();
        assert_eq!(read(&folder), Some(mine.clone()));

        // A newer server took over: the old one must not remove its record.
        let newer = Record {
            port: 4001,
            ..mine.clone()
        };
        write(&folder, &newer).unwrap();
        remove(&folder, &mine);
        assert_eq!(read(&folder), Some(newer.clone()));
        remove(&folder, &newer);
        assert_eq!(read(&folder), None);
    }

    #[test]
    fn a_damaged_record_reads_as_none() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join(RECORD), "{ not json").unwrap();
        assert_eq!(read(dir.path()), None);
    }
}
