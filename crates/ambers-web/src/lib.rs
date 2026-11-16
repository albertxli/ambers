//! The browser viewer behind `am open`.
//!
//! A server on `127.0.0.1` holds the open files (`ambers-dataset`) and
//! answers a small JSON API; the page it serves asks for the rows on
//! screen and never holds the data. The page is embedded in the binary
//! when the front end was built before compiling (`build.rs`), so the
//! command stays one file that needs nothing installed.
//!
//! - [`run`] is the command: join the running server or start one, say
//!   where it is, open the browser, wait.
//! - [`start`] starts a server on a background thread and returns at
//!   once with a [`Handle`]; it does not own the main thread or Ctrl+C,
//!   so a notebook can use it.
//! - [`api`] is the contract with the page; its TypeScript side is
//!   generated from it.

pub mod api;
mod assets;
mod browser;
pub mod instance;
mod library;
mod server;
mod session;

use std::io::{self, Write};
use std::net::{Ipv4Addr, SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

use instance::Record;
use library::Library;
use server::App;

/// The ambers version this server belongs to.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// The first port tried when none is asked for; if it is taken, the next
/// free one after it.
pub const DEFAULT_PORT: u16 = 2627;
/// How many ports after `DEFAULT_PORT` are tried.
const PORT_TRIES: u16 = 100;
/// How long a second `am open` waits for a running server to answer.
const JOIN_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The file could not be opened or is not a readable SPSS file.
    #[error(transparent)]
    Read(#[from] ambers::error::SpssError),
    #[error("{0} is a folder; give a .sav or .zsav file")]
    Folder(PathBuf),
    #[error("port {0} is in use; choose another with --port")]
    PortTaken(u16),
    #[error("no free port from {0} to {1}; choose one with --port")]
    NoPort(u16, u16),
    /// The running server refused to open the file.
    #[error("{0}")]
    Refused(String),
    #[error(transparent)]
    Io(#[from] io::Error),
}

#[derive(Debug, Clone)]
pub struct Options {
    /// The file to open. `None` starts the server with no file.
    pub path: Option<PathBuf>,
    /// The folder whose data files the launch page lists (with its
    /// sub-folders). `None`: the page lists only files opened before.
    pub root: Option<PathBuf>,
    /// The port to listen on. `None`: `DEFAULT_PORT`, or the next free
    /// one. `Some(0)`: any free port.
    pub port: Option<u16>,
    /// Open the address in the browser.
    pub browser: bool,
    /// Rows to load at most.
    pub limit: Option<usize>,
    /// Memory to load at most, in GB; by default half of what is free.
    pub memory_gb: Option<f64>,
    /// Stop when no page has been in touch for this long. `None`: run
    /// until stopped.
    pub idle: Option<Duration>,
    /// Where the record of the running server is kept. `None`: nowhere,
    /// so no other `am open` can join this server.
    pub state_dir: Option<PathBuf>,
    /// Stop on Ctrl+C (and Ctrl+Break, SIGTERM). Only for a process that
    /// exists to run the server.
    pub signals: bool,
}

impl Default for Options {
    fn default() -> Options {
        Options {
            path: None,
            root: None,
            port: None,
            browser: false,
            limit: None,
            memory_gb: None,
            idle: Some(Duration::from_secs(180)),
            state_dir: None,
            signals: false,
        }
    }
}

/// A running server.
pub struct Handle {
    app: Arc<App>,
    thread: JoinHandle<()>,
    /// The dataset opened at start, when a file was given.
    pub dataset: Option<String>,
}

impl Handle {
    pub fn port(&self) -> u16 {
        self.app.port
    }

    pub fn token(&self) -> &str {
        &self.app.token
    }

    /// The address to open: it carries the token, which the server trades
    /// for a cookie on the first visit.
    pub fn url(&self) -> String {
        address(self.app.port, &self.app.token, self.dataset.as_deref())
    }

    /// Ask the server to stop; `wait` returns when it has.
    pub fn stop(&self) {
        self.app.stop.notify_one();
    }

    /// Block until the server has stopped: Ctrl+C, no page for the idle
    /// time, or `stop`.
    pub fn wait(self) {
        let _ = self.thread.join();
    }
}

fn address(port: u16, token: &str, dataset: Option<&str>) -> String {
    let dataset = dataset.map_or(String::new(), |id| format!("&dataset={id}"));
    format!("http://127.0.0.1:{port}/?token={token}{dataset}")
}

/// 32 hexadecimal characters from the system's random source.
///
/// For front-end development only, `AMBERS_DEV_TOKEN` fixes the token, so
/// the Vite dev server can be configured once to pass requests on (see
/// `web/vite.config.ts`). Every other check stays in force.
fn new_token() -> io::Result<String> {
    if let Some(fixed) = std::env::var_os("AMBERS_DEV_TOKEN").filter(|token| !token.is_empty()) {
        return Ok(fixed.to_string_lossy().into_owned());
    }
    let mut bytes = [0u8; 16];
    getrandom::fill(&mut bytes).map_err(io::Error::other)?;
    Ok(bytes.iter().map(|byte| format!("{byte:02x}")).collect())
}

/// Listen on this machine only.
fn bind(port: Option<u16>) -> Result<TcpListener, Error> {
    let listen = |port: u16| TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, port)));
    match port {
        Some(port) => listen(port).map_err(|_| Error::PortTaken(port)),
        None => {
            let last = DEFAULT_PORT + PORT_TRIES - 1;
            (DEFAULT_PORT..=last)
                .find_map(|port| listen(port).ok())
                .ok_or(Error::NoPort(DEFAULT_PORT, last))
        }
    }
}

/// A path as the server should be given it: absolute, and a file.
fn file_path(path: &Path) -> Result<PathBuf, Error> {
    if path.is_dir() {
        return Err(Error::Folder(path.to_path_buf()));
    }
    Ok(std::path::absolute(path)?)
}

/// Start a server on a background thread. Returns once it is listening
/// and, when a file was given, that file is open and loading.
pub fn start(options: &Options) -> Result<Handle, Error> {
    let listener = bind(options.port)?;
    let port = listener.local_addr()?.port();
    let roots: Vec<PathBuf> = options.root.iter().cloned().collect();
    let library = Library::new(&roots, options.state_dir.clone());
    let app = Arc::new(App::new(
        new_token()?,
        port,
        options.limit,
        options.memory_gb,
        library,
    ));
    // Before anything is announced: an unreadable file is an error now,
    // not a page that says so.
    let dataset = match &options.path {
        Some(path) => Some(app.open_named(&file_path(path)?, None, None)?.id.clone()),
        None => None,
    };

    listener.set_nonblocking(true)?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_name("ambers-web")
        .enable_all()
        .build()?;
    let record = options.state_dir.as_ref().map(|dir| {
        let record = Record {
            port,
            token: app.token.clone(),
            version: VERSION.to_string(),
        };
        // Without the record the server still works; it just cannot be
        // joined.
        let _ = instance::write(dir, &record);
        (dir.clone(), record)
    });

    let (serving, idle, signals) = (Arc::clone(&app), options.idle, options.signals);
    let thread = std::thread::Builder::new()
        .name("ambers-web".to_string())
        .spawn(move || {
            let _ = runtime.block_on(serve(listener, serving, idle, signals));
            if let Some((dir, record)) = record {
                instance::remove(&dir, &record);
            }
        })?;
    Ok(Handle {
        app,
        thread,
        dataset,
    })
}

async fn serve(
    listener: TcpListener,
    app: Arc<App>,
    idle: Option<Duration>,
    signals: bool,
) -> io::Result<()> {
    let listener = tokio::net::TcpListener::from_std(listener)?;
    let router = server::router(Arc::clone(&app));
    let (stopping, stopped) = tokio::sync::oneshot::channel::<()>();
    let stop = async move {
        tokio::select! {
            () = app.stop.notified() => {}
            () = quiet(&app, idle) => {}
            () = interrupted(signals) => {}
        }
        let _ = stopping.send(());
    };
    tokio::select! {
        served = axum::serve(listener, router).with_graceful_shutdown(stop) => served,
        // A browser may keep a connection open; do not wait on it for long.
        () = async {
            let _ = stopped.await;
            tokio::time::sleep(Duration::from_secs(2)).await;
        } => Ok(()),
    }
}

/// Returns when no page has been in touch for `idle`. Never, without one.
async fn quiet(app: &App, idle: Option<Duration>) {
    let Some(idle) = idle else {
        return std::future::pending().await;
    };
    let step = (idle / 4).clamp(Duration::from_millis(20), Duration::from_secs(5));
    while app.quiet_for() < idle {
        tokio::time::sleep(step).await;
    }
}

/// Returns on Ctrl+C, and on the other ways a terminal or the system asks
/// a program to end. Never, when `signals` is off.
async fn interrupted(signals: bool) {
    if !signals {
        return std::future::pending().await;
    }
    #[cfg(windows)]
    {
        let mut ctrl_break = tokio::signal::windows::ctrl_break().ok();
        let on_break = async {
            match ctrl_break.as_mut() {
                Some(signal) => drop(signal.recv().await),
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            () = on_break => {}
        }
    }
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut terminate = signal(SignalKind::terminate()).ok();
        let on_terminate = async {
            match terminate.as_mut() {
                Some(signal) => drop(signal.recv().await),
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            () = on_terminate => {}
        }
    }
    #[cfg(not(any(windows, unix)))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

/// What a look at the record of a running server came to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Join {
    /// A server of this version is running and has the file open now.
    /// The address shows it.
    Joined(String),
    /// A server of another ambers version is running; it is left alone.
    OtherVersion { version: String, port: u16 },
    /// No server to join.
    None,
}

/// Look for a running server and, when it is this version, ask it to
/// open the file. A record whose server is gone is removed.
pub fn join(options: &Options) -> Result<Join, Error> {
    let Some(dir) = &options.state_dir else {
        return Ok(Join::None);
    };
    let Some(record) = instance::read(dir) else {
        return Ok(Join::None);
    };
    let authorization = format!("Bearer {}", record.token);
    let headers = [("Authorization", authorization.as_str())];
    let ask = |method: &str, path: &str, body: Option<&str>| {
        instance::request(record.port, method, path, &headers, body, JOIN_TIMEOUT)
    };

    // Is it still there, and is it what the record says? Only its version
    // is read: a server of another ambers version may describe itself with
    // other fields, and must still be recognised as running.
    #[derive(serde::Deserialize)]
    struct Running {
        version: String,
    }
    let alive = match ask("GET", &format!("{}/server", api::API), None) {
        Ok(answer) if answer.status == 200 => serde_json::from_str::<Running>(&answer.body).ok(),
        _ => None,
    };
    let Some(running) = alive else {
        instance::remove(dir, &record);
        return Ok(Join::None);
    };
    if running.version != VERSION {
        return Ok(Join::OtherVersion {
            version: running.version,
            port: record.port,
        });
    }

    // A file to open there, or else a folder for its launch page to list.
    let named = match &options.path {
        Some(path) => Some(file_path(path)?),
        None => match &options.root {
            Some(root) => Some(std::path::absolute(root)?),
            None => None,
        },
    };
    let dataset = match named {
        None => None,
        Some(path) => {
            let request = api::OpenRequest {
                path: path.display().to_string(),
                limit: options
                    .limit
                    .map(|limit| limit.min(u32::MAX as usize) as u32),
                memory_gb: options.memory_gb,
            };
            let body = serde_json::to_string(&request).map_err(io::Error::other)?;
            let answer = ask("POST", &format!("{}/open", api::API), Some(&body))?;
            if answer.status != 200 {
                let said = serde_json::from_str::<api::ApiError>(&answer.body)
                    .map_or(answer.body, |error| error.error);
                return Err(Error::Refused(said));
            }
            let opened: api::OpenAnswer =
                serde_json::from_str(&answer.body).map_err(io::Error::other)?;
            opened.dataset.map(|info| info.id)
        }
    };
    Ok(Join::Joined(address(
        record.port,
        &record.token,
        dataset.as_deref(),
    )))
}

/// The `am open` command: show the file in the browser, through the
/// server that is already running or a new one. Returns when there is
/// nothing more to do: at once after joining, or when the new server
/// stops.
pub fn run(options: &Options, out: &mut dyn Write) -> Result<(), Error> {
    match join(options)? {
        Join::Joined(url) => {
            writeln!(out, "Opened in the ambers server that is already running:")?;
            writeln!(out, "  {url}")?;
            if options.browser {
                browser::open(&url);
            }
            return Ok(());
        }
        Join::OtherVersion { version, port } => {
            writeln!(
                out,
                "A server from ambers {version} is also running on port {port}; \
                 starting one for ambers {VERSION}."
            )?;
        }
        Join::None => {}
    }

    let handle = start(options)?;
    let url = handle.url();
    writeln!(out, "ambers {VERSION} is serving on this machine only:")?;
    writeln!(out, "  {url}")?;
    if !assets::has_interface() {
        writeln!(
            out,
            "This build does not include the full browser interface; a basic page is shown."
        )?;
    }
    match options.idle {
        Some(_) => writeln!(
            out,
            "Press Ctrl+C to stop. It also stops a few minutes after the page is closed."
        )?,
        None => writeln!(out, "Press Ctrl+C to stop.")?,
    }
    out.flush()?;
    if options.browser {
        browser::open(&url);
    }
    handle.wait();
    Ok(())
}

/// Whether the full front end is built into this binary.
pub fn has_interface() -> bool {
    assets::has_interface()
}
