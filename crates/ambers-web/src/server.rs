//! The routes, and the checks every request passes first.
//!
//! Survey data goes through this server, so it answers only the person at
//! the machine: it listens on `127.0.0.1`, refuses any `Host` that is not
//! its own (which stops a web page from reaching it through DNS
//! rebinding), refuses a foreign `Origin` on requests that change
//! something, and wants the token it made at start.
//! See `docs/decisions/0005-local-server-security.md`.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::extract::{Path as UrlPath, Query, Request, State};
use axum::http::header::{self, HeaderMap, HeaderValue};
use axum::http::{Method, StatusCode, Uri};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use tokio::sync::Notify;

use crate::api::{self, API};
use crate::assets;
use crate::library::{Library, plain};
use crate::session::{Session, VALUES_PAGE};

/// Everything the routes share.
pub struct App {
    pub token: String,
    pub port: u16,
    /// Names this run of the server; see `api::ServerInfo::instance`.
    instance: String,
    sessions: RwLock<Vec<Arc<Session>>>,
    next_id: AtomicU64,
    /// When a request with the token last arrived.
    last_seen: Mutex<Instant>,
    /// Wakes the server to stop it.
    pub stop: Notify,
    /// Loading limits for files opened without their own.
    limit: Option<usize>,
    memory_gb: Option<f64>,
    /// The files the launch page may list and open.
    pub library: Library,
}

impl App {
    pub fn new(
        token: String,
        port: u16,
        limit: Option<usize>,
        memory_gb: Option<f64>,
        library: Library,
    ) -> App {
        // Not a secret: only different from one start to the next.
        let started = (SystemTime::now().duration_since(UNIX_EPOCH)).map_or(0, |t| t.as_nanos());
        App {
            token,
            port,
            instance: format!("{:x}-{started:x}", std::process::id()),
            sessions: RwLock::new(Vec::new()),
            next_id: AtomicU64::new(1),
            last_seen: Mutex::new(Instant::now()),
            stop: Notify::new(),
            limit,
            memory_gb,
            library,
        }
    }

    /// Open a file, or return the session that already has it. Whether
    /// the caller may open this file is the caller's to check.
    pub fn open(
        &self,
        path: &Path,
        limit: Option<usize>,
        memory_gb: Option<f64>,
    ) -> ambers::error::Result<Arc<Session>> {
        // One name per file, however it was written.
        let path = plain(path);
        let session = {
            let mut sessions = self.sessions.write().unwrap_or_else(|p| p.into_inner());
            if let Some(open) = sessions.iter().find(|session| session.path == path) {
                Arc::clone(open)
            } else {
                let id = format!("d{}", self.next_id.fetch_add(1, Ordering::Relaxed));
                let session = Session::open(
                    id,
                    &path,
                    limit.or(self.limit),
                    memory_gb.or(self.memory_gb),
                )?;
                sessions.push(Arc::clone(&session));
                session
            }
        };
        self.library.opened(&path);
        Ok(session)
    }

    /// Open a file named on a command line: the page may open it again.
    pub fn open_named(
        &self,
        path: &Path,
        limit: Option<usize>,
        memory_gb: Option<f64>,
    ) -> ambers::error::Result<Arc<Session>> {
        let session = self.open(path, limit, memory_gb)?;
        self.library.allow(path);
        Ok(session)
    }

    /// What the launch page shows.
    fn files(&self) -> api::FileList {
        let open: HashMap<PathBuf, String> = (self.sessions().iter())
            .map(|session| (session.path.clone(), session.id.clone()))
            .collect();
        self.library.list(&open)
    }

    fn sessions(&self) -> Vec<Arc<Session>> {
        (self.sessions.read().unwrap_or_else(|p| p.into_inner())).clone()
    }

    fn session(&self, id: &str) -> Result<Arc<Session>, Failure> {
        (self.sessions().into_iter())
            .find(|session| session.id == id)
            .ok_or_else(|| Failure(StatusCode::NOT_FOUND, format!("no open dataset {id}")))
    }

    /// Close a dataset. Its rows are freed when the last request using
    /// them ends, and its loader stops.
    fn close(&self, id: &str) -> bool {
        let mut sessions = self.sessions.write().unwrap_or_else(|p| p.into_inner());
        let before = sessions.len();
        sessions.retain(|session| session.id != id);
        sessions.len() < before
    }

    fn touch(&self) {
        *self.last_seen.lock().unwrap_or_else(|p| p.into_inner()) = Instant::now();
    }

    /// How long ago a page, or anything else with the token, was in touch.
    pub fn quiet_for(&self) -> Duration {
        (self.last_seen.lock().unwrap_or_else(|p| p.into_inner())).elapsed()
    }

    /// The cookie that carries the token. Cookies are not kept apart by
    /// port, so the name has the port in it: two servers do not overwrite
    /// each other's.
    fn cookie_name(&self) -> String {
        format!("ambers_{}", self.port)
    }

    fn own_host(&self, host: &str) -> bool {
        host == format!("127.0.0.1:{}", self.port) || host == format!("localhost:{}", self.port)
    }

    fn own_origin(&self, origin: &str) -> bool {
        (origin.strip_prefix("http://")).is_some_and(|host| self.own_host(host))
    }

    fn info(&self) -> api::ServerInfo {
        api::ServerInfo {
            version: crate::VERSION.to_string(),
            instance: self.instance.clone(),
            interface: assets::has_interface(),
            datasets: self
                .sessions()
                .iter()
                .map(|session| session.info())
                .collect(),
        }
    }
}

/// An error answer: a status and a sentence, sent as JSON.
pub struct Failure(pub StatusCode, pub String);

impl IntoResponse for Failure {
    fn into_response(self) -> Response {
        (self.0, Json(api::ApiError { error: self.1 })).into_response()
    }
}

fn refuse(status: StatusCode, message: &str) -> Response {
    Failure(status, message.to_string()).into_response()
}

pub fn router(app: Arc<App>) -> Router {
    let api = Router::new()
        .route("/server", get(server))
        .route("/heartbeat", post(heartbeat))
        .route("/open", post(open))
        .route("/files", get(files))
        .route("/files/open", post(open_file))
        .route("/files/star", post(star_file))
        .route("/datasets", get(datasets))
        .route("/datasets/{id}", get(dataset).delete(close))
        .route("/datasets/{id}/variables", get(variables))
        .route("/datasets/{id}/rows", post(rows))
        .route("/datasets/{id}/variables/{index}/values", get(values));
    Router::new()
        .nest(API, api)
        .fallback(page)
        .layer(middleware::from_fn_with_state(Arc::clone(&app), guard))
        .with_state(app)
}

// --- the checks ------------------------------------------------------------

/// Same length and same bytes, without stopping at the first difference.
fn same(given: &str, expected: &str) -> bool {
    given.len() == expected.len()
        && (given.bytes().zip(expected.bytes())).fold(0, |differ, (a, b)| differ | (a ^ b)) == 0
}

fn header_text(headers: &HeaderMap, name: header::HeaderName) -> &str {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
}

/// The token sent as `Authorization: Bearer …`, which a page cannot do:
/// its copy of the token is a cookie its scripts cannot read.
fn bearer(headers: &HeaderMap) -> Option<&str> {
    header_text(headers, header::AUTHORIZATION).strip_prefix("Bearer ")
}

fn cookie<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    header_text(headers, header::COOKIE)
        .split(';')
        .filter_map(|pair| pair.trim().split_once('='))
        .find(|(key, _)| *key == name)
        .map(|(_, value)| value)
}

/// The value of `token=` in a query string, and the query without it.
fn split_token(query: &str) -> (Option<&str>, String) {
    let mut token = None;
    let mut rest = Vec::new();
    for pair in query.split('&').filter(|pair| !pair.is_empty()) {
        match pair.strip_prefix("token=") {
            Some(value) => token = Some(value),
            None => rest.push(pair),
        }
    }
    (token, rest.join("&"))
}

async fn guard(State(app): State<Arc<App>>, request: Request, next: Next) -> Response {
    let headers = request.headers();
    if !app.own_host(header_text(headers, header::HOST)) {
        return refuse(
            StatusCode::FORBIDDEN,
            "this server answers only as 127.0.0.1 or localhost",
        );
    }
    let reads = matches!(*request.method(), Method::GET | Method::HEAD);
    if !reads
        && let Some(origin) = headers.get(header::ORIGIN)
        && !app.own_origin(origin.to_str().unwrap_or(""))
    {
        return refuse(
            StatusCode::FORBIDDEN,
            "requests from another site are refused",
        );
    }

    // The link `am open` prints carries the token once. It is traded for
    // a cookie, and the address is shown again without it.
    if reads
        && let (Some(token), rest) = split_token(request.uri().query().unwrap_or(""))
        && same(token, &app.token)
    {
        let mut location = request.uri().path().to_string();
        if !rest.is_empty() {
            location = format!("{location}?{rest}");
        }
        let cookie = format!(
            "{}={}; Path=/; HttpOnly; SameSite=Strict",
            app.cookie_name(),
            app.token
        );
        return (
            StatusCode::SEE_OTHER,
            [(header::LOCATION, location), (header::SET_COOKIE, cookie)],
        )
            .into_response();
    }

    let known = bearer(headers).is_some_and(|token| same(token, &app.token))
        || cookie(headers, &app.cookie_name()).is_some_and(|token| same(token, &app.token));
    if !known {
        return refuse(
            StatusCode::UNAUTHORIZED,
            "open the address `am open` printed: it carries the access token",
        );
    }
    app.touch();
    next.run(request).await
}

// --- the page ----------------------------------------------------------------

/// Everything that is not the API: an embedded file, or the page itself
/// (the front end handles its own addresses).
async fn page(method: Method, uri: Uri) -> Response {
    let path = uri.path();
    if path.starts_with(API) || path.starts_with("/api/") {
        return refuse(StatusCode::NOT_FOUND, "no such API address");
    }
    if !matches!(method, Method::GET | Method::HEAD) {
        return refuse(StatusCode::METHOD_NOT_ALLOWED, "only GET is answered here");
    }
    let (bytes, kind) = assets::file(path).unwrap_or_else(assets::index);
    (
        [
            (header::CONTENT_TYPE, HeaderValue::from_static(kind)),
            // The page changes with the binary; never serve an old copy.
            (header::CACHE_CONTROL, HeaderValue::from_static("no-cache")),
        ],
        bytes,
    )
        .into_response()
}

// --- the API -----------------------------------------------------------------

async fn server(State(app): State<Arc<App>>) -> Json<api::ServerInfo> {
    Json(app.info())
}

/// The page says it is still open. `guard` has already noted the time.
async fn heartbeat() -> StatusCode {
    StatusCode::NO_CONTENT
}

async fn datasets(State(app): State<Arc<App>>) -> Json<Vec<api::DatasetInfo>> {
    Json(app.info().datasets)
}

async fn dataset(
    State(app): State<Arc<App>>,
    UrlPath(id): UrlPath<String>,
) -> Result<Json<api::DatasetInfo>, Failure> {
    Ok(Json(app.session(&id)?.info()))
}

async fn close(State(app): State<Arc<App>>, UrlPath(id): UrlPath<String>) -> StatusCode {
    if app.close(&id) {
        StatusCode::NO_CONTENT
    } else {
        StatusCode::NOT_FOUND
    }
}

/// Run work that reads many cells off the threads that answer requests.
async fn off_thread<T: Send + 'static>(
    work: impl FnOnce() -> T + Send + 'static,
) -> Result<T, Failure> {
    (tokio::task::spawn_blocking(work).await).map_err(|_| {
        Failure(
            StatusCode::INTERNAL_SERVER_ERROR,
            "the request could not be completed".to_string(),
        )
    })
}

async fn variables(
    State(app): State<Arc<App>>,
    UrlPath(id): UrlPath<String>,
) -> Result<Json<api::Variables>, Failure> {
    let session = app.session(&id)?;
    Ok(Json(off_thread(move || session.variables()).await?))
}

async fn rows(
    State(app): State<Arc<App>>,
    UrlPath(id): UrlPath<String>,
    Json(request): Json<api::RowsRequest>,
) -> Result<Json<api::RowsPage>, Failure> {
    let session = app.session(&id)?;
    Ok(Json(off_thread(move || session.rows(&request)).await?))
}

#[derive(Debug, Default, Deserialize)]
struct Paging {
    offset: Option<usize>,
    limit: Option<usize>,
    /// Only values whose code or label contains this.
    search: Option<String>,
    /// The most frequent values first, and no more than this many.
    top: Option<usize>,
}

async fn values(
    State(app): State<Arc<App>>,
    UrlPath((id, index)): UrlPath<(String, usize)>,
    Query(paging): Query<Paging>,
) -> Result<Json<api::ValueTable>, Failure> {
    let session = app.session(&id)?;
    let offset = paging.offset.unwrap_or(0);
    let limit = paging.limit.unwrap_or(VALUES_PAGE).min(VALUES_PAGE * 10);
    let top = paging.top.map(|top| top.min(VALUES_PAGE));
    off_thread(move || session.values(index, offset, limit, paging.search.as_deref(), top))
        .await?
        .map(Json)
        .ok_or_else(|| Failure(StatusCode::NOT_FOUND, format!("no variable {index}")))
}

/// A second `am open` asks this server to open a file, or to list a
/// folder too. Only for the command line: it must carry the token as a
/// bearer header, which a page cannot, so a page cannot name an arbitrary
/// path.
async fn open(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    body: String,
) -> Result<Json<api::OpenAnswer>, Failure> {
    // Who is asking is settled before the request is even read.
    if !bearer(&headers).is_some_and(|token| same(token, &app.token)) {
        return Err(Failure(
            StatusCode::FORBIDDEN,
            "only the ambers command can open a file by its path".to_string(),
        ));
    }
    let request: api::OpenRequest = serde_json::from_str(&body)
        .map_err(|error| Failure(StatusCode::BAD_REQUEST, error.to_string()))?;
    let limit = request.limit.map(|limit| limit as usize);
    let opened = off_thread(move || {
        let path = Path::new(&request.path);
        if path.is_dir() {
            app.library.add_root(path);
            return Ok(None);
        }
        app.open_named(path, limit, request.memory_gb)
            .map(|session| Some(session.info()))
    })
    .await?;
    opened
        .map(|dataset| Json(api::OpenAnswer { dataset }))
        .map_err(|error| Failure(StatusCode::UNPROCESSABLE_ENTITY, error.to_string()))
}

// --- the launch page's files ---------------------------------------------------

async fn files(State(app): State<Arc<App>>) -> Result<Json<api::FileList>, Failure> {
    Ok(Json(off_thread(move || app.files()).await?))
}

/// The page opens a file it was shown. Any other path is refused: the
/// page cannot reach into the disk.
async fn open_file(
    State(app): State<Arc<App>>,
    Json(request): Json<api::FileRequest>,
) -> Result<Json<api::DatasetInfo>, Failure> {
    let opened = off_thread(move || {
        let path = Path::new(&request.path);
        if !app.library.may_open(path) {
            return Err(Failure(
                StatusCode::FORBIDDEN,
                "this file is not among the files ambers was started with".to_string(),
            ));
        }
        app.open(path, None, None)
            .map(|session| session.info())
            .map_err(|error| Failure(StatusCode::UNPROCESSABLE_ENTITY, error.to_string()))
    })
    .await?;
    opened.map(Json)
}

async fn star_file(
    State(app): State<Arc<App>>,
    Json(request): Json<api::StarRequest>,
) -> Result<StatusCode, Failure> {
    let starred =
        off_thread(move || app.library.star(Path::new(&request.path), request.starred)).await?;
    if starred {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(Failure(
            StatusCode::FORBIDDEN,
            "this file is not among the files ambers was started with".to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_token_is_taken_out_of_the_address() {
        assert_eq!(split_token("token=abc"), (Some("abc"), String::new()));
        assert_eq!(
            split_token("dataset=d2&token=abc&x=1"),
            (Some("abc"), "dataset=d2&x=1".to_string())
        );
        assert_eq!(split_token("dataset=d2"), (None, "dataset=d2".to_string()));
        assert_eq!(split_token(""), (None, String::new()));
    }

    #[test]
    fn tokens_compare_whole() {
        assert!(same("abc123", "abc123"));
        assert!(!same("abc124", "abc123"));
        assert!(!same("abc12", "abc123"));
        assert!(!same("", "abc123"));
    }

    #[test]
    fn only_its_own_names_are_its_host() {
        let app = App::new("t".to_string(), 4321, None, None, Library::new(&[], None));
        assert!(app.own_host("127.0.0.1:4321"));
        assert!(app.own_host("localhost:4321"));
        // Another port, another name, or no port at all.
        assert!(!app.own_host("127.0.0.1:4322"));
        assert!(!app.own_host("evil.example:4321"));
        assert!(!app.own_host("127.0.0.1"));
        assert!(!app.own_host(""));
        assert!(app.own_origin("http://localhost:4321"));
        assert!(!app.own_origin("https://localhost:4321"));
        assert!(!app.own_origin("http://evil.example"));
        assert_eq!(app.cookie_name(), "ambers_4321");
    }
}
