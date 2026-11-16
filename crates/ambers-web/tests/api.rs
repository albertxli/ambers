//! The server as a program sees it: started on a free port, asked over
//! HTTP, on the committed fixtures in `tests/data/`.

use std::fs;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use ambers_web::instance::{self, Answer, Record};
use ambers_web::{Handle, Join, Options, api, join, start};
use serde::de::DeserializeOwned;

const WAIT: Duration = Duration::from_secs(10);

fn fixture(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/data")
        .join(name)
}

/// A server on a free port that runs until the test stops it.
fn serve(file: Option<&str>) -> Handle {
    start(&Options {
        path: file.map(fixture),
        port: Some(0),
        idle: None,
        ..Options::default()
    })
    .unwrap()
}

/// A request as the command line makes it: with the token as a bearer.
fn ask(handle: &Handle, method: &str, path: &str, body: Option<&str>) -> Answer {
    let authorization = format!("Bearer {}", handle.token());
    let headers = [("Authorization", authorization.as_str())];
    instance::request(handle.port(), method, path, &headers, body, WAIT).unwrap()
}

fn get<T: DeserializeOwned>(handle: &Handle, path: &str) -> T {
    let answer = ask(handle, "GET", path, None);
    assert_eq!(answer.status, 200, "{path}: {}", answer.body);
    serde_json::from_str(&answer.body).unwrap()
}

fn post<T: DeserializeOwned>(handle: &Handle, path: &str, body: &str) -> T {
    let answer = ask(handle, "POST", path, Some(body));
    assert_eq!(answer.status, 200, "{path}: {}", answer.body);
    serde_json::from_str(&answer.body).unwrap()
}

/// Wait until the dataset has loaded, and return what it says then.
fn loaded(handle: &Handle, id: &str) -> api::DatasetInfo {
    let started = Instant::now();
    loop {
        let info: api::DatasetInfo = get(handle, &format!("/api/v1/datasets/{id}"));
        if info.load != api::LoadState::Loading {
            return info;
        }
        assert!(started.elapsed() < WAIT, "still loading: {info:?}");
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// The texts of a page of rows, row by row.
fn texts(page: &api::RowsPage) -> Vec<Vec<&str>> {
    (page.rows.iter())
        .map(|row| row.cells.iter().map(|cell| cell.text.as_str()).collect())
        .collect()
}

fn rows(handle: &Handle, body: &str) -> api::RowsPage {
    post(handle, "/api/v1/datasets/d1/rows", body)
}

/// The position of a variable in `survey.sav`.
fn variable(handle: &Handle, name: &str) -> u32 {
    let all: api::Variables = get(handle, "/api/v1/datasets/d1/variables");
    (all.variables.iter())
        .find(|variable| variable.name == name)
        .unwrap_or_else(|| panic!("no variable {name}"))
        .index
}

#[test]
fn a_file_given_at_start_is_open_and_described() {
    let handle = serve(Some("survey.sav"));
    assert_eq!(handle.dataset.as_deref(), Some("d1"));
    assert!(
        handle
            .url()
            .ends_with(&format!("/?token={}&dataset=d1", handle.token()))
    );

    let info = loaded(&handle, "d1");
    assert_eq!(info.name, "survey.sav");
    assert_eq!(
        (info.rows, info.total_rows, info.variables),
        (20, Some(20), 13)
    );
    assert_eq!(info.load, api::LoadState::Done);
    assert_eq!(info.label, "ambers synthetic survey fixture");
    assert_eq!(info.counted_rows, 20);
    assert!(info.warnings.is_empty());

    let server: api::ServerInfo = get(&handle, "/api/v1/server");
    assert_eq!(server.version, env!("CARGO_PKG_VERSION"));
    assert_eq!(server.datasets.len(), 1);

    let all: api::Variables = get(&handle, "/api/v1/datasets/d1/variables");
    assert_eq!(all.variables.len(), 13);
    let gender = &all.variables[1];
    assert_eq!(
        (gender.name.as_str(), gender.kind.as_str()),
        ("gender", "categorical")
    );
    assert_eq!(gender.label, "Gender");
    assert_eq!((gender.valid, gender.missing_count), (20, 0));
    assert!(gender.values.iter().any(|value| value.label == "Male"));
    assert_eq!(gender.label_check, "ok");
    let city = all.variables.iter().find(|one| one.name == "city").unwrap();
    assert_eq!((city.kind.as_str(), city.numeric), ("text", false));
    handle.stop();
    handle.wait();
}

#[test]
fn rows_come_as_a_window_in_the_mode_asked_for() {
    let handle = serve(Some("survey.sav"));
    loaded(&handle, "d1");
    let (id, gender, city) = (
        variable(&handle, "id"),
        variable(&handle, "gender"),
        variable(&handle, "city"),
    );
    let window = |offset: u32, limit: u32, mode: &str| {
        rows(
            &handle,
            &format!(
                r#"{{"offset":{offset},"limit":{limit},"variables":[{id},{gender},{city}],"mode":"{mode}"}}"#
            ),
        )
    };

    let page = window(0, 3, "labels");
    assert_eq!((page.offset, page.shown, page.loaded), (0, 20, 20));
    assert_eq!(page.variables, [id, gender, city]);
    assert_eq!(
        texts(&page),
        [
            ["1", "Male", "Lisbon"],
            ["2", "Female", "Osaka"],
            ["3", "Male", "Lima"]
        ]
    );
    let numbers: Vec<u32> = page.rows.iter().map(|row| row.number).collect();
    assert_eq!(numbers, [1, 2, 3]);

    // Across the loader's batches, and past the end.
    let page = window(18, 5, "codes");
    assert_eq!(texts(&page).len(), 2);
    assert_eq!(page.rows[0].number, 19);
    assert_eq!(page.rows[0].cells[1].text, "1.00");
    let page = window(1, 1, "both");
    assert_eq!(page.rows[0].cells[1].text, "2.00 Female");
    assert!(window(500, 5, "labels").rows.is_empty());

    // With no variables named, every variable comes.
    let page = rows(&handle, r#"{"offset":0,"limit":1}"#);
    assert_eq!(page.rows[0].cells.len(), 13);
    handle.stop();
    handle.wait();
}

#[test]
fn filters_narrow_the_rows_and_stack() {
    let handle = serve(Some("survey.sav"));
    loaded(&handle, "d1");
    let (id, city) = (variable(&handle, "id"), variable(&handle, "city"));
    let satisfaction = variable(&handle, "satisfaction");
    let filter = |variable: u32, operator: &str, value: &str| {
        format!(r#"{{"variable":{variable},"operator":"{operator}","value":"{value}"}}"#)
    };
    let numbers = |filters: &[String], mode: &str| {
        let body = format!(
            r#"{{"offset":0,"limit":50,"variables":[{id}],"filters":[{}],"filter_mode":"{mode}"}}"#,
            filters.join(",")
        );
        let page = rows(&handle, &body);
        let numbers: Vec<u32> = page.rows.iter().map(|row| row.number).collect();
        (page.shown, numbers, page.problems)
    };

    let lisbon = filter(city, "starts_with", "lis");
    let (shown, kept, problems) = numbers(std::slice::from_ref(&lisbon), "all");
    assert_eq!((shown, kept), (5, vec![1, 5, 9, 13, 17]));
    assert!(problems.is_empty());
    // Asked again, the kept rows are reused, and a window of them works.
    // Without a mode, a row must pass every filter.
    let body = format!(r#"{{"offset":3,"limit":10,"variables":[{id}],"filters":[{lisbon}]}}"#);
    let page = rows(&handle, &body);
    assert_eq!((page.offset, page.shown), (3, 5));
    assert_eq!(texts(&page), [["13"], ["17"]]);

    // Two conditions: every one of them, or any of them.
    let two = [lisbon.clone(), filter(id, "greater_than", "10")];
    assert_eq!(numbers(&two, "all").1, [13, 17]);
    assert_eq!(
        numbers(&two, "any").1,
        [1, 5, 9, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20]
    );

    // A missing value passes only a condition about missing values.
    let refused = [filter(satisfaction, "not_equals", "Refused")];
    assert_eq!(numbers(&refused, "all").0, 14);
    let empty = format!(r#"{{"variable":{satisfaction},"operator":"is_empty"}}"#);
    assert_eq!(numbers(&[empty], "all").1, [7, 14]);
    // A pattern, with and without case.
    assert_eq!(numbers(&[filter(city, "like", "l%")], "all").0, 0);
    assert_eq!(numbers(&[filter(city, "ilike", "l%")], "all").0, 10);

    // A condition that cannot be used is reported by its place; the
    // others still apply.
    let half = [
        lisbon,
        filter(id, "greater_than", ""),
        filter(city, "less_than", "3"),
        filter(99, "equals", "x"),
    ];
    let (shown, _, problems) = numbers(&half, "all");
    assert_eq!(shown, 5);
    let said: Vec<(u32, &str)> = (problems.iter())
        .map(|one| (one.filter, one.message.as_str()))
        .collect();
    assert_eq!(
        said,
        [
            (1, "enter a value"),
            (2, "city is text: \"less than\" does not apply"),
            (3, "no such variable")
        ]
    );
    // An operator the server does not know is refused outright.
    let unknown = format!(
        r#"{{"offset":0,"limit":5,"filters":[{{"variable":{id},"operator":"sounds_like","value":"x"}}]}}"#
    );
    let answer = ask(&handle, "POST", "/api/v1/datasets/d1/rows", Some(&unknown));
    assert_eq!(answer.status, 422, "{}", answer.body);
    handle.stop();
    handle.wait();
}

#[test]
fn a_variable_says_which_conditions_it_offers() {
    let handle = serve(Some("survey.sav"));
    loaded(&handle, "d1");
    let all: api::Variables = get(&handle, "/api/v1/datasets/d1/variables");
    let kind = |name: &str| {
        let found = all.variables.iter().find(|one| one.name == name).unwrap();
        found.filter_kind
    };
    assert_eq!(kind("id"), api::FilterKind::Number);
    assert_eq!(kind("gender"), api::FilterKind::Categorical);
    assert_eq!(kind("city"), api::FilterKind::Text);
    assert_eq!(kind("interview_date"), api::FilterKind::Date);

    let offered = |kind: api::FilterKind| {
        let found = all.operators.iter().find(|one| one.kind == kind).unwrap();
        found.operators.clone()
    };
    assert_eq!(all.operators.len(), 4);
    assert_eq!(offered(api::FilterKind::Categorical).len(), 14);
    let number = offered(api::FilterKind::Number);
    assert!(number.contains(&api::FilterOperator::GreaterThan));
    assert!(!number.contains(&api::FilterOperator::Contains));
    let text = offered(api::FilterKind::Text);
    assert!(text.contains(&api::FilterOperator::Ilike));
    assert!(!text.contains(&api::FilterOperator::LessOrEqual));
    handle.stop();
    handle.wait();
}

#[test]
fn a_value_table_lists_every_value_with_its_count() {
    let handle = serve(Some("survey.sav"));
    loaded(&handle, "d1");
    let table = |name: &str, paging: &str| -> api::ValueTable {
        let index = variable(&handle, name);
        get(
            &handle,
            &format!("/api/v1/datasets/d1/variables/{index}/values{paging}"),
        )
    };

    let gender = table("gender", "");
    assert!(gender.categorical && gender.has_labels);
    assert_eq!(gender.rows_loaded, 20);
    let male = gender.rows.iter().find(|row| row.label == "Male").unwrap();
    assert_eq!((male.count, male.share_valid), (10, Some(0.5)));
    assert_eq!(male.kind, api::ValueKind::Plain);

    // A text variable: its distinct values, counted from the rows.
    let city = table("city", "");
    assert!(!city.categorical);
    let lisbon = city.rows.iter().find(|row| row.code == "Lisbon").unwrap();
    assert_eq!(lisbon.count, 5);
    // One page of the table.
    let page = table("city", "?offset=1&limit=2");
    assert_eq!(
        (page.offset, page.rows.len(), page.total),
        (1, 2, city.total)
    );
    assert_eq!(page.rows[0].code, city.rows[1].code);

    // What the filter bar suggests: the most frequent values that contain
    // what was typed, by code or by label, without the missing row.
    let top = table("satisfaction", "?top=3");
    assert_eq!(top.rows.len(), 3);
    assert!(
        top.rows
            .iter()
            .all(|row| row.kind != api::ValueKind::Missing)
    );
    assert!(top.rows[0].count >= top.rows[1].count && top.rows[1].count >= top.rows[2].count);
    let found = table("satisfaction", "?search=DISSAT&top=5");
    let labels: Vec<&str> = found.rows.iter().map(|row| row.label.as_str()).collect();
    assert_eq!(found.total, 2);
    assert!(labels.contains(&"Very dissatisfied") && labels.contains(&"Dissatisfied"));
    let by_code = table("city", "?search=lis&top=5");
    assert_eq!(
        (by_code.total, by_code.rows[0].code.as_str()),
        (1, "Lisbon")
    );
    assert_eq!(by_code.rows[0].count, 5);
    assert_eq!(table("city", "?search=zzz&top=5").rows.len(), 0);

    let missing = ask(
        &handle,
        "GET",
        "/api/v1/datasets/d1/variables/99/values",
        None,
    );
    assert_eq!(missing.status, 404);
    handle.stop();
    handle.wait();
}

#[test]
fn only_its_own_host_with_the_token_is_answered() {
    let handle = serve(Some("survey.sav"));
    let port = handle.port();
    let bearer = format!("Bearer {}", handle.token());
    let send = |method: &str, path: &str, headers: &[(&str, &str)]| {
        let body = (method == "POST").then_some("{}");
        instance::request(port, method, path, headers, body, WAIT).unwrap()
    };
    let server = "/api/v1/server";

    // DNS rebinding: the request reaches this port under another name.
    let rebound = send(
        "GET",
        server,
        &[("Host", "evil.example"), ("Authorization", &bearer)],
    );
    assert_eq!(rebound.status, 403, "{}", rebound.body);
    let other_port = format!("127.0.0.1:{}", port.wrapping_add(1));
    assert_eq!(
        send(
            "GET",
            server,
            &[("Host", &other_port), ("Authorization", &bearer)]
        )
        .status,
        403
    );
    let localhost = format!("localhost:{port}");
    assert_eq!(
        send(
            "GET",
            server,
            &[("Host", &localhost), ("Authorization", &bearer)]
        )
        .status,
        200
    );

    // No token, or a wrong one.
    assert_eq!(send("GET", server, &[]).status, 401);
    assert_eq!(send("GET", "/", &[]).status, 401);
    assert_eq!(
        send("GET", server, &[("Authorization", "Bearer nope")]).status,
        401
    );
    assert_eq!(send("GET", "/?token=nope", &[]).status, 401);

    // The printed address trades the token for a cookie and drops it
    // from the address; the cookie then opens the page and the API.
    let first = send(
        "GET",
        &format!("/?token={}&dataset=d1", handle.token()),
        &[],
    );
    assert_eq!(first.status, 303);
    assert_eq!(first.header("location"), Some("/?dataset=d1"));
    let cookie = first.header("set-cookie").unwrap();
    assert!(
        cookie.contains("HttpOnly") && cookie.contains("SameSite=Strict"),
        "{cookie}"
    );
    let cookie = cookie.split(';').next().unwrap();
    assert_eq!(cookie, format!("ambers_{port}={}", handle.token()));
    let page = send("GET", "/?dataset=d1", &[("Cookie", cookie)]);
    assert_eq!(page.status, 200);
    assert!(
        page.header("content-type")
            .unwrap()
            .starts_with("text/html")
    );
    assert!(page.body.contains("<html"), "{}", page.body);
    assert_eq!(send("GET", server, &[("Cookie", cookie)]).status, 200);

    // A request that changes something must not come from another site.
    let heartbeat = "/api/v1/heartbeat";
    let own = format!("http://127.0.0.1:{port}");
    assert_eq!(
        send("POST", heartbeat, &[("Cookie", cookie), ("Origin", &own)]).status,
        204
    );
    let foreign = [("Cookie", cookie), ("Origin", "http://evil.example")];
    assert_eq!(send("POST", heartbeat, &foreign).status, 403);

    // A page (cookie only) cannot open a file by its path.
    let open = "/api/v1/open";
    assert_eq!(send("POST", open, &[("Cookie", cookie)]).status, 403);
    // Unknown addresses under the API are not the page.
    assert_eq!(
        send("GET", "/api/v1/nothing", &[("Cookie", cookie)]).status,
        404
    );
    handle.stop();
    handle.wait();
}

#[test]
fn a_dataset_can_be_closed() {
    let handle = serve(Some("survey.sav"));
    loaded(&handle, "d1");
    assert_eq!(
        ask(&handle, "DELETE", "/api/v1/datasets/d1", None).status,
        204
    );
    assert_eq!(ask(&handle, "GET", "/api/v1/datasets/d1", None).status, 404);
    assert_eq!(
        ask(&handle, "DELETE", "/api/v1/datasets/d1", None).status,
        404
    );
    let server: api::ServerInfo = get(&handle, "/api/v1/server");
    assert!(server.datasets.is_empty());
    handle.stop();
    handle.wait();
}

#[test]
fn the_server_stops_when_no_page_is_in_touch() {
    let state = tempfile::tempdir().unwrap();
    let handle = start(&Options {
        port: Some(0),
        idle: Some(Duration::from_millis(300)),
        state_dir: Some(state.path().to_path_buf()),
        ..Options::default()
    })
    .unwrap();
    let port = handle.port();
    assert_eq!(
        instance::read(state.path()).map(|record| record.port),
        Some(port)
    );

    // A request with the token counts as being in touch.
    std::thread::sleep(Duration::from_millis(200));
    assert_eq!(
        ask(&handle, "POST", "/api/v1/heartbeat", Some("{}")).status,
        204
    );

    let (done, waiting) = mpsc::channel();
    let started = Instant::now();
    std::thread::spawn(move || {
        handle.wait();
        let _ = done.send(());
    });
    waiting
        .recv_timeout(WAIT)
        .expect("the server should stop by itself");
    assert!(
        started.elapsed() >= Duration::from_millis(250),
        "it was just in touch"
    );
    // Its record is gone, and so is the port.
    assert_eq!(instance::read(state.path()), None);
    assert!(instance::request(port, "GET", "/", &[], None, Duration::from_secs(1)).is_err());
}

#[test]
fn a_second_open_joins_the_running_server() {
    let state = tempfile::tempdir().unwrap();
    let options = Options {
        port: Some(0),
        idle: None,
        state_dir: Some(state.path().to_path_buf()),
        ..Options::default()
    };
    // Nothing is running yet.
    assert_eq!(join(&options).unwrap(), Join::None);

    let handle = start(&options).unwrap();
    let second = Options {
        path: Some(fixture("survey.sav")),
        ..options.clone()
    };
    let expected = format!(
        "http://127.0.0.1:{}/?token={}&dataset=d1",
        handle.port(),
        handle.token()
    );
    assert_eq!(join(&second).unwrap(), Join::Joined(expected.clone()));
    assert_eq!(loaded(&handle, "d1").rows, 20);
    // The same file again is the same dataset; another file is a second.
    assert_eq!(join(&second).unwrap(), Join::Joined(expected));
    let other = Options {
        path: Some(fixture("variable_types.sav")),
        ..options.clone()
    };
    let Join::Joined(url) = join(&other).unwrap() else {
        panic!("should have joined");
    };
    assert!(url.ends_with("&dataset=d2"), "{url}");
    let server: api::ServerInfo = get(&handle, "/api/v1/server");
    assert_eq!(server.datasets.len(), 2);

    // A file the server cannot read is an error, said in its words.
    let broken = Options {
        path: Some(fixture("no_such_file.sav")),
        ..options.clone()
    };
    assert!(matches!(join(&broken), Err(ambers_web::Error::Refused(_))));

    // With the server gone, its record is stale: removed, and not joined.
    handle.stop();
    handle.wait();
    assert_eq!(join(&second).unwrap(), Join::None);
    assert_eq!(instance::read(state.path()), None);
}

#[test]
fn a_server_of_another_version_is_not_joined() {
    // Something that answers like a server of ambers 0.0.1.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { return };
            let mut request = [0u8; 2048];
            let _ = stream.read(&mut request);
            let body = r#"{"version":"0.0.1","interface":false,"datasets":[]}"#;
            let _ = write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
        }
    });
    let state = tempfile::tempdir().unwrap();
    let record = Record {
        port,
        token: "old".to_string(),
        version: "0.0.1".to_string(),
    };
    instance::write(state.path(), &record).unwrap();

    let options = Options {
        path: Some(fixture("survey.sav")),
        state_dir: Some(state.path().to_path_buf()),
        ..Options::default()
    };
    let other = Join::OtherVersion {
        version: "0.0.1".to_string(),
        port,
    };
    assert_eq!(join(&options).unwrap(), other);
    // Its record is left for it.
    assert_eq!(instance::read(state.path()), Some(record));
}

#[test]
fn an_unreadable_file_is_an_error_before_anything_is_served() {
    let missing = start(&Options {
        path: Some(fixture("no_such_file.sav")),
        port: Some(0),
        ..Options::default()
    });
    assert!(matches!(missing, Err(ambers_web::Error::Read(_))));
    let folder = start(&Options {
        path: Some(fixture("")),
        port: Some(0),
        ..Options::default()
    });
    assert!(matches!(folder, Err(ambers_web::Error::Folder(_))));

    // A port that is taken, when that port was asked for.
    let taken = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = taken.local_addr().unwrap().port();
    let clash = start(&Options {
        port: Some(port),
        ..Options::default()
    });
    assert!(matches!(clash, Err(ambers_web::Error::PortTaken(taken)) if taken == port));
}

/// A folder with two data files and one in a sub-folder, and a server
/// started on it with no file open.
fn library() -> (tempfile::TempDir, Handle) {
    let folder = tempfile::tempdir().unwrap();
    fs::create_dir_all(folder.path().join("waves/2025")).unwrap();
    for (from, to) in [
        ("survey.sav", "survey.sav"),
        ("variable_types.sav", "types.sav"),
        ("survey.sav", "waves/2025/wave.sav"),
    ] {
        fs::copy(fixture(from), folder.path().join(to)).unwrap();
    }
    let handle = start(&Options {
        root: Some(folder.path().to_path_buf()),
        port: Some(0),
        idle: None,
        ..Options::default()
    })
    .unwrap();
    (folder, handle)
}

/// The launch page's list, once every file has been described.
fn listed(handle: &Handle) -> api::FileList {
    let started = Instant::now();
    loop {
        let list: api::FileList = get(handle, "/api/v1/files");
        if list.pending == 0 {
            return list;
        }
        assert!(started.elapsed() < WAIT, "still describing files");
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn the_launch_page_lists_the_folder_and_opens_from_it() {
    let (folder, handle) = library();
    assert_eq!(handle.dataset, None);
    let list = listed(&handle);
    assert_eq!(list.roots.len(), 1);
    let names: Vec<(&str, &str)> = (list.files.iter())
        .map(|file| (file.name.as_str(), file.folder.as_str()))
        .collect();
    assert_eq!(
        names,
        [
            ("survey.sav", ""),
            ("types.sav", ""),
            ("wave.sav", "waves/2025")
        ]
    );
    let survey = &list.files[0];
    assert_eq!((survey.rows, survey.variables), (Some(20), Some(13)));
    assert_eq!(survey.format, "SAV");
    assert!(survey.dataset.is_none() && survey.opened.is_none() && !survey.starred);

    // The page (a cookie, no bearer) opens a file it was shown.
    let cookie = format!("ambers_{}={}", handle.port(), handle.token());
    let page = |path: &str, body: &str| {
        let headers = [("Cookie", cookie.as_str())];
        instance::request(handle.port(), "POST", path, &headers, Some(body), WAIT).unwrap()
    };
    let body = serde_json::to_string(&api::FileRequest {
        path: survey.path.clone(),
    })
    .unwrap();
    let opened = page("/api/v1/files/open", &body);
    assert_eq!(opened.status, 200, "{}", opened.body);
    let info: api::DatasetInfo = serde_json::from_str(&opened.body).unwrap();
    assert_eq!((info.id.as_str(), info.name.as_str()), ("d1", "survey.sav"));

    // It is then marked as open and as opened, and can be starred.
    let star = serde_json::to_string(&api::StarRequest {
        path: survey.path.clone(),
        starred: true,
    })
    .unwrap();
    assert_eq!(page("/api/v1/files/star", &star).status, 204);
    let list = listed(&handle);
    let survey = &list.files[0];
    assert_eq!(survey.dataset.as_deref(), Some("d1"));
    assert!(survey.opened.is_some() && survey.starred);

    // A path the page was not shown is refused: outside the folder, by
    // `..`, or not a data file.
    let outside = fixture("survey.sav").display().to_string();
    let around = (folder.path().join("..").join("x.sav"))
        .display()
        .to_string();
    let manifest = fixture("../../Cargo.toml").display().to_string();
    for path in [outside, around, manifest] {
        let body = serde_json::to_string(&api::FileRequest { path: path.clone() }).unwrap();
        let refused = page("/api/v1/files/open", &body);
        assert_eq!(refused.status, 403, "{path}: {}", refused.body);
        let star = serde_json::to_string(&api::StarRequest {
            path,
            starred: true,
        })
        .unwrap();
        assert_eq!(page("/api/v1/files/star", &star).status, 403);
    }
    handle.stop();
    handle.wait();
}

#[test]
fn a_second_open_with_a_folder_adds_it_to_the_launch_page() {
    let state = tempfile::tempdir().unwrap();
    let (_folder, _unused) = library();
    let first = tempfile::tempdir().unwrap();
    let handle = start(&Options {
        root: Some(first.path().to_path_buf()),
        port: Some(0),
        idle: None,
        state_dir: Some(state.path().to_path_buf()),
        ..Options::default()
    })
    .unwrap();
    assert!(listed(&handle).files.is_empty());

    // `am open` run again in a folder that has data files.
    let second = Options {
        root: Some(fixture("")),
        state_dir: Some(state.path().to_path_buf()),
        ..Options::default()
    };
    let expected = format!(
        "http://127.0.0.1:{}/?token={}",
        handle.port(),
        handle.token()
    );
    assert_eq!(join(&second).unwrap(), Join::Joined(expected));
    let list = listed(&handle);
    assert_eq!(list.roots.len(), 2);
    assert!(list.files.iter().any(|file| file.name == "survey.sav"));
    handle.stop();
    handle.wait();
}

/// Once a file has loaded, no handle to it is left: another program can
/// open it with exclusive access, so it can be replaced or moved while
/// its rows are still shown.
#[cfg(windows)]
#[test]
fn the_file_is_released_once_it_has_loaded() {
    use std::os::windows::fs::OpenOptionsExt;

    let folder = tempfile::tempdir().unwrap();
    let copy = folder.path().join("copy.sav");
    fs::copy(fixture("survey.sav"), &copy).unwrap();
    let handle = start(&Options {
        path: Some(copy.clone()),
        port: Some(0),
        idle: None,
        ..Options::default()
    })
    .unwrap();
    loaded(&handle, "d1");

    // Share mode 0: fails while anyone else has the file open.
    let exclusive = || fs::OpenOptions::new().read(true).share_mode(0).open(&copy);
    let started = Instant::now();
    while exclusive().is_err() {
        assert!(started.elapsed() < WAIT, "the file is still held open");
        std::thread::sleep(Duration::from_millis(10));
    }
    // The rows are still there without the file.
    fs::remove_file(&copy).unwrap();
    let page = rows(&handle, r#"{"offset":0,"limit":1}"#);
    assert_eq!(page.rows.len(), 1);
    handle.stop();
    handle.wait();
}
