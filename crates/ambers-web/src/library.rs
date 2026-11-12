//! The files the launch page can show and open.
//!
//! The page cannot browse the disk. It sees the `.sav` and `.zsav` files
//! under the folders the server was started with (`roots`), the files
//! named on a command line, and the files opened before (kept in the
//! state folder with the starred ones). Opening anything else is refused.
//!
//! Row and variable counts come from each file's dictionary only, read on
//! a background thread, so a folder of large files lists at once.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{SystemTime, UNIX_EPOCH};
use std::{fs, thread};

use serde::{Deserialize, Serialize};

use crate::api;

/// The name of the saved recent and starred files inside the state folder.
const SAVED: &str = "library.json";
/// Files listed at most; a larger folder is cut and says so.
const MAX_FILES: usize = 2_000;
/// Folders below a root that are looked into.
const MAX_DEPTH: usize = 8;
/// Recently opened files remembered.
const MAX_RECENT: usize = 20;
/// Folders that hold tools' files, never data.
const SKIPPED: [&str; 4] = ["node_modules", "target", "__pycache__", "site-packages"];

/// What a file's dictionary says, and the file it was read from.
#[derive(Debug, Clone)]
struct Facts {
    /// Size and modification time: a changed file is described again.
    stamp: (u64, Option<u64>),
    rows: Option<u32>,
    variables: u32,
    label: String,
    error: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct Saved {
    /// Most recent first.
    #[serde(default)]
    recent: Vec<Recent>,
    #[serde(default)]
    starred: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Recent {
    path: String,
    /// Seconds since 1970.
    opened: u64,
}

pub struct Library {
    roots: Mutex<Vec<PathBuf>>,
    /// Files named on a command line in this run.
    allowed: Mutex<HashSet<PathBuf>>,
    facts: Arc<Mutex<HashMap<PathBuf, Facts>>>,
    /// Files a background thread is reading now.
    describing: Arc<Mutex<HashSet<PathBuf>>>,
    saved: Mutex<Saved>,
    /// Where `saved` is kept; `None` keeps it for this run only.
    state_dir: Option<PathBuf>,
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn now() -> u64 {
    (SystemTime::now().duration_since(UNIX_EPOCH)).map_or(0, |since| since.as_secs())
}

/// A canonical path without the Windows `\\?\` prefix, which is correct
/// but not how anyone writes or reads a path.
pub fn plain(path: &Path) -> PathBuf {
    resolved(path).unwrap_or_else(|| unprefixed(path))
}

/// Where a path really leads, with `..` and links followed; `None` when
/// nothing is there. Permission checks use this, never the path as given.
fn resolved(path: &Path) -> Option<PathBuf> {
    path.canonicalize().ok().map(|real| unprefixed(&real))
}

fn unprefixed(path: &Path) -> PathBuf {
    let text = path.to_string_lossy();
    match text.strip_prefix(r"\\?\") {
        Some(rest) => match rest.strip_prefix(r"UNC\") {
            Some(share) => format!(r"\\{share}").into(),
            None => rest.to_string().into(),
        },
        None => path.to_path_buf(),
    }
}

/// `SAV` or `ZSAV` for a data file ambers reads, by its extension.
fn format_of(path: &Path) -> Option<&'static str> {
    let extension = path.extension()?.to_str()?;
    if extension.eq_ignore_ascii_case("sav") {
        Some("SAV")
    } else if extension.eq_ignore_ascii_case("zsav") {
        Some("ZSAV")
    } else {
        None
    }
}

fn stamp(path: &Path) -> Option<(u64, Option<u64>)> {
    let meta = fs::metadata(path).ok().filter(|meta| meta.is_file())?;
    let modified = (meta.modified().ok())
        .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
        .map(|since| since.as_secs());
    Some((meta.len(), modified))
}

fn describe(path: &Path, stamp: (u64, Option<u64>)) -> Facts {
    match ambers::read_sav_metadata(path) {
        Ok(meta) => Facts {
            stamp,
            rows: meta.number_rows.and_then(|rows| u32::try_from(rows).ok()),
            variables: meta.number_columns as u32,
            label: meta.file_label,
            error: None,
        },
        Err(error) => Facts {
            stamp,
            rows: None,
            variables: 0,
            label: String::new(),
            error: Some(error.to_string()),
        },
    }
}

impl Library {
    pub fn new(roots: &[PathBuf], state_dir: Option<PathBuf>) -> Library {
        let saved = (state_dir.as_ref())
            .and_then(|dir| fs::read_to_string(dir.join(SAVED)).ok())
            .and_then(|text| serde_json::from_str(&text).ok())
            .unwrap_or_default();
        Library {
            roots: Mutex::new(roots.iter().map(|root| plain(root)).collect()),
            allowed: Mutex::new(HashSet::new()),
            facts: Arc::new(Mutex::new(HashMap::new())),
            describing: Arc::new(Mutex::new(HashSet::new())),
            saved: Mutex::new(saved),
            state_dir,
        }
    }

    fn save(&self, saved: &Saved) {
        // Losing this file costs a list of recent files, nothing more.
        if let Some(dir) = &self.state_dir
            && let Ok(text) = serde_json::to_string_pretty(saved)
            && fs::create_dir_all(dir).is_ok()
        {
            let _ = fs::write(dir.join(SAVED), text);
        }
    }

    /// Another folder whose files the page may list and open.
    pub fn add_root(&self, root: &Path) {
        let root = plain(root);
        let mut roots = lock(&self.roots);
        if !roots.contains(&root) {
            roots.push(root);
        }
    }

    /// A file was named on a command line: the page may open it again.
    pub fn allow(&self, path: &Path) {
        lock(&self.allowed).insert(plain(path));
    }

    /// Whether the page may open this file: it is a data file under a
    /// root, was named on a command line, or was opened before.
    pub fn may_open(&self, path: &Path) -> bool {
        // Where the path really leads: `..` and links are followed first,
        // and a path to nothing is refused, so no spelling of a path can
        // reach outside the roots.
        let Some(path) = resolved(path) else {
            return false;
        };
        if format_of(&path).is_none() {
            return false;
        }
        let text = path.display().to_string();
        lock(&self.roots).iter().any(|root| path.starts_with(root))
            || lock(&self.allowed).contains(&path)
            || lock(&self.saved)
                .recent
                .iter()
                .any(|recent| recent.path == text)
    }

    /// Note that a file was opened now.
    pub fn opened(&self, path: &Path) {
        let text = plain(path).display().to_string();
        let mut saved = lock(&self.saved);
        saved.recent.retain(|recent| recent.path != text);
        saved.recent.insert(
            0,
            Recent {
                path: text,
                opened: now(),
            },
        );
        saved.recent.truncate(MAX_RECENT);
        self.save(&saved);
    }

    /// Star or unstar a file the page may open. Returns whether it did.
    pub fn star(&self, path: &Path, starred: bool) -> bool {
        if !self.may_open(path) {
            return false;
        }
        let text = plain(path).display().to_string();
        let mut saved = lock(&self.saved);
        saved.starred.retain(|one| *one != text);
        if starred {
            saved.starred.push(text);
        }
        self.save(&saved);
        true
    }

    /// Every data file under the roots: its path and the folder it is in,
    /// relative to its root. Whether the list was cut at `MAX_FILES`.
    fn scan(&self) -> (Vec<(PathBuf, String)>, bool) {
        let mut found = Vec::new();
        let mut cut = false;
        for root in lock(&self.roots).iter() {
            let mut folders = vec![(root.clone(), 0usize)];
            while let Some((folder, depth)) = folders.pop() {
                let Ok(entries) = fs::read_dir(&folder) else {
                    continue;
                };
                for entry in entries.flatten() {
                    let path = entry.path();
                    let name = entry.file_name().to_string_lossy().into_owned();
                    let Ok(kind) = entry.file_type() else {
                        continue;
                    };
                    if kind.is_dir() {
                        let tool = name.starts_with('.') || SKIPPED.contains(&name.as_str());
                        if !tool && depth < MAX_DEPTH {
                            folders.push((path, depth + 1));
                        }
                    } else if format_of(&path).is_some() {
                        if found.len() >= MAX_FILES {
                            cut = true;
                            break;
                        }
                        let inside = (folder.strip_prefix(root).ok())
                            .map(|inside| inside.to_string_lossy().replace('\\', "/"))
                            .unwrap_or_default();
                        found.push((path, inside));
                    }
                }
            }
        }
        (found, cut)
    }

    /// What the launch page shows. `open` maps the path of each open
    /// dataset to its id.
    pub fn list(&self, open: &HashMap<PathBuf, String>) -> api::FileList {
        let (mut found, truncated) = self.scan();
        let saved = lock(&self.saved).clone();
        // Files opened before or starred that are outside the roots (named
        // on a command line once) are listed too, with their own folder.
        let listed: HashSet<PathBuf> = found.iter().map(|(path, _)| path.clone()).collect();
        let outside = (saved.recent.iter().map(|recent| &recent.path)).chain(&saved.starred);
        let mut extra = HashSet::new();
        for text in outside {
            let path = PathBuf::from(text);
            if !listed.contains(&path) && extra.insert(path.clone()) && path.is_file() {
                let folder = (path.parent()).map_or(String::new(), |p| p.display().to_string());
                found.push((path, folder));
            }
        }

        let mut files = Vec::new();
        let mut wanted = Vec::new();
        {
            let facts = lock(&self.facts);
            for (path, folder) in found {
                let Some(current) = stamp(&path) else {
                    continue;
                };
                let known = facts.get(&path).filter(|facts| facts.stamp == current);
                if known.is_none() {
                    wanted.push((path.clone(), current));
                }
                let text = path.display().to_string();
                files.push(api::FileEntry {
                    name: (path.file_name())
                        .map_or_else(String::new, |name| name.to_string_lossy().into_owned()),
                    folder,
                    format: format_of(&path).unwrap_or("SAV").to_string(),
                    size: current.0,
                    modified: current.1,
                    rows: known.and_then(|facts| facts.rows),
                    variables: known.filter(|f| f.error.is_none()).map(|f| f.variables),
                    label: known.map(|facts| facts.label.clone()),
                    described: known.is_some(),
                    error: known.and_then(|facts| facts.error.clone()),
                    starred: saved.starred.contains(&text),
                    opened: (saved.recent.iter())
                        .find(|recent| recent.path == text)
                        .map(|recent| recent.opened),
                    dataset: open.get(&path).cloned(),
                    path: text,
                });
            }
        }
        files.sort_by(|a, b| {
            (a.name.to_lowercase(), &a.path).cmp(&(b.name.to_lowercase(), &b.path))
        });
        let pending = wanted.len() as u32;
        self.describe_later(wanted);
        api::FileList {
            roots: (lock(&self.roots).iter())
                .map(|root| root.display().to_string())
                .collect(),
            files,
            pending,
            truncated,
        }
    }

    /// Read the dictionaries of these files on a background thread.
    fn describe_later(&self, wanted: Vec<(PathBuf, (u64, Option<u64>))>) {
        let fresh: Vec<_> = {
            let mut describing = lock(&self.describing);
            (wanted.into_iter())
                .filter(|(path, _)| describing.insert(path.clone()))
                .collect()
        };
        if fresh.is_empty() {
            return;
        }
        let (facts, describing) = (Arc::clone(&self.facts), Arc::clone(&self.describing));
        thread::spawn(move || {
            for (path, stamp) in fresh {
                let described = describe(&path, stamp);
                lock(&facts).insert(path.clone(), described);
                lock(&describing).remove(&path);
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    fn fixtures() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/data")
    }

    /// List until every file has been described.
    fn settled(library: &Library) -> api::FileList {
        let started = Instant::now();
        loop {
            let list = library.list(&HashMap::new());
            if list.pending == 0 {
                return list;
            }
            assert!(
                started.elapsed() < Duration::from_secs(20),
                "still describing"
            );
            thread::sleep(Duration::from_millis(10));
        }
    }

    #[test]
    fn data_files_under_a_root_are_listed_and_described() {
        let library = Library::new(&[fixtures()], None);
        let first = library.list(&HashMap::new());
        assert!(first.files.iter().any(|file| file.name == "survey.sav"));
        assert!(!first.truncated);
        // Only data files: the folder also holds JSON.
        assert!(
            first
                .files
                .iter()
                .all(|file| file.format == "SAV" || file.format == "ZSAV")
        );

        let list = settled(&library);
        let survey = list
            .files
            .iter()
            .find(|file| file.name == "survey.sav")
            .unwrap();
        assert_eq!((survey.rows, survey.variables), (Some(20), Some(13)));
        assert_eq!(
            survey.label.as_deref(),
            Some("ambers synthetic survey fixture")
        );
        assert!(survey.described && survey.error.is_none());
        assert_eq!(survey.folder, "");
        assert!(list.files.iter().any(|file| file.format == "ZSAV"));
        // Sorted by name, whatever the case.
        let names: Vec<String> = list
            .files
            .iter()
            .map(|file| file.name.to_lowercase())
            .collect();
        let mut sorted = names.clone();
        sorted.sort();
        assert_eq!(names, sorted);
    }

    #[test]
    fn only_files_under_a_root_or_named_before_may_be_opened() {
        let folder = tempfile::tempdir().unwrap();
        let inside = folder.path().join("inside.sav");
        fs::copy(fixtures().join("survey.sav"), &inside).unwrap();
        let library = Library::new(&[folder.path().to_path_buf()], None);

        assert!(library.may_open(&inside));
        let elsewhere = fixtures().join("survey.sav");
        assert!(!library.may_open(&elsewhere), "outside the root");
        // Not a data file, even under the root.
        let text = folder.path().join("notes.txt");
        fs::write(&text, "x").unwrap();
        assert!(!library.may_open(&text));
        // `..` does not lead out of the root: not to a file that is not
        // there, and not to one that is.
        let sneaky = folder.path().join("..").join("outside.sav");
        assert!(!library.may_open(&sneaky));
        let outside = tempfile::tempdir().unwrap();
        let real = outside.path().join("real.sav");
        fs::copy(fixtures().join("survey.sav"), &real).unwrap();
        let name = outside.path().file_name().unwrap();
        let around = folder.path().join("..").join(name).join("real.sav");
        assert!(around.is_file(), "the path does lead to the file");
        assert!(!library.may_open(&around));

        // Named on a command line, or opened before: allowed.
        library.allow(&elsewhere);
        assert!(library.may_open(&elsewhere));
        let other = fixtures().join("variable_types.sav");
        assert!(!library.may_open(&other));
        library.opened(&other);
        assert!(library.may_open(&other));
    }

    #[test]
    fn recent_and_starred_files_are_kept_between_runs() {
        let state = tempfile::tempdir().unwrap();
        let state_dir = Some(state.path().to_path_buf());
        let survey = fixtures().join("survey.sav");
        let types = fixtures().join("variable_types.sav");
        {
            let library = Library::new(&[fixtures()], state_dir.clone());
            library.opened(&survey);
            library.opened(&types);
            library.opened(&survey);
            assert!(library.star(&types, true));
            // A file the page may not open cannot be starred.
            assert!(!library.star(Path::new("C:/nowhere/x.sav"), true));
        }

        let library = Library::new(&[fixtures()], state_dir);
        let list = settled(&library);
        let entry = |name: &str| list.files.iter().find(|file| file.name == name).unwrap();
        assert!(entry("variable_types.sav").starred);
        assert!(!entry("survey.sav").starred);
        // Both were opened; the survey last.
        let (first, second) = (
            entry("survey.sav").opened,
            entry("variable_types.sav").opened,
        );
        assert!(first.is_some() && second.is_some() && first >= second);
        assert!(entry("imperfect_labels.sav").opened.is_none());
        assert!(library.star(&types, false));
        assert!(!settled(&library).files.iter().any(|file| file.starred));
    }

    #[test]
    fn a_file_outside_the_roots_is_listed_once_it_was_opened() {
        let folder = tempfile::tempdir().unwrap();
        let library = Library::new(&[folder.path().to_path_buf()], None);
        assert!(settled(&library).files.is_empty());
        let survey = fixtures().join("survey.sav");
        library.allow(&survey);
        library.opened(&survey);
        let list = settled(&library);
        assert_eq!(list.files.len(), 1);
        assert_eq!(list.files[0].rows, Some(20));
        // An open dataset is marked with its id.
        let open = HashMap::from([(plain(&survey), "d1".to_string())]);
        assert_eq!(library.list(&open).files[0].dataset.as_deref(), Some("d1"));
    }

    #[test]
    fn a_file_that_is_not_spss_says_so_instead_of_failing_the_list() {
        let folder = tempfile::tempdir().unwrap();
        fs::write(folder.path().join("broken.sav"), "this is not a sav file").unwrap();
        let library = Library::new(&[folder.path().to_path_buf()], None);
        let list = settled(&library);
        assert_eq!(list.files.len(), 1);
        let broken = &list.files[0];
        assert!(broken.described && broken.error.is_some());
        assert_eq!((broken.rows, broken.variables), (None, None));
    }

    #[test]
    fn a_path_is_shown_without_the_verbatim_prefix() {
        let shown = |path: &str| unprefixed(Path::new(path)).display().to_string();
        assert_eq!(shown(r"\\?\C:\data\survey.sav"), r"C:\data\survey.sav");
        assert_eq!(shown(r"\\?\UNC\nas\share\a.sav"), r"\\nas\share\a.sav");
        assert_eq!(shown("/home/me/a.sav"), "/home/me/a.sav");
        assert_eq!(shown(r"C:\data\a.sav"), r"C:\data\a.sav");
    }

    #[test]
    fn tool_folders_are_not_searched() {
        let folder = tempfile::tempdir().unwrap();
        for sub in ["data/2025", ".hidden", "node_modules/x"] {
            let dir = folder.path().join(sub);
            fs::create_dir_all(&dir).unwrap();
            fs::copy(fixtures().join("survey.sav"), dir.join("copy.sav")).unwrap();
        }
        let library = Library::new(&[folder.path().to_path_buf()], None);
        let list = settled(&library);
        let folders: Vec<&str> = list.files.iter().map(|file| file.folder.as_str()).collect();
        assert_eq!(folders, ["data/2025"]);
    }
}
