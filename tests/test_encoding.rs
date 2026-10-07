//! Code-page encoded files: every piece of dictionary text (variable names,
//! long names, MR sets, labels) must be decoded with the file's declared
//! encoding, not as UTF-8 (GitHub issue #5).
//!
//! Fixtures are writer-produced UTF-8 files whose encoding records and name
//! bytes are patched to a single-byte code page, so the tests run everywhere.
//! The reporter's windows-1250 sample is used when present.

use std::io::Cursor;
use std::sync::Arc;

use ambers::{Compression, SpssMetadata, read_sav, read_sav_from_reader, write_sav_to_writer};
use arrow::array::{Float64Builder, RecordBatch, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};

// --------------------------------------------------------------------------
// Fixture helpers: walk the dictionary and splice records.
// --------------------------------------------------------------------------

fn i32_at(bytes: &[u8], o: usize) -> i32 {
    i32::from_le_bytes(bytes[o..o + 4].try_into().unwrap())
}

/// (offset, length) of every record in the dictionary, in order, plus the
/// type-2 short names and the info-record subtypes.
struct Dict {
    type2: Vec<(usize, String)>,
    /// (offset, subtype, total length including the 16-byte header)
    info: Vec<(usize, i32, usize)>,
}

fn walk(bytes: &[u8]) -> Dict {
    let mut pos = 176;
    let mut type2 = Vec::new();
    let mut info = Vec::new();
    loop {
        match i32_at(bytes, pos) {
            2 => {
                let has_label = i32_at(bytes, pos + 8);
                let nmiss = i32_at(bytes, pos + 12);
                let name = String::from_utf8_lossy(&bytes[pos + 24..pos + 32])
                    .trim_end()
                    .to_string();
                type2.push((pos, name));
                pos += 32;
                if has_label == 1 {
                    let ll = i32_at(bytes, pos) as usize;
                    pos += 4 + ll.div_ceil(4) * 4;
                }
                pos += 8 * nmiss.unsigned_abs() as usize;
            }
            3 => {
                let n = i32_at(bytes, pos + 4) as usize;
                pos += 8;
                for _ in 0..n {
                    pos += 8;
                    let ll = bytes[pos] as usize;
                    pos += (ll + 1).div_ceil(8) * 8;
                }
            }
            4 => pos += 8 + 4 * i32_at(bytes, pos + 4) as usize,
            6 => pos += 8 + 80 * i32_at(bytes, pos + 4) as usize,
            7 => {
                let sub = i32_at(bytes, pos + 4);
                let sz = i32_at(bytes, pos + 8) as usize;
                let cnt = i32_at(bytes, pos + 12) as usize;
                info.push((pos, sub, 16 + sz * cnt));
                pos += 16 + sz * cnt;
            }
            999 => return Dict { type2, info },
            other => panic!("unexpected record type {other} at {pos}"),
        }
    }
}

fn info_record(subtype: i32, payload: &[u8]) -> Vec<u8> {
    let mut rec = Vec::with_capacity(16 + payload.len());
    rec.extend_from_slice(&7i32.to_le_bytes());
    rec.extend_from_slice(&subtype.to_le_bytes());
    rec.extend_from_slice(&1i32.to_le_bytes());
    rec.extend_from_slice(&(payload.len() as i32).to_le_bytes());
    rec.extend_from_slice(payload);
    rec
}

/// Replace the payload of the info record with `subtype`, or insert a new
/// record just before the encoding record (subtype 20) if absent.
fn set_info_record(bytes: &[u8], subtype: i32, payload: &[u8]) -> Vec<u8> {
    let d = walk(bytes);
    let rec = info_record(subtype, payload);
    if let Some(&(off, _, len)) = d.info.iter().find(|&&(_, s, _)| s == subtype) {
        [&bytes[..off], &rec, &bytes[off + len..]].concat()
    } else {
        let (off, _, _) = d
            .info
            .iter()
            .find(|&&(_, s, _)| s == 20)
            .copied()
            .expect("encoding record");
        [&bytes[..off], &rec, &bytes[off..]].concat()
    }
}

/// Declare the file's encoding: subtype 20 name and subtype 3 code page.
fn declare_encoding(bytes: &[u8], name: &str, code_page: i32) -> Vec<u8> {
    let mut out = set_info_record(bytes, 20, name.as_bytes());
    let d = walk(&out);
    let (off, _, _) = d
        .info
        .iter()
        .find(|&&(_, s, _)| s == 3)
        .copied()
        .expect("integer info record");
    // payload: 8 x i32, code page is the 8th
    out[off + 16 + 28..off + 16 + 32].copy_from_slice(&code_page.to_le_bytes());
    out
}

fn set_short_name(bytes: &mut [u8], current: &str, new: &[u8; 8]) {
    let (off, _) = walk(bytes)
        .type2
        .into_iter()
        .find(|(_, n)| n.eq_ignore_ascii_case(current))
        .unwrap_or_else(|| panic!("no variable {current}"));
    bytes[off + 24..off + 32].copy_from_slice(new);
}

fn sample_file() -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("age", DataType::Float64, true),
        Field::new("score", DataType::Float64, true),
        Field::new("name", DataType::Utf8, true),
    ]));
    let mut age = Float64Builder::new();
    let mut score = Float64Builder::new();
    let mut name = StringBuilder::new();
    for i in 0..3 {
        age.append_value(20.0 + i as f64);
        score.append_value(i as f64);
        name.append_value("x");
    }
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(age.finish()),
            Arc::new(score.finish()),
            Arc::new(name.finish()),
        ],
    )
    .unwrap();
    let mut meta = SpssMetadata::from_arrow_schema(batch.schema().as_ref());
    meta.variable_formats
        .insert("name".to_string(), "A8".to_string());
    let mut cursor = Cursor::new(Vec::new());
    write_sav_to_writer(&mut cursor, &batch, &meta, Compression::Bytecode, None).unwrap();
    cursor.into_inner()
}

/// The sample with variable AGE renamed to windows-1250 "KORKVÓTA" (0xD3 = Ó)
/// in the type-2 record and the long-names record, plus an MR set whose label
/// holds the 1250 bytes for "őŐűŰ".
fn code_page_fixture(encoding_name: &str, code_page: i32) -> Vec<u8> {
    let mut bytes = sample_file();
    set_short_name(&mut bytes, "AGE", b"KORKV\xd3TA");
    let bytes = set_info_record(
        &bytes,
        13,
        b"KORKV\xd3TA=KORKV\xd3TA\tSCORE=score\tNAME=name",
    );
    let bytes = set_info_record(
        &bytes,
        7,
        b"$Q15M=C 12 Mik(\xf5\xd5\xfb\xdb) ok KORKV\xd3TA SCORE\n",
    );
    declare_encoding(&bytes, encoding_name, code_page)
}

// --------------------------------------------------------------------------

#[test]
fn utf8_file_is_unchanged() {
    let bytes = sample_file();
    let (batch, meta) = read_sav_from_reader(Cursor::new(bytes)).unwrap();
    assert_eq!(meta.file_encoding, "UTF-8");
    assert_eq!(meta.variable_names, vec!["age", "score", "name"]);
    assert_eq!(batch.num_rows(), 3);
    assert!(meta.warnings.is_empty());
}

#[test]
fn windows_1250_names_and_mr_set_decode_with_file_encoding() {
    let bytes = code_page_fixture("windows-1250", 1250);
    let (batch, meta) = read_sav_from_reader(Cursor::new(bytes)).unwrap();
    assert_eq!(meta.file_encoding, "windows-1250");
    assert_eq!(meta.variable_names, vec!["KORKVÓTA", "score", "name"]);
    assert_eq!(batch.schema().field(0).name(), "KORKVÓTA");
    let mr = meta.mr_sets.get("Q15M").expect("MR set");
    assert_eq!(mr.label, "Mik(őŐűŰ) ok");
    assert_eq!(mr.variables, vec!["KORKVÓTA", "score"]);
    assert!(meta.warnings.is_empty(), "{:?}", meta.warnings);
    for name in &meta.variable_names {
        assert!(!name.contains('\u{FFFD}'), "{name:?}");
    }
}

#[test]
fn same_bytes_declared_windows_1252_decode_differently() {
    // Proves the mapping is driven by the file's declaration, not hard-coded.
    let bytes = code_page_fixture("windows-1252", 1252);
    let (_, meta) = read_sav_from_reader(Cursor::new(bytes)).unwrap();
    assert_eq!(meta.file_encoding, "windows-1252");
    assert_eq!(meta.variable_names[0], "KORKVÓTA"); // 0xD3 is Ó in both pages
    assert_eq!(meta.mr_sets["Q15M"].label, "Mik(õÕûÛ) ok"); // 0xF5.. differ
}

#[test]
fn two_names_differing_only_by_an_accent_are_distinct() {
    // Before the fix both decoded to "KORKV\u{FFFD}TA" and were rejected as duplicates.
    let mut bytes = sample_file();
    set_short_name(&mut bytes, "AGE", b"KORKV\xd3TA"); // Ó
    set_short_name(&mut bytes, "SCORE", b"KORKV\xdaTA"); // Ú
    let bytes = set_info_record(
        &bytes,
        13,
        b"KORKV\xd3TA=KORKV\xd3TA\tKORKV\xdaTA=KORKV\xdaTA\tNAME=name",
    );
    let bytes = declare_encoding(&bytes, "windows-1250", 1250);
    let (batch, meta) = read_sav_from_reader(Cursor::new(bytes)).unwrap();
    assert_eq!(meta.variable_names, vec!["KORKVÓTA", "KORKVÚTA", "name"]);
    assert_eq!(batch.num_columns(), 3);
}

#[test]
fn utf8_accented_labels_roundtrip() {
    // Writer output is UTF-8; accented labels and value labels must survive.
    let schema = Arc::new(Schema::new(vec![Field::new("q1", DataType::Float64, true)]));
    let mut q1 = Float64Builder::new();
    q1.append_value(1.0);
    let batch = RecordBatch::try_new(schema, vec![Arc::new(q1.finish())]).unwrap();
    let mut meta = SpssMetadata::from_arrow_schema(batch.schema().as_ref());
    meta.variable_labels
        .insert("q1".to_string(), "Korcsoport kvóta (őŐűŰ)".to_string());
    let mut cursor = Cursor::new(Vec::new());
    write_sav_to_writer(&mut cursor, &batch, &meta, Compression::Bytecode, None).unwrap();
    let (_, back) = read_sav_from_reader(Cursor::new(cursor.into_inner())).unwrap();
    assert_eq!(back.variable_labels["q1"], "Korcsoport kvóta (őŐűŰ)");
}

#[test]
fn issue5_sample_file() {
    let path = "test_data/github_issues/issue5_mrset_parsing_panic_non_utf8_encoding.sav";
    if !std::path::Path::new(path).exists() {
        eprintln!("Skipping: {path} not present (reporter-supplied sample)");
        return;
    }
    let (batch, meta) = read_sav(path).unwrap();
    assert_eq!(batch.num_rows(), 10);
    assert_eq!(meta.file_encoding, "windows-1250");
    for expected in ["KORKVÓTA", "ISKOLAKVÓTA", "RÉGIÓKVÓTA"] {
        assert!(
            meta.variable_names.iter().any(|n| n == expected),
            "missing {expected}"
        );
    }
    assert_eq!(meta.variable_labels["KORKVÓTA"], "Korcsoport kvóta");
    let mr = &meta.mr_sets["Q15M"];
    assert_eq!(mr.label, "Mikor szokott leggyakrabban olvasni(őŐűŰ)?");
    assert_eq!(mr.variables.len(), 10);
    assert!(meta.warnings.is_empty(), "{:?}", meta.warnings);
    let all_text = meta
        .variable_names
        .iter()
        .chain(meta.variable_labels.values())
        .chain(meta.variable_value_labels.values().flat_map(|m| m.values()));
    for t in all_text {
        assert!(!t.contains('\u{FFFD}'), "replacement char in {t:?}");
    }
}
