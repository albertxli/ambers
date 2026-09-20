//! Malformed-file handling: the reader must return `Err`, never read out of
//! bounds, on files whose header disagrees with their dictionary.
//!
//! Regression tests for GitHub issue #1 (heap buffer over-read when the header's
//! case size is smaller than the number of dictionary slots). All fixtures are
//! synthesized in memory with the writer and then byte-patched, so these tests
//! run everywhere. The reporter's fuzzer files are used only when present.

use std::io::Cursor;
use std::sync::Arc;

use ambers::error::SpssError;
use ambers::{
    Compression, SpssMetadata, read_sav, read_sav_from_reader, scan_sav_from_reader,
    write_sav_to_writer,
};
use arrow::array::{Float64Builder, RecordBatch, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};

/// Byte offset of the header's `nominal_case_size` (i32, little-endian on
/// files written by ambers): magic (4) + product name (60) + layout_code (4).
const CASE_SIZE_OFFSET: usize = 68;
/// Byte offset of the header's `ncases`: CASE_SIZE_OFFSET + case size (4) +
/// compression (4) + weight index (4).
const CASE_COUNT_OFFSET: usize = 80;
/// Byte offset of the header's compression bias (f64): CASE_COUNT_OFFSET + 4.
const BIAS_OFFSET: usize = 84;
/// Bias value found in the reporter's fuzzer files.
const GARBAGE_BIAS: f64 = 9.597803938502254e-308;

fn sample_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("age", DataType::Float64, true),
        Field::new("score", DataType::Float64, true),
        Field::new("name", DataType::Utf8, true), // 12 bytes wide -> 2 slots
    ]));
    let mut age = Float64Builder::new();
    let mut score = Float64Builder::new();
    let mut name = StringBuilder::new();
    for (i, n) in ["Alice", "Bob", "Carol-Louise", "Dan", "Eve"]
        .iter()
        .enumerate()
    {
        age.append_value(20.0 + i as f64);
        if i == 3 {
            score.append_null();
        } else {
            score.append_value(i as f64 * 1.5);
        }
        name.append_value(n);
    }
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(age.finish()),
            Arc::new(score.finish()),
            Arc::new(name.finish()),
        ],
    )
    .unwrap()
}

/// Write the sample batch to bytes. Returns (bytes, slots_per_row as written).
fn sample_sav(compression: Compression) -> (Vec<u8>, i32) {
    let batch = sample_batch();
    let mut meta = SpssMetadata::from_arrow_schema(batch.schema().as_ref());
    // Without an explicit format the writer infers A255 (32 slots); pin A12 so
    // the row is exactly 4 slots: age(1) + score(1) + name(2).
    meta.variable_formats
        .insert("name".to_string(), "A12".to_string());
    let mut cursor = Cursor::new(Vec::new());
    write_sav_to_writer(&mut cursor, &batch, &meta, compression, None).unwrap();
    let bytes = cursor.into_inner();
    let declared = i32::from_le_bytes(
        bytes[CASE_SIZE_OFFSET..CASE_SIZE_OFFSET + 4]
            .try_into()
            .unwrap(),
    );
    (bytes, declared)
}

fn with_case_size(mut bytes: Vec<u8>, value: i32) -> Vec<u8> {
    bytes[CASE_SIZE_OFFSET..CASE_SIZE_OFFSET + 4].copy_from_slice(&value.to_le_bytes());
    bytes
}

fn with_case_count(mut bytes: Vec<u8>, value: i32) -> Vec<u8> {
    bytes[CASE_COUNT_OFFSET..CASE_COUNT_OFFSET + 4].copy_from_slice(&value.to_le_bytes());
    bytes
}

const ALL_COMPRESSIONS: [Compression; 3] =
    [Compression::None, Compression::Bytecode, Compression::Zlib];

fn with_bias(mut bytes: Vec<u8>, value: f64) -> Vec<u8> {
    bytes[BIAS_OFFSET..BIAS_OFFSET + 8].copy_from_slice(&value.to_le_bytes());
    bytes
}

fn age_column(batch: &RecordBatch) -> Vec<Option<f64>> {
    use arrow::array::{Array, Float64Array};
    let col = batch
        .column_by_name("age")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    (0..col.len())
        .map(|i| {
            if col.is_null(i) {
                None
            } else {
                Some(col.value(i))
            }
        })
        .collect()
}

fn assert_invalid_dictionary(result: Result<RecordBatch, SpssError>, expect_in_msg: &[&str]) {
    match result {
        Err(SpssError::InvalidDictionary(msg)) => {
            for needle in expect_in_msg {
                assert!(msg.contains(needle), "message {msg:?} lacks {needle:?}");
            }
        }
        Err(other) => panic!("expected InvalidDictionary, got {other:?}"),
        Ok(batch) => panic!("expected an error, read {} rows", batch.num_rows()),
    }
}

#[test]
fn unpatched_roundtrip_unchanged() {
    for compression in [Compression::None, Compression::Bytecode] {
        let (bytes, declared) = sample_sav(compression);
        // age(1) + score(1) + name A12 (2 slots)
        assert_eq!(declared, 4, "writer should declare 4 slots per case");
        let (batch, meta) = read_sav_from_reader(Cursor::new(bytes)).unwrap();
        assert_eq!(batch.num_rows(), 5);
        assert_eq!(batch.num_columns(), 3);
        assert_eq!(meta.variable_names, vec!["age", "score", "name"]);
    }
}

#[test]
fn header_slot_count_larger_than_dictionary_is_rejected() {
    let (bytes, declared) = sample_sav(Compression::Bytecode);
    let patched = with_case_size(bytes, declared + 3);
    let result = read_sav_from_reader(Cursor::new(patched.clone())).map(|(b, _)| b);
    assert_invalid_dictionary(result, &["7 slots per case", "defines 4"]);
    // The metadata-only path must reject the file too.
    assert!(scan_sav_from_reader(Cursor::new(patched), 100).is_err());
}

#[test]
fn header_slot_count_smaller_than_dictionary_is_rejected() {
    // This is the GitHub issue #1 shape: header says fewer slots than the
    // dictionary defines, so the last columns would be read past the row end.
    for compression in [Compression::None, Compression::Bytecode] {
        let (bytes, declared) = sample_sav(compression);
        let patched = with_case_size(bytes, declared - 1);
        let result = read_sav_from_reader(Cursor::new(patched)).map(|(b, _)| b);
        assert_invalid_dictionary(result, &["3 slots per case", "defines 4"]);
    }
}

#[test]
fn non_positive_header_slot_count_is_tolerated() {
    // PSPP documents that some writers store -1 or 0 here. The dictionary is
    // authoritative, so such files must read exactly like the correct file.
    for compression in [Compression::None, Compression::Bytecode] {
        let (bytes, _) = sample_sav(compression);
        let (expected, _) = read_sav_from_reader(Cursor::new(bytes.clone())).unwrap();
        for bogus in [0, -1] {
            let (batch, meta) =
                read_sav_from_reader(Cursor::new(with_case_size(bytes.clone(), bogus)))
                    .unwrap_or_else(|e| panic!("case size {bogus} should be tolerated: {e}"));
            assert_eq!(batch, expected, "case size {bogus}");
            assert_eq!(meta.variable_names, vec!["age", "score", "name"]);
        }
    }
}

#[test]
fn issue1_repro_file_is_rejected() {
    let path = "test_data/github_issues/issue1_buffer_overflow.sav";
    if !std::path::Path::new(path).exists() {
        eprintln!("Skipping: {path} not present (reporter-supplied fuzzer file)");
        return;
    }
    let result = read_sav(path).map(|(b, _)| b);
    assert_invalid_dictionary(result, &["7 slots per case", "defines 10"]);
}

#[test]
fn issue3_slot_count_files_are_rejected() {
    // Two of the issue #3 fuzzer files also carry a header/dictionary slot
    // mismatch. They may fail earlier for other reasons; only require an Err.
    for path in [
        "test_data/github_issues/issue3_case-size.sav",
        "test_data/github_issues/issue3_missing-count.sav",
    ] {
        if !std::path::Path::new(path).exists() {
            eprintln!("Skipping: {path} not present");
            continue;
        }
        assert!(
            read_sav(path).is_err(),
            "{path} should not read successfully"
        );
    }
}

// ---------------------------------------------------------------------------
// GitHub issue #2: header declares 0 cases but data follows. The header count
// is only a capacity hint; the data section is the truth (SPSS reads the rows).
// ---------------------------------------------------------------------------

#[test]
fn zero_or_negative_case_count_reads_all_rows() {
    for compression in ALL_COMPRESSIONS {
        let (bytes, _) = sample_sav(compression);
        let declared = i32::from_le_bytes(
            bytes[CASE_COUNT_OFFSET..CASE_COUNT_OFFSET + 4]
                .try_into()
                .unwrap(),
        );
        assert_eq!(
            declared, 5,
            "writer should declare 5 cases ({compression:?})"
        );
        let (expected, _) = read_sav_from_reader(Cursor::new(bytes.clone())).unwrap();
        assert_eq!(expected.num_rows(), 5);

        for bogus in [0, -1] {
            let patched = with_case_count(bytes.clone(), bogus);
            let (batch, _) = read_sav_from_reader(Cursor::new(patched.clone()))
                .unwrap_or_else(|e| panic!("ncases={bogus} {compression:?}: {e}"));
            assert_eq!(batch, expected, "ncases={bogus} {compression:?}");

            // Streaming path with a small batch size exercises the per-batch buffer.
            let mut scanner = scan_sav_from_reader(Cursor::new(patched), 2).unwrap();
            let mut parts = Vec::new();
            while let Some(b) = scanner.next_batch().unwrap() {
                parts.push(b);
            }
            let streamed = arrow::compute::concat_batches(&expected.schema(), &parts).unwrap();
            assert_eq!(
                streamed, expected,
                "streamed ncases={bogus} {compression:?}"
            );
            assert_eq!(scanner.rows_read(), 5);
        }
    }
}

#[test]
fn empty_file_reads_zero_rows_without_panic() {
    // Runs in debug under `cargo test`: the old debug_assert! fired here.
    let batch = sample_batch().slice(0, 0);
    for compression in ALL_COMPRESSIONS {
        let mut meta = SpssMetadata::from_arrow_schema(batch.schema().as_ref());
        meta.variable_formats
            .insert("name".to_string(), "A12".to_string());
        let mut cursor = Cursor::new(Vec::new());
        write_sav_to_writer(&mut cursor, &batch, &meta, compression, None).unwrap();
        let (read, read_meta) = read_sav_from_reader(Cursor::new(cursor.into_inner()))
            .unwrap_or_else(|e| panic!("empty file {compression:?}: {e}"));
        assert_eq!(read.num_rows(), 0, "{compression:?}");
        assert_eq!(read.num_columns(), 3, "{compression:?}");
        assert_eq!(read_meta.number_rows, Some(0));
    }
}

#[test]
fn issue2_repro_file_does_not_crash() {
    let path = "test_data/github_issues/issue2_oob-write.sav";
    if !std::path::Path::new(path).exists() {
        eprintln!("Skipping: {path} not present (reporter-supplied fuzzer file)");
        return;
    }
    // Header says 0 cases; SPSS shows 5 rows and 7 variables for this file.
    match read_sav(path) {
        Ok((batch, meta)) => {
            assert_eq!(batch.num_columns(), 7);
            assert_eq!(batch.num_rows(), 5, "SPSS reads 5 rows from this file");
            assert_eq!(meta.number_rows, Some(0), "header value is reported as-is");
        }
        Err(e) => panic!("expected a clean read (SPSS opens this file), got {e}"),
    }
}

// ---------------------------------------------------------------------------
// File-damage warnings: read literally, but record what disagrees with the header.
// ---------------------------------------------------------------------------

#[test]
fn healthy_file_has_no_warnings() {
    for compression in ALL_COMPRESSIONS {
        let (bytes, _) = sample_sav(compression);
        let (_, meta) = read_sav_from_reader(Cursor::new(bytes.clone())).unwrap();
        assert!(
            meta.warnings.is_empty(),
            "{compression:?}: {:?}",
            meta.warnings
        );
        let scanner = scan_sav_from_reader(Cursor::new(bytes), 2).unwrap();
        assert!(scanner.metadata().warnings.is_empty());
    }
}

#[test]
fn zero_case_count_warns_after_read() {
    for compression in ALL_COMPRESSIONS {
        let (bytes, _) = sample_sav(compression);
        let patched = with_case_count(bytes, 0);

        // Eager: metadata returned after the read carries the finding.
        let (batch, meta) = read_sav_from_reader(Cursor::new(patched.clone())).unwrap();
        assert_eq!(batch.num_rows(), 5);
        assert_eq!(
            meta.warnings.len(),
            1,
            "{compression:?}: {:?}",
            meta.warnings
        );
        assert!(meta.warnings[0].contains("declares 0 rows but 5 rows were read"));

        // Streaming: nothing before the data is read, the finding once exhausted.
        let mut scanner = scan_sav_from_reader(Cursor::new(patched.clone()), 2).unwrap();
        assert!(scanner.metadata().warnings.is_empty());
        while scanner.next_batch().unwrap().is_some() {}
        assert_eq!(scanner.metadata().warnings.len(), 1);

        // A row limit makes a mismatch expected: no warning.
        let mut limited = scan_sav_from_reader(Cursor::new(patched), 100).unwrap();
        limited.limit(2);
        let b = limited.collect_single().unwrap();
        assert_eq!(b.num_rows(), 2);
        assert!(limited.metadata().warnings.is_empty());
    }
}

#[test]
fn non_standard_bias_warns_and_values_are_read_literally() {
    let (clean, _) = sample_sav(Compression::Bytecode);
    let (clean_batch, _) = read_sav_from_reader(Cursor::new(clean)).unwrap();
    assert_eq!(
        age_column(&clean_batch),
        vec![Some(20.0), Some(21.0), Some(22.0), Some(23.0), Some(24.0)]
    );

    for compression in [Compression::Bytecode, Compression::Zlib] {
        for (bias, shift) in [(GARBAGE_BIAS, 100.0 - GARBAGE_BIAS), (50.0, 50.0)] {
            let (bytes, _) = sample_sav(compression);
            let (batch, meta) = read_sav_from_reader(Cursor::new(with_bias(bytes, bias))).unwrap();
            assert_eq!(
                meta.warnings.len(),
                1,
                "{compression:?} bias={bias}: {:?}",
                meta.warnings
            );
            assert!(meta.warnings[0].contains("bias") && meta.warnings[0].contains("assuming 100"));
            // Small integers were stored as code = value + 100; decoding with the
            // patched bias yields value + 100 - bias. We report, we do not correct.
            let ages = age_column(&batch);
            assert_eq!(ages[0], Some(20.0 + shift), "{compression:?} bias={bias}");
            assert_eq!(ages[4], Some(24.0 + shift), "{compression:?} bias={bias}");
        }
    }

    // Uncompressed data never consults the bias: no warning, values unchanged.
    let (bytes, _) = sample_sav(Compression::None);
    let (batch, meta) = read_sav_from_reader(Cursor::new(with_bias(bytes, GARBAGE_BIAS))).unwrap();
    assert!(meta.warnings.is_empty());
    assert_eq!(age_column(&batch)[0], Some(20.0));
}

#[test]
fn case_size_zero_header_warns_but_reads() {
    let (bytes, _) = sample_sav(Compression::Bytecode);
    let (expected, _) = read_sav_from_reader(Cursor::new(bytes.clone())).unwrap();
    let (batch, meta) = read_sav_from_reader(Cursor::new(with_case_size(bytes, 0))).unwrap();
    assert_eq!(batch, expected);
    assert_eq!(meta.warnings.len(), 1);
    assert!(
        meta.warnings[0].contains("values per row"),
        "{:?}",
        meta.warnings
    );
}

#[test]
fn issue2_repro_file_reports_two_warnings() {
    let path = "test_data/github_issues/issue2_oob-write.sav";
    if !std::path::Path::new(path).exists() {
        eprintln!("Skipping: {path} not present (reporter-supplied fuzzer file)");
        return;
    }
    let (batch, meta) = read_sav(path).unwrap();
    assert_eq!(batch.num_rows(), 5);
    assert_eq!(meta.warnings.len(), 2, "{:?}", meta.warnings);
    assert!(
        meta.warnings
            .iter()
            .any(|w| w.contains("declares 0 rows but 5 rows"))
    );
    assert!(meta.warnings.iter().any(|w| w.contains("compression bias")));
}
