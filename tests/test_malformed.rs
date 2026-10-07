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

/// Offsets of interest in a file written by ambers, found by walking the
/// dictionary records (same structure the reader parses).
struct Layout {
    /// (record offset, short name) for every type 2 record, in order.
    type2: Vec<(usize, String)>,
    /// (record offset, subtype) for every type 7 (info) record, in order.
    info: Vec<(usize, i32)>,
    /// First byte of the data section (after the type 999 record).
    data_start: usize,
}

fn layout(bytes: &[u8]) -> Layout {
    let i32_at = |o: usize| i32::from_le_bytes(bytes[o..o + 4].try_into().unwrap());
    let mut pos = 176;
    let mut type2 = Vec::new();
    let mut info = Vec::new();
    loop {
        match i32_at(pos) {
            2 => {
                let has_label = i32_at(pos + 8);
                let nmiss = i32_at(pos + 12);
                let name = String::from_utf8_lossy(&bytes[pos + 24..pos + 32])
                    .trim_end()
                    .to_string();
                type2.push((pos, name));
                pos += 32;
                if has_label == 1 {
                    let ll = i32_at(pos) as usize;
                    pos += 4 + ll.div_ceil(4) * 4;
                }
                pos += 8 * nmiss.unsigned_abs() as usize;
            }
            3 => {
                let n = i32_at(pos + 4) as usize;
                pos += 8;
                for _ in 0..n {
                    pos += 8;
                    let ll = bytes[pos] as usize;
                    pos += (ll + 1).div_ceil(8) * 8;
                }
            }
            4 => pos += 8 + 4 * i32_at(pos + 4) as usize,
            6 => pos += 8 + 80 * i32_at(pos + 4) as usize,
            7 => {
                info.push((pos, i32_at(pos + 4)));
                let sz = i32_at(pos + 8) as usize;
                let cnt = i32_at(pos + 12) as usize;
                pos += 16 + sz * cnt;
            }
            999 => {
                return Layout {
                    type2,
                    info,
                    data_start: pos + 8,
                };
            }
            other => panic!("unexpected record type {other} at {pos}"),
        }
    }
}

fn type2_offset(bytes: &[u8], short_name: &str) -> usize {
    layout(bytes)
        .type2
        .into_iter()
        .find(|(_, n)| n.eq_ignore_ascii_case(short_name))
        .map(|(o, _)| o)
        .unwrap_or_else(|| panic!("no type 2 record named {short_name}"))
}

fn put_i32(bytes: &mut [u8], offset: usize, value: i32) {
    bytes[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

fn put_f64(bytes: &mut [u8], offset: usize, value: f64) {
    bytes[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
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

// ---------------------------------------------------------------------------
// Issue #1 revisited: a header/dictionary width mismatch is a finding, not an
// error (SPSS ignores the header field). Duplicate variable names are the error.
// ---------------------------------------------------------------------------

#[test]
fn header_slot_count_mismatch_warns_and_reads() {
    for compression in [Compression::None, Compression::Bytecode] {
        let (bytes, declared) = sample_sav(compression);
        let (expected, _) = read_sav_from_reader(Cursor::new(bytes.clone())).unwrap();
        for bogus in [declared + 3, declared - 1, 1_895_825_415] {
            let patched = with_case_size(bytes.clone(), bogus);
            let (batch, meta) = read_sav_from_reader(Cursor::new(patched.clone()))
                .unwrap_or_else(|e| panic!("{compression:?} case size {bogus}: {e}"));
            assert_eq!(batch, expected, "{compression:?} case size {bogus}");
            assert_eq!(meta.warnings.len(), 1, "{:?}", meta.warnings);
            assert!(meta.warnings[0].contains(&format!("header declares {bogus} values per row")));
            // Header-level: visible from the metadata-only path too.
            let scanner = scan_sav_from_reader(Cursor::new(patched), 100).unwrap();
            assert_eq!(scanner.metadata().warnings.len(), 1);
        }
    }
}

#[test]
fn duplicate_variable_names_are_rejected() {
    let (mut bytes, _) = sample_sav(Compression::None);
    // Give the second variable the first one's short name; the long-name
    // record then maps both to "age".
    let first = type2_offset(&bytes, "AGE");
    let second = type2_offset(&bytes, "SCORE");
    let name: [u8; 8] = bytes[first + 24..first + 32].try_into().unwrap();
    bytes[second + 24..second + 32].copy_from_slice(&name);
    let result = read_sav_from_reader(Cursor::new(bytes)).map(|(b, _)| b);
    assert_invalid_dictionary(result, &["duplicate variable name", "age"]);
}

#[test]
fn issue1_repro_file_is_rejected_for_duplicate_names() {
    let path = "test_data/github_issues/issue1_buffer_overflow.sav";
    if !std::path::Path::new(path).exists() {
        eprintln!("Skipping: {path} not present (reporter-supplied fuzzer file)");
        return;
    }
    let result = read_sav(path).map(|(b, _)| b);
    assert_invalid_dictionary(result, &["duplicate variable name"]);
}

// ---------------------------------------------------------------------------
// Issues #3 and #4: impossible structures are rejected before they can cost
// memory or time; everything else is read with findings.
// ---------------------------------------------------------------------------

fn assert_invalid_variable(result: Result<RecordBatch, SpssError>, expect_in_msg: &[&str]) {
    match result {
        Err(SpssError::InvalidVariable(msg)) => {
            for needle in expect_in_msg {
                assert!(msg.contains(needle), "message {msg:?} lacks {needle:?}");
            }
        }
        Err(other) => panic!("expected InvalidVariable, got {other:?}"),
        Ok(batch) => panic!("expected an error, read {} rows", batch.num_rows()),
    }
}

#[test]
fn invalid_type_field_is_rejected_quickly() {
    let (bytes, _) = sample_sav(Compression::Bytecode);
    let off = type2_offset(&bytes, "NAME");
    // A negative value other than -1 is tolerated as a continuation slot with a
    // finding (SPSS opens such files); the string column simply disappears.
    {
        let mut patched = bytes.clone();
        put_i32(&mut patched, off + 4, -5);
        let (batch, meta) = read_sav_from_reader(Cursor::new(patched)).unwrap();
        assert_eq!(batch.num_columns(), 2);
        assert!(
            meta.warnings.iter().any(|w| w.contains("declares type -5")),
            "{:?}",
            meta.warnings
        );
    }
    for bogus in [256, 905_969_664, 272_302_081] {
        let mut patched = bytes.clone();
        put_i32(&mut patched, off + 4, bogus);
        let t0 = std::time::Instant::now();
        let result = read_sav_from_reader(Cursor::new(patched)).map(|(b, _)| b);
        assert_invalid_variable(result, &[&format!("declares type {bogus}"), "1-255"]);
        assert!(
            t0.elapsed().as_secs_f64() < 1.0,
            "type {bogus} took {:?}",
            t0.elapsed()
        );
    }
}

#[test]
fn too_many_missing_values_is_rejected_before_allocation() {
    let (bytes, _) = sample_sav(Compression::Bytecode);
    let off = type2_offset(&bytes, "AGE");
    for bogus in [4, -4, 675_295_820] {
        let mut patched = bytes.clone();
        put_i32(&mut patched, off + 12, bogus);
        let t0 = std::time::Instant::now();
        let result = read_sav_from_reader(Cursor::new(patched)).map(|(b, _)| b);
        assert_invalid_variable(result, &[&format!("declares {bogus} missing values")]);
        assert!(t0.elapsed().as_secs_f64() < 1.0);
    }
}

#[test]
fn oversized_info_record_is_rejected_before_allocation() {
    let (mut bytes, _) = sample_sav(Compression::Bytecode);
    // Subtype 13 (long variable names) is read with one bulk allocation of
    // size * count bytes; fixed-layout subtypes such as 3 ignore the count.
    let off = layout(&bytes)
        .info
        .into_iter()
        .find(|&(_, sub)| sub == 13)
        .map(|(o, _)| o)
        .expect("long names record");
    put_i32(&mut bytes, off + 8, 1); // size
    put_i32(&mut bytes, off + 12, 1_300_000_000); // count -> 1.3 GB declared
    let t0 = std::time::Instant::now();
    let err = read_sav_from_reader(Cursor::new(bytes))
        .err()
        .expect("must fail");
    assert!(
        matches!(err, SpssError::TruncatedFile { .. }),
        "got {err:?}"
    );
    assert!(t0.elapsed().as_secs_f64() < 1.0, "took {:?}", t0.elapsed());
}

#[test]
fn huge_declared_case_count_reads_quickly() {
    let (bytes, _) = sample_sav(Compression::Bytecode);
    let (expected, _) = read_sav_from_reader(Cursor::new(bytes.clone())).unwrap();
    let t0 = std::time::Instant::now();
    let (batch, meta) =
        read_sav_from_reader(Cursor::new(with_case_count(bytes, 6_881_281))).unwrap();
    assert!(t0.elapsed().as_secs_f64() < 1.0, "took {:?}", t0.elapsed());
    assert_eq!(batch, expected);
    assert!(
        meta.warnings
            .iter()
            .any(|w| w.contains("declares 6881281 rows but 5 rows were read"))
    );
}

#[test]
fn temporal_out_of_range_becomes_null_with_finding() {
    use arrow::array::{Date32Builder, Float64Builder};
    let schema = Arc::new(Schema::new(vec![
        Field::new("d", DataType::Date32, true),
        Field::new("x", DataType::Float64, true),
    ]));
    let mut d = Date32Builder::new();
    let mut x = Float64Builder::new();
    for i in 0..5 {
        d.append_value(19_723 + i); // 2024-01-01 ..
        x.append_value(i as f64);
    }
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(d.finish()), Arc::new(x.finish())]).unwrap();
    let meta = SpssMetadata::from_arrow_schema(batch.schema().as_ref());
    let mut cursor = Cursor::new(Vec::new());
    write_sav_to_writer(&mut cursor, &batch, &meta, Compression::None, None).unwrap();
    let mut bytes = cursor.into_inner();

    let (clean, clean_meta) = read_sav_from_reader(Cursor::new(bytes.clone())).unwrap();
    assert!(clean_meta.warnings.is_empty());
    assert_eq!(clean.column(0).null_count(), 0);

    // Column "d" is slot 0; rows are 2 slots wide (uncompressed).
    let start = layout(&bytes).data_start;
    put_f64(&mut bytes, start + 16, 1e300); // row 1: absurd seconds
    put_f64(&mut bytes, start + 3 * 16, f64::NAN); // row 3: not a number
    let (batch, meta) = read_sav_from_reader(Cursor::new(bytes)).unwrap();
    let d = batch.column(0);
    assert_eq!(d.null_count(), 2);
    assert!(d.is_null(1) && d.is_null(3));
    assert_eq!(d.slice(0, 1).as_ref(), clean.column(0).slice(0, 1).as_ref());
    assert_eq!(meta.warnings.len(), 1, "{:?}", meta.warnings);
    assert!(
        meta.warnings[0].starts_with("2 date/time values were outside the representable range")
    );
}

#[test]
fn issue3_and_issue4_files_read_or_fail_fast() {
    use std::time::Instant;
    let cases: [(&str, &str); 6] = [
        ("issue3_case-size", "reads"),
        ("issue3_case-count", "reads"),
        ("issue3_extension", "errors"),
        ("issue3_missing-count", "errors"),
        ("issue4_slow-bytecode", "errors"),
        ("issue4_slow-uncompressed", "errors"),
    ];
    for (name, expect) in cases {
        let path = format!("test_data/github_issues/{name}.sav");
        if !std::path::Path::new(&path).exists() {
            eprintln!("Skipping: {path} not present");
            continue;
        }
        let t0 = Instant::now();
        let result = read_sav(&path);
        let took = t0.elapsed().as_secs_f64();
        assert!(took < 1.0, "{name} took {took:.2}s");
        match (expect, result) {
            ("reads", Ok((batch, meta))) => {
                assert!(batch.num_rows() >= 1, "{name}");
                assert!(!meta.warnings.is_empty(), "{name} should carry findings");
            }
            ("errors", Err(_)) => {}
            (e, r) => panic!(
                "{name}: expected {e}, got {:?}",
                r.map(|(b, _)| b.num_rows())
            ),
        }
    }
}
