//! The committed synthetic fixtures in `tests/data/` (written by
//! `python/tests/make_fixtures.py`). Unlike `test_data/`, these are always
//! present, so nothing here skips.

#[path = "../src/test_support.rs"]
mod test_support;

use ambers::{Compression, read_sav, read_sav_metadata};
use arrow::datatypes::{DataType, TimeUnit};

fn fixture(name: &str) -> String {
    test_support::data_path(&format!("tests/data/{name}"))
}

#[test]
fn survey_reads_in_every_compression() {
    for (name, compression) in [
        ("survey.sav", Compression::Bytecode),
        ("survey_uncompressed.sav", Compression::None),
        ("survey.zsav", Compression::Zlib),
    ] {
        let (batch, meta) = read_sav(fixture(name)).unwrap();
        assert_eq!((batch.num_rows(), batch.num_columns()), (20, 13), "{name}");
        assert_eq!(meta.compression, compression, "{name}");
        assert!(meta.warnings.is_empty(), "{name}: {:?}", meta.warnings);
    }
}

#[test]
fn survey_metadata() {
    let meta = read_sav_metadata(fixture("survey.sav")).unwrap();
    assert_eq!(meta.file_label, "ambers synthetic survey fixture");
    assert_eq!(meta.number_rows, Some(20));
    assert_eq!(meta.weight_variable.as_deref(), Some("weight"));
    assert_eq!(meta.label("satisfaction"), Some("Overall satisfaction"));
    assert_eq!(meta.variable_value_labels["satisfaction"].len(), 6);
    assert_eq!(meta.variable_missing_values.len(), 3);
    assert_eq!(meta.mr_sets["brands"].variables.len(), 3);
}

#[test]
fn survey_temporal_columns() {
    let (batch, _) = read_sav(fixture("survey.sav")).unwrap();
    let schema = batch.schema();
    let dtype = |name: &str| schema.field_with_name(name).unwrap().data_type().clone();
    assert_eq!(dtype("interview_date"), DataType::Date32);
    assert_eq!(
        dtype("interview_start"),
        DataType::Timestamp(TimeUnit::Microsecond, None)
    );
    assert_eq!(dtype("duration"), DataType::Duration(TimeUnit::Microsecond));
    // Every seventh `satisfaction` answer is system-missing.
    let satisfaction = batch.column_by_name("satisfaction").unwrap();
    assert_eq!(satisfaction.null_count(), 2);
}

#[test]
fn long_strings_keep_their_full_width() {
    let (batch, meta) = read_sav(fixture("long_strings.sav")).unwrap();
    assert_eq!((batch.num_rows(), batch.num_columns()), (3, 3));
    assert_eq!(meta.format("comment"), Some("A1000"));
    let comment = batch
        .column_by_name("comment")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::StringViewArray>()
        .unwrap();
    assert_eq!(comment.value(0).len(), 600);
    assert!(comment.value(1).contains("とても良い"));
}
