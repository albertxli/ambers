use ambers::read_sav;
use arrow::array::Array;
use arrow::datatypes::DataType;

#[test]
fn test_temporal_types_real_file() {
    let path = std::env::var("SAV_TEST_FILE")
        .unwrap_or_else(|_| "test_data/test_2_medium.sav".to_string());
    let (batch, meta) = match read_sav(&path) {
        Ok(result) => result,
        Err(_) => {
            eprintln!("Skipping test: file not found at {path}");
            eprintln!("Set SAV_TEST_FILE env var or place a .sav file at the default path");
            return;
        }
    };

    println!("Rows: {}, Columns: {}", batch.num_rows(), batch.num_columns());

    // Collect all temporal columns from the schema
    let schema = batch.schema();
    let mut date_cols = Vec::new();
    let mut timestamp_cols = Vec::new();
    let mut duration_cols = Vec::new();
    let mut float_cols = 0;
    let mut string_cols = 0;

    for field in schema.fields() {
        match field.data_type() {
            DataType::Date32 => date_cols.push(field.name().clone()),
            DataType::Timestamp(_, _) => timestamp_cols.push(field.name().clone()),
            DataType::Duration(_) => duration_cols.push(field.name().clone()),
            DataType::Float64 => float_cols += 1,
            DataType::Utf8View => string_cols += 1,
            other => panic!("Unexpected type: {:?} for column {}", other, field.name()),
        }
    }

    println!("\n=== Type Summary ===");
    println!("Float64 columns: {}", float_cols);
    println!("String columns:  {}", string_cols);
    println!("Date32 columns:  {} {:?}", date_cols.len(), date_cols);
    println!("Timestamp columns: {} {:?}", timestamp_cols.len(), timestamp_cols);
    println!("Duration columns:  {} {:?}", duration_cols.len(), duration_cols);

    // Check that at least some temporal columns were detected
    let total_temporal = date_cols.len() + timestamp_cols.len() + duration_cols.len();
    println!("\nTotal temporal columns: {}", total_temporal);

    // Print rust_variable_types for temporal columns
    println!("\n=== Metadata rust_variable_types for temporal cols ===");
    for name in date_cols.iter().chain(timestamp_cols.iter()).chain(duration_cols.iter()) {
        if let Some(rust_type) = meta.rust_variable_types.get(name.as_str()) {
            println!("  {} -> rust_type={}", name, rust_type);
        }
        if let Some(spss_type) = meta.spss_variable_types.get(name.as_str()) {
            println!("  {} -> spss_type={}", name, spss_type);
        }
    }

    // Print first few values of each temporal column
    println!("\n=== Sample values ===");
    for name in date_cols.iter().take(3) {
        let col = batch.column_by_name(name).unwrap();
        let arr = col.as_any().downcast_ref::<arrow::array::Date32Array>().unwrap();
        let vals: Vec<String> = (0..5.min(arr.len()))
            .map(|i| {
                if arr.is_null(i) {
                    "null".to_string()
                } else {
                    // Date32 = days since 1970-01-01
                    let days = arr.value(i);
                    format!("{} days (Date32)", days)
                }
            })
            .collect();
        println!("  {} (Date32): {:?}", name, vals);
    }

    for name in timestamp_cols.iter().take(3) {
        let col = batch.column_by_name(name).unwrap();
        let arr = col.as_any().downcast_ref::<arrow::array::TimestampMicrosecondArray>().unwrap();
        let vals: Vec<String> = (0..5.min(arr.len()))
            .map(|i| {
                if arr.is_null(i) {
                    "null".to_string()
                } else {
                    let us = arr.value(i);
                    format!("{} us (Timestamp)", us)
                }
            })
            .collect();
        println!("  {} (Timestamp): {:?}", name, vals);
    }

    for name in duration_cols.iter().take(3) {
        let col = batch.column_by_name(name).unwrap();
        let arr = col.as_any().downcast_ref::<arrow::array::DurationMicrosecondArray>().unwrap();
        let vals: Vec<String> = (0..5.min(arr.len()))
            .map(|i| {
                if arr.is_null(i) {
                    "null".to_string()
                } else {
                    let us = arr.value(i);
                    format!("{} us (Duration)", us)
                }
            })
            .collect();
        println!("  {} (Duration): {:?}", name, vals);
    }

    // Verify no nulls where there shouldn't be (first non-null date value should be reasonable)
    for name in &date_cols {
        let col = batch.column_by_name(name).unwrap();
        let arr = col.as_any().downcast_ref::<arrow::array::Date32Array>().unwrap();
        for i in 0..arr.len() {
            if !arr.is_null(i) {
                let days = arr.value(i);
                // Reasonable range: 1900-01-01 to 2100-01-01
                // 1900-01-01 = -25567 days from Unix epoch
                // 2100-01-01 = 47482 days from Unix epoch
                assert!(
                    days >= -25567 && days <= 47482,
                    "Date32 value {} out of reasonable range for column {}",
                    days,
                    name
                );
                break; // only check first non-null
            }
        }
    }

    for name in &timestamp_cols {
        let col = batch.column_by_name(name).unwrap();
        let arr = col.as_any().downcast_ref::<arrow::array::TimestampMicrosecondArray>().unwrap();
        for i in 0..arr.len() {
            if !arr.is_null(i) {
                let us = arr.value(i);
                // Reasonable range: 1900-01-01 to 2100-01-01 in microseconds
                let min_us: i64 = -25567 * 86400 * 1_000_000;
                let max_us: i64 = 47482 * 86400 * 1_000_000;
                assert!(
                    us >= min_us && us <= max_us,
                    "Timestamp value {} out of reasonable range for column {}",
                    us,
                    name
                );
                break;
            }
        }
    }

    assert!(total_temporal > 0, "Expected at least one temporal column in the test file");
}
