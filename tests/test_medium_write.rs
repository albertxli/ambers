use std::io::Cursor;
use std::time::Instant;

#[test]
fn test_medium_write_only() {
    let path = "test_data/test_2_medium.sav";
    if !std::path::Path::new(path).exists() {
        return;
    }

    let t0 = Instant::now();
    let (batch, meta) = ambers::read_sav(path).unwrap();
    eprintln!(
        "Read: {:.2}s  rows={} cols={}",
        t0.elapsed().as_secs_f64(),
        batch.num_rows(),
        batch.num_columns()
    );

    // Print storage widths for string columns to understand slot count
    let mut total_storage = 0usize;
    let mut n_vls = 0;
    for name in &meta.variable_names {
        if let Some(&w) = meta.variable_storage_widths.get(name.as_str()) {
            total_storage += w;
            if w > 255 {
                n_vls += 1;
            }
        }
    }
    eprintln!(
        "Storage total: {} bytes, VLS vars: {}",
        total_storage, n_vls
    );

    // Try writing just first 10 rows to measure speed per row
    let small_batch = batch.slice(0, 10);
    let t1 = Instant::now();
    let mut cursor = Cursor::new(Vec::new());
    ambers::write_sav_to_writer(
        &mut cursor,
        &small_batch,
        &meta,
        ambers::Compression::None,
        None,
    )
    .unwrap();
    let bytes_10 = cursor.into_inner().len();
    eprintln!(
        "Write 10 rows: {:.4}s  bytes={} (per_row={})",
        t1.elapsed().as_secs_f64(),
        bytes_10,
        bytes_10 / 10
    );

    // Try 100 rows
    let med_batch = batch.slice(0, 100);
    let t2 = Instant::now();
    let mut cursor = Cursor::new(Vec::new());
    ambers::write_sav_to_writer(
        &mut cursor,
        &med_batch,
        &meta,
        ambers::Compression::None,
        None,
    )
    .unwrap();
    eprintln!("Write 100 rows: {:.4}s", t2.elapsed().as_secs_f64());

    // Try 1000 rows
    let big_batch = batch.slice(0, 1000);
    let t3 = Instant::now();
    let mut cursor = Cursor::new(Vec::new());
    ambers::write_sav_to_writer(
        &mut cursor,
        &big_batch,
        &meta,
        ambers::Compression::None,
        None,
    )
    .unwrap();
    eprintln!("Write 1000 rows: {:.4}s", t3.elapsed().as_secs_f64());

    eprintln!("All write tests passed!");
}
