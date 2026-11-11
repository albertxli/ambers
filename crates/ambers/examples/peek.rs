//! Print a few metadata fields and the first rows of a .sav/.zsav file.
//!
//! ```text
//! cargo run -p ambers --example peek -- tests/data/survey.sav
//! ```

use arrow::util::pretty::print_batches;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args()
        .nth(1)
        .ok_or("usage: peek <file.sav|file.zsav>")?;

    let mut scanner = ambers::scan_sav(&path)?;
    let meta = scanner.metadata();
    println!("label:       {}", meta.file_label);
    println!("encoding:    {}", meta.file_encoding);
    println!("compression: {:?}", meta.compression);
    println!("rows:        {:?}", meta.number_rows);
    println!("columns:     {}", meta.number_columns);
    for name in meta.variable_names.iter().take(5) {
        println!("  {name}: {}", meta.label(name).unwrap_or(""));
    }

    scanner.limit(5);
    let batch = scanner.collect_single()?;
    print_batches(&[batch])?;
    Ok(())
}
