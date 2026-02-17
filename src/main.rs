use std::env;

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        eprintln!("Usage: ambers <file.sav>");
        eprintln!("  Reads an SPSS .sav or .zsav file and prints a summary.");
        std::process::exit(1);
    }

    let path = &args[1];

    match ambers::read_sav(path) {
        Ok((batch, meta)) => {
            // File-level metadata
            println!("=== FILE-LEVEL METADATA ===");
            println!("  file_label:      {:?}", meta.file_label);
            println!("  file_encoding:   {}", meta.file_encoding);
            println!("  compression:     {:?}", meta.compression);
            println!("  file_format:     {}", meta.file_format);
            println!("  creation_time:   {}", meta.creation_time);
            println!("  modification_time: {}", meta.modification_time);
            println!("  number_rows:     {:?}", meta.number_rows);
            println!("  number_columns:  {}", meta.number_columns);
            println!("  weight_variable: {:?}", meta.weight_variable);

            println!("\n  Data (Arrow RecordBatch):");
            println!("    Rows: {}", batch.num_rows());
            println!("    Columns: {}", batch.num_columns());

            // Notes
            if !meta.notes.is_empty() {
                println!("\n=== NOTES ({}) ===", meta.notes.len());
                for (i, note) in meta.notes.iter().enumerate() {
                    println!("  [{}] {}", i, note);
                }
            } else {
                println!("\n=== NOTES: (none) ===");
            }

            // MR sets
            if !meta.mr_sets.is_empty() {
                println!("\n=== MR_SETS ({}) ===", meta.mr_sets.len());
                for (name, mr) in &meta.mr_sets {
                    println!("  {name}: type={:?}, label={:?}, vars={:?}", mr.mr_type, mr.label, mr.variables);
                }
            } else {
                println!("\n=== MR_SETS: (none) ===");
            }

            let n = 10.min(meta.variable_names.len());
            let preview_names = &meta.variable_names[..n];

            // variable_names
            println!("\n=== variable_names (first {n} of {}) ===", meta.variable_names.len());
            for name in preview_names {
                println!("  {name}");
            }

            // variable_labels
            println!("\n=== variable_labels (first {n}) ===");
            for name in preview_names {
                let label = meta.variable_labels.get(name).map(|s| s.as_str()).unwrap_or("(none)");
                println!("  {name:30} => {label:?}");
            }

            // spss_variable_types
            println!("\n=== spss_variable_types (first {n}) ===");
            for name in preview_names {
                let fmt = meta.spss_variable_types.get(name).map(|s| s.as_str()).unwrap_or("?");
                println!("  {name:30} => {fmt}");
            }

            // rust_variable_types
            println!("\n=== rust_variable_types (first {n}) ===");
            for name in preview_names {
                let rt = meta.rust_variable_types.get(name).map(|s| s.as_str()).unwrap_or("?");
                println!("  {name:30} => {rt}");
            }

            // variable_measure
            println!("\n=== variable_measure (first {n}) ===");
            for name in preview_names {
                let m = meta.variable_measure.get(name).map(|m| m.as_str()).unwrap_or("?");
                println!("  {name:30} => {m}");
            }

            // variable_alignment
            println!("\n=== variable_alignment (first {n}) ===");
            for name in preview_names {
                let a = meta.variable_alignment.get(name).map(|a| a.as_str()).unwrap_or("?");
                println!("  {name:30} => {a}");
            }

            // variable_display_width
            println!("\n=== variable_display_width (first {n}) ===");
            for name in preview_names {
                let w = meta.variable_display_width.get(name).map(|w| w.to_string()).unwrap_or("?".to_string());
                println!("  {name:30} => {w}");
            }

            // variable_storage_width
            println!("\n=== variable_storage_width (first {n}) ===");
            for name in preview_names {
                let w = meta.variable_storage_width.get(name).map(|w| w.to_string()).unwrap_or("?".to_string());
                println!("  {name:30} => {w}");
            }

            // variable_missing
            if !meta.variable_missing.is_empty() {
                let missing_preview: Vec<_> = preview_names.iter()
                    .filter(|n| meta.variable_missing.contains_key(*n))
                    .collect();
                println!("\n=== variable_missing (from first {n}, {} have missing) ===", missing_preview.len());
                for name in missing_preview {
                    let specs = &meta.variable_missing[name];
                    println!("  {name}:");
                    for spec in specs {
                        match spec {
                            ambers::MissingSpec::Value(v) => println!("    discrete: {v}"),
                            ambers::MissingSpec::Range { lo, hi } => println!("    range: {lo}..{hi}"),
                            ambers::MissingSpec::StringValue(s) => println!("    string: {s:?}"),
                        }
                    }
                }
                // Also show total count
                println!("  ... ({} variables total have missing value specs)", meta.variable_missing.len());
            } else {
                println!("\n=== variable_missing: (none) ===");
            }

            // variable_value_labels
            let vvl_preview: Vec<_> = preview_names.iter()
                .filter(|n| meta.variable_value_labels.contains_key(*n))
                .collect();
            println!("\n=== variable_value_labels (from first {n}, {} have labels) ===", vvl_preview.len());
            for name in vvl_preview {
                let labels = &meta.variable_value_labels[name];
                println!("  {name}:");
                let mut entries: Vec<_> = labels.iter().collect();
                entries.sort_by(|a, b| a.0.cmp(b.0));
                for (value, label) in entries {
                    println!("    {value} => {label:?}");
                }
            }
            // Total count
            println!("  ... ({} variables total have value labels)", meta.variable_value_labels.len());
        }
        Err(e) => {
            eprintln!("Error reading {path}: {e}");
            std::process::exit(1);
        }
    }
}
