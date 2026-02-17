use crate::io_utils;

/// Parse subtype 13: long variable names.
///
/// Format: `SHORT_NAME=LongVariableName\tSHORT2=LongName2\t...`
///
/// Returns a vector of (short_name, long_name) pairs.
pub fn parse_long_var_names(data: &[u8]) -> Vec<(String, String)> {
    let text = io_utils::bytes_to_string_lossy(data);
    let mut result = Vec::new();

    for pair in text.split('\t') {
        let pair = pair.trim();
        if pair.is_empty() {
            continue;
        }
        if let Some((short, long)) = pair.split_once('=') {
            result.push((
                short.trim().to_uppercase(),
                long.trim().to_string(),
            ));
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_long_var_names() {
        let data = b"Q1=Question1\tQ2=Question_Two\tAGE=RespondentAge\t";
        let names = parse_long_var_names(data);

        assert_eq!(names.len(), 3);
        assert_eq!(names[0], ("Q1".to_string(), "Question1".to_string()));
        assert_eq!(names[1], ("Q2".to_string(), "Question_Two".to_string()));
        assert_eq!(names[2], ("AGE".to_string(), "RespondentAge".to_string()));
    }
}
