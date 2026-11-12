//! Text fitted to terminal columns. The viewer's data comes from
//! `ambers-dataset`; cutting and padding it for a grid of fixed-width
//! cells is the terminal's own concern.

use unicode_width::{UnicodeWidthChar, UnicodeWidthStr};

/// Fit `text` into exactly `width` terminal columns: padded on the left or
/// right, or cut with `…` when too long. Wide (CJK) characters count as two.
pub fn fit(text: &str, width: usize, right: bool) -> String {
    let text_width = text.width();
    if text_width <= width {
        let pad = " ".repeat(width - text_width);
        return if right {
            format!("{pad}{text}")
        } else {
            format!("{text}{pad}")
        };
    }
    if width == 0 {
        return String::new();
    }
    let mut cut = String::new();
    let mut used = 0;
    for ch in text.chars() {
        let ch_width = ch.width().unwrap_or(0);
        if used + ch_width > width - 1 {
            break;
        }
        cut.push(ch);
        used += ch_width;
    }
    cut.push('…');
    used += 1;
    cut.push_str(&" ".repeat(width - used));
    cut
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fit_pads_and_cuts() {
        assert_eq!(fit("ab", 4, false), "ab  ");
        assert_eq!(fit("ab", 4, true), "  ab");
        assert_eq!(fit("abcdef", 4, false), "abc…");
        assert_eq!(fit("abcd", 4, false), "abcd");
        // Wide characters take two columns each.
        assert_eq!(fit("満足度", 5, false), "満足…");
        assert_eq!(fit("満足", 5, false), "満足 ");
        assert_eq!(fit("abc", 0, false), "");
    }
}
