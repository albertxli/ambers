/// SPSS system-missing value (specific NaN bit pattern used by SPSS).
pub const SYSMIS_BITS: u64 = 0xFFEF_FFFF_FFFF_FFFF;

/// Highest representable double in SPSS.
pub const HIGHEST_BITS: u64 = 0x7FEF_FFFF_FFFF_FFFF;

/// Lowest representable double in SPSS.
pub const LOWEST_BITS: u64 = 0xFFEF_FFFF_FFFF_FFFE;

/// Default compression bias (added to bytecodes 1..=251).
pub const DEFAULT_BIAS: f64 = 100.0;

// -- Bytecode compression control codes --

/// Padding / skip.
pub const COMPRESS_SKIP: u8 = 0;
/// End of file marker.
pub const COMPRESS_END_OF_FILE: u8 = 252;
/// Next 8 raw bytes follow as uncompressed data.
pub const COMPRESS_RAW_FOLLOWS: u8 = 253;
/// Represents 8 ASCII spaces (0x20).
pub const COMPRESS_EIGHT_SPACES: u8 = 254;
/// System-missing value.
pub const COMPRESS_SYSMIS: u8 = 255;

// -- SAV record type codes --

pub const RECORD_TYPE_VARIABLE: i32 = 2;
pub const RECORD_TYPE_VALUE_LABEL: i32 = 3;
pub const RECORD_TYPE_VALUE_LABEL_VARS: i32 = 4;
pub const RECORD_TYPE_DOCUMENT: i32 = 6;
pub const RECORD_TYPE_INFO: i32 = 7;
pub const RECORD_TYPE_DICT_TERMINATION: i32 = 999;

// -- Info record subtypes --

pub const INFO_MR_SETS: i32 = 7;
pub const INFO_INTEGER: i32 = 3;
pub const INFO_FLOAT: i32 = 4;
pub const INFO_VAR_DISPLAY: i32 = 11;
pub const INFO_LONG_NAMES: i32 = 13;
pub const INFO_VERY_LONG_STRINGS: i32 = 14;
pub const INFO_ENCODING: i32 = 20;
pub const INFO_LONG_STRING_LABELS: i32 = 21;
pub const INFO_LONG_STRING_MISSING: i32 = 22;

// -- Enums --

/// SPSS compression type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compression {
    None,
    Bytecode,
    Zlib,
}

impl Compression {
    pub fn from_i32(val: i32) -> Option<Compression> {
        match val {
            0 => Some(Compression::None),
            1 => Some(Compression::Bytecode),
            2 => Some(Compression::Zlib),
            _ => None,
        }
    }
}

/// Variable measurement level.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Measure {
    Unknown,
    Nominal,
    Ordinal,
    Scale,
}

impl Measure {
    pub fn from_i32(val: i32) -> Measure {
        match val {
            1 => Measure::Nominal,
            2 => Measure::Ordinal,
            3 => Measure::Scale,
            _ => Measure::Unknown,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Measure::Unknown => "unknown",
            Measure::Nominal => "nominal",
            Measure::Ordinal => "ordinal",
            Measure::Scale => "scale",
        }
    }
}

/// Variable alignment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Alignment {
    Left,
    Right,
    Center,
}

impl Alignment {
    pub fn from_i32(val: i32) -> Alignment {
        match val {
            0 => Alignment::Left,
            1 => Alignment::Right,
            2 => Alignment::Center,
            _ => Alignment::Left,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Alignment::Left => "left",
            Alignment::Right => "right",
            Alignment::Center => "center",
        }
    }
}

/// SPSS variable type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VarType {
    Numeric,
    String(usize), // width in bytes
}

/// SPSS print/write format type codes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum FormatType {
    A = 1,
    Ahex = 2,
    Comma = 3,
    Dollar = 4,
    F = 5,
    Ib = 6,
    PibHex = 7,
    P = 8,
    Pib = 9,
    Pk = 10,
    Rb = 11,
    RbHex = 12,
    Z = 15,
    N = 16,
    E = 17,
    Date = 20,
    Time = 21,
    DateTime = 22,
    ADate = 23,
    JDate = 24,
    DTime = 25,
    Wkday = 26,
    Month = 27,
    Moyr = 28,
    Qyr = 29,
    Wkyr = 30,
    Pct = 31,
    Dot = 32,
    Cca = 33,
    Ccb = 34,
    Ccc = 35,
    Ccd = 36,
    Cce = 37,
    EDate = 38,
    SDate = 39,
    MTime = 40,
    YmDhms = 41,
}

impl FormatType {
    pub fn from_u8(val: u8) -> Option<FormatType> {
        match val {
            1 => Some(FormatType::A),
            2 => Some(FormatType::Ahex),
            3 => Some(FormatType::Comma),
            4 => Some(FormatType::Dollar),
            5 => Some(FormatType::F),
            6 => Some(FormatType::Ib),
            7 => Some(FormatType::PibHex),
            8 => Some(FormatType::P),
            9 => Some(FormatType::Pib),
            10 => Some(FormatType::Pk),
            11 => Some(FormatType::Rb),
            12 => Some(FormatType::RbHex),
            15 => Some(FormatType::Z),
            16 => Some(FormatType::N),
            17 => Some(FormatType::E),
            20 => Some(FormatType::Date),
            21 => Some(FormatType::Time),
            22 => Some(FormatType::DateTime),
            23 => Some(FormatType::ADate),
            24 => Some(FormatType::JDate),
            25 => Some(FormatType::DTime),
            26 => Some(FormatType::Wkday),
            27 => Some(FormatType::Month),
            28 => Some(FormatType::Moyr),
            29 => Some(FormatType::Qyr),
            30 => Some(FormatType::Wkyr),
            31 => Some(FormatType::Pct),
            32 => Some(FormatType::Dot),
            33 => Some(FormatType::Cca),
            34 => Some(FormatType::Ccb),
            35 => Some(FormatType::Ccc),
            36 => Some(FormatType::Ccd),
            37 => Some(FormatType::Cce),
            38 => Some(FormatType::EDate),
            39 => Some(FormatType::SDate),
            40 => Some(FormatType::MTime),
            41 => Some(FormatType::YmDhms),
            _ => None,
        }
    }

    pub fn prefix(&self) -> &'static str {
        match self {
            FormatType::A => "A",
            FormatType::Ahex => "AHEX",
            FormatType::Comma => "COMMA",
            FormatType::Dollar => "DOLLAR",
            FormatType::F => "F",
            FormatType::Ib => "IB",
            FormatType::PibHex => "PIBHEX",
            FormatType::P => "P",
            FormatType::Pib => "PIB",
            FormatType::Pk => "PK",
            FormatType::Rb => "RB",
            FormatType::RbHex => "RBHEX",
            FormatType::Z => "Z",
            FormatType::N => "N",
            FormatType::E => "E",
            FormatType::Date => "DATE",
            FormatType::Time => "TIME",
            FormatType::DateTime => "DATETIME",
            FormatType::ADate => "ADATE",
            FormatType::JDate => "JDATE",
            FormatType::DTime => "DTIME",
            FormatType::Wkday => "WKDAY",
            FormatType::Month => "MONTH",
            FormatType::Moyr => "MOYR",
            FormatType::Qyr => "QYR",
            FormatType::Wkyr => "WKYR",
            FormatType::Pct => "PCT",
            FormatType::Dot => "DOT",
            FormatType::Cca => "CCA",
            FormatType::Ccb => "CCB",
            FormatType::Ccc => "CCC",
            FormatType::Ccd => "CCD",
            FormatType::Cce => "CCE",
            FormatType::EDate => "EDATE",
            FormatType::SDate => "SDATE",
            FormatType::MTime => "MTIME",
            FormatType::YmDhms => "YMDHMS",
        }
    }

    /// Whether this format type represents a string variable.
    pub fn is_string(&self) -> bool {
        matches!(self, FormatType::A | FormatType::Ahex)
    }
}

/// Decoded SPSS print/write format.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpssFormat {
    pub format_type: FormatType,
    pub width: u8,
    pub decimals: u8,
}

impl SpssFormat {
    /// Decode a packed i32 format specification.
    /// Layout: `(type << 16) | (width << 8) | decimals`
    pub fn from_packed(packed: i32) -> Option<SpssFormat> {
        let raw = packed as u32;
        let format_type_byte = ((raw >> 16) & 0xFF) as u8;
        let width = ((raw >> 8) & 0xFF) as u8;
        let decimals = (raw & 0xFF) as u8;

        FormatType::from_u8(format_type_byte).map(|format_type| SpssFormat {
            format_type,
            width,
            decimals,
        })
    }

    /// Render as a human-readable SPSS format string like "F8.2" or "A50".
    pub fn to_spss_string(&self) -> String {
        if self.format_type.is_string() {
            format!("{}{}", self.format_type.prefix(), self.width)
        } else if self.decimals > 0 {
            format!(
                "{}{}.{}",
                self.format_type.prefix(),
                self.width,
                self.decimals
            )
        } else {
            format!("{}{}", self.format_type.prefix(), self.width)
        }
    }
}

/// Check if a raw f64 bit pattern is SYSMIS.
#[inline]
pub fn is_sysmis(val: f64) -> bool {
    val.to_bits() == SYSMIS_BITS
}

/// Get the SYSMIS value as f64.
#[inline]
pub fn sysmis() -> f64 {
    f64::from_bits(SYSMIS_BITS)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sysmis_is_negative_max() {
        // SYSMIS is -DBL_MAX, the most negative finite double, NOT NaN
        let val = sysmis();
        assert!(val.is_finite());
        assert!(val < 0.0);
        assert_eq!(val, -f64::MAX);
    }

    #[test]
    fn test_is_sysmis() {
        assert!(is_sysmis(sysmis()));
        assert!(!is_sysmis(0.0));
        assert!(!is_sysmis(f64::NAN)); // regular NaN != SYSMIS
    }

    #[test]
    fn test_format_decode() {
        // F8.2 = type 5, width 8, decimals 2
        let packed = (5 << 16) | (8 << 8) | 2;
        let fmt = SpssFormat::from_packed(packed).unwrap();
        assert_eq!(fmt.format_type, FormatType::F);
        assert_eq!(fmt.width, 8);
        assert_eq!(fmt.decimals, 2);
        assert_eq!(fmt.to_spss_string(), "F8.2");
    }

    #[test]
    fn test_format_string_type() {
        // A50 = type 1, width 50, decimals 0
        let packed = (1 << 16) | (50 << 8) | 0;
        let fmt = SpssFormat::from_packed(packed).unwrap();
        assert_eq!(fmt.format_type, FormatType::A);
        assert_eq!(fmt.to_spss_string(), "A50");
    }

    #[test]
    fn test_compression_from_i32() {
        assert_eq!(Compression::from_i32(0), Some(Compression::None));
        assert_eq!(Compression::from_i32(1), Some(Compression::Bytecode));
        assert_eq!(Compression::from_i32(2), Some(Compression::Zlib));
        assert_eq!(Compression::from_i32(99), None);
    }

    #[test]
    fn test_measure_from_i32() {
        assert_eq!(Measure::from_i32(1), Measure::Nominal);
        assert_eq!(Measure::from_i32(2), Measure::Ordinal);
        assert_eq!(Measure::from_i32(3), Measure::Scale);
        assert_eq!(Measure::from_i32(0), Measure::Unknown);
    }
}
