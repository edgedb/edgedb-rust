use crate::value::Value;

pub(crate) const EMPTY: usize = 0x01;
pub(crate) const LB_INC: usize = 0x02;
pub(crate) const UB_INC: usize = 0x04;
pub(crate) const LB_INF: usize = 0x08;
pub(crate) const UB_INF: usize = 0x10;


#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "with-serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Range<T> {
    pub(crate) lower: Option<T>,
    pub(crate) upper: Option<T>,
    pub(crate) inc_lower: bool,
    pub(crate) inc_upper: bool,
    pub(crate) empty: bool,
}

impl<T> From<std::ops::Range<T>> for Range<T> {
    fn from(src: std::ops::Range<T>) -> Range<T> {
        Range {
            lower: Some(src.start),
            upper: Some(src.end),
            inc_lower: true,
            inc_upper: false,
            empty: false,
        }
    }
}

impl<T: Into<Value>> From<std::ops::Range<T>> for Value {
    fn from(src: std::ops::Range<T>) -> Value {
        Range::from(src).into_value()
    }
}

impl<T> Range<T> {
    /// Constructor of the empty range
    pub fn empty() -> Range<T> {
        Range {
            lower: None,
            upper: None,
            inc_lower: true,
            inc_upper: false,
            empty: true,
        }
    }
    pub fn lower(&self) -> Option<&T> {
        self.lower.as_ref()
    }
    pub fn upper(&self) -> Option<&T> {
        self.upper.as_ref()
    }
    pub fn inc_lower(&self) -> bool {
        self.inc_lower
    }
    pub fn inc_upper(&self) -> bool {
        self.inc_upper
    }
    pub fn is_empty(&self) -> bool {
        self.empty
    }
}

impl<T: Into<Value>> Range<T> {
    pub fn into_value(self) -> Value {
        Value::Range(Range {
            lower: self.lower.map(|v| Box::new(v.into())),
            upper: self.upper.map(|v| Box::new(v.into())),
            inc_lower: self.inc_lower,
            inc_upper: self.inc_upper,
            empty: self.empty,
        })
    }
}
