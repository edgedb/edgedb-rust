use crate::queryable::Queryable;
use crate::value::Value;
use core::ops::{self, Bound};
use std::convert::TryFrom;
use std::ops::RangeBounds;

pub(crate) const EMPTY: usize = 0x01;
pub(crate) const LB_INC: usize = 0x02;
pub(crate) const UB_INC: usize = 0x04;
pub(crate) const LB_INF: usize = 0x08;
pub(crate) const UB_INF: usize = 0x10;

mod edgedb_ord {
    pub struct PrivateToken;
    use crate::model::{Datetime, LocalDate, LocalDatetime};
    use crate::value::Value;
    use std::cmp::Ordering;

    pub trait EdgedbOrd {
        // the token makes this uncallable and the trait unimplementable
        // can be removed once the design is stable
        fn edgedb_cmp(&self, other: &Self, _: PrivateToken) -> Ordering;
    }

    pub fn edgedb_cmp<T: EdgedbOrd>(x: &T, y: &T) -> Ordering {
        EdgedbOrd::edgedb_cmp(x, y, PrivateToken)
    }

    impl EdgedbOrd for i32 {
        fn edgedb_cmp(&self, other: &Self, _: PrivateToken) -> Ordering {
            self.cmp(other)
        }
    }

    impl EdgedbOrd for i64 {
        fn edgedb_cmp(&self, other: &Self, _: PrivateToken) -> Ordering {
            self.cmp(other)
        }
    }

    impl EdgedbOrd for Datetime {
        fn edgedb_cmp(&self, other: &Self, _: PrivateToken) -> Ordering {
            self.cmp(other)
        }
    }

    impl EdgedbOrd for LocalDatetime {
        fn edgedb_cmp(&self, other: &Self, _: PrivateToken) -> Ordering {
            self.cmp(other)
        }
    }

    impl EdgedbOrd for LocalDate {
        fn edgedb_cmp(&self, other: &Self, _: PrivateToken) -> Ordering {
            self.cmp(other)
        }
    }

    // NaN is bigger than all other values
    impl EdgedbOrd for f32 {
        fn edgedb_cmp(&self, other: &Self, _: PrivateToken) -> Ordering {
            self.partial_cmp(other)
                .unwrap_or_else(|| self.is_nan().cmp(&other.is_nan()))
        }
    }

    // NaN is bigger than all other values
    impl EdgedbOrd for f64 {
        fn edgedb_cmp(&self, other: &Self, _: PrivateToken) -> Ordering {
            self.partial_cmp(other)
                .unwrap_or_else(|| self.is_nan().cmp(&other.is_nan()))
        }
    }

    impl EdgedbOrd for Value {
        fn edgedb_cmp(&self, other: &Self, _: PrivateToken) -> Ordering {
            match (self, other) {
                (Value::Int32(x), Value::Int32(y)) => edgedb_cmp(x, y),
                (Value::Int64(x), Value::Int64(y)) => edgedb_cmp(x, y),
                (Value::Float32(x), Value::Float32(y)) => edgedb_cmp(x, y),
                (Value::Float64(x), Value::Float64(y)) => edgedb_cmp(x, y),
                (Value::Datetime(x), Value::Datetime(y)) => edgedb_cmp(x, y),
                (Value::LocalDatetime(x), Value::LocalDatetime(y)) => edgedb_cmp(x, y),
                (Value::LocalDate(x), Value::LocalDate(y)) => edgedb_cmp(x, y),
                (_, _) => panic!(
                    "Both values in a range need to have the same type. Found {} and {}",
                    self.kind(),
                    other.kind()
                ),
            }
        }
    }

    impl<T: EdgedbOrd> EdgedbOrd for &T {
        fn edgedb_cmp(&self, other: &Self, _: PrivateToken) -> Ordering {
            edgedb_cmp(*self, *other)
        }
    }

    impl<T> EdgedbOrd for Box<T>
    where
        T: EdgedbOrd,
    {
        fn edgedb_cmp(&self, other: &Self, _: PrivateToken) -> Ordering {
            edgedb_cmp(self, other)
        }
    }
}

mod range_scalar {
    use std::ops::{self, Bound};

    use super::edgedb_ord::EdgedbOrd;
    use crate::model::{Datetime, LocalDate, LocalDatetime, OutOfRangeError};
    use crate::value::Value;

    pub struct PrivateToken;

    pub trait RangeScalar: EdgedbOrd + Sized {
        fn is_discrete(&self, _: PrivateToken) -> bool {
            false
        }

        fn step_up(&mut self, _: PrivateToken) -> Result<(), OutOfRangeError> {
            Ok(())
        }

        fn empty_value() -> Self;
    }

    impl RangeScalar for i32 {
        fn is_discrete(&self, _: PrivateToken) -> bool {
            true
        }

        fn step_up(&mut self, _: PrivateToken) -> Result<(), OutOfRangeError> {
            *self = self.checked_add(1).ok_or(OutOfRangeError)?;
            Ok(())
        }

        fn empty_value() -> Self {
            0
        }
    }

    impl RangeScalar for i64 {
        fn is_discrete(&self, _: PrivateToken) -> bool {
            true
        }

        fn step_up(&mut self, _: PrivateToken) -> Result<(), OutOfRangeError> {
            *self = self.checked_add(1).ok_or(OutOfRangeError)?;
            Ok(())
        }

        fn empty_value() -> Self {
            0
        }
    }
    impl RangeScalar for f32 {
        fn empty_value() -> Self {
            0f32
        }
    }

    impl RangeScalar for f64 {
        fn empty_value() -> Self {
            0f64
        }
    }

    // impl RangeScalar for Decimal {} isn't possible because it doesn't support comparisons yet
    impl RangeScalar for Value {
        fn empty_value() -> Self {
            panic!("Range<Value> is not supported");
        }

        fn is_discrete(&self, _: PrivateToken) -> bool {
            match self {
                Value::Int32(x) => i32::is_discrete(x, PrivateToken),
                Value::Int64(x) => i64::is_discrete(x, PrivateToken),
                Value::Float32(x) => f32::is_discrete(x, PrivateToken),
                Value::Float64(x) => f64::is_discrete(x, PrivateToken),
                Value::Datetime(x) => Datetime::is_discrete(x, PrivateToken),
                Value::LocalDatetime(x) => LocalDatetime::is_discrete(x, PrivateToken),
                Value::LocalDate(x) => LocalDate::is_discrete(x, PrivateToken),
                _ => panic!("Unexpected Value kind {}", self.kind()),
            }
        }

        fn step_up(&mut self, _: PrivateToken) -> Result<(), OutOfRangeError> {
            todo!()
        }
    }

    impl RangeScalar for Datetime {
        fn empty_value() -> Self {
            Datetime::from_unix_micros(0)
        }
    }

    impl RangeScalar for LocalDatetime {
        fn empty_value() -> Self {
            Datetime::from_unix_micros(0).into()
        }
    }

    impl RangeScalar for LocalDate {
        fn is_discrete(&self, _: PrivateToken) -> bool {
            true
        }

        fn step_up(&mut self, _: PrivateToken) -> Result<(), OutOfRangeError> {
            *self = Self::try_from_days(self.to_days() + 1)?;
            Ok(())
        }

        fn empty_value() -> Self {
            Self::from_days(0)
        }
    }

    impl<T: RangeScalar> RangeScalar for Box<T> {
        fn is_discrete(&self, _: PrivateToken) -> bool {
            self.as_ref().is_discrete(PrivateToken)
        }

        fn step_up(&mut self, _: PrivateToken) -> Result<(), OutOfRangeError> {
            self.as_mut().step_up(PrivateToken)
        }

        fn empty_value() -> Self {
            Box::new(T::empty_value())
        }
    }

    pub fn start_to_inclusive<T: RangeScalar>(
        bound: Bound<T>,
    ) -> Result<Bound<T>, OutOfRangeError> {
        match bound {
            Bound::Excluded(mut value) => {
                if value.is_discrete(PrivateToken) {
                    value.step_up(PrivateToken)?;
                }
                Ok(Bound::Included(value))
            }
            other => Ok(other),
        }
    }

    pub fn end_to_exclusive<T: RangeScalar>(bound: Bound<T>) -> Result<Bound<T>, OutOfRangeError> {
        match bound {
            Bound::Included(mut value) => {
                if value.is_discrete(PrivateToken) {
                    value.step_up(PrivateToken)?;
                }
                Ok(Bound::Excluded(value))
            }
            other => Ok(other),
        }
    }
}

pub use range_scalar::RangeScalar;

use super::OutOfRangeError;

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct BoundedRange<T> {
    start_bound: Bound<T>,
    end_bound: Bound<T>,
}

impl<T> BoundedRange<T> {
    pub fn start_bound(&self) -> Bound<&T> {
        self.start_bound.as_ref()
    }

    pub fn end_bound(&self) -> Bound<&T> {
        self.end_bound.as_ref()
    }

    pub fn as_ref(&self) -> BoundedRange<&T> {
        BoundedRange {
            start_bound: self.start_bound(),
            end_bound: self.end_bound(),
        }
    }  

    pub fn into_bounds(self) -> (Bound<T>, Bound<T>) {
        (self.start_bound, self.end_bound)
    }

    pub const fn full() -> Self {
        BoundedRange {
            start_bound: Bound::Unbounded,
            end_bound: Bound::Unbounded,
        }
    }
}

fn are_range_bounds_empty<T: RangeScalar>(start_bound: Bound<&T>, end_bound: Bound<&T>) -> bool {
    use edgedb_ord::edgedb_cmp;

    match (start_bound, end_bound) {
        (Bound::Unbounded, _) => false,
        (_, Bound::Unbounded) => false,
        (Bound::Included(start), Bound::Included(end)) => edgedb_cmp(start, end).is_gt(),
        (Bound::Excluded(start), Bound::Excluded(end)) => edgedb_cmp(start, end).is_ge(),
        (Bound::Included(start), Bound::Excluded(end)) => edgedb_cmp(start, end).is_ge(),
        (Bound::Excluded(start), Bound::Included(end)) => edgedb_cmp(start, end).is_ge(),
    }
}

impl<T: RangeScalar> BoundedRange<T> {
    fn from_bounds(
        start_bound: Bound<T>,
        end_bound: Bound<T>,
    ) -> Result<Option<BoundedRange<T>>, OutOfRangeError> {
        let start_bound = range_scalar::start_to_inclusive(start_bound)?;
        let end_bound = range_scalar::end_to_exclusive(end_bound)?;

        Ok(
            if are_range_bounds_empty(start_bound.as_ref(), end_bound.as_ref()) {
                Some(BoundedRange {
                    start_bound,
                    end_bound,
                })
            } else {
                None
            },
        )
    }
}

impl<T> RangeBounds<T> for BoundedRange<T> {
    fn start_bound(&self) -> Bound<&T> {
        self.start_bound()
    }

    fn end_bound(&self) -> Bound<&T> {
        self.end_bound()
    }
}

#[cfg_attr(
    feature = "with-serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(try_from = "RangeFields", into = "RangeFields")
)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum Range<T> {
    Empty,
    NonEmpty(BoundedRange<T>),
}

impl<T: RangeScalar> Range<T> {
    pub fn from_bounds(
        start_bound: Bound<T>,
        end_bound: Bound<T>,
    ) -> Result<Range<T>, OutOfRangeError> {
        Ok(match BoundedRange::from_bounds(start_bound, end_bound)? {
            Some(bounded) => Range::NonEmpty(bounded),
            None => Range::Empty,
        })
    }
}

impl<T: RangeScalar> From<ops::Range<T>> for Range<T> {
    fn from(src: ops::Range<T>) -> Range<T> {
        Self::from_bounds(Bound::Included(src.start), Bound::Excluded(src.end))
            .expect("OutOfBoundsErrors can't occur because the start is already inclusive, and the end exclusive")
    }
}

impl<T> From<ops::RangeFull> for Range<T> {
    fn from(_: ops::RangeFull) -> Range<T> {
        Range::full()
    }
}

impl<T: RangeScalar> From<ops::RangeTo<T>> for Range<T> {
    fn from(src: ops::RangeTo<T>) -> Range<T> {
        Self::from_bounds(Bound::Unbounded, Bound::Excluded(src.end))
            .expect("OutOfBoundsErrors can't occur because the start is unbounded, and the end exclusive")
    }
}

impl<T: RangeScalar> TryFrom<ops::RangeInclusive<T>> for Range<T> {
    type Error = OutOfRangeError;

    fn try_from(src: ops::RangeInclusive<T>) -> Result<Self, Self::Error> {
        let (start, end) = src.into_inner();
        Self::from_bounds(Bound::Included(start), Bound::Included(end))
    }    
}

impl<T: RangeScalar> TryFrom<ops::RangeToInclusive<T>> for Range<T> {
    type Error = OutOfRangeError;

    fn try_from(src: ops::RangeToInclusive<T>) -> Result<Self, Self::Error> {
        Self::from_bounds(Bound::Unbounded, Bound::Included(src.end))
    }    
}

struct FromRangeError(&'static &'static str);

impl<T: RangeScalar> TryFrom<Range<T>> for ops::Range<T> {
    type Error = FromRangeError;

    fn try_from(value: Range<T>) -> Result<Self, Self::Error> {
        match value {
            Range::Empty => Ok(ops::Range {
                start: T::empty_value(),
                end: T::empty_value()
            }),
            Range::NonEmpty(bounded) => {
                let inclusive_start = match bounded.start_bound {
                    Bound::Included(x) => x,
                    Bound::Excluded(_) => return Err(FromRangeError(&"start_bound must be Included, was Excluded")),
                    Bound::Unbounded => return Err(FromRangeError(&"start_bound must be Included, was Unbounded")),
                };
                let exclusive_end = match bounded.end_bound {
                    Bound::Included(_) => return Err(FromRangeError(&"end_bound must be Excluded, was Included")),
                    Bound::Excluded(x) => x,
                    Bound::Unbounded => return Err(FromRangeError(&"end_bound must be Excluded, was Unbounded")),
                };
                Ok(ops::Range {
                    start: inclusive_start,
                    end: exclusive_end
                })
            }
        }
    }
}

impl<T: Into<Value> + RangeScalar> From<ops::Range<T>> for Value {
    fn from(src: ops::Range<T>) -> Value {
        Range::from(src).into_value()
    }
}

impl<T> Range<T> {
    /// Constructor of the empty range
    pub const fn empty() -> Self {
        Range::Empty
    }

    pub const fn full() -> Range<T> {
        Range::NonEmpty(BoundedRange::full())
    }

    //tbd: should this exist? Should it return an option?
    pub fn lower(&self) -> Option<&T> {
        match self {
            Range::Empty => None,
            Range::NonEmpty(bounded) => match bounded.start_bound() {
                Bound::Included(value) => Some(value),
                Bound::Excluded(value) => Some(value),
                Bound::Unbounded => None,
            },
        }
    }

    //tbd: should this exist? Should it return an option?
    pub fn upper(&self) -> Option<&T> {
        match self {
            Range::Empty => None,
            Range::NonEmpty(bounded) => match bounded.end_bound() {
                Bound::Included(value) => Some(value),
                Bound::Excluded(value) => Some(value),
                Bound::Unbounded => None,
            },
        }
    }

    //tbd: should this exist? Should it return an option?
    pub fn inc_lower(&self) -> bool {
        match self {
            Range::Empty => false,
            Range::NonEmpty(bounded) => match bounded.start_bound() {
                Bound::Included(_) => true,
                Bound::Excluded(_) => false,
                Bound::Unbounded => false,
            },
        }
    }

    //tbd: should this exist? Should it return an option?
    pub fn inc_upper(&self) -> bool {
        match self {
            Range::Empty => false,
            Range::NonEmpty(bounded) => match bounded.end_bound() {
                Bound::Included(_) => true,
                Bound::Excluded(_) => false,
                Bound::Unbounded => false,
            },
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            Range::Empty => true,
            Range::NonEmpty(_) => false,
        }
    }

    pub fn as_ref(&self) -> Range<&T> {
        match self {
            Range::Empty => Range::Empty,
            Range::NonEmpty(bounded) => Range::NonEmpty(bounded.as_ref()),
        }
    }
}

impl<T: Into<Value>> Range<T> {
    pub fn into_value(self) -> Value {
        Value::Range(match self {
            Range::Empty => Range::Empty,
            Range::NonEmpty(non_empty) => {
                let start = non_empty.start_bound.map(|v| Box::new(v.into()));
                let end = non_empty.end_bound.map(|v| Box::new(v.into()));
                Range::from_bounds(start, end)
                    .expect("Converting into `Value` should not affect the validity of the bounds. T::Into<Value> appears to be broken.")
            }
        })
    }
}

#[cfg_attr(feature = "with-serde", derive(serde::Serialize, serde::Deserialize))]
struct RangeFields<T> {
    lower: Option<T>,
    upper: Option<T>,
    inc_lower: Option<bool>,
    inc_upper: Option<bool>,
    empty: Option<bool>,
}

impl<'t, T> From<&'t Range<T>> for RangeFields<&'t T> {
    fn from(value: &'t Range<T>) -> Self {
        match value {
            Range::Empty => RangeFields {
                lower: None,
                upper: None,
                inc_lower: None,
                inc_upper: None,
                empty: Some(true),
            },
            Range::NonEmpty(_) => RangeFields {
                lower: value.lower(),
                upper: value.upper(),
                inc_lower: Some(value.inc_lower()),
                inc_upper: Some(value.inc_upper()),
                empty: None,
            },
        }
    }
}
