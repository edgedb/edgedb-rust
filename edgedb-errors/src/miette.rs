//! Miette support for EdgeDB error
//!
//! [miette](https://docs.io/miette) allows nice formatting of error
//!
use std::fmt::Display;
use miette::{SourceCode, LabeledSpan};


use crate::Error;

impl miette::Diagnostic for Error {
    fn code(&self) -> Option<Box<dyn Display + '_>> {
        Some(Box::new(self.kind_name()))
    }
    fn source_code(&self) -> Option<&dyn SourceCode> {
        self.0.source_code.as_ref().map(|s| s as _)
    }
    fn labels(&self) -> Option<Box<dyn Iterator<Item = LabeledSpan> + '_>> {
        let (start, end) = self.position_start().zip(self.position_end())?;
        let len = end - start;
        Some(Box::new(
                Some(LabeledSpan::new(self.hint().map(Into::into), start, len))
                .into_iter()))
    }
    fn help(&self) -> Option<Box<dyn Display + '_>> {
        self.details().map(|v| Box::new(v) as Box<dyn Display>)
    }
}
