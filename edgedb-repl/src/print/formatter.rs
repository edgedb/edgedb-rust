use crate::print::stream::Output;
use crate::print::Printer;

use colorful::{Colorful, core::color_string::CString};
use uuid::Uuid;

use crate::print::buffer::Result;


pub(in crate::print) trait ColorfulExt {
    fn clear(&self) -> CString;
}

impl<'a> ColorfulExt for &'a str {
    fn clear(&self) -> CString {
        CString::new(*self)
    }
}


pub trait Formatter {
    type Error;
    fn const_scalar<T: ToString>(&mut self, s: T) -> Result<Self::Error>;
    fn nil(&mut self) -> Result<Self::Error>;
    fn typed<S: ToString>(&mut self, typ: &str, s: S) -> Result<Self::Error>;
    fn error<S: ToString>(&mut self, typ: &str, s: S) -> Result<Self::Error>;
    fn set<F>(&mut self, f: F) -> Result<Self::Error>
        where F: FnMut(&mut Self) -> Result<Self::Error>;
    fn tuple<F>(&mut self, f: F) -> Result<Self::Error>
        where F: FnMut(&mut Self) -> Result<Self::Error>;
    fn array<F>(&mut self, f: F) -> Result<Self::Error>
        where F: FnMut(&mut Self) -> Result<Self::Error>;
    fn object<F>(&mut self, type_id: Option<&Uuid>, f: F)
        -> Result<Self::Error>
        where F: FnMut(&mut Self) -> Result<Self::Error>;
    fn named_tuple<F>(&mut self, f: F) -> Result<Self::Error>
        where F: FnMut(&mut Self) -> Result<Self::Error>;
    fn comma(&mut self) -> Result<Self::Error>;
    fn ellipsis(&mut self) -> Result<Self::Error>;
    fn object_field(&mut self, f: &str) -> Result<Self::Error>;
    fn tuple_field(&mut self, f: &str) -> Result<Self::Error>;

    fn implicit_properties(&self) -> bool;
    fn max_items(&self) -> Option<usize>;
}

impl<'a, T: Output> Formatter for Printer<'a, T> {
    type Error = T::Error;
    fn const_scalar<S: ToString>(&mut self, s: S) -> Result<Self::Error> {
        self.delimit()?;
        self.write(s.to_string().green())
    }
    fn nil(&mut self) -> Result<Self::Error> {
        self.delimit()?;
        self.write("{}".dark_gray())
    }
    fn typed<S: ToString>(&mut self, typ: &str, s: S) -> Result<Self::Error> {
        self.delimit()?;
        self.write(format!("<{}>", typ).red())?;
        self.write(format!("'{}'", s.to_string().escape_default()).green())?;
        Ok(())
    }
    fn error<S: ToString>(&mut self, typ: &str, s: S) -> Result<Self::Error> {
        self.delimit()?;
        self.write(format!("<err-{}>", typ).red())?;
        self.write(format!("'{}'", s.to_string().escape_default()).red())?;
        Ok(())
    }
    fn set<F>(&mut self, f: F) -> Result<Self::Error>
        where F: FnMut(&mut Self) -> Result<Self::Error>
    {
        self.delimit()?;
        self.block("{".clear(), f, "}".clear())?;
        Ok(())
    }
    fn comma(&mut self) -> Result<Self::Error> {
        Printer::comma(self)
    }
    fn ellipsis(&mut self) -> Result<Self::Error> {
        Printer::ellipsis(self)
    }
    fn object<F>(&mut self, type_id: Option<&Uuid>, f: F)
        -> Result<Self::Error>
        where F: FnMut(&mut Self) -> Result<Self::Error>
    {
        self.delimit()?;
        match (type_id, self.type_names) {
            (Some(tid), Some(names)) => {
                if let Some(name) = names.get(tid) {
                    self.block((String::from(name) + " {").blue(),
                               f, "}".blue())?;
                } else {
                    self.block("Object {".blue(), f, "}".blue())?;
                }
            }
            _ => {
                self.block("Object {".blue(), f, "}".blue())?;
            }
        }
        Ok(())
    }
    fn object_field(&mut self, f: &str) -> Result<Self::Error> {
        self.delimit()?;
        self.write(f.green())?;
        self.field()?;
        Ok(())
    }
    fn tuple<F>(&mut self, f: F) -> Result<Self::Error>
        where F: FnMut(&mut Self) -> Result<Self::Error>
    {
        self.delimit()?;
        self.block("(".clear(), f, ")".clear())?;
        Ok(())
    }
    fn named_tuple<F>(&mut self, f: F) -> Result<Self::Error>
        where F: FnMut(&mut Self) -> Result<Self::Error>
    {
        self.delimit()?;
        self.block("(".blue(), f, ")".blue())?;
        Ok(())
    }
    fn tuple_field(&mut self, f: &str) -> Result<Self::Error> {
        self.delimit()?;
        self.write(f.clear())?;
        self.write(" := ".clear())?;
        Ok(())
    }
    fn array<F>(&mut self, f: F) -> Result<Self::Error>
        where F: FnMut(&mut Self) -> Result<Self::Error>
    {
        self.delimit()?;
        self.block("[".clear(), f, "]".clear())?;
        Ok(())
    }

    fn implicit_properties(&self) -> bool {
        self.implicit_properties
    }

    fn max_items(&self) -> Option<usize> {
        self.max_items
    }
}
