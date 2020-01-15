use crate::print::stream::Stream;
use crate::print::Printer;

use colorful::{Colorful, core::color_string::CString};

trait ColorfulExt {
    fn clear(&self) -> CString;
}

impl<'a> ColorfulExt for &'a str {
    fn clear(&self) -> CString {
        CString::new(*self)
    }
}


pub trait Formatter {
    type Error;
    fn const_scalar<T: ToString>(&mut self, s: T) -> Result<(), Self::Error>;
    fn typed<S: ToString>(&mut self, typ: &str, s: S)
        -> Result<(), Self::Error>;
    fn error<S: ToString>(&mut self, typ: &str, s: S)
        -> Result<(), Self::Error>;
    fn set<F>(&mut self, f: F)
        -> Result<(), Self::Error>
        where F: FnMut(&mut Self) -> Result<(), Self::Error>;
    fn tuple<F>(&mut self, f: F)
        -> Result<(), Self::Error>
        where F: FnMut(&mut Self) -> Result<(), Self::Error>;
    fn array<F>(&mut self, f: F)
        -> Result<(), Self::Error>
        where F: FnMut(&mut Self) -> Result<(), Self::Error>;
    fn object<F>(&mut self, f: F)
        -> Result<(), Self::Error>
        where F: FnMut(&mut Self) -> Result<(), Self::Error>;
    fn named_tuple<F>(&mut self, f: F)
        -> Result<(), Self::Error>
        where F: FnMut(&mut Self) -> Result<(), Self::Error>;
    fn comma(&mut self) -> Result<(), Self::Error>;
    fn object_field(&mut self, f: &str) -> Result<(), Self::Error>;
    fn tuple_field(&mut self, f: &str) -> Result<(), Self::Error>;
}

impl<T: Stream<Error=E>, E> Formatter for Printer<T, E> {
    type Error = E;
    fn const_scalar<S: ToString>(&mut self, s: S) -> Result<(), Self::Error> {
        self.write(s.to_string().green())
    }
    fn typed<S: ToString>(&mut self, typ: &str, s: S)
        -> Result<(), Self::Error>
    {
        self.write(format!("<{}>", typ).red())?;
        self.write(format!("'{}'", s.to_string().escape_default()).green())?;
        Ok(())
    }
    fn error<S: ToString>(&mut self, typ: &str, s: S)
        -> Result<(), Self::Error>
    {
        self.write(format!("<err-{}>", typ).red())?;
        self.write(format!("'{}'", s.to_string().escape_default()).red())?;
        Ok(())
    }
    fn set<F>(&mut self, mut f: F)
        -> Result<(), Self::Error>
        where F: FnMut(&mut Self) -> Result<(), Self::Error>
    {
        self.write("{".clear())?;
        f(self)?;
        self.write("}".clear())?;
        Ok(())
    }
    fn comma(&mut self) -> Result<(), Self::Error> {
        self.write(", ".clear())
    }
    fn object<F>(&mut self, mut f: F)
        -> Result<(), Self::Error>
        where F: FnMut(&mut Self) -> Result<(), Self::Error>
    {
        self.write("Object {".blue())?;
        f(self)?;
        self.write("}".blue())?;
        Ok(())
    }
    fn object_field(&mut self, f: &str) -> Result<(), Self::Error> {
        self.write(f.green())?;
        self.write(": ".clear())?;
        Ok(())
    }
    fn tuple<F>(&mut self, mut f: F)
        -> Result<(), Self::Error>
        where F: FnMut(&mut Self) -> Result<(), Self::Error>
    {
        self.write("(".clear())?;
        f(self)?;
        self.write(")".clear())?;
        Ok(())
    }
    fn named_tuple<F>(&mut self, mut f: F)
        -> Result<(), Self::Error>
        where F: FnMut(&mut Self) -> Result<(), Self::Error>
    {
        self.write("(".blue())?;
        f(self)?;
        self.write(")".blue())?;
        Ok(())
    }
    fn tuple_field(&mut self, f: &str) -> Result<(), Self::Error> {
        self.write(f.clear())?;
        self.write(" := ".clear())?;
        Ok(())
    }
    fn array<F>(&mut self, mut f: F)
        -> Result<(), Self::Error>
        where F: FnMut(&mut Self) -> Result<(), Self::Error>
    {
        self.write("[".clear())?;
        f(self)?;
        self.write("]".clear())?;
        Ok(())
    }
}

impl<T: Stream<Error=E>, E> Printer<T, E> {
    pub(in crate::print) fn open_brace(&mut self) -> Result<(), E> {
        self.write("{".clear())
    }
    pub(in crate::print) fn comma(&mut self) -> Result<(), E> {
        self.write(", ".clear())
    }
    pub(in crate::print) fn close_brace(&mut self) -> Result<(), E> {
        self.write("}".clear())
    }
}
