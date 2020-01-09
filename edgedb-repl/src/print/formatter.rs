use crate::print::stream::Stream;
use crate::print::Printer;

use colored::Colorize;


pub(in crate::print) trait Formatter {
    type Error;
    fn const_scalar<T: ToString>(&mut self, s: T) -> Result<(), Self::Error>;
    fn typed<S: ToString>(&mut self, typ: &str, s: S)
        -> Result<(), Self::Error>;
    fn error<S: ToString>(&mut self, typ: &str, s: S)
        -> Result<(), Self::Error>;
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
}
