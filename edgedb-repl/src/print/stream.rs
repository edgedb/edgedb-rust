use std::io::{self, Write};
use std::convert::Infallible;


pub(in crate::print) trait Stream {
    type Error;
    fn write(&mut self, data: &str) -> Result<(), Self::Error>;
}


impl<'a> Stream for &'a mut String {
    type Error = Infallible;
    fn write(&mut self, data: &str) -> Result<(), Infallible> {
        self.push_str(data);
        Ok(())
    }
}

impl<'a> Stream for io::StdoutLock<'a> {
    type Error = io::Error;
    fn write(&mut self, data: &str) -> Result<(), io::Error> {
        self.write_all(data.as_bytes())?;
        Ok(())
    }
}
