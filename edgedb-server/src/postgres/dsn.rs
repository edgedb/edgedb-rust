use std::str::FromStr;

use async_std::net::ToSocketAddrs;

use url::{Url, ParseError};
use snafu::{Snafu, ResultExt};


#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("url syntax error"))]
    Syntax { source: ParseError },
    #[snafu(display("username is mandatory"))]
    UserRequired,
    #[snafu(display("database name is mandatory"))]
    DatabaseNameRequired,
    #[snafu(display("host name is mandatory"))]
    HostRequired,
}


#[derive(Debug)]
pub struct Dsn {
    url: Url,
}


impl FromStr for Dsn {
    type Err = Error;
    fn from_str(s: &str) -> Result<Dsn, Error> {
        let url: Url = s.parse().context(Syntax)?;
        if url.username() == "" {
            UserRequired.fail()?;
        }
        if url.path().len() <= 1 {
            DatabaseNameRequired.fail()?;
        }
        if url.host_str().is_none() {
            HostRequired.fail()?;
        }
        Ok(Dsn { url })
    }
}

impl Dsn {
    pub fn addr<'x>(&'x self) -> impl ToSocketAddrs + 'x {
        (self.url.host_str().unwrap(), self.url.port().unwrap_or(5656))
    }
    pub fn username(&self) -> &str {
        self.url.username()
    }
    pub fn database(&self) -> &str {
        &self.url.path()[1..]
    }
}
