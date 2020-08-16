use bytes::Buf;
use crate::codec::raw::RawCodec;
use crate::{queryable::Queryable, errors::{self, DecodeError}};
use snafu::ensure;

pub struct Reader<'t>
{
    raw:Option<&'t [u8]>
}

impl<'t> std::fmt::Debug for Reader<'t> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Reader ")?;
        match self.raw {
            Some(raw) => { f.write_fmt(format_args!("{:x?}", raw))?; }
            None => { f.write_str("errored")?; }
        }
        Ok(())
    }
}

impl<'t> Reader<'t>
{
    fn error<E>(&mut self, e:E) -> E {
        self.raw = None;
        e
    }

    fn len(&self) -> usize {
        self.raw_bytes().len()
    }

    pub fn complete(self) -> Result<(), DecodeError> {
        ensure!(self.len() == 0, errors::ExtraData);
        Ok(())
    }

    fn raw_bytes(&self) -> &'t [u8] {
        &mut self.raw.expect("attempt to access an errored reader")
    }

    fn raw_mut(&mut self) -> &mut &'t [u8] {
        self.raw.as_mut().expect("attempt to access an errored reader")
    }

    pub(crate) fn decode_raw<T:RawCodec<'t>>(&mut self) -> Result<T, DecodeError> {
        T::decode_raw(self.raw_mut())
    }

    pub fn from_bytes(bytes:&'t [u8]) -> Self {
        Reader { raw:Some(bytes) }
    }

    fn split(&mut self, position:usize) -> Result<Self, DecodeError> {
        ensure!(self.len() >= position, self.error(errors::Underflow));
        let buf = self.raw_mut();
        let result = Reader::from_bytes(&buf[..position]);
        buf.advance(position);
        Ok(result)
    }

    pub fn read_object_element(&mut self) -> Result<Option<Self>, DecodeError> {
        let buf = self.raw_mut();
        ensure!(buf.remaining() >= 8, self.error(errors::Underflow));
        let _reserved = buf.get_i32();
        let len = buf.get_i32();
        if len < 0 {
            ensure!(len == -1, self.error(errors::InvalidMarker));
            return Ok(None);
        }
        let len = len as usize;
        Ok(Some(self.split(len)?))
    }

    pub(crate) fn read_tuple_element(&mut self) -> Result<Self, DecodeError> {
        let buf = self.raw_mut();
        ensure!(buf.remaining() >= 8, self.error(errors::Underflow));
        let _reserved = buf.get_i32();
        let len = buf.get_i32() as usize;
        Ok(self.split(len)?)
    }
    
    pub(crate) fn read_array_like_element(&mut self) -> Result<Self, DecodeError> {
        let buf = self.raw_mut();
        ensure!(buf.remaining() >= 4, self.error(errors::Underflow));
        let len = buf.get_i32() as usize;
        Ok(self.split(len)?)        
    }
    
    pub fn read_tuple_like_header(&mut self) -> Result<usize, DecodeError> {
        let buf = self.raw_mut();
        ensure!(buf.remaining() >= 4, self.error(errors::Underflow));
        Ok(buf.get_u32() as usize)
    }

    pub(crate) fn read_array_like_header(&mut self, ensure_shape: impl Fn(bool) -> Result<(), DecodeError>) -> Result<usize, DecodeError> {
        let buf = self.raw_mut();
        ensure!(buf.remaining() >= 12, self.error(errors::Underflow));
        let ndims = buf.get_u32();
        let _reserved0 = buf.get_u32();
        let _reserved1 = buf.get_u32();
        if ndims == 0 {
            return Ok(0);
        }
        ensure_shape(ndims == 1)?;
        ensure!(buf.remaining() >= 8, self.error(errors::Underflow));
        let size = buf.get_u32() as usize;
        let lower = buf.get_u32();
        ensure_shape(lower == 1)?;
        Ok(size)
    }

    pub fn get_object_element<T:Queryable>(&mut self) -> Result<T, DecodeError> {
        let element = self.read_object_element()?;
        // this doesn't handle the empty set case, but the original code didn't handle it either
        ensure!(element.is_some(), self.error(errors::Underflow));
        T::decode(element.unwrap())
    }
}