use crate::errors::{self, DecodeError};
use snafu::ensure;
use self::inner::DecodeCompositeInner;


pub struct DecodeTupleLike<'t> {
    inner:DecodeCompositeInner<'t>
}

impl<'t> DecodeTupleLike<'t> {
    fn new(buf:&'t [u8]) -> Result<Self, DecodeError> {
        let inner = DecodeCompositeInner::read_tuple_like_header(buf)?;
        Ok(DecodeTupleLike{inner})
    }

    pub fn new_object(buf:&'t [u8], expected_count:usize) -> Result<Self, DecodeError> {
        let elements = Self::new(buf)?;
        ensure!(elements.inner.count() == expected_count, errors::ObjectSizeMismatch);
        Ok(elements)
    }

    pub fn new_tuple(buf:&'t [u8], expected_count:usize) -> Result<Self, DecodeError> {
        let elements = Self::new(buf)?;
        ensure!(elements.inner.count() == expected_count, errors::TupleSizeMismatch);
        Ok(elements)
    }

    pub fn read(&mut self) -> Result<Option<&[u8]>, DecodeError> {
        self.inner.read_object_element()
    }

    pub fn skip_element(&mut self) -> Result<(), DecodeError> {
        self.read()?;
        Ok(())
    }
}

pub struct DecodeArrayLike<'t> {
    inner:DecodeCompositeInner<'t>
}

impl<'t> DecodeArrayLike<'t> {
    pub fn new_array(buf:&'t [u8]) -> Result<Self, DecodeError> {
        let inner = DecodeCompositeInner::read_array_like_header(buf, || errors::InvalidArrayShape.build())?;
        Ok(DecodeArrayLike{inner})
    }

    pub fn new_set(buf:&'t [u8]) -> Result<Self, DecodeError> {
        let inner = DecodeCompositeInner::read_array_like_header(buf, || errors::InvalidSetShape.build())?;
        Ok(DecodeArrayLike{inner})
    }

    pub fn new_collection(buf:&'t [u8]) -> Result<Self, DecodeError> {
        let inner = DecodeCompositeInner::read_array_like_header(buf, || errors::InvalidArrayOrSetShape.build())?;
        Ok(DecodeArrayLike{inner})
    }
}

impl<'t> Iterator for DecodeArrayLike<'t> {
    type Item = Result<&'t [u8], DecodeError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.len() > 0 {
            Some(self.inner.read_array_like_element())
        } else {
            None
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.len();
        (len, Some(len))
    }
}

impl<'t> ExactSizeIterator for DecodeArrayLike<'t> {
    fn len(&self) -> usize {
        self.inner.count()
    }
}

mod inner {
    use crate::errors::{self, DecodeError};
    use snafu::ensure;
    use bytes::Buf;

    pub(super) struct DecodeCompositeInner<'t>
    {
        raw:&'t [u8],
        count: usize,
    }

    impl<'t> std::fmt::Debug for DecodeCompositeInner<'t> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_fmt(format_args!("count = {} data = {:x?}", self.count, self.raw))
        }
    }

    impl<'t> DecodeCompositeInner<'t>
    {
        fn underflow(&mut self) -> errors::Underflow {
            // after one underflow happened, all further reads should underflow as well
            // all other errors should be recoverable, since they only affect the content of one element and not the size of that element
            self.raw = &[0u8;0];
            errors::Underflow
        }

        pub fn count(&self) -> usize {
            self.count
        }

        fn new(bytes:&'t [u8], count: usize) -> Self {
            DecodeCompositeInner { raw:bytes, count }
        }

        fn read_element(&mut self, position:usize) -> Result<&'t [u8], DecodeError> {
            assert!(self.count() > 0, "reading from a finished elements sequence");
            self.count -= 1;
            ensure!(self.raw.len() >= position, self.underflow());
            let result = &self.raw[..position];
            self.raw.advance(position);
            ensure!(self.count > 0 || self.raw.len() == 0, errors::ExtraData);
            Ok(result)
        }

        pub fn read_object_element(&mut self) -> Result<Option<&'t [u8]>, DecodeError> {
            ensure!(self.raw.remaining() >= 8, self.underflow());
            let _reserved = self.raw.get_i32();
            let len = self.raw.get_i32();
            if len < 0 {
                ensure!(len == -1, errors::InvalidMarker);
                return Ok(None);
            }
            let len = len as usize;
            Ok(Some(self.read_element(len)?))
        }

        pub fn read_array_like_element(&mut self) -> Result<&'t [u8], DecodeError> {
            ensure!(self.raw.remaining() >= 4, self.underflow());
            let len = self.raw.get_i32() as usize;
            Ok(self.read_element(len)?)
        }

        pub fn read_tuple_like_header(mut buf:&'t [u8]) -> Result<Self, DecodeError> {
            ensure!(buf.remaining() >= 4, errors::Underflow);
            let count = buf.get_u32() as usize;
            Ok(Self::new(buf, count))
        }

        pub fn read_array_like_header(mut buf:&'t [u8], error: impl Fn() -> DecodeError) -> Result<Self, DecodeError> {
            ensure!(buf.remaining() >= 12, errors::Underflow);
            let ndims = buf.get_u32();
            let _reserved0 = buf.get_u32();
            let _reserved1 = buf.get_u32();
            if ndims == 0 {
                return Ok(Self::new(buf, 0));
            }
            if ndims != 1 {
                return Err(error());
            }
            ensure!(buf.remaining() >= 8, errors::Underflow);
            let size = buf.get_u32() as usize;
            let lower = buf.get_u32();
            if lower != 1 {
                return Err(error());
            }
            Ok(Self::new(buf, size))
        }
    }
}
