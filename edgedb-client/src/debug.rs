use std::fmt;
use std::io::Cursor;
use std::str;

pub struct PartialDebug<V>(pub V);

impl<V: fmt::Debug> fmt::Display for PartialDebug<V> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use std::io::Write;

        let mut buf = [0u8; 32];
        let mut cur = Cursor::new(&mut buf[..]);
        // Suppress error, in case buffer is overflown
        write!(&mut cur, "{:?}", self.0).ok();
        let end = cur.position() as usize;
        if end >= buf.len() {
            buf[buf.len() - 3] = b'.';
            buf[buf.len() - 2] = b'.';
            buf[buf.len() - 1] = b'.';
        }
        fmt::Write::write_str(f, str::from_utf8(&buf[..end]).unwrap())
    }
}
