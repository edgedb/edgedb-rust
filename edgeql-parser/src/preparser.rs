/// Returns index of semicolon if present (and not quoted) else None
pub fn full_statement(data: &[u8]) -> Option<usize> {
    let mut iter = data.iter().enumerate().peekable();
    'outer: while let Some((idx, b)) = iter.next() {
        match b {
            b'"' => {
                while let Some((_, b)) = iter.next() {
                    match b {
                        b'\\' => {
                            // skip any next char, even quote
                            iter.next();
                        }
                        b'"' => break,
                        _ => continue,
                    }
                }
            }
            b'\'' => {
                while let Some((_, b)) = iter.next() {
                    match b {
                        b'\\' => {
                            // skip any next char, even quote
                            iter.next();
                        }
                        b'\'' => break,
                        _ => continue,
                    }
                }
            }
            b'`' => {
                while let Some((_, b)) = iter.next() {
                    match b {
                        b'`' => break,
                        _ => continue,
                    }
                }
            }
            b'#' => {
                while let Some((_, &b)) = iter.next() {
                    if b == b'\n' {
                        continue 'outer;
                    }
                }
            }
            b';' => return Some(idx),
            _ => continue,
        }
    }
    return None
}
