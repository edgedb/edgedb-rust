use twoway::find_bytes;

/// Returns index of semicolon, or position where to continue search on new
/// data
pub fn full_statement(data: &[u8]) -> Result<usize, usize> {
    let mut iter = data.iter().enumerate().peekable();
    let mut braces_buf = Vec::with_capacity(8);
    'outer: while let Some((idx, b)) = iter.next() {
        match b {
            b'"' => {
                while let Some((_, b)) = iter.next() {
                    match b {
                        b'\\' => {
                            // skip any next char, even quote
                            iter.next();
                        }
                        b'"' => continue 'outer,
                        _ => continue,
                    }
                }
                return Err(idx);
            }
            b'\'' => {
                while let Some((_, b)) = iter.next() {
                    match b {
                        b'\\' => {
                            // skip any next char, even quote
                            iter.next();
                        }
                        b'\'' => continue 'outer,
                        _ => continue,
                    }
                }
                return Err(idx);
            }
            b'`' => {
                while let Some((_, b)) = iter.next() {
                    match b {
                        b'`' => continue 'outer,
                        _ => continue,
                    }
                }
                return Err(idx);
            }
            b'#' => {
                while let Some((_, &b)) = iter.next() {
                    if b == b'\n' {
                        continue 'outer;
                    }
                }
                return Err(idx);
            }
            b'$' => {
                if let Some((end_idx, &b)) = iter.next() {
                    match b {
                        b'$' => {
                            if let Some(end) = find_bytes(&data[end_idx+1..],
                                                          b"$$")
                            {
                                iter.nth(end + end_idx - idx);
                                continue 'outer;
                            }
                            return Err(idx);
                        }
                        b'A'..=b'Z' | b'a'..=b'z' | b'_' => { }
                        // Not a dollar-quote
                        _ => continue 'outer,
                    }
                }
                while let Some((end_idx, &b)) = iter.next() {
                    match b {
                        b'$' => {
                            if let Some(end) = find_bytes(&data[end_idx+1..],
                                                          &data[idx..end_idx+1])
                            {
                                iter.nth(end + end_idx - idx);
                                continue 'outer;
                            }
                            return Err(idx);
                        }
                        b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'_'
                        => continue,
                        // Not a dollar-quote
                        _ => continue 'outer,

                    }
                }
            }
            b'{' => braces_buf.push(b'}'),
            b'(' => braces_buf.push(b')'),
            b'[' => braces_buf.push(b']'),
            b'}' | b')' | b']'
            if braces_buf.last() == Some(b)
            => { braces_buf.pop(); }
            b';' if braces_buf.len() == 0 => return Ok(idx),
            _ => continue,
        }
    }
    return Err(data.len());
}
