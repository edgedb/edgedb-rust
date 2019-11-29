use twoway::find_bytes;

/// Returns index of semicolon, or position where to continue search on new
/// data
pub fn full_statement(data: &[u8]) -> Result<usize, usize> {
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
                while let Some((end_idx, &b)) = iter.next() {
                    if b == b'$' {
                        if let Some(end) = find_bytes(&data[end_idx+1..],
                                                      &data[idx..end_idx+1])
                        {
                            iter.nth(end + end_idx - idx);
                            continue 'outer;
                        }
                    }
                }
                return Err(idx);
            }
            b';' => return Ok(idx),
            _ => continue,
        }
    }
    return Err(data.len());
}
