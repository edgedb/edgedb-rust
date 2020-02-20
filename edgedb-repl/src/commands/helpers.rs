use bytes::Bytes;


pub fn print_result(res: Bytes) {
    eprintln!("  -> {}: Ok", String::from_utf8_lossy(&res[..]));
}

