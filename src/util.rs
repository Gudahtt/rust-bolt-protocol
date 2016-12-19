use std::fmt::Write;
use std::fmt::Error;

pub fn pretty_print_hex(bytes: &[u8]) -> Result<String, Error> {
    let mut s = String::new();

    let mut chunks_in_line = 0;
    let mut bytes_in_chunk = 0;
    for byte in bytes {
        try!(write!(&mut s, "{}{:X} ", if *byte < 16 { "0" } else { "" }, byte));
        
        bytes_in_chunk += 1;

        if bytes_in_chunk >= 4 {
            bytes_in_chunk = 0;
            chunks_in_line += 1;

            if chunks_in_line >= 4 {
                try!(write!(&mut s, "\n"));
                chunks_in_line = 0;
            } else {
                try!(write!(&mut s, " "));
            }
        }
    }

    Ok(s)
}
