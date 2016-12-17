extern crate byteorder;

use std::net::TcpStream;
use std::io;
use std::io::Read;
use std::io::Write;
use std::time::Duration;
use self::byteorder::{BigEndian, ByteOrder, WriteBytesExt};

const BOLT_PREAMBLE: [u8; 4] = [0x60, 0x60, 0xB0, 0x17];
const BOLT_SUPPORTED_VERSIONS: [u32; 1] = [ 1 ];
const BOLT_VERSION_NONE : u32 = 0;

pub struct BoltSession {
    stream: TcpStream,
}

impl BoltSession {
    fn new(mut stream: TcpStream) -> Result<BoltSession, io::Error> {
        try!(BoltSession::handshake(&mut stream));

        Ok(BoltSession { stream: stream })
    }

    fn handshake(mut stream: &TcpStream) -> Result<(), io::Error> {
        // send preamble
        stream.write(&BOLT_PREAMBLE);

        // send compatible versions
        for &version in BOLT_SUPPORTED_VERSIONS.into_iter() {
            let mut buf = [0x0; 4];
            BigEndian::write_u32(&mut buf, version);
            stream.write(&buf);
        }

        // fill remaining spaces with 'none' version
        for _ in BOLT_SUPPORTED_VERSIONS.len()..4 {
            let mut buf = [0x0; 4];
            BigEndian::write_u32(&mut buf, BOLT_VERSION_NONE);
            stream.write(&buf);
        }

        let mut responseBuffer = [0x0; 4];

        stream.set_read_timeout(Some(Duration::new(5, 0)));
        let response = try!(stream.read_exact(&mut responseBuffer));
        stream.set_read_timeout(None);

        let version = BigEndian::read_u32(&responseBuffer);

        if version == 0 {
            panic!("No supported versions; Exiting.");
        }

        Ok(())
    }
}

pub fn connect(server: &str, username: &str, password: &str) -> io::Result<BoltSession> {
    let mut stream = TcpStream::connect(server);

    match stream {
        Ok(stream) => Ok(
            try!(BoltSession::new(stream))
        ),
        Err(error) => Err(error),
    }
}
