extern crate byteorder;

use std;
use std::net::TcpStream;
use std::io;
use std::io::Read;
use std::io::Write;
use std::time::Duration;
use std::slice::Chunks;
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

        let response = try!(stream.read_exact(&mut responseBuffer));

        let version = BigEndian::read_u32(&responseBuffer);

        if version == 0 {
            panic!("No supported versions; Exiting.");
        }

        Ok(())
    }

    fn send_message(&mut self, message: &[u8]) -> Result<(), io::Error> {
        let message_size = message.len();

        for chunk in message.chunks(std::u16::MAX as usize) {
            let chunk_size = chunk.len() as u16;
            let mut buf = [0x0; 2];
            BigEndian::write_u16(&mut buf, chunk_size);

            try!(self.stream.write(&buf));
            try!(self.stream.write(chunk));

            let mut buf = [0x0; 2];
            try!(self.stream.write(&buf));
        }

        Ok(())
    }

    fn read_message(&mut self) -> Result<Vec<u8>, io::Error> {
        let mut message: Vec<u8> = Vec::new();
        let mut message_length = std::u16::MAX;

        while message_length > 0 {
            // read header
            let mut buf = [0x0; 2];
            try!(self.stream.read_exact(&mut buf));
            message_length = BigEndian::read_u16(&buf);

            // read message
            let mut buf = Vec::with_capacity(message_length as usize);
            try!(self.stream.read_exact(&mut buf));

            message.append(&mut buf);
        }

        Ok(message)
    }
}

pub fn connect(server: &str, username: &str, password: &str) -> io::Result<BoltSession> {
    let mut stream = try!(TcpStream::connect(server));
    stream.set_read_timeout(Some(Duration::new(5, 0)));

    let mut session = try!(BoltSession::new(stream));

    Ok(session)
}
