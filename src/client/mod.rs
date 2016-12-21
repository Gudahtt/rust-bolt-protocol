use std;
use std::net::TcpStream;
use std::io;
use std::io::Read;
use std::io::Write;
use std::time::Duration;
use std::collections::HashMap;
use byteorder::{BigEndian, ByteOrder};
use num::ToPrimitive;

use ::util::pretty_print_hex;
mod serialize;

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
        try!(stream.write(&BOLT_PREAMBLE));

        // send compatible versions
        for &version in BOLT_SUPPORTED_VERSIONS.into_iter() {
            let mut buf = [0x0; 4];
            BigEndian::write_u32(&mut buf, version);
            try!(stream.write(&buf));
        }

        // fill remaining spaces with 'none' version
        for _ in BOLT_SUPPORTED_VERSIONS.len()..4 {
            let mut buf = [0x0; 4];
            BigEndian::write_u32(&mut buf, BOLT_VERSION_NONE);
            try!(stream.write(&buf));
        }

        let mut response_buffer = [0x0; 4];
        try!(stream.read_exact(&mut response_buffer));

        let version = BigEndian::read_u32(&response_buffer);

        if version == 0 {
            panic!("No supported versions; Exiting.");
        }

        println!("Using version {}", version);

        Ok(())
    }

    fn send_message(&mut self, message: &[u8]) -> Result<(), io::Error> {
        let pretty_message = pretty_print_hex(&message).unwrap();
        println!("Writing message:\n{}", pretty_message);

        for chunk in message.chunks(std::u16::MAX as usize) {
            let chunk_size = chunk.len() as u16;
            let mut buf = [0x0; 2];
            BigEndian::write_u16(&mut buf, chunk_size);

            try!(self.stream.write(&buf));
            try!(self.stream.write(chunk));

            let buf = [0x0; 2];
            try!(self.stream.write(&buf));
        }

        Ok(())
    }

    fn read_message(&mut self) -> Result<Vec<u8>, io::Error> {
        let mut message: Vec<u8> = Vec::new();
        let mut message_length = std::u16::MAX;

        println!("Reading message");

        loop {
            // read header
            let mut buf = [0x0; 2];
            try!(self.stream.read_exact(&mut buf));
            message_length = BigEndian::read_u16(&buf);

            if message_length == 0 { break };

            // read message
            let mut message_chunk = vec![0x0; message_length as usize];
            try!(self.stream.read_exact(&mut message_chunk[..]));

            message.append(&mut message_chunk);
        }

        println!("size: {}", message.len());
        let pretty_message = pretty_print_hex(&message.as_slice()).unwrap();
        println!("{}", pretty_message);

        Ok(message)
    }

    fn init(&mut self) -> Result<(), io::Error> {
        let mut map = HashMap::new();
        map.insert("scheme", "basic");
        map.insert("principal", "neo4j");
        map.insert("credentials", "password");

        let init_message = try!(serialize::serialize_init_message("MyClient/1.0", &map));

        try!(self.send_message(&init_message[..]));
        
        let message = try!(self.read_message());

        Ok(())
    }

    pub fn run(&mut self, statement: String) -> Result<(), io::Error> {
        let parameters = HashMap::<&str, &str>::new();

        let run_message = try!(serialize::serialize_run_message(&statement, &parameters));

        try!(self.send_message(&run_message[..]));

        let message = try!(self.read_message());

        Ok(())
    }
}

pub fn connect(server: &str, username: &str, password: &str) -> Result<BoltSession, io::Error> {
    let stream = try!(TcpStream::connect(server));
    try!(stream.set_read_timeout(Some(Duration::new(5, 0))));

    let mut session = try!(BoltSession::new(stream));
    try!(session.init());

    Ok(session)
}
