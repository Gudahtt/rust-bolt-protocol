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

const BOLT_PREAMBLE: [u8; 4] = [0x60, 0x60, 0xB0, 0x17];
const BOLT_SUPPORTED_VERSIONS: [u32; 1] = [ 1 ];
const BOLT_VERSION_NONE : u32 = 0;

trait BoltSerialize {
    fn serialize(&self) -> Result<Vec<u8>, io::Error> ;
}

impl BoltSerialize for Null {
    fn serialize(&self) -> Result<Vec<u8>, io::Error>  {
        Ok(serialize_null())
    }
}

impl BoltSerialize for bool {
    fn serialize(&self) -> Result<Vec<u8>, io::Error>  {
        Ok(serialize_boolean(*self))
    }
}

impl BoltSerialize for i8 {
    fn serialize(&self) -> Result<Vec<u8>, io::Error>  {
        serialize_integer(*self)
    }
}

impl BoltSerialize for i16 {
    fn serialize(&self) -> Result<Vec<u8>, io::Error>  {
        serialize_integer(*self)
    }
}

impl BoltSerialize for i32 {
    fn serialize(&self) -> Result<Vec<u8>, io::Error>  {
        serialize_integer(*self)
    }
}

impl BoltSerialize for i64 {
    fn serialize(&self) -> Result<Vec<u8>, io::Error>  {
        serialize_integer(*self)
    }
}

impl BoltSerialize for u8 {
    fn serialize(&self) -> Result<Vec<u8>, io::Error>  {
        serialize_integer(*self)
    }
}

impl BoltSerialize for u16 {
    fn serialize(&self) -> Result<Vec<u8>, io::Error>  {
        serialize_integer(*self)
    }
}

impl BoltSerialize for u32 {
    fn serialize(&self) -> Result<Vec<u8>, io::Error>  {
        serialize_integer(*self)
    }
}

impl BoltSerialize for u64 {
    fn serialize(&self) -> Result<Vec<u8>, io::Error>  {
        serialize_integer(*self)
    }
}

impl BoltSerialize for f64 {
    fn serialize(&self) -> Result<Vec<u8>, io::Error>  {
        Ok(serialize_float(*self))
    }
}

impl<'a> BoltSerialize for &'a str {
    fn serialize(&self) -> Result<Vec<u8>, io::Error>  {
        serialize_string(&self)
    }
}

impl<T: BoltSerialize> BoltSerialize for Vec<T> {
    fn serialize(&self) -> Result<Vec<u8>, io::Error>  {
        serialize_list(&self)
    }
}

impl<'a, T: BoltSerialize> BoltSerialize for HashMap<&'a str, T> {
    fn serialize(&self) -> Result<Vec<u8>, io::Error>  {
        serialize_map(&self)
    }
}

struct Null;

struct Node<T: BoltSerialize, Y: BoltSerialize> {
    node_identity: u64,
    labels: Vec<T>,
    properties: HashMap<String, Y>,
}

struct Relationship<T: BoltSerialize> {
    rel_identity: u64,
    start_node_identity: u64,
    end_node_identity: u64,
    rel_type: String,
    properties: HashMap<String, T>
}

struct Path<T: BoltSerialize, Y: BoltSerialize, Z: BoltSerialize> {
    nodes: Vec<Node<T, Y>>,
    relationships: Vec<UnboundRelationship<Z>>,
    sequence: Vec<u64>,
}

struct UnboundRelationship<T: BoltSerialize> {
    rel_identity: u64,
    rel_type: String,
    properties: HashMap<String, T>,
}

fn serialize_null() -> Vec<u8> {
    vec![0xC0]
}

fn serialize_boolean(value: bool) -> Vec<u8> {
    if value { vec![0xC3] } else { vec![0xC2] }
}

fn serialize_integer<T: ToPrimitive>(value: T) -> Result<Vec<u8>, io::Error> {
    match value.to_i64().unwrap() {
        value_i64 @ -9223372036854775808 ... -2147483649 | value_i64 @ 2147483648 ... 9223372036854775807 => {
            let mut buf = [0x0; 8];
            BigEndian::write_i64(&mut buf, value_i64);
            let mut v = vec![0xCB];
            v.extend_from_slice(&buf);
            Ok(v)
        },
        -2147483648 ... -32769 | 32768 ... 2147483647 => {
            let mut buf = [0x0; 4];
            BigEndian::write_i32(&mut buf, value.to_i32().unwrap());
            let mut v = vec![0xCA];
            v.extend_from_slice(&buf);
            Ok(v)
        },
        -32768 ... -129 | 128 ... 32767 => {
            let mut buf = [0x0; 2];
            BigEndian::write_i16(&mut buf, value.to_i16().unwrap());
            let mut v = vec![0xC9];
            v.extend_from_slice(&buf);
            Ok(v)
        },
        -128 ... -17 => Ok(vec![0xC8, value.to_i8().unwrap() as u8]),
        -16 ... 127 => Ok(vec![value.to_i8().unwrap() as u8]),
        _ => Err(io::Error::new(io::ErrorKind::Other, "Integer too large")),
    }
}

fn serialize_float(value: f64) -> Vec<u8> {
    let mut buf = [0x0; 8];
    BigEndian::write_f64(&mut buf, value);
    let mut v = vec![0xC1];
    v.extend_from_slice(&buf);
    v
}

fn serialize_string(s: &str) -> Result<Vec<u8>, io::Error> {
    let mut message = match s.len() {
        len @ 0 ... 15 => vec![0x80 + (len as u8)],
        len @ 16 ... 255 => vec![0xD0, len as u8],
        len @ 256 ... 65535 => {
            let mut buf = [0x0; 2];
            BigEndian::write_u16(&mut buf, len as u16);
            let mut v = vec![0xD1];
            v.extend_from_slice(&buf);
            v
        },
        len @ 65536 ... 4294967295 => {
            let mut buf = [0x0; 4];
            BigEndian::write_u32(&mut buf, len as u32);
            let mut v = vec![0xD2];
            v.extend_from_slice(&buf);
            v
        },
        _ => return Err(io::Error::new(io::ErrorKind::Other, "String too large")),
    };

    message.extend_from_slice(s.as_bytes());
    Ok(message)
}

fn serialize_list<T: BoltSerialize>(list: &Vec<T>) -> Result<Vec<u8>, io::Error> {
    let mut message = match list.len() {
        len @ 0 ... 15 => vec![0x90 + (len as u8)],
        len @ 16 ... 255 => vec![0xD4, len as u8],
        len @ 256 ... 65535 => {
            let mut v = vec![0xD5];
            let mut buf = [0x0; 2];
            BigEndian::write_u16(&mut buf, len as u16);
            v.extend_from_slice(&buf);
            v
        },
        len @ 65536 ... 4294967295 => {
            let mut v = vec![0xD6];
            let mut buf = [0x0; 4];
            BigEndian::write_u32(&mut buf, len as u32);
            v.extend_from_slice(&buf);
            v
        },
        _ => return Err(io::Error::new(io::ErrorKind::Other, "List too large")),
    };

    for entry in list {
        message.append(&mut try!(entry.serialize()));
    }
    Ok(message)
}

fn serialize_map<T: BoltSerialize>(map: &HashMap<&str, T>) -> Result<Vec<u8>, io::Error> {
    let mut message = match map.len() {
        len @ 0 ... 15 => vec![0xA0 + (len as u8)],
        len @ 16 ... 255 => vec![0xD8, len as u8],
        len @ 256 ... 65535 => {
            let mut v = vec![0xD9];
            let mut buf = [0x0; 2];
            BigEndian::write_u16(&mut buf, len as u16);
            v.extend_from_slice(&buf);
            v
        },
        len @ 65536 ... 4294967295 => {
            let mut v = vec![0xDA];
            let mut buf = [0x0; 4];
            BigEndian::write_u32(&mut buf, len as u32);
            v.extend_from_slice(&buf);
            v
        },
        _ => return Err(io::Error::new(io::ErrorKind::Other, "Map too large")),
    };

    for (key, entry) in map.iter() {
        message.append(&mut try!(serialize_string(key)));
        message.append(&mut try!(entry.serialize()));
    }

    Ok(message)
}

fn get_struct_header(size: i32) -> Result<Vec<u8>, io::Error> {
    match size {
        s @ 0 ... 15 => Ok(vec![0xB0 + (s as u8)]),
        16 ... 255 => Ok(vec![0xDC, size as u8]),
        256 ... 65535 => {
            let mut buf = [0x0; 2];
            BigEndian::write_u16(&mut buf, size as u16);
            let mut v = vec![0xDD];
            v.extend_from_slice(&buf);
            Ok(v)
        },
        _ => Err(io::Error::new(io::ErrorKind::Other, "Struct too large")),
    }
}

fn serialize_node(node_identity: u64, labels: &Vec<&str>, properties: &HashMap<&str, &str>) -> Result<Vec<u8>, io::Error> {
    let signature = 0x4E;
    let size = 3;
    let mut message = try!(get_struct_header(size));

    message.push(signature);
    message.append(&mut try!(serialize_integer(node_identity)));
    message.append(&mut try!(serialize_list(labels)));
    message.append(&mut try!(serialize_map(properties)));

    Ok(message)
}

fn serialize_relationship(rel_identity: u64, start_node_identity: u64, end_node_identity: u64, rel_type: &str, properties: &HashMap<&str, &str>) -> Result<Vec<u8>, io::Error> {
    let signature = 0x52;
    let size = 5;
    let mut message = try!(get_struct_header(size));

    message.push(signature);
    message.append(&mut try!(serialize_integer(rel_identity)));
    message.append(&mut try!(serialize_integer(start_node_identity)));
    message.append(&mut try!(serialize_integer(end_node_identity)));
    message.append(&mut try!(serialize_string(rel_type)));
    message.append(&mut try!(serialize_map(properties)));

    Ok(message)
}

fn serialize_path(nodes: &Vec<&str>, relationships: &Vec<&str>, sequence: &Vec<i32>) -> Result<Vec<u8>, io::Error> {
    let signature = 0x50;
    let size = 3;
    let mut message = try!(get_struct_header(size));

    message.push(signature);
    message.append(&mut try!(serialize_list(nodes)));
    message.append(&mut try!(serialize_list(relationships)));
    message.append(&mut try!(serialize_list(sequence)));

    Ok(message)
}

fn serialize_unbound_relationship(rel_identity: u64, rel_type: &str, properties: &HashMap<&str, &str>) -> Result<Vec<u8>, io::Error> {
    let signature = 0x72;
    let size = 5;
    let mut message = try!(get_struct_header(size));

    message.push(signature);
    message.append(&mut try!(serialize_integer(rel_identity)));
    message.append(&mut try!(serialize_string(rel_type)));
    message.append(&mut try!(serialize_map(properties)));

    Ok(message)
}

fn serialize_init_message(client_name: &str, auth_token: &HashMap<&str, &str>) -> Result<Vec<u8>, io::Error> {
    let signature = 0x1;
    let size = 2;
    let mut message = try!(get_struct_header(size));

    message.push(signature);
    message.append(&mut try!(serialize_string(client_name)));
    message.append(&mut try!(serialize_map(auth_token)));

    Ok(message)
}

fn serialize_run_message(statement: &str, parameters: &HashMap<&str, &str>) -> Result<Vec<u8>, io::Error> {
    let signature = 0x10;
    let size = 2;
    let mut message = try!(get_struct_header(size));

    message.push(signature);
    message.append(&mut try!(serialize_string(statement)));
    message.append(&mut try!(serialize_map(parameters)));

    Ok(message)
}

fn serialize_discard_all_message() -> Result<Vec<u8>, io::Error> {
    let signature = 0x2F;
    let size = 0;
    let mut message = try!(get_struct_header(size));
    message.push(signature);
    
    Ok(message)
}

fn serialize_pull_all_message() -> Result<Vec<u8>, io::Error> {
    let signature = 0x3F;
    let size = 0;
    let mut message = try!(get_struct_header(size));
    message.push(signature);
    
    Ok(message)
}

fn serialize_ack_failure_message() -> Result<Vec<u8>, io::Error> {
    let signature = 0x0E;
    let size = 0;
    let mut message = try!(get_struct_header(size));
    message.push(signature);
    
    Ok(message)
}

fn serialize_reset_message() -> Result<Vec<u8>, io::Error> {
    let signature = 0x0F;
    let size = 0;
    let mut message = try!(get_struct_header(size));
    message.push(signature);
    
    Ok(message)
}

fn serialize_record_message(fields: &HashMap<&str, &str>) -> Result<Vec<u8>, io::Error> {
    let signature = 0x71;
    let size = 1;
    let mut message = try!(get_struct_header(size));

    message.push(signature);
    message.append(&mut try!(serialize_map(fields)));

    Ok(message)
}

fn serialize_success_message(metadata: &HashMap<&str, &str>) -> Result<Vec<u8>, io::Error> {
    let signature = 0x70;
    let size = 1;
    let mut message = try!(get_struct_header(size));

    message.push(signature);
    message.append(&mut try!(serialize_map(metadata)));

    Ok(message)
}

fn serialize_failure_message(metadata: &HashMap<&str, &str>) -> Result<Vec<u8>, io::Error> {
    let signature = 0x7F;
    let size = 1;
    let mut message = try!(get_struct_header(size));

    message.push(signature);
    message.append(&mut try!(serialize_map(metadata)));

    Ok(message)
}

fn serialize_ignored_message(metadata: &HashMap<&str, &str>) -> Result<Vec<u8>, io::Error> {
    let signature = 0x7E;
    let size = 1;
    let mut message = try!(get_struct_header(size));

    message.push(signature);
    message.append(&mut try!(serialize_map(metadata)));

    Ok(message)
}

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

        let init_message = try!(serialize_init_message("MyClient/1.0", &map));

        try!(self.send_message(&init_message[..]));
        
        let message = try!(self.read_message());

        Ok(())
    }

    pub fn run(&mut self, statement: String) -> Result<(), io::Error> {
        let parameters = HashMap::<&str, &str>::new();

        let run_message = try!(serialize_run_message(&statement, &parameters));

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
