use std;
use std::net::TcpStream;
use std::io;
use std::io::Read;
use std::io::Write;
use std::time::Duration;
use std::collections::HashMap;
use byteorder::{BigEndian, ByteOrder};

use ::util::pretty_print_hex;

const BOLT_PREAMBLE: [u8; 4] = [0x60, 0x60, 0xB0, 0x17];
const BOLT_SUPPORTED_VERSIONS: [u32; 1] = [ 1 ];
const BOLT_VERSION_NONE : u32 = 0;

enum DataKind {
    Null,
    Boolean(bool),
    Integer(IntegerKind),
    Float(f64),
    String(StringKind),
    List(ListKind),
    Map(MapKind),
    Structure(StructureKind),
}

impl DataKind {
    fn serialize(&self) -> Vec<u8> {
        match *self {
            DataKind::Null | DataKind::Boolean(..) => vec![self.get_marker()],
            DataKind::Integer(ref kind) => kind.serialize(),
            DataKind::Float(value) => {
                let mut buf = [0x0; 64];
                BigEndian::write_f64(&mut buf, value);
                let mut v = vec![self.get_marker()];
                v.extend_from_slice(&buf);
                v
            },
            DataKind::String(ref kind) => kind.serialize(),
            DataKind::List(ref kind) => kind.serialize(),
            DataKind::Map(ref kind) => kind.serialize(),
            DataKind::Structure(ref kind) => kind.serialize(),
        }
    }

    fn get_marker(&self) -> u8 {
        match *self {
            DataKind::Null => 0xC0,
            DataKind::Boolean(value) => {
                match value {
                    true => 0xC3,
                    false => 0xC2,
                }
            },
            DataKind::Integer(ref kind) => kind.get_marker(),
            DataKind::Float(..) => 0xC1,
            DataKind::String(ref kind) => kind.get_marker(),
            DataKind::List(ref kind) => kind.get_marker(),
            DataKind::Map(ref kind) => kind.get_marker(),
            DataKind::Structure(ref kind) => kind.get_marker(),
        }
    }
}

enum IntegerKind {
    TinyInt(i8),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
}

impl IntegerKind {
    fn serialize(&self) -> Vec<u8> {
        match *self {
            IntegerKind::TinyInt(value) => vec![self.get_marker()],
            IntegerKind::Int8(value) => vec![self.get_marker(), value as u8],
            IntegerKind::Int16(value) => {
                let mut buf = [0x0; 2];
                BigEndian::write_i16(&mut buf, value);
                let mut v = vec![self.get_marker()];
                v.extend_from_slice(&buf);
                v
            },
            IntegerKind::Int32(value) => {
                let mut buf = [0x0; 4];
                BigEndian::write_i32(&mut buf, value);
                let mut v = vec![self.get_marker()];
                v.extend_from_slice(&buf);
                v
            },
            IntegerKind::Int64(value) => {
                let mut buf = [0x0; 8];
                BigEndian::write_i64(&mut buf, value);
                let mut v = vec![self.get_marker()];
                v.extend_from_slice(&buf);
                v
            },
        }
    }

    fn get_marker(&self) -> u8 {
        match *self {
            IntegerKind::TinyInt(value) => value as u8,
            IntegerKind::Int8(value) => 0xC8,
            IntegerKind::Int16(value) => 0xC9,
            IntegerKind::Int32(value) => 0xCA,
            IntegerKind::Int64(value) => 0xCB,
        }
    }
}

#[derive(Debug, Eq, Hash, PartialEq)]
enum StringKind {
    TinyString(String),
    String8(String),
    String16(String),
    String32(String),
}

impl StringKind {
    fn new(s : String) -> Option<StringKind> {
        match s.len() {
            0 ... 15 => Some(StringKind::TinyString(s)),
            16 ... 255 => Some(StringKind::String8(s)),
            256 ... 65535 => Some(StringKind::String16(s)),
            65536 ... 4294967295 => Some(StringKind::String32(s)),
            _ => None
        }
    }
    fn serialize(&self) -> Vec<u8> {
        match *self {
            StringKind::TinyString(ref value) => {
                let mut v = vec![self.get_marker()];
                v.extend_from_slice(value.as_bytes());
                v
            },
            StringKind::String8(ref value) => {
                let mut v = vec![self.get_marker(), value.len() as u8];
                v.extend_from_slice(value.as_bytes());
                v
            },
            StringKind::String16(ref value) => {
                let mut v = vec![self.get_marker()];
                let mut buf = [0x0; 2];
                BigEndian::write_u16(&mut buf, value.len() as u16);
                v.extend_from_slice(&buf);
                v.extend_from_slice(value.as_bytes());
                v
            },
            StringKind::String32(ref value) => {
                let mut v = vec![self.get_marker()];
                let mut buf = [0x0; 4];
                BigEndian::write_u32(&mut buf, value.len() as u32);
                v.extend_from_slice(&buf);
                v.extend_from_slice(value.as_bytes());
                v
            },
        }
    }

    fn get_marker(&self) -> u8 {
        match *self {
            StringKind::TinyString(ref value) => 0x80 + (value.len() as u8),
            StringKind::String8(..) => 0xD0,
            StringKind::String16(..) => 0xD1,
            StringKind::String32(..) => 0xD2,
        }
    }
}

enum ListKind {
    TinyList(Vec<DataKind>),
    List8(Vec<DataKind>),
    List16(Vec<DataKind>),
    List32(Vec<DataKind>),
}

impl ListKind {
    fn serialize(&self) -> Vec<u8> {
        match *self {
            ListKind::TinyList(ref value) => {
                let mut v = vec![self.get_marker()];

                for entry in value {
                    v.append(&mut entry.serialize());
                }
                v
            },
            ListKind::List8(ref value) => {
                let mut v = vec![self.get_marker(), value.len() as u8];

                for entry in value {
                    v.append(&mut entry.serialize());
                }
                v
            },
            ListKind::List16(ref value) => {
                let mut v = vec![self.get_marker()];
                let mut buf = [0x0; 2];
                BigEndian::write_u16(&mut buf, value.len() as u16);
                v.extend_from_slice(&buf);

                for entry in value {
                    v.append(&mut entry.serialize());
                }
                v
            },
            ListKind::List32(ref value) => {
                let mut v = vec![self.get_marker()];
                let mut buf = [0x0; 4];
                BigEndian::write_u32(&mut buf, value.len() as u32);
                v.extend_from_slice(&buf);

                for entry in value {
                    v.append(&mut entry.serialize());
                }
                v
            },
        }
    }

    fn get_marker(&self) -> u8 {
        match *self {
            ListKind::TinyList(ref value) => 0x90 + (value.len() as u8),
            ListKind::List8(..) => 0xD4,
            ListKind::List16(..) => 0xD5,
            ListKind::List32(..) => 0xD6,
        }
    }
}

enum MapKind {
    TinyMap(HashMap<StringKind, DataKind>),
    Map8(HashMap<StringKind, DataKind>),
    Map16(HashMap<StringKind, DataKind>),
    Map32(HashMap<StringKind, DataKind>),
}

impl MapKind {
    fn new(map: HashMap<StringKind, DataKind>) -> Option<MapKind> {
        match map.len() {
            0 ... 15 => Some(MapKind::TinyMap(map)),
            16 ... 255 => Some(MapKind::Map8(map)),
            256 ... 65535 => Some(MapKind::Map16(map)),
            65536 ... 4294967295 => Some(MapKind::Map32(map)),
            _ => None
        }
    }

    fn serialize(&self) -> Vec<u8> {
        match *self {
            MapKind::TinyMap(ref value) => {
                let mut v = vec![self.get_marker()];

                for (key, entry) in value.into_iter() {
                    v.append(&mut key.serialize());
                    v.append(&mut entry.serialize());
                }
                v
            },
            MapKind::Map8(ref value) => {
                let mut v = vec![self.get_marker(), value.len() as u8];

                for (key, entry) in value.into_iter() {
                    v.append(&mut key.serialize());
                    v.append(&mut entry.serialize());
                }
                v
            },
            MapKind::Map16(ref value) => {
                let mut v = vec![self.get_marker()];
                let mut buf = [0x0; 2];
                BigEndian::write_u16(&mut buf, value.len() as u16);
                v.extend_from_slice(&buf);

                for (key, entry) in value.into_iter() {
                    v.append(&mut key.serialize());
                    v.append(&mut entry.serialize());
                }
                v
            },
            MapKind::Map32(ref value) => {
                let mut v = vec![self.get_marker()];
                let mut buf = [0x0; 4];
                BigEndian::write_u32(&mut buf, value.len() as u32);
                v.extend_from_slice(&buf);

                for (key, entry) in value.into_iter() {
                    v.append(&mut key.serialize());
                    v.append(&mut entry.serialize());
                }
                v
            },
        }
    }

    fn get_marker(&self) -> u8 {
        match *self {
            MapKind::TinyMap(ref value) => 0xA0 + (value.len() as u8),
            MapKind::Map8(..) => 0xD8,
            MapKind::Map16(..) => 0xD9,
            MapKind::Map32(..) => 0xDA,
        }
    }
}

enum StructureKind {
    Message(MessageStructureKind),
    Data(DataStructureKind),
}

impl StructureKind {
    fn serialize(&self) -> Vec<u8> {
        match *self {
            StructureKind::Message(ref kind) => {
                let mut v = self.get_header();
                v.append(&mut kind.serialize_contents());
                v
            },
            StructureKind::Data(ref kind) => {
                let mut v = self.get_header();
                v.append(&mut kind.serialize_contents());
                v
            },
        }
    }
    fn get_signature(&self) -> u8 {
        match *self {
            StructureKind::Message(ref kind) => kind.get_signature(),
            StructureKind::Data(ref kind) => kind.get_signature(),
        }
    }

    fn get_size(&self) -> u16 {
        match *self {
            StructureKind::Message(ref kind) => kind.get_size(),
            StructureKind::Data(ref kind) => kind.get_size(),
        }
    }

    fn get_marker(&self) -> u8 {
        match self.get_size() {
            s @ 0 ... 15 => 0xB0 + (s as u8),
            16 ... 255 => 0xDC,
            _ => 0xDD,
        }
    }

    fn get_header(&self) -> Vec<u8> {
        let signature = self.get_signature();
        let size = self.get_size();

        let marker = self.get_marker();

        match marker {
            0xDD => {
                let mut buf = [0x0; 2];
                BigEndian::write_u16(&mut buf, size as u16);
                vec![marker, buf[0], buf[1], signature]
            },
            0xDC => vec![marker, size as u8, signature],
            _ => vec![marker, signature]
        }
    }
}

enum MessageStructureKind {
    Init(StringKind, MapKind),
    Run(StringKind, MapKind),
    DiscardAll,
    PullAll,
    AckFailure,
    Reset,
    Record(ListKind),
    Success(MapKind),
    Failure(MapKind),
    Ignored(MapKind),
}

impl MessageStructureKind {
    fn serialize_contents(&self) -> Vec<u8> {
        match *self {
            MessageStructureKind::Init(ref string, ref map) | MessageStructureKind::Run(ref string, ref map) => {
                let mut v = string.serialize();
                v.append(&mut map.serialize());
                v
            },
            MessageStructureKind::DiscardAll |
            MessageStructureKind::PullAll |
            MessageStructureKind::AckFailure |
            MessageStructureKind::Reset => Vec::new(),
            MessageStructureKind::Record(ref list) => list.serialize(),
            MessageStructureKind::Success(ref map) |
            MessageStructureKind::Failure(ref map) |
            MessageStructureKind::Ignored(ref map) => map.serialize(),
        }
    }

    fn get_signature(&self) -> u8 {
        match *self {
            MessageStructureKind::Init(..) => 0x1,
            MessageStructureKind::Run(..) => 0x10,
            MessageStructureKind::DiscardAll => 0x2F,
            MessageStructureKind::PullAll => 0x3F,
            MessageStructureKind::AckFailure => 0x0E,
            MessageStructureKind::Reset => 0x0F,
            MessageStructureKind::Record(..) => 0x71,
            MessageStructureKind::Success(..) => 0x70,
            MessageStructureKind::Failure(..) => 0x7F,
            MessageStructureKind::Ignored(..) => 0x7E,
        }
    }

    fn get_size(&self) -> u16 {
        match *self {
            MessageStructureKind::Init(..) => 2,
            MessageStructureKind::Run(..) => 2,
            MessageStructureKind::DiscardAll => 0,
            MessageStructureKind::PullAll => 0,
            MessageStructureKind::AckFailure => 0,
            MessageStructureKind::Reset => 0,
            MessageStructureKind::Record(..) => 1,
            MessageStructureKind::Success(..) => 1,
            MessageStructureKind::Failure(..) => 1,
            MessageStructureKind::Ignored(..) => 1,
        }
    }
}

enum DataStructureKind {
    Node,
    Relationship,
    Path,
    UnboundRelationship,
    NonStandard { signature: u8, size: u16 },
}

impl DataStructureKind {
    fn serialize_contents(&self) -> Vec<u8> {
        vec![0x0]
        //TODO
    }

    fn get_signature(&self) -> u8 {
        match *self {
            DataStructureKind::Node => 0x4E,
            DataStructureKind::Relationship => 0x52,
            DataStructureKind::Path => 0x50,
            DataStructureKind::UnboundRelationship => 0x72,
            DataStructureKind::NonStandard {signature: signature, .. } => signature,
        }
    }

    fn get_size(&self) -> u16 {
        match *self {
            DataStructureKind::Node => 3,
            DataStructureKind::Relationship => 5,
            DataStructureKind::Path => 3,
            DataStructureKind::UnboundRelationship => 3,
            DataStructureKind::NonStandard { size: size, .. } => size,
        }
    }
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

        while message_length > 0 {
            // read header
            let mut buf = [0x0; 2];
            try!(self.stream.read_exact(&mut buf));
            message_length = BigEndian::read_u16(&buf);

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
        map.insert(StringKind::new(String::from("scheme")).unwrap(), DataKind::String(StringKind::new(String::from("basic")).unwrap()));
        map.insert(StringKind::new(String::from("principal")).unwrap(), DataKind::String(StringKind::new(String::from("neo4j")).unwrap()));
        map.insert(StringKind::new(String::from("credentials")).unwrap(), DataKind::String(StringKind::new(String::from("neo4j")).unwrap()));

        let init = DataKind::Structure(
            StructureKind::Message(
                MessageStructureKind::Init(
                    StringKind::new(String::from("bolt-protocol/0.1")).unwrap(),
                    MapKind::new(map).unwrap()
                )
            )
        );

        try!(self.send_message(init.serialize().as_slice()));
        
        let message = try!(self.read_message());

        Ok(())
    }

    pub fn run(&mut self, statement: String) -> Result<(), io::Error> {
        let parameters = HashMap::<StringKind, DataKind>::new();

        let run = DataKind::Structure(
            StructureKind::Message(
                MessageStructureKind::Run(
                    StringKind::new(statement).unwrap(),
                    MapKind::new(parameters).unwrap()
                )
            )
        );

        try!(self.send_message(run.serialize().as_slice()));

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
