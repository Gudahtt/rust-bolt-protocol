
pub mod bolt_client {
    use std::net::TcpStream;
    use std::io;

    pub struct DatabaseSession {
        stream: TcpStream,
    }

    pub fn connect(server: &str, username: &str, password: &str) -> io::Result<DatabaseSession> {
        let mut stream = TcpStream::connect(server);

        match stream {
            Ok(stream) => Ok(
                DatabaseSession { stream: stream }
            ),
            Err(error) => Err(error),
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
    }
}
