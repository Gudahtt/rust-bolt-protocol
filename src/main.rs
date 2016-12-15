extern crate clap;
extern crate bolt_protocol;

use clap::{Arg, App, SubCommand};
use bolt_protocol::bolt_client;

const VERSION: Option<&'static str> = option_env!("CARGO_PKG_VERSION");

fn main() {
    let matches = App::new("Bolt protocol")
        .version(VERSION.unwrap_or("Unknown"))
        .arg(
            Arg::with_name("server")
                .short("s")
                .long("server")
                .value_name("server")
                .help("URL of Bolt server")
                .takes_value(true)
        )
        .arg(
            Arg::with_name("username")
                .short("u")
                .long("username")
                .value_name("username")
                .help("Bolt server username")
                .required(true)
                .takes_value(true)
        )
        .arg(
            Arg::with_name("password")
                .short("p")
                .long("password")
                .value_name("password")
                .help("Bolt server password")
                .required(true)
                .takes_value(true)
        )
        .arg(
            Arg::with_name("statement")
                .short("c")
                .long("statement")
                .value_name("STATEMENT")
                .help("Cypher statement to run")
                .required(true)
                .takes_value(true)
        )
        .get_matches();
    
    let url = matches.value_of("url").unwrap_or("bolt://localhost:7687");
    let username = matches.value_of("username").unwrap();
    let password = matches.value_of("password").unwrap();

    let statement = matches.value_of("statement").unwrap();

    let session = bolt_client::connect(url, username, password);

    if let Some(session) = session {
        //let result = session.run(statement);

        //println!("{:?}", result);
    } else {
        println!("Connection failed")
    }
}
