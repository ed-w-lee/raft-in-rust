mod connections;
mod msgparse;
use connections::{ClientAddr, Connections, NodeAddr};
use msgparse::{ClientParser, NodeParser, ParseStatus};

use libc;
use std::cmp::min;
use std::io;
use std::io::prelude::*;
use std::io::Error;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::os::unix::io::{AsRawFd, RawFd};
use std::thread;
use std::time::{Duration, Instant};

const ELECTION_TIMEOUT: u64 = 5000;

fn main() {
	let other_addrs = [SocketAddr::from(([127, 0, 0, 1], 8484))];
	let addrs = [SocketAddr::from(([127, 0, 0, 1], 4242))];
	let listener = TcpListener::bind(&addrs[..]).unwrap();

	listener
		.set_nonblocking(true)
		.expect("Cannot set non-blocking");

	let mut deadline = Instant::now()
		.checked_add(Duration::from_millis(ELECTION_TIMEOUT))
		.unwrap();

	let n_parser: NodeParser<TcpStream> = NodeParser::new();
	let c_parser: ClientParser<TcpStream, i32> = ClientParser::new();
	let mut conn_manager = Connections::new(&listener, Box::new(n_parser), Box::new(c_parser));

	loop {
		thread::sleep(Duration::from_secs(1));

		match deadline.checked_duration_since(Instant::now()) {
			Some(timeout) => {
				println!("time til election timeout: {:?}", timeout);
				let result = conn_manager.poll(min(ELECTION_TIMEOUT as i32, timeout.as_millis() as i32));
				println!("result: {}", result);
				if result < 0 {
					panic!("poll error: {}", Error::last_os_error());
				} else if result > 0 {
					// TODO: we can probably use result to reduce the amount of time spent looking for
					// readable sockets, but probably too little to matter

					// listener
					match listener.accept() {
						Ok((stream, addr)) => {
							stream
								.set_nonblocking(true)
								.expect("stream.set_nonblocking failed");
							if other_addrs.contains(&addr) {
								conn_manager.register_node(NodeAddr::new(addr), stream);
							} else {
								conn_manager.register_client(ClientAddr::new(addr), stream);
							}
						}
						Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
							println!("no new connections!");
						}
						Err(e) => panic!("encountered IO error: {}", e),
					}

					// handle messages from nodes first
					let node_msgs = conn_manager.get_node_msgs();

					// then handle messages from clients
					let client_msgs = conn_manager.get_client_msgs();
					thread::sleep(Duration::from_millis(1000));
					client_msgs.iter().for_each(|(k, v)| {
						println!("client: {:?}, msgs: {:?}", k, v);
						conn_manager.send_client(k, &format!("{:?}\n", v));
					});

					conn_manager.regenerate_pollfds();
				}
				deadline = Instant::now()
					.checked_add(Duration::from_millis(ELECTION_TIMEOUT))
					.unwrap();
			}
			None => {
				// election timeout
				panic!("election timeout!");
			}
		}
	}
}
