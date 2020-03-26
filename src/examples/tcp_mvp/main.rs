mod connections;
use connections::Connections;

use libc;
use std::cmp::min;
use std::io;
use std::io::prelude::*;
use std::io::Error;
use std::net::{SocketAddr, TcpListener};
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

	let mut connections = Connections::new(&listener);

	loop {
		thread::sleep(Duration::from_secs(1));

		match deadline.checked_duration_since(Instant::now()) {
			Some(timeout) => {
				println!("time til election timeout: {:?}", timeout);
				let tup = connections.get_pollfds();
				println!("num to poll: {}", tup.1);
				let result = unsafe {
					libc::poll(
						tup.0,
						tup.1 as u64,
						min(ELECTION_TIMEOUT as i32, timeout.as_millis() as i32),
					)
				};
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
								connections.register_other(addr, stream);
							} else {
								connections.register_client(stream);
							}
						}
						Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
							println!("no new connections!");
						}
						Err(e) => panic!("encountered IO error: {}", e),
					}

					connections.act_on_connections(
						// other nodes
						|stream| {
							let mut buf: Vec<u8> = vec![]; // TODO: probably have management of streams -> buffers be its own struct
							match stream.read_to_end(&mut buf) {
								Ok(_) => {
									println!("node: {:?}", buf);
									true // somehow hit EOF, we should drop connection
								}
								Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
									println!("node: {:?}", buf);
									false // connection still up, don't delete
								}
								Err(e) => panic!("encountered IO error: {}", e),
							}
						},
						// client nodes
						|stream| {
							let mut buf: Vec<u8> = vec![];
							match stream.read_to_end(&mut buf) {
								Ok(_) => {
									println!("client: {:?}", buf);
									true // done with client node, stop polling
								}
								Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
									println!("client: {:?}", buf);
									false
								}
								Err(e) => panic!("encountered IO error: {}", e),
							}
						},
					);
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
