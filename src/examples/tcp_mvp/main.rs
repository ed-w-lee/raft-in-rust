mod connections;
mod entry;
mod msgparse;
mod serialize;
mod storage;
use connections::{ClientAddr, Connections, NodeAddr};
// use entry::Entry;
use msgparse::{ClientParser, NodeParser};
use storage::FileStorage;

use rafted::{Node, Storage};

use std::io;
use std::io::Error;
use std::net::{IpAddr, SocketAddr, TcpListener};
use std::time::{Duration, Instant};

const ELECTION_TIMEOUT: Duration = Duration::from_millis(5000);
const HEARTBEAT_TIMEOUT: Duration = Duration::from_millis(2000);
pub const LISTEN_PORT: u16 = 4242;

fn main() {
	let ips: Vec<IpAddr> = vec![
		"127.0.0.11".parse().unwrap(),
		"127.0.0.21".parse().unwrap(),
		"127.0.0.31".parse().unwrap(),
		// "127.0.0.41".parse().unwrap(),
		// "127.0.0.51".parse().unwrap(),
	];
	let bind_addrs: Vec<SocketAddr> = ips
		.iter()
		.map(|addr| SocketAddr::new(addr.clone(), LISTEN_PORT))
		.collect();
	let listener = TcpListener::bind(&bind_addrs[..]).unwrap();
	println!("running on {:?}", listener.local_addr().unwrap());

	let my_addr = listener.local_addr().unwrap().ip();
	let other_ips: Vec<IpAddr> = ips
		.iter()
		.map(|addr| addr.clone())
		.filter(|&addr| addr != my_addr)
		.collect();

	let storage: FileStorage<IpAddr, u64> = FileStorage::new(my_addr);
	let mut node: Node<IpAddr, u64> = Node::new(
		my_addr,
		other_ips,
		Instant::now(),
		ELECTION_TIMEOUT,
		HEARTBEAT_TIMEOUT,
		storage.get_data(),
	);

	listener
		.set_nonblocking(true)
		.expect("Cannot set non-blocking");

	let n_parser: NodeParser<IpAddr> = NodeParser::new();
	let c_parser: ClientParser = ClientParser::new();
	let mut conn_manager: Connections<u64, u64> =
		Connections::new(my_addr, &listener, Box::new(n_parser), Box::new(c_parser));

	loop {
		match node
			.get_next_deadline()
			.checked_duration_since(Instant::now())
		{
			Some(timeout) => {
				println!("time til election timeout: {:?}", timeout);
				let result = conn_manager.poll(timeout.as_millis() as i32 + 1);
				println!("result: {}", result);
				if result < 0 {
					panic!("poll error: {}", Error::last_os_error());
				} else if result == 0 {
					// timed out
					let to_send = node.tick(Instant::now());
					for tup in to_send {
						let (addr, msg) = tup;
						conn_manager.send_node(&NodeAddr::new(addr), vec![msg]);
					}
				} else {
					// TODO: we can probably use result to reduce the amount of time spent looking for
					// readable sockets, but probably too little to matter

					// listener
					match listener.accept() {
						Ok((stream, addr)) => {
							stream
								.set_nonblocking(true)
								.expect("stream.set_nonblocking failed");
							let ip = addr.ip();
							if node.is_other_node(&ip) {
								conn_manager.register_node(NodeAddr::new(ip), stream);
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

					node_msgs.iter().for_each(|(_k, v)| {
						for msg in v {
							for tup in node.receive(msg, Instant::now()) {
								let (addr, msg_send) = tup;
								conn_manager.send_node(&NodeAddr::new(addr), vec![msg_send]);
							}
						}
					});

					// then handle messages from clients
					let client_msgs = conn_manager.get_client_msgs();
					client_msgs.iter().for_each(|(k, v)| {
						println!("client: {:?}, msgs: {:?}", k, v);
						conn_manager.send_client(k, &format!("{:?}\n", v));
					});

					conn_manager.regenerate_pollfds();
				}
			}
			None => {
				println!("iteration went past deadline");
				let to_send = node.tick(Instant::now());
				for tup in to_send {
					let (addr, msg) = tup;
					conn_manager.send_node(&NodeAddr::new(addr), vec![msg]);
				}
			}
		}
	}
}
