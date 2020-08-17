extern crate clap;

use clap::{App, Arg};

use rafted::message::ClientRequest;
use rafted::{Node, Storage};

mod connections;
mod entry;
mod msgparse;
mod serialize;
mod statemachines;
mod storage;
use connections::{ClientAddr, Connections, NodeAddr};
use msgparse::{ClientParser, NodeParser};
use statemachines::BasicStateMachine;
use std::os::unix::io::AsRawFd;
use storage::FileStorage;

use std::io;
use std::io::Error;
use std::net::{IpAddr, TcpListener};
use std::process::exit;
use std::time::{Duration, Instant};

const ELECTION_TIMEOUT: Duration = Duration::from_millis(5000);
const LISTEN_PORT: u16 = 4242;

struct Config {
	my_addr: IpAddr,
	other_ips: Vec<IpAddr>,
	listen_port: u16,
	connect_port: u16,
	election_timeout: Duration,
	heartbeat_timeout: Duration,
}

impl Config {
	fn validate(&self) -> Result<(), String> {
		if self.other_ips.contains(&self.my_addr) {
			Err(format!(
				"my_addr {} should not be contained inside other ips {:?}",
				self.my_addr, self.other_ips
			))
		} else if self.election_timeout < self.heartbeat_timeout * 2 {
			Err(format!(
				"heartbeat_timeout {:?} should be at most half of election_timeout {:?}",
				self.heartbeat_timeout, self.election_timeout
			))
		} else {
			Ok(())
		}
	}
}

fn run_node(config: Config) {
	match config.validate() {
		Ok(_) => {
			let storage: FileStorage<IpAddr, u64> = FileStorage::new(config.my_addr, config.listen_port);
			let mut node: Node<IpAddr, u64, ClientAddr, (), u64, BasicStateMachine> = Node::new(
				config.my_addr,
				config.other_ips.clone(),
				Instant::now(),
				config.election_timeout,
				config.heartbeat_timeout,
				storage.get_data(),
			);

			let listener = TcpListener::bind((config.my_addr, config.listen_port)).unwrap();
			println!("running on {:?}", listener.local_addr().unwrap());

			listener
				.set_nonblocking(true)
				.expect("Cannot set non-blocking");

			let n_parser: NodeParser<IpAddr> = NodeParser::new();
			let c_parser: ClientParser = ClientParser::new();
			let mut conn_manager: Connections<u64, u64, u64> = Connections::new(
				config.my_addr,
				config.other_ips.clone(),
				&listener,
				config.connect_port,
				Box::new(n_parser),
				Box::new(c_parser),
			);

			loop {
				match node
					.get_next_deadline()
					.checked_duration_since(Instant::now())
				{
					Some(timeout) => {
						println!("time til election timeout: {:?}", timeout);
						conn_manager.regenerate_pollfds();
						let result = conn_manager.poll(timeout.as_millis() as i32 + 1);
						println!("result: {}", result);
						if result < 0 {
							panic!("poll error: {}", Error::last_os_error());
						} else if result == 0 {
							// timed out
							let to_send = node.tick(Instant::now());
							for msg in to_send {
								conn_manager.send_message(msg);
							}
						} else {
							// TODO: we can probably use result to reduce the amount of time spent looking for
							// readable sockets, but probably too little to matter

							// handle messages from nodes first
							let node_msgs = conn_manager.get_node_msgs();

							node_msgs.iter().for_each(|(_k, v)| {
								v.into_iter().for_each(|msg| {
									node
										.receive(msg, Instant::now())
										.into_iter()
										.for_each(|to_send| {
											conn_manager.send_message(to_send);
										})
								});
							});

							// then handle messages from clients
							let client_msgs = conn_manager.get_client_msgs();
							client_msgs.into_iter().for_each(|(k, v)| {
								v.into_iter().for_each(|val| {
									let client_req = {
										if val == 0 {
											ClientRequest::Read(k, ())
										} else {
											ClientRequest::Apply(k, val)
										}
									};
									node
										.receive_client(client_req, Instant::now())
										.into_iter()
										.for_each(|msg| {
											conn_manager.send_message(msg);
										});
								})
							});

							// cleanup any fds that have closed unexpectedly
							conn_manager.clean_err_fds();

							// handle listener after receiving messages and cleaning up so we don't
							// affect the polled file descriptors
							loop {
								match listener.accept() {
									Ok((stream, addr)) => {
										stream
											.set_nonblocking(true)
											.expect("stream.set_nonblocking failed");
										let ip = addr.ip();
										if node.is_other_node(&ip) {
											let other_idx: usize =
												config.other_ips.iter().position(|&a| a == ip).unwrap();
											conn_manager.register_node(NodeAddr::new(ip, other_idx), stream);
										} else {
											conn_manager
												.register_client(ClientAddr::new(addr, stream.as_raw_fd()), stream);
										}
									}
									Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
										println!("no new connections!");
										break;
									}
									Err(e) => panic!("encountered IO error: {}", e),
								}
							}
						}
					}
					None => {
						println!("iteration went past deadline");
						let msgs = node.tick(Instant::now());
						for to_send in msgs {
							conn_manager.send_message(to_send);
						}
					}
				}
			}
		}
		Err(msg) => {
			println!("{}", msg);
			exit(1);
		}
	}
}

fn main() {
	let matches = App::new("Basic Raft implementation through TCP")
		.author("Edward L. <edwardlee.259@gmail.com>")
		.about("Example use of Raft library to keep a distributed counter on top of TCP")
		.arg(
			Arg::with_name("addr")
				.short("a")
				.long("addr")
				.value_name("ADDR")
				.takes_value(true)
				.required(true)
				.help("IP address to run on"),
		)
		.arg(
			Arg::with_name("other_addrs")
				.short("o")
				.value_name("ADDRS")
				.multiple(true)
				.takes_value(true)
				.required(true)
				.help("IP addresses of other nodes"),
		)
		.arg(
			Arg::with_name("listen_port")
				.short("p")
				.value_name("PORT")
				.takes_value(true)
				.help(&format!(
					"port to listen for new connections on, defaults to {}",
					LISTEN_PORT
				)),
		)
		.arg(
			Arg::with_name("connect_port")
				.short("c")
				.value_name("PORT")
				.takes_value(true)
				.help("port the node attempts to connect to, useful for proxies. defaults to listen_port"),
		)
		.arg(
			Arg::with_name("election_timeout")
				.short("e")
				.value_name("TIME_MS")
				.takes_value(true)
				.help(
					&format!("election timeout of the node in ms. currently only supports a single value, and not a range. defaults to {}", ELECTION_TIMEOUT.as_millis()),
				),
		)
		.arg(
			Arg::with_name("heartbeat_timeout")
			.short("h")
			.value_name("TIME_MS")
			.takes_value(true)
			.help(
				"heartbeat of the node in ms. defaults to a third of election timeout"
			)
		)
		.get_matches();

	let my_addr_str = matches.value_of("addr").unwrap();
	let my_addr: IpAddr = my_addr_str.parse().expect(&format!(
		"unable to parse {} into an ip address",
		my_addr_str
	));
	let other_ips_str: Vec<&str> = matches.values_of("other_addrs").unwrap().collect();
	let other_ips: Vec<IpAddr> = other_ips_str
		.iter()
		.map(|s| {
			s.parse()
				.expect(&format!("unable to parse {} into an ip address", s))
		})
		.collect();

	let listen_port_str = matches.value_of("listen_port").unwrap_or("4242");
	let listen_port: u16 = listen_port_str
		.parse()
		.expect(&format!("unable to parse {} into a port", listen_port_str));
	let connect_port: u16 = match matches.value_of("connect_port") {
		Some(connect_port_str) => connect_port_str
			.parse()
			.expect(&format!("unable to parse {} into a port", connect_port_str)),
		None => listen_port,
	};

	let election_timeout = match matches.value_of("election_timeout") {
		Some(election_timeout_str) => {
			let election_timeout_ms: u64 = election_timeout_str.parse().expect(&format!(
				"unable to parse {} into u64",
				election_timeout_str
			));
			Duration::from_millis(election_timeout_ms)
		}
		None => ELECTION_TIMEOUT,
	};
	let heartbeat_timeout = match matches.value_of("heartbeat_timeout") {
		Some(heartbeat_timeout_str) => {
			let heartbeat_timeout_ms: u64 = heartbeat_timeout_str.parse().expect(&format!(
				"unable to parse {} into u64",
				heartbeat_timeout_str
			));
			Duration::from_millis(heartbeat_timeout_ms)
		}
		None => ELECTION_TIMEOUT / 3,
	};

	run_node(Config {
		my_addr,
		other_ips,
		connect_port,
		listen_port,
		election_timeout,
		heartbeat_timeout,
	});
}
