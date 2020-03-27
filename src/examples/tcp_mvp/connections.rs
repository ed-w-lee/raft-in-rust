/**
 * Maintains information on how to translate between file descriptors and TcpStreams.
 * It also maintains a distinction between clients and other raft nodes.
 * Admittedly, many places for optimization, but I'll ignore that for in an attempt
 * to get the logic done.
 */
use crate::msgparse::{ClientParse, NodeParse, ParseStatus};

use rafted::Message;

use libc::{self, POLLIN};
use std::collections::HashMap;
use std::io::Write;
use std::mem::MaybeUninit;
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::os::unix::io::{AsRawFd, RawFd};
use std::ptr::{copy, drop_in_place};

const MAX_FDS: usize = 900;

#[derive(Hash, PartialEq, Eq, Copy, Clone)]
pub struct NodeAddr {
	addr: SocketAddr,
}
impl NodeAddr {
	pub fn new(addr: SocketAddr) -> NodeAddr {
		NodeAddr { addr }
	}
}

#[derive(Hash, PartialEq, Eq, Copy, Clone)]
pub struct ClientAddr {
	addr: SocketAddr,
}
impl ClientAddr {
	pub fn new(addr: SocketAddr) -> ClientAddr {
		ClientAddr { addr }
	}
}

pub struct NoUpdates;
pub struct NodeUpdated {
	node_updated: bool,
}
pub struct ClientUpdated {
	node_updated: bool,
	client_updated: bool,
}

pub struct Connections<S, M> {
	pollfds: [MaybeUninit<libc::pollfd>; MAX_FDS],
	node_end: usize,
	curr_len: usize,
	node_parse: Box<dyn NodeParse<TcpStream>>,
	node_fds: HashMap<RawFd, NodeAddr>,
	node_streams: HashMap<NodeAddr, TcpStream>,
	client_parse: Box<dyn ClientParse<TcpStream, M>>,
	client_fds: HashMap<RawFd, (bool, ClientAddr)>,
	client_streams: HashMap<ClientAddr, TcpStream>,
	updates: S,
}

// TODO: maybe investigate lifetimes to keep track of RawFds?
impl<M> Connections<NoUpdates, M> {
	pub fn new(
		listener: &TcpListener,
		node_parse: Box<dyn NodeParse<TcpStream>>,
		client_parse: Box<dyn ClientParse<TcpStream, M>>,
	) -> Connections<NoUpdates, M> {
		Connections {
			pollfds: unsafe {
				let mut data: [MaybeUninit<libc::pollfd>; MAX_FDS] = MaybeUninit::uninit().assume_init();
				*(&mut data[0]) = MaybeUninit::new(pollfd(listener.as_raw_fd()));
				data
			},
			node_end: 1,
			curr_len: 1,
			node_parse,
			node_fds: HashMap::new(),
			node_streams: HashMap::new(),
			client_parse,
			client_fds: HashMap::new(),
			client_streams: HashMap::new(),
			updates: NoUpdates,
		}
	}

	pub fn poll(&mut self, timeout: i32) -> i32 {
		println!(
			"last pollfd {{ fd: {}, events: {} }}",
			unsafe { *self.pollfds[self.curr_len - 1].as_ptr() }.fd,
			unsafe { *self.pollfds[self.curr_len - 1].as_ptr() }.events
		);
		unsafe { libc::poll(self.pollfds[0].as_mut_ptr(), self.curr_len as u64, timeout) }
	}

	pub fn register_node(&mut self, addr: NodeAddr, stream: TcpStream) {
		if self.curr_len + 1 >= MAX_FDS {
			panic!("hit self-imposed limit on file descriptors");
		}

		// shift over any client pollfds
		if self.node_end < self.curr_len {
			unsafe {
				copy(
					self.pollfds[self.node_end].as_ptr(),
					self.pollfds[self.node_end + 1].as_mut_ptr(),
					self.curr_len - self.node_end,
				)
			}
		}

		// insert node pollfd
		*(&mut self.pollfds[self.node_end]) = MaybeUninit::new(pollfd(stream.as_raw_fd()));
		self.node_end += 1;
		self.curr_len += 1;

		self.node_fds.insert(stream.as_raw_fd(), addr);
		self.node_streams.insert(addr, stream);
	}

	pub fn register_client(&mut self, addr: ClientAddr, stream: TcpStream) {
		if self.curr_len + 1 >= MAX_FDS {
			panic!("hit self-imposed limit on file descriptors");
		}

		*(&mut self.pollfds[self.curr_len]) = MaybeUninit::new(pollfd(stream.as_raw_fd()));
		self.curr_len += 1;
		println!(
			"registered client: pollfd {{ fd: {}, events: {} }}",
			unsafe { *self.pollfds[self.curr_len - 1].as_ptr() }.fd,
			unsafe { *self.pollfds[self.curr_len - 1].as_ptr() }.events
		);

		self.client_fds.insert(stream.as_raw_fd(), (true, addr));
		self.client_streams.insert(addr, stream);
	}

	pub fn get_node_msgs(mut self) -> (Connections<NodeUpdated, M>, HashMap<NodeAddr, Vec<Message>>) {
		let pfds_to_read: Vec<&libc::pollfd> = self.pollfds[1..self.node_end]
			.iter()
			.map(|pfd| unsafe { &*pfd.as_ptr() })
			.filter(|pfd| pfd.revents & POLLIN != 0)
			.collect();

		println!("nodes polled successfully: {}", pfds_to_read.len());

		let mut msg_map: HashMap<NodeAddr, Vec<Message>> = HashMap::new();
		let mut node_updated = false;
		for pfd in pfds_to_read {
			let addr = self
				.node_fds
				.get(&pfd.fd)
				.expect("unexpected key for node_fds");
			let stream = self
				.node_streams
				.get_mut(addr)
				.expect("unexpected key for node_streams");

			let tup = self.node_parse.parse(addr, stream);
			let msgs = tup.0;
			msg_map.insert(addr.clone(), msgs);

			let status = tup.1;
			match status {
				ParseStatus::Waiting => {}
				ParseStatus::Unexpected(err) => {
					node_updated = true;
					println!("unexpected parse error: {}", err);
					let addr_to_rem = addr.clone();
					self.node_fds.remove_entry(&stream.as_raw_fd());
					self.node_streams.remove_entry(&addr_to_rem);
				}
				ParseStatus::Done => {
					node_updated = true;
					let addr_to_rem = addr.clone();
					self.node_fds.remove_entry(&stream.as_raw_fd());
					self.node_streams.remove_entry(&addr_to_rem);
				}
			}
		}

		(
			Connections {
				pollfds: self.pollfds,
				node_end: self.node_end,
				curr_len: self.curr_len,
				node_parse: self.node_parse,
				node_fds: self.node_fds,
				node_streams: self.node_streams,
				client_parse: self.client_parse,
				client_fds: self.client_fds,
				client_streams: self.client_streams,
				updates: NodeUpdated { node_updated },
			},
			msg_map,
		)
	}

	pub fn send_node_msg(&mut self, addr: NodeAddr, msg: Message) {
		unimplemented!()
	}

	pub fn send_client_msg(&mut self, addr: ClientAddr, msg: Message) {
		unimplemented!()
	}
}

impl<M> Connections<NodeUpdated, M> {
	pub fn get_client_msgs(&mut self) -> HashMap<ClientAddr, Vec<M>> {
		let pfds_to_read: Vec<&libc::pollfd> = self.pollfds[self.node_end..self.curr_len]
			.iter()
			.map(|pfd| unsafe { &*pfd.as_ptr() })
			.filter(|pfd| pfd.revents & POLLIN != 0)
			.collect();

		println!("nodes polled successfully: {}", pfds_to_read.len());

		let mut msg_map: HashMap<ClientAddr, Vec<M>> = HashMap::new();
		let mut client_updated = false;
		for pfd in pfds_to_read {
			let tup = self
				.client_fds
				.get(&pfd.fd)
				.expect("unexpected key for node_fds");
			let should_track = tup.0;
			assert_eq!(should_track, true);

			let addr = tup.1;
			let stream = self
				.client_streams
				.get_mut(&addr)
				.expect("unexpected key for node_streams");

			let tup = self.client_parse.parse(&addr, stream);
			let msgs = tup.0;
			msg_map.insert(addr.clone(), msgs);

			let status = tup.1;
			match status {
				ParseStatus::Waiting => {}
				ParseStatus::Unexpected(err) => {
					client_updated = true;
					println!("unexpected parse error: {}", err);
					stream.write(b"parser error");

					let addr_to_rem = addr.clone();
					self.client_fds.remove_entry(&stream.as_raw_fd());
					self.client_streams.remove_entry(&addr_to_rem);
				}
				ParseStatus::Done => {
					client_updated = true;
					let addr_to_rem = addr.clone();
					let tup = self.client_fds.get_mut(&pfd.fd).unwrap();
					tup.0 = false;
					assert_eq!(self.client_fds.get(&pfd.fd).unwrap().0, false);
				}
			}
		}

		unimplemented!()
	}

	pub fn unregister_client_conns(
		mut self,
		addrs: Vec<ClientAddr>,
	) -> Connections<ClientUpdated, M> {
		unimplemented!()
		// Connections {
		// 	pollfds: self.pollfds,
		// 	node_end: self.node_end,
		// 	curr_len: self.curr_len,
		// 	node_fds: self.node_fds,
		// 	node_streams: self.node_streams,
		// 	client_fds: self.client_fds,
		// 	client_streams: self.client_streams,
		// 	updates: NodeUpdated { node_updated },
		// }
	}
}

impl<M> Connections<ClientUpdated, M> {
	pub fn regenerate_pollfds(mut self) -> Connections<NoUpdates, M> {
		match (self.updates.client_updated, self.updates.node_updated) {
			(false, false) => {
				println!("no regeneration");
			}
			(true, false) => {
				// only client -> regenerate client, don't touch node
				for i in self.node_end..self.curr_len {
					unsafe { drop_in_place(self.pollfds[i].as_mut_ptr()) };
				}
				let clients = self._get_clients_to_poll();
				// copy clients into end of pollfds
				self.curr_len = self.node_end + clients.len();
				for i in self.node_end..self.curr_len {
					*(&mut self.pollfds[i]) = MaybeUninit::new(clients[i - self.node_end]);
				}
			}
			(false, true) => {
				// only node -> regenerate node, move client over
				for i in 1..self.node_end {
					unsafe { drop_in_place(self.pollfds[i].as_mut_ptr()) };
				}

				let nodes = self._get_nodes_to_poll();
				let new_node_end = 1 + nodes.len();

				// shift over any client pollfds
				if self.node_end < self.curr_len {
					unsafe {
						copy(
							self.pollfds[self.node_end].as_ptr(),
							self.pollfds[new_node_end].as_mut_ptr(),
							self.curr_len - self.node_end,
						)
					}
				}

				// copy nodes into pollfds
				self.curr_len = (self.curr_len + new_node_end) - self.node_end;
				self.node_end = new_node_end;
				for i in 1..self.node_end {
					*(&mut self.pollfds[i]) = MaybeUninit::new(nodes[i - 1]);
				}
			}
			(true, true) => {
				// both -> regenerate both
				for i in 1..self.curr_len {
					unsafe { drop_in_place(self.pollfds[i].as_mut_ptr()) };
				}

				let clients = self._get_clients_to_poll();
				let nodes = self._get_nodes_to_poll();

				self.node_end = 1 + nodes.len();
				self.curr_len = self.node_end + clients.len();

				for i in 0..nodes.len() {
					*(&mut self.pollfds[i + 1]) = MaybeUninit::new(nodes[i]);
				}
				for i in 0..clients.len() {
					*(&mut self.pollfds[i + self.node_end]) = MaybeUninit::new(clients[i]);
				}
			}
		}
		Connections {
			pollfds: self.pollfds,
			node_end: self.node_end,
			curr_len: self.curr_len,
			node_parse: self.node_parse,
			node_fds: self.node_fds,
			node_streams: self.node_streams,
			client_parse: self.client_parse,
			client_fds: self.client_fds,
			client_streams: self.client_streams,
			updates: NoUpdates,
		}
	}

	fn _get_clients_to_poll(&self) -> Vec<libc::pollfd> {
		self
			.client_fds
			.iter()
			.filter(|(_k, v)| v.0)
			.map(|(k, _v)| k)
			.map(|&k| pollfd(k))
			.collect()
	}

	fn _get_nodes_to_poll(&self) -> Vec<libc::pollfd> {
		self
			.node_fds
			.iter()
			.map(|(k, _v)| k)
			.map(|&k| pollfd(k))
			.collect()
	}
}

fn pollfd(fd: RawFd) -> libc::pollfd {
	libc::pollfd {
		fd,
		events: POLLIN,
		revents: 0,
	}
}
