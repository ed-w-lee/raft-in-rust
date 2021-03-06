// use crate::entry::Entry;
use crate::msgparse::{ClientParse, NodeParse, ParseStatus};
use crate::serialize::Serialize;

use rafted::message::{ClientResponse, Message, NodeMessage};

use libc::{self, POLLERR, POLLHUP, POLLIN, POLLNVAL};
use net2::TcpBuilder;
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::io::Write;
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::net::{IpAddr, SocketAddr, TcpListener, TcpStream};
use std::os::unix::io::{AsRawFd, RawFd};
use std::ptr::{copy, drop_in_place};

const MAX_FDS: usize = 900;

#[derive(PartialEq, Eq, Copy, Clone, Debug)]
pub struct NodeAddr {
	addr: IpAddr,
	idx: usize,
}
impl NodeAddr {
	pub fn new(addr: IpAddr, idx: usize) -> Self {
		Self { addr, idx }
	}
}
impl Hash for NodeAddr {
	fn hash<H>(&self, state: &mut H)
	where
		H: Hasher,
	{
		&self.idx.hash(state);
	}
}

#[derive(PartialEq, Eq, Copy, Clone, Debug)]
pub struct ClientAddr {
	addr: SocketAddr,
	fd: i32,
}
impl ClientAddr {
	pub fn new(addr: SocketAddr, fd: i32) -> ClientAddr {
		ClientAddr { addr, fd }
	}
}
impl Hash for ClientAddr {
	fn hash<H>(&self, state: &mut H)
	where
		H: Hasher,
	{
		&self.fd.hash(state);
	}
}

pub struct ConnUpdates {
	node_updated: bool,
	client_updated: bool,
}

///
/// Maintains information on how to translate between sockets and messages.
/// It also maintains a distinction between clients and other raft nodes.
///
/// --- Notes ---
///
/// Admittedly, many places for optimization, but I'll ignore that for in an attempt
/// to get the logic done.
/// May be interesting to add in FSM-like behavior to prevent getting messages before
/// polling, only being able to poll once fds are regenerated, etc. I tried doing that
/// but it got too complicated imo.
///
pub struct Connections<M, E, RES> {
	my_addr: IpAddr,
	other_ips: Vec<IpAddr>,
	connect_port: u16,
	pollfds: [MaybeUninit<libc::pollfd>; MAX_FDS],
	node_end: usize,
	curr_len: usize,
	node_parse: Box<dyn NodeParse<TcpStream, IpAddr, E>>,
	node_fds: HashMap<RawFd, NodeAddr>,
	node_streams: HashMap<NodeAddr, TcpStream>,
	client_parse: Box<dyn ClientParse<TcpStream, M>>,
	client_fds: HashMap<RawFd, (bool, ClientAddr)>,
	client_streams: HashMap<ClientAddr, TcpStream>,
	updates: ConnUpdates,
	_response: PhantomData<RES>,
}

// TODO: maybe investigate lifetimes to keep track of RawFds?
impl<M, E, RES> Connections<M, E, RES>
where
	E: Serialize + Debug,
	RES: ToString,
{
	pub fn new(
		my_addr: IpAddr,
		other_ips: Vec<IpAddr>,
		listener: &TcpListener,
		connect_port: u16,
		node_parse: Box<dyn NodeParse<TcpStream, IpAddr, E>>,
		client_parse: Box<dyn ClientParse<TcpStream, M>>,
	) -> Self {
		Connections {
			my_addr,
			other_ips,
			connect_port,
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
			updates: ConnUpdates {
				node_updated: false,
				client_updated: false,
			},
			_response: PhantomData,
		}
	}

	pub fn poll(&mut self, timeout: i32) -> i32 {
		println!(
			"poll status: \n\tlast pollfd {{ fd: {}, events: {} }}\n\tcurr_len: {}, node fds: {}, client fds: {}",
			unsafe { *self.pollfds[self.curr_len - 1].as_ptr() }.fd,
			unsafe { *self.pollfds[self.curr_len - 1].as_ptr() }.events,
			self.curr_len,
			self.node_fds.len(),
			self.client_fds.len()
		);
		unsafe { libc::poll(self.pollfds[0].as_mut_ptr(), self.curr_len as u64, timeout) }
	}

	pub fn register_node(&mut self, addr: NodeAddr, stream: TcpStream) {
		if self.curr_len + 1 >= MAX_FDS {
			panic!("hit self-imposed limit on file descriptors");
		}

		// prefer existing connections to this new one
		if self.node_streams.contains_key(&addr) {
			if self.my_addr < addr.addr {
				return;
			} else {
				println!("cleared existing node entry");
				let old_stream = self
					.node_streams
					.remove_entry(&addr)
					.expect("node_streams doesn't contain stream to remove");
				self.node_fds.remove_entry(&old_stream.1.as_raw_fd());
			}
		}

		self.node_fds.insert(stream.as_raw_fd(), addr);
		self.node_streams.insert(addr, stream);
		self.updates.node_updated = true;
	}

	pub fn register_client(&mut self, addr: ClientAddr, stream: TcpStream) {
		if self.curr_len + 1 >= MAX_FDS {
			panic!("hit self-imposed limit on file descriptors");
		}

		println!("registered client with fd: {}", stream.as_raw_fd(),);

		if self.client_streams.contains_key(&addr) {
			println!("cleared existing client entry");

			let old_stream = self
				.client_streams
				.remove_entry(&addr)
				.expect("node streams doesn't contain stream to remove");
			self.client_fds.remove_entry(&old_stream.1.as_raw_fd());
		}

		self.client_fds.insert(stream.as_raw_fd(), (true, addr));
		self.client_streams.insert(addr, stream);
		self.updates.client_updated = true;
	}

	pub fn get_node_msgs(&mut self) -> HashMap<NodeAddr, Vec<NodeMessage<IpAddr, E>>> {
		let pfds_to_read: Vec<&libc::pollfd> = self.pollfds[1..self.node_end]
			.iter()
			.map(|pfd| unsafe { &*pfd.as_ptr() })
			.filter(|pfd| pfd.revents & POLLIN != 0)
			.collect();

		let mut msg_map: HashMap<NodeAddr, Vec<NodeMessage<IpAddr, E>>> = HashMap::new();
		let mut node_updated = self.updates.node_updated;
		for pfd in pfds_to_read {
			let addr = self
				.node_fds
				.get(&pfd.fd)
				.expect("unexpected key for node_fds");
			let stream = self
				.node_streams
				.get_mut(addr)
				.expect("unexpected key for node_streams");

			let tup = self.node_parse.parse(&addr.addr, stream);
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
					// node somehow hit EOF, this shouldn't happen so we clean up connection
					node_updated = true;
					let addr_to_rem = addr.clone();
					println!(
						"removing addr: {:?}, with fd: {}",
						&addr_to_rem,
						stream.as_raw_fd()
					);
					self.node_fds.remove_entry(&stream.as_raw_fd());
					self.node_streams.remove_entry(&addr_to_rem);
				}
			}
		}

		self.updates.node_updated = node_updated;
		msg_map
	}

	pub fn clean_err_fds(&mut self) {
		let pfds_to_clean: Vec<&libc::pollfd> = self.pollfds[1..self.curr_len]
			.iter()
			.map(|pfd| unsafe { &*pfd.as_ptr() })
			.filter(|pfd| pfd.revents & (POLLHUP | POLLNVAL | POLLERR) != 0)
			.collect();

		let to_print: Vec<RawFd> = pfds_to_clean.iter().map(|pfd| pfd.fd).collect();
		println!("{:?}", to_print);
		for pfd in pfds_to_clean {
			if let Some(addr) = self.node_fds.get(&pfd.fd) {
				let stream = self
					.node_streams
					.get_mut(addr)
					.expect("unexpected key for node_streams");
				// disconnect
				let addr_to_rem = addr.clone();
				self.node_fds.remove_entry(&stream.as_raw_fd());
				self.node_streams.remove_entry(&addr_to_rem);
				self.updates.node_updated = true;
			} else if let Some((_, addr)) = self.client_fds.get(&pfd.fd) {
				let stream = self
					.client_streams
					.get_mut(addr)
					.expect("unexpected key for client_streams");
				// disconnect
				let addr_to_rem = addr.clone();
				self.client_fds.remove_entry(&stream.as_raw_fd());
				self.client_streams.remove_entry(&addr_to_rem);
				self.updates.client_updated = true;
			} else {
				// this is likely a pfd that got cleaned up through an unsuccessful read/write
				println!("WARN - found pfd that wasn't registered")
			}
		}
	}

	pub fn get_client_msgs(&mut self) -> HashMap<ClientAddr, Vec<M>> {
		let pfds_to_read: Vec<&libc::pollfd> = self.pollfds[self.node_end..self.curr_len]
			.iter()
			.map(|pfd| unsafe { &*pfd.as_ptr() })
			.filter(|pfd| pfd.revents & POLLIN != 0)
			.collect();

		let mut msg_map: HashMap<ClientAddr, Vec<M>> = HashMap::new();
		let mut client_updated = self.updates.client_updated;
		for pfd in pfds_to_read {
			let tup = self
				.client_fds
				.get(&pfd.fd)
				.expect("unexpected key for client_fds");
			let should_track = tup.0;
			assert_eq!(should_track, true);

			let addr = tup.1;
			let stream = self
				.client_streams
				.get_mut(&addr)
				.expect("unexpected key for client_streams");

			let tup = self.client_parse.parse(&addr, stream);
			let msgs = tup.0;
			let msg_len = msgs.len();
			msg_map.insert(addr.clone(), msgs);

			let status = tup.1;
			match status {
				ParseStatus::Waiting => {}
				ParseStatus::Unexpected(err) => {
					client_updated = true;
					println!("unexpected parse error --- {}", err);
					stream
						.write(b"parser error")
						.expect("writing fucked up lol");

					// disconnect
					let addr_to_rem = addr.clone();
					self.client_fds.remove_entry(&stream.as_raw_fd());
					self.client_streams.remove_entry(&addr_to_rem);
				}
				ParseStatus::Done => {
					if msg_len > 0 {
						println!(
							"mark stream {} as shouldn't poll since {} msgs",
							stream.as_raw_fd(),
							msg_len
						);
						client_updated = true;
						// mark that we shouldn't poll anymore (we may still want to attempt to send things to them later)
						let tup = self.client_fds.get_mut(&pfd.fd).unwrap();
						tup.0 = false;
					} else {
						println!(
							"clean up stream {} address {:?} since finished reading",
							stream.as_raw_fd(),
							addr
						);
						client_updated = true;
						let addr_to_rem = addr.clone();
						self.client_fds.remove_entry(&stream.as_raw_fd());
						self.client_streams.remove_entry(&addr_to_rem);
					}
				}
			}
		}

		self.updates.client_updated = client_updated;
		msg_map
	}

	pub fn send_message(&mut self, mesg: Message<IpAddr, E, ClientAddr, RES>) {
		match mesg {
			Message::Node(addr, msg) => {
				let other_idx: usize = self.other_ips.iter().position(|&a| a == addr).unwrap();
				self.send_node(&NodeAddr::new(addr, other_idx), vec![msg])
			}
			Message::Client(addr, res) => match res {
				ClientResponse::Response(s) => {
					self.send_client(&addr, &format!("{}\n", s.to_string()), false)
				}
				ClientResponse::Redirect(a) => self.send_client(
					&addr,
					&format!("leader is likely: {}\n", a.to_string()),
					true,
				),
				ClientResponse::TryAgain => {
					self.send_client(
						&addr,
						"no known leader. try again another node another time\n",
						true,
					);
				}
			},
		}
	}

	fn send_node(&mut self, addr: &NodeAddr, msgs: Vec<NodeMessage<IpAddr, E>>) {
		println!("sent to node {:?} message: {:?}", addr, msgs);

		let mut to_send = vec![];
		for msg in msgs {
			let mut to_add = match msg {
				NodeMessage::AppendReq(msg) => {
					let mut v: Vec<u8> = b"aq".to_vec();
					v.append(&mut msg.to_bytes());
					v
				}
				NodeMessage::AppendRes(msg) => {
					let mut v: Vec<u8> = b"as".to_vec();
					v.append(&mut msg.to_bytes());
					v
				}
				NodeMessage::VoteReq(msg) => {
					let mut v: Vec<u8> = b"vq".to_vec();
					v.append(&mut msg.to_bytes());
					v
				}
				NodeMessage::VoteRes(msg) => {
					let mut v: Vec<u8> = b"vs".to_vec();
					v.append(&mut msg.to_bytes());
					v
				}
			};
			to_send.append(&mut to_add);
		}

		if self.node_streams.get_mut(addr).is_none() {
			let build = TcpBuilder::new_v4().unwrap();
			loop {
				match build.bind(SocketAddr::new(self.my_addr, 0)) {
					Ok(b) => {
						if let Ok(stream) = b.connect(SocketAddr::new(addr.addr, self.connect_port)) {
							stream
								.set_nonblocking(true)
								.expect("unable to set nonblocking");
							self.register_node(addr.clone(), stream);
						} else {
							println!("couldn't connect, aborting");
							return;
						}
						break;
					}
					Err(_) => (),
				}
			}
		}

		if let Some(stream) = self.node_streams.get_mut(addr) {
			match stream.write_all(&to_send) {
				Ok(_) => {}
				Err(_) => {
					self.node_fds.remove_entry(&stream.as_raw_fd());
					self.node_streams.remove_entry(addr);
					self.updates.node_updated = true;
				}
			}
		}
	}

	fn send_client(&mut self, addr: &ClientAddr, msg: &str, close: bool) {
		println!("sent to client {:?} message: {:?}", addr, msg);

		match self.client_streams.get_mut(addr) {
			Some(stream) => {
				let mut to_clean: bool = close || !self.client_fds.get(&stream.as_raw_fd()).unwrap().0;
				match stream.write_all(msg.as_bytes()) {
					Ok(_) => {}
					Err(_) => {
						to_clean = true;
					}
				};
				println!(
					"identified stream {:?} with to_clean as: {}",
					stream.as_raw_fd(),
					to_clean
				);
				if to_clean {
					println!("removing client from connections");
					self.client_fds.remove_entry(&stream.as_raw_fd());
					self.client_streams.remove_entry(addr);

					self.updates.client_updated = true;
				}
			}
			None => {
				println!("couldn't find client, likely cleaned up.");
			}
		}
	}

	pub fn regenerate_pollfds(&mut self) {
		println!(
			"regenerating pollfds with ({},{})",
			self.updates.client_updated, self.updates.node_updated
		);
		match (self.updates.client_updated, self.updates.node_updated) {
			(false, false) => {}
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
		self.updates = ConnUpdates {
			client_updated: false,
			node_updated: false,
		};
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
