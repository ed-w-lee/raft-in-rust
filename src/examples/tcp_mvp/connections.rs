/**
 * Maintains information on how to translate between file descriptors and TcpStreams.
 * It also maintains a distinction between clients and other raft nodes.
 * Admittedly, many places for optimization, but I'll ignore that for in an attempt
 * to get the logic done.
 */
use rafted::NodeId;

use libc::{self, POLLIN};
use std::collections::HashMap;
use std::mem::MaybeUninit;
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::os::unix::io::{AsRawFd, RawFd};
use std::ptr::{copy, drop_in_place};

const MAX_FDS: usize = 900;

pub struct Connections {
	pollfds: [MaybeUninit<libc::pollfd>; MAX_FDS],
	other_end: usize,
	curr_len: usize,
	other_fds: HashMap<RawFd, NodeId>,
	other_streams: HashMap<NodeId, TcpStream>,
	client_fds: HashMap<RawFd, (bool, TcpStream)>,
}

// TODO: maybe investigate lifetimes to keep track of RawFds?
impl Connections {
	pub fn new(listener: &TcpListener) -> Connections {
		Connections {
			pollfds: unsafe {
				let mut data: [MaybeUninit<libc::pollfd>; MAX_FDS] = MaybeUninit::uninit().assume_init();
				*(&mut data[0]) = MaybeUninit::new(Connections::pollfd(listener.as_raw_fd()));
				data
			},
			other_end: 1,
			curr_len: 1,
			other_fds: HashMap::new(),
			other_streams: HashMap::new(),
			client_fds: HashMap::new(),
		}
	}

	pub fn get_pollfds(&mut self) -> (*mut libc::pollfd, usize) {
		println!(
			"last pollfd {{ fd: {}, events: {} }}",
			unsafe { *self.pollfds[self.curr_len - 1].as_ptr() }.fd,
			unsafe { *self.pollfds[self.curr_len - 1].as_ptr() }.events
		);
		(self.pollfds[0].as_mut_ptr(), self.curr_len)
	}

	pub fn register_other(&mut self, addr: SocketAddr, stream: TcpStream) {
		if self.curr_len + 1 >= MAX_FDS {
			panic!("hit self-imposed limit on file descriptors");
		}

		// shift over any client pollfds
		if self.other_end < self.curr_len {
			unsafe {
				copy(
					self.pollfds[self.other_end].as_ptr(),
					self.pollfds[self.other_end + 1].as_mut_ptr(),
					self.curr_len - self.other_end,
				)
			}
		}

		// insert other pollfd
		*(&mut self.pollfds[self.other_end]) =
			MaybeUninit::new(Connections::pollfd(stream.as_raw_fd()));
		self.other_end += 1;
		self.curr_len += 1;

		self.other_fds.insert(stream.as_raw_fd(), addr);
		self.other_streams.insert(addr, stream);
	}

	pub fn register_client(&mut self, stream: TcpStream) {
		if self.curr_len + 1 >= MAX_FDS {
			panic!("hit self-imposed limit on file descriptors");
		}

		*(&mut self.pollfds[self.curr_len]) = MaybeUninit::new(Connections::pollfd(stream.as_raw_fd()));
		self.curr_len += 1;
		println!(
			"registered client: pollfd {{ fd: {}, events: {} }}",
			unsafe { *self.pollfds[self.curr_len - 1].as_ptr() }.fd,
			unsafe { *self.pollfds[self.curr_len - 1].as_ptr() }.events
		);

		self.client_fds.insert(stream.as_raw_fd(), (true, stream));
	}

	pub fn act_on_connections(
		&mut self,
		f_others: impl FnMut(&mut TcpStream) -> bool,
		f_clients: impl FnMut(&mut TcpStream) -> bool,
	) {
		let others_updated = self._act_on_others(f_others);
		let client_updated = self._act_on_clients(f_clients);
		self._regenerate_pollfds(client_updated, others_updated);
	}

	fn _act_on_others(&mut self, mut f: impl FnMut(&mut TcpStream) -> bool) -> bool {
		let pfds_to_read: Vec<&libc::pollfd> = self.pollfds[1..self.other_end]
			.iter()
			.map(|pfd| unsafe { &*pfd.as_ptr() })
			.filter(|pfd| {
				println!("other: {}", pfd.revents);
				pfd.revents & POLLIN != 0
			})
			.collect();

		println!("others polled successfully: {}", pfds_to_read.len());

		let mut fds_to_delete: Vec<RawFd> = Vec::new();
		for pfd in pfds_to_read {
			let addr = self.other_fds.get(&pfd.fd).unwrap();
			if let Some(stream) = self.other_streams.get_mut(addr) {
				if f(stream) {
					fds_to_delete.push(stream.as_raw_fd());
				}
			}
		}

		let updated = fds_to_delete.len() > 0;

		for fd in fds_to_delete {
			let addr = self.other_fds.get(&fd).unwrap();
			self.other_streams.remove_entry(&addr);
			self.other_fds.remove_entry(&fd);
		}

		updated
	}

	fn _act_on_clients(&mut self, mut f: impl FnMut(&mut TcpStream) -> bool) -> bool {
		let pfds_to_read: Vec<&libc::pollfd> = self.pollfds[self.other_end..self.curr_len]
			.iter()
			.map(|pfd| unsafe { &*pfd.as_ptr() })
			.filter(|pfd| {
				println!("client: {}", pfd.revents);
				pfd.revents & POLLIN != 0
			})
			.collect();

		println!("clients polled successfully: {}", pfds_to_read.len());

		let mut updated = false;
		for pfd in pfds_to_read {
			let tup = self.client_fds.get_mut(&pfd.fd).unwrap();
			if tup.0 {
				let to_ignore = f(&mut tup.1);
				if to_ignore {
					tup.0 = false;
					updated = true;
				}
			}
		}
		updated
	}

	fn _regenerate_pollfds(&mut self, client_updated: bool, others_updated: bool) {
		match (client_updated, others_updated) {
			(false, false) => {
				println!("no regeneration");
			}
			(true, false) => {
				// only client -> regenerate client, don't touch other
				for i in self.other_end..self.curr_len {
					unsafe { drop_in_place(self.pollfds[i].as_mut_ptr()) };
				}
				let clients = self._get_clients_to_poll();
				// copy clients into end of pollfds
				self.curr_len = self.other_end + clients.len();
				for i in self.other_end..self.curr_len {
					*(&mut self.pollfds[i]) = MaybeUninit::new(clients[i - self.other_end]);
				}
			}
			(false, true) => {
				// only other -> regenerate other, move client over
				for i in 1..self.other_end {
					unsafe { drop_in_place(self.pollfds[i].as_mut_ptr()) };
				}

				let others = self._get_others_to_poll();
				let new_other_end = 1 + others.len();

				// shift over any client pollfds
				if self.other_end < self.curr_len {
					unsafe {
						copy(
							self.pollfds[self.other_end].as_ptr(),
							self.pollfds[new_other_end].as_mut_ptr(),
							self.curr_len - self.other_end,
						)
					}
				}

				// copy others into pollfds
				self.curr_len = (self.curr_len + new_other_end) - self.other_end;
				self.other_end = new_other_end;
				for i in 1..self.other_end {
					*(&mut self.pollfds[i]) = MaybeUninit::new(others[i - 1]);
				}
			}
			(true, true) => {
				// both -> regenerate both
				for i in 1..self.curr_len {
					unsafe { drop_in_place(self.pollfds[i].as_mut_ptr()) };
				}

				let clients = self._get_clients_to_poll();
				let others = self._get_others_to_poll();

				self.other_end = 1 + others.len();
				self.curr_len = self.other_end + clients.len();

				for i in 0..others.len() {
					*(&mut self.pollfds[i + 1]) = MaybeUninit::new(others[i]);
				}
				for i in 0..clients.len() {
					*(&mut self.pollfds[i + self.other_end]) = MaybeUninit::new(clients[i]);
				}
			}
		}
	}

	fn _get_clients_to_poll(&self) -> Vec<libc::pollfd> {
		self
			.client_fds
			.iter()
			.filter(|(_k, v)| v.0)
			.map(|(k, _v)| k)
			.map(|&k| Connections::pollfd(k))
			.collect()
	}

	fn _get_others_to_poll(&self) -> Vec<libc::pollfd> {
		self
			.other_fds
			.iter()
			.map(|(k, _v)| k)
			.map(|&k| Connections::pollfd(k))
			.collect()
	}

	fn pollfd(fd: RawFd) -> libc::pollfd {
		libc::pollfd {
			fd,
			events: POLLIN,
			revents: 0,
		}
	}
}
