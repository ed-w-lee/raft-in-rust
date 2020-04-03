/**
 * Manages the translation from streams that implement Read to messages that can be used
 * by the nodes for state management.
 */
use crate::connections::ClientAddr;
use crate::serialize::{SerialStatus, Serialize};
use rafted::message::{
	AppendEntries, AppendEntriesResponse, NodeMessage, RequestVote, RequestVoteResponse,
};

use std::collections::HashMap;
use std::hash::Hash;
use std::io::{self, Read};
use std::str::{self, FromStr};

pub enum ParseStatus {
	Waiting,
	Done,
	Unexpected(String),
}

/////////////////////////////////////////////////////////////////
////////////////////// CLIENT PARSING CODE //////////////////////
/////////////////////////////////////////////////////////////////

pub trait ClientParse<S, M>
where
	S: Read,
{
	fn parse(&mut self, key: &ClientAddr, stream: &mut S) -> (Vec<M>, ParseStatus);
}

pub struct ClientParser {
	buffers: HashMap<ClientAddr, Vec<u8>>,
}

impl ClientParser {
	pub fn new() -> ClientParser {
		ClientParser {
			buffers: HashMap::new(),
		}
	}
}

impl<S, M> ClientParse<S, M> for ClientParser
where
	S: Read,
	M: FromStr,
{
	fn parse(&mut self, key: &ClientAddr, stream: &mut S) -> (Vec<M>, ParseStatus) {
		let mut buf = vec![];
		let mut status = match stream.read_to_end(&mut buf) {
			Ok(_) => ParseStatus::Done,
			Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => ParseStatus::Waiting,
			Err(e) => panic!("some io error: {}", e),
		};

		let newbuf = match self.buffers.remove(key) {
			Some(mut b) => {
				b.append(&mut buf);
				b
			}
			None => buf,
		};

		match str::from_utf8(&newbuf) {
			Ok(s) => {
				if s.is_ascii() {
					let mut msgs: Vec<M> = vec![];
					let mut tokens: Vec<&str> = s.split('\n').collect();

					if tokens.len() > 0 {
						let last = tokens.pop().unwrap();
						if !last.is_empty() {
							let mut buf_to_store: Vec<u8> = vec![0; last.len()];
							buf_to_store.clone_from_slice(last.as_bytes());

							self.buffers.insert(key.clone(), buf_to_store);
						}

						for tok in tokens {
							match tok.parse() {
								Ok(msg) => msgs.push(msg),
								Err(_) => {
									status = ParseStatus::Unexpected(format!("unable to parse token: {}", tok));
									break;
								}
							}
						}
					}

					(msgs, status)
				} else {
					(vec![], ParseStatus::Unexpected("not ascii".to_string()))
				}
			}
			Err(_) => (vec![], ParseStatus::Unexpected("not utf8".to_string())),
		}
	}
}

///////////////////////////////////////////////////////////////
////////////////////// NODE PARSING CODE //////////////////////
///////////////////////////////////////////////////////////////

pub trait NodeParse<S, A, E>
where
	S: Read,
	A: Serialize,
	E: Serialize,
{
	fn parse(&mut self, key: &A, stream: &mut S) -> (Vec<NodeMessage<A, E>>, ParseStatus);
}

pub struct NodeParser<A> {
	buffers: HashMap<A, Vec<u8>>,
}

impl<A> NodeParser<A>
where
	A: Hash + Eq,
{
	pub fn new() -> Self {
		Self {
			buffers: HashMap::new(),
		}
	}
}

impl<S, A, E> NodeParse<S, A, E> for NodeParser<A>
where
	S: Read,
	A: Hash + Eq + Clone + Serialize,
	E: Serialize,
{
	fn parse(&mut self, key: &A, stream: &mut S) -> (Vec<NodeMessage<A, E>>, ParseStatus) {
		let mut buf = vec![];
		let status = match stream.read_to_end(&mut buf) {
			Ok(_) => ParseStatus::Done,
			Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => ParseStatus::Waiting,
			Err(e) => panic!("some io error: {}", e),
		};

		let newbuf = match self.buffers.remove(key) {
			Some(mut b) => {
				b.append(&mut buf);
				b
			}
			None => buf,
		};
		println!("current buf len: {}", newbuf.len());

		let mut msgs: Vec<NodeMessage<A, E>> = vec![];
		let mut curr_ind = 0;
		loop {
			if newbuf.len() < 2 + curr_ind {
				let mut buf_to_store: Vec<u8> = vec![0; newbuf.len() - curr_ind];
				buf_to_store.clone_from_slice(&newbuf[curr_ind..]);
				self.buffers.insert(key.clone(), buf_to_store);
				return (msgs, status);
			}
			match str::from_utf8(&newbuf[curr_ind..curr_ind + 2]) {
				Ok(s) if s == "aq" => match AppendEntries::<A, E>::from_bytes(&newbuf[curr_ind + 2..]) {
					Ok(tup) => {
						let num_read = tup.0;
						let msg = tup.1;

						msgs.push(NodeMessage::AppendReq(*msg));
						curr_ind += 2 + num_read;
					}
					Err(SerialStatus::Incomplete) => {
						let mut buf_to_store: Vec<u8> = vec![0; newbuf.len() - curr_ind];
						buf_to_store.clone_from_slice(&newbuf[curr_ind..]);
						self.buffers.insert(key.clone(), buf_to_store);
						return (msgs, status);
					}
					Err(SerialStatus::Error) => {
						return (
							msgs,
							ParseStatus::Unexpected("serialize failed".to_string()),
						);
					}
				},
				Ok(s) if s == "as" => match AppendEntriesResponse::from_bytes(&newbuf[curr_ind + 2..]) {
					Ok(tup) => {
						let num_read = tup.0;
						let msg = tup.1;

						msgs.push(NodeMessage::AppendRes(*msg));
						curr_ind += 2 + num_read;
					}
					Err(SerialStatus::Incomplete) => {
						let mut buf_to_store: Vec<u8> = vec![0; newbuf.len() - curr_ind];
						buf_to_store.clone_from_slice(&newbuf[curr_ind..]);
						self.buffers.insert(key.clone(), buf_to_store);
						return (msgs, status);
					}
					Err(SerialStatus::Error) => {
						return (
							msgs,
							ParseStatus::Unexpected("serialize failed".to_string()),
						);
					}
				},
				Ok(s) if s == "vq" => match RequestVote::from_bytes(&newbuf[curr_ind + 2..]) {
					Ok(tup) => {
						let num_read = tup.0;
						let msg = tup.1;

						msgs.push(NodeMessage::VoteReq(*msg));
						curr_ind += 2 + num_read;
					}
					Err(SerialStatus::Incomplete) => {
						let mut buf_to_store: Vec<u8> = vec![0; newbuf.len() - curr_ind];
						buf_to_store.clone_from_slice(&newbuf[curr_ind..]);
						self.buffers.insert(key.clone(), buf_to_store);
						return (msgs, status);
					}
					Err(SerialStatus::Error) => {
						return (
							msgs,
							ParseStatus::Unexpected("serialize failed".to_string()),
						);
					}
				},
				Ok(s) if s == "vs" => match RequestVoteResponse::from_bytes(&newbuf[curr_ind + 2..]) {
					Ok(tup) => {
						let num_read = tup.0;
						let msg = tup.1;

						msgs.push(NodeMessage::VoteRes(*msg));
						curr_ind += 2 + num_read;
					}
					Err(SerialStatus::Incomplete) => {
						let mut buf_to_store: Vec<u8> = vec![0; newbuf.len() - curr_ind];
						buf_to_store.clone_from_slice(&newbuf[curr_ind..]);
						self.buffers.insert(key.clone(), buf_to_store);
						return (msgs, status);
					}
					Err(SerialStatus::Error) => {
						return (
							msgs,
							ParseStatus::Unexpected("serialize failed".to_string()),
						);
					}
				},
				_ => return (msgs, ParseStatus::Unexpected("msg incorrect".to_string())),
			}
		}
	}
}
