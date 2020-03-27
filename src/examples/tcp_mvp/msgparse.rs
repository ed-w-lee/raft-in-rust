/**
 * Manages the translation from streams that implement Read to messages that can be used
 * by the nodes for state management.
 */
use crate::connections::{ClientAddr, NodeAddr};
use rafted::Message;

use std::collections::HashMap;
use std::io::{self, Read};
use std::marker::PhantomData;
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

pub struct ClientParser<S, M>
where
	S: Read,
{
	buffers: HashMap<ClientAddr, Vec<u8>>,
	_stream: PhantomData<S>,
	_message: PhantomData<M>,
}

impl<S, M> ClientParser<S, M>
where
	S: Read,
{
	pub fn new() -> ClientParser<S, M> {
		ClientParser {
			buffers: HashMap::new(),
			_stream: PhantomData,
			_message: PhantomData,
		}
	}
}

impl<S, M> ClientParse<S, M> for ClientParser<S, M>
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

pub trait NodeParse<S>
where
	S: Read,
{
	fn parse(&mut self, key: &NodeAddr, stream: &mut S) -> (Vec<Message>, ParseStatus);
}

pub struct NodeParser<S: Read> {
	buffers: HashMap<NodeAddr, Vec<u8>>,
	_marker: PhantomData<S>,
}

impl<S> NodeParser<S>
where
	S: Read,
{
	pub fn new() -> NodeParser<S> {
		NodeParser {
			buffers: HashMap::new(),
			_marker: PhantomData,
		}
	}
}

// FIXME - Actually add in parsing logic
impl<S> NodeParse<S> for NodeParser<S>
where
	S: Read,
{
	fn parse(&mut self, key: &NodeAddr, stream: &mut S) -> (Vec<Message>, ParseStatus) {
		unimplemented!();
	}
}
