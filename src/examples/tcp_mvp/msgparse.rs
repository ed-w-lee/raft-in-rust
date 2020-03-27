/**
 * Manages the translation from streams that implement Read to messages that can be used
 * by the nodes for state management.
 */
use crate::connections::{ClientAddr, NodeAddr};
use rafted::Message;

use std::collections::HashMap;
use std::io::Read;
use std::marker::PhantomData;

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
{
	fn parse(&mut self, key: &ClientAddr, stream: &mut S) -> (Vec<M>, ParseStatus) {
		unimplemented!()
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

impl<S> NodeParse<S> for NodeParser<S>
where
	S: Read,
{
	fn parse(&mut self, key: &NodeAddr, stream: &mut S) -> (Vec<Message>, ParseStatus) {
		unimplemented!();
	}
}
