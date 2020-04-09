use rafted::message::{ClientRequest, Message, NodeMessage};
use rafted::statemachine::StateMachine;
use rafted::{LogIndex, Node, NodeStatus, PersistentData, Storage, Term};

use std::cell::RefCell;
use std::cmp::{min, Ord, Ordering, PartialOrd};
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::convert::TryFrom;
use std::fmt::Debug;
use std::time::{Duration, Instant};

use rand_core::{RngCore, SeedableRng};
// use chacha for reproducibility
use rand_chacha::ChaChaRng;

// mostly so we guarantee client messages come after all other events
const CLIENT_OFFS: Duration = Duration::from_nanos(1000);

pub type NodeId = u64;
pub type ClientAddr = u64;
pub type Entry = u64;
pub type ClientRes = u64;

const DEFAULT_SEED: [u8; 32] = [
	20, 21, 22, 23, 1, 2, 3, 4, 0, 10, 20, 30, 69, 79, 89, 99, 14, 24, 34, 44, 88, 98, 108, 118, 45,
	56, 67, 78, 90, 09, 98, 87,
];

// our simulation is more for handling inter-node wonkiness
// so we're only going to care about manipulating / dropping
// inter-node messages at this point
pub struct SimMessage {
	at: Instant,
	dst: NodeId,
	msg: NodeMessage<NodeId, Entry>,
}

pub enum Event {
	Message(SimMessage),
	Tick(NodeId, Instant),
}

// ordered based on delivery time
// we're reversing the ordering so binary heap becomes min-heap
impl Ord for SimMessage {
	fn cmp(&self, other: &Self) -> Ordering {
		match self.at.cmp(&other.at) {
			Ordering::Equal => Ordering::Equal,
			Ordering::Less => Ordering::Greater,
			Ordering::Greater => Ordering::Less,
		}
	}
}

impl PartialOrd for SimMessage {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

impl PartialEq for SimMessage {
	fn eq(&self, other: &Self) -> bool {
		self.at == other.at
	}
}

impl Eq for SimMessage {}

#[derive(Debug, Clone)]
struct StorageEntry {
	pub curr_term: Term,
	pub voted_for: Option<NodeId>,
	pub first_index: LogIndex,
	pub first_term: Term,
	pub entries: Vec<(Term, Entry)>,
}

struct SimulationStorage {
	storage_map: HashMap<NodeId, RefCell<StorageEntry>>,
}

#[derive(Debug, Clone)]
struct StorageHandle {
	entry: RefCell<StorageEntry>,
}

impl StorageEntry {
	pub fn new() -> Self {
		Self {
			curr_term: 0,
			voted_for: None,
			first_index: 0,
			first_term: 0,
			entries: vec![],
		}
	}
}

impl SimulationStorage {
	pub fn new(ids: &[NodeId]) -> Self {
		Self {
			storage_map: ids
				.iter()
				.map(|n| (n.clone(), RefCell::new(StorageEntry::new())))
				.collect(),
		}
	}

	pub fn get_handle_for(&self, node: &NodeId) -> StorageHandle {
		StorageHandle {
			entry: self.storage_map.get(node).unwrap().clone(),
		}
	}
}

impl<'a> Storage<'a, NodeId, Entry> for StorageHandle {
	fn get_data(self) -> PersistentData<'a, NodeId, Entry> {
		let storage = self.entry.borrow();
		let curr_term = storage.curr_term;
		let voted_for = storage.voted_for;
		let first_index = storage.first_index;
		let first_term = storage.first_term;
		let entries = storage.entries.clone();

		PersistentData::from_existing(
			Box::new(self.clone()),
			curr_term,
			voted_for,
			first_index,
			first_term,
			entries,
		)
	}

	fn update_state(
		&mut self,
		term: Term,
		voted_for: &Option<NodeId>,
		first_index: LogIndex,
		first_term: Term,
	) {
		let mut storage = self.entry.borrow_mut();
		storage.curr_term = term;
		storage.voted_for = voted_for.clone();
		storage.first_index = first_index;
		storage.first_term = first_term;
	}

	fn update_entries(&mut self, start: LogIndex, entries: &[(Term, Entry)]) {
		if start == 0 {
			assert!(entries.is_empty());
		}

		let mut storage = self.entry.borrow_mut();
		storage.entries.truncate((start - 1) as usize);
		storage.entries.extend_from_slice(entries);
	}
}

pub struct SimStateMachine {
	my_num: u64,
}

impl StateMachine<(), u64, u64> for SimStateMachine {
	fn new() -> Self {
		Self { my_num: 1 }
	}

	fn read(&self, _req: &()) -> u64 {
		self.my_num
	}

	fn apply(&mut self, ent: &u64) -> u64 {
		if self.my_num > *ent {
			0
		} else {
			self.my_num = *ent;
			self.my_num
		}
	}
}

pub struct Simulation<'a> {
	rng: ChaChaRng,
	prob_drop_msg: u32,
	dropped_conns: HashSet<(NodeId, NodeId)>,

	election_timeout: (Duration, Duration),
	heartbeat_timeout: Duration,
	msg_delay: Duration,

	last_event: Instant,
	storage: SimulationStorage,
	nodes: Vec<Option<Node<'a, NodeId, Entry, ClientAddr, (), ClientRes, SimStateMachine>>>,
	messages: BinaryHeap<SimMessage>,
	next_tick: Vec<Instant>,
}

pub struct SimulationOpts {
	pub num_nodes: u64,
	pub election_timeout: Option<(Duration, Duration)>,
	pub heartbeat_timeout: Option<Duration>,
	pub msg_delay: Option<Duration>,
	pub seed: Option<[u8; 32]>,
}

impl<'a> TryFrom<SimulationOpts> for Simulation<'a> {
	type Error = &'static str;

	fn try_from(opts: SimulationOpts) -> Result<Self, Self::Error> {
		let (my_e, my_h) = match (opts.election_timeout, opts.heartbeat_timeout) {
			(Some(tup), Some(h)) => {
				let (e_low, e_high) = tup;
				if e_high > e_low {
					return Err("timeout range should go from low to high");
				}
				(tup, h)
			}
			(Some(tup), None) => {
				let (e_low, e_high) = tup;
				if e_high > e_low {
					return Err("timeout range should go from low to high");
				}

				((e_low, e_high), e_low.div_f32(2.2))
			}
			(None, Some(h)) => ((h.mul_f32(2.1), h.mul_f32(2.3)), h),
			(None, None) => (
				(Duration::from_millis(5000), Duration::from_millis(5200)),
				Duration::from_millis(2000),
			),
		};

		if my_h > my_e.0 {
			return Err("heartbeat timeout must be smaller than election timeout");
		} else if my_h * 2 > my_e.0 {
			println!("warning: decrease heartbeat_timeout for better results");
		}

		let my_delay = match opts.msg_delay {
			Some(d) => d,
			None => my_h.div_f32(1000.0),
		};
		if my_delay * 100 > my_h {
			return Err("delay should be significantly smaller than heartbeat timeout");
		}

		let mut rng = ChaChaRng::from_seed(opts.seed.unwrap_or(DEFAULT_SEED));
		let now = Instant::now();

		let node_ids: Vec<NodeId> = (0..opts.num_nodes).collect();
		let storage = SimulationStorage::new(&node_ids);

		// we're just gonna assume micros granularity
		let e_range = (my_e.1 - my_e.0).as_micros() as u64;

		let mut nodes = vec![];
		for i in node_ids {
			let mut addrs: Vec<NodeId> = (0..opts.num_nodes).collect();
			addrs.remove(i as usize);

			let timeout_off = Duration::from_micros(rng.next_u64() % e_range);
			let e_timeout = my_e.0 + timeout_off;
			nodes.push(Some(Node::new(
				i,
				addrs,
				now + Duration::from_millis(i),
				e_timeout,
				my_h,
				storage.get_handle_for(&i).get_data(),
			)));
		}

		let next_tick = nodes
			.iter()
			.map(|n| n.as_ref().unwrap().get_next_deadline())
			.collect();

		Ok(Simulation {
			rng,
			prob_drop_msg: 0,
			dropped_conns: HashSet::new(),

			election_timeout: my_e,
			heartbeat_timeout: my_h,
			msg_delay: my_delay,

			last_event: now,
			storage,
			nodes,
			messages: BinaryHeap::new(),
			next_tick,
		})
	}
}

impl<'a> Simulation<'a> {
	pub fn set_drop_rate(&mut self, drop_rate: u32) {
		assert!(drop_rate <= 100);
		self.prob_drop_msg = drop_rate;
	}

	/// prevents any **new** messages from being sent from src to dst.
	pub fn drop_conn(&mut self, src: NodeId, dst: NodeId) {
		self.dropped_conns.insert((src, dst));
	}

	/// allows **new** messages to be sent from src to dst.
	pub fn enable_conn(&mut self, src: NodeId, dst: NodeId) {
		self.dropped_conns.remove(&(src, dst));
	}

	pub fn stop_node(&mut self, node_id: NodeId) {
		assert!(self.nodes[node_id as usize].is_some());
		self.nodes[node_id as usize] = None;
	}

	pub fn next_event_time(&self) -> Instant {
		let (_, t_next_tick) = self.get_next_tick_el();

		match self.messages.peek() {
			Some(msg) => min(msg.at, t_next_tick),
			None => t_next_tick,
		}
	}

	pub fn start_node(&mut self, node_id: NodeId, at: Instant) {
		assert!(self.nodes[node_id as usize].is_none());
		self.nodes[node_id as usize] = {
			let mut addrs: Vec<NodeId> = (0..(self.nodes.len() as u64)).collect();
			addrs.remove(node_id as usize);

			let my_e = self.election_timeout;
			let e_range = (my_e.1 - my_e.0).as_micros() as u64;
			let timeout_off = Duration::from_micros(self.rng.next_u64() % e_range);
			let e_timeout = my_e.0 + timeout_off;

			Some(Node::new(
				node_id,
				addrs,
				at,
				e_timeout,
				self.heartbeat_timeout,
				self.storage.get_handle_for(&node_id).get_data(),
			))
		}
	}

	pub fn run_for(
		&mut self,
		num_events: u64,
		validate_fn: &mut dyn FnMut(Vec<Option<NodeStatus<NodeId>>>),
	) {
		for _ in 0..num_events {
			println!("new iteration -- # messages: {}", self.messages.len());
			let (idx, t_next_tick) = self.get_next_tick_el();

			let event = {
				if !self.messages.is_empty() && self.messages.peek().unwrap().at < t_next_tick {
					let to_ret = self.messages.pop().unwrap();
					if self.rng.next_u32() % 100 < self.prob_drop_msg {
						continue;
					}
					Event::Message(to_ret)
				} else {
					Event::Tick(idx as NodeId, t_next_tick)
				}
			};

			let (src, msgs_to_send, at) = {
				match event {
					Event::Tick(node_id, at) => {
						self.last_event = at;
						if let Some(node) = &mut self.nodes[node_id as usize] {
							(node_id, node.tick(at), at)
						} else {
							(node_id, vec![], at)
						}
					}
					Event::Message(packet) => {
						let at = packet.at;
						self.last_event = at;
						let node_id = packet.dst;
						if let Some(node) = &mut self.nodes[node_id as usize] {
							(node_id, node.receive(&packet.msg, at), at)
						} else {
							(node_id, vec![], at)
						}
					}
				}
			};

			// update tick
			let src_idx = src as usize;
			self.next_tick[src_idx] = {
				if let Some(node) = &self.nodes[src_idx] {
					node.get_next_deadline()
				} else {
					at
				}
			};

			// register all messages
			for to_send in msgs_to_send {
				self.register_msg(src, to_send, at)
			}

			let statuses = self
				.nodes
				.iter()
				.map(|n| match n {
					Some(node) => Some(node.get_status()),
					None => None,
				})
				.collect();
			validate_fn(statuses);
		}
	}

	pub fn client_msg(&mut self, to: NodeId, req: ClientRequest<ClientAddr, (), Entry>) {
		// TODO - once client messages work
		let at = self.last_event + CLIENT_OFFS;
		if let Some(node) = &mut self.nodes[to as usize] {
			let msgs_to_send = node.receive_client(req, at);
			println!("after client, send: {}", msgs_to_send.len());
			for to_send in msgs_to_send {
				self.register_msg(to, to_send, at);
			}
		}
	}

	fn register_msg(
		&mut self,
		src: NodeId,
		to_send: Message<NodeId, Entry, ClientAddr, u64>,
		at: Instant,
	) {
		match to_send {
			Message::Node(dst, msg) => {
				if !self.dropped_conns.contains(&(src, dst)) {
					self.messages.push(SimMessage {
						at: at + self.msg_delay,
						dst: dst,
						msg: msg,
					});
				}
			}
			Message::Client(_, _) => {
				// just don't do anything for right now
				// TODO - allow clients to receive messages in simulation
				()
			}
		}
	}

	fn get_next_tick_el(&self) -> (usize, Instant) {
		self
			.next_tick
			.iter()
			.enumerate()
			.filter(|(i, _)| self.nodes[*i].is_some())
			.min_by(|(_, a), (_, b)| a.cmp(b))
			.map(|(i, a)| (i, a.clone()))
			.unwrap()
	}
}
