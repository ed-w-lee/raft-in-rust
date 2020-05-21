use crate::message::{
	AppendEntries, AppendEntriesResponse, ClientRequest, ClientResponse, Message, NodeMessage,
	RequestVote, RequestVoteResponse,
};
use crate::persistent::PersistentData;
use crate::statemachine::StateMachine;
use crate::types::{LogIndex, ReaderIndex, Term};

use std::cmp::min;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt::{self, Debug};
use std::hash::Hash;
use std::iter::FromIterator;
use std::marker::PhantomData;
use std::time::{Duration, Instant};

struct VolatileData {
	commit_index: LogIndex,
	last_applied: LogIndex,
}

struct LeaderData<CA, REQ> {
	next_index: Vec<LogIndex>,
	match_index: Vec<LogIndex>,

	reader_next: ReaderIndex,
	reader_index: Vec<ReaderIndex>,
	reader_queue: VecDeque<(ReaderIndex, CA, REQ)>,
}

struct CandidateData<NA> {
	votes: HashSet<NA>,
}

enum NodeType<NA, CA, REQ> {
	Leader(LeaderData<CA, REQ>),
	Candidate(CandidateData<NA>),
	Follower,
}

#[derive(PartialEq, Copy, Clone)]
pub enum NodeRole {
	Leader,
	Candidate,
	Follower,
}

#[derive(Copy, Clone)]
pub struct NodeStatus<A> {
	pub id: A,
	pub role: NodeRole,
	pub term: Term,
}

/// Represents a node in a Raft system.
///
/// Type parameters:
/// NA - node address
/// ENT - log entry
/// CA - client address
/// REQ - client request
/// RES - client response
/// SM - state machine
pub struct Node<'a, NA, ENT, CA, REQ, RES, SM> {
	curr_type: NodeType<NA, CA, REQ>,
	my_id: NA,
	other_addrs: Vec<NA>,

	election_timeout: Duration,
	heartbeat_timeout: Duration,
	next_deadline: Instant,

	last_known_leader: Option<NA>,

	hard_state: PersistentData<'a, NA, ENT>,
	soft_state: VolatileData,
	statemachine: SM,

	client_addrs: HashMap<LogIndex, CA>,
	_client_req: PhantomData<REQ>,
	_client_res: PhantomData<RES>,
}

impl<'a, NA, ENT, CA, REQ, RES, SM> Debug for Node<'a, NA, ENT, CA, REQ, RES, SM>
where
	NA: Debug,
{
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("Node")
			.field("id", &self.my_id)
			.field(
				"type",
				&(match self.curr_type {
					NodeType::Leader(_) => "Leader",
					NodeType::Candidate(_) => "Candidate",
					NodeType::Follower => "Follower",
				})
				.to_string(),
			)
			.finish()
	}
}

impl VolatileData {
	fn new() -> Self {
		Self {
			commit_index: 0,
			last_applied: 0,
		}
	}
}

impl<CA, REQ> LeaderData<CA, REQ> {
	fn new(num_others: usize, last_log_index: LogIndex) -> Self {
		LeaderData {
			next_index: vec![last_log_index + 1; num_others],
			match_index: vec![0; num_others],

			reader_next: 0,
			reader_index: vec![0; num_others],
			reader_queue: VecDeque::new(),
		}
	}
}

// Assumes that {receive, propose, tick} will be called with an `at` that monotonically increases
impl<'a, NA, ENT, CA, REQ, RES, SM> Node<'a, NA, ENT, CA, REQ, RES, SM>
where
	NA: Eq + Copy + Clone + Debug + Hash,
	ENT: Clone + Debug,
	CA: Clone + Debug,
	REQ: Debug,
	SM: StateMachine<REQ, ENT, RES>,
{
	pub fn new(
		my_id: NA,
		other_addrs: Vec<NA>,
		created_on: Instant,
		election_timeout: Duration,
		heartbeat_timeout: Duration,
		curr_state: PersistentData<'a, NA, ENT>,
	) -> Self {
		Self {
			curr_type: NodeType::Follower,
			my_id,
			other_addrs,

			election_timeout,
			heartbeat_timeout,
			next_deadline: created_on + election_timeout,

			last_known_leader: curr_state.voted_for,

			hard_state: curr_state,
			soft_state: VolatileData::new(),
			statemachine: SM::new(),

			client_addrs: HashMap::new(),
			_client_req: PhantomData,
			_client_res: PhantomData,
		}
	}

	pub fn is_other_node(&self, addr: &NA) -> bool {
		self.other_addrs.contains(addr)
	}

	pub fn receive(
		&mut self,
		msg: &NodeMessage<NA, ENT>,
		at: Instant,
	) -> Vec<Message<NA, ENT, CA, RES>> {
		let mut to_ret = self._receive(msg, at);
		let mut updates = self._update_statemachine();
		let mut client_updates = self._respond_to_reads();
		// we just want to make sure the persistent data gets flushed before any messages get sent
		self.hard_state.flush();
		updates.append(&mut to_ret);
		updates.append(&mut client_updates);
		updates
	}

	fn _receive(
		&mut self,
		msg: &NodeMessage<NA, ENT>,
		at: Instant,
	) -> Vec<Message<NA, ENT, CA, RES>> {
		println!("received at {:?} for {:?} -- message: {:?}", at, self, msg);

		// TODO - maybe extract this into its own fn and add it to the wrapper
		let mut to_ret = vec![];
		if msg.get_term() > self.hard_state.curr_term {
			self.hard_state.curr_term = msg.get_term();
			self.hard_state.voted_for = None;
			if let NodeType::Leader(_) = &self.curr_type {
				let mut client_updates: Vec<Message<NA, ENT, CA, RES>> = self
					.client_addrs
					.values()
					.map(|addr| Message::Client(addr.clone(), ClientResponse::TryAgain))
					.collect();

				to_ret.append(&mut client_updates);
			}
			self.curr_type = NodeType::Follower;
		}

		match &mut self.curr_type {
			NodeType::Follower => match msg {
				NodeMessage::AppendReq(req) => {
					if req.term >= self.hard_state.curr_term {
						// should be current leader
						self.next_deadline = at + self.election_timeout;
					}
					to_ret.append(&mut vec![self._handle_append_entries(req)]);
					to_ret
				}
				NodeMessage::VoteReq(req) => {
					to_ret.append(&mut vec![self._handle_request_vote(req, at)]);
					to_ret
				}
				_ => to_ret,
			},
			NodeType::Candidate(cand) => {
				match msg {
					NodeMessage::AppendReq(req) => {
						self.curr_type = NodeType::Follower;
						vec![self._handle_append_entries(req)]
					}
					// we know this is going to reject, but whatever
					NodeMessage::VoteReq(req) => vec![self._handle_request_vote(req, at)],
					NodeMessage::VoteRes(res) => {
						if res.term == self.hard_state.curr_term && res.vote_granted {
							cand.votes.insert(res.from);

							if cand.votes.len() > (self.other_addrs.len() + 1) / 2 {
								return self._to_leader(at);
							}
						}
						vec![]
					}
					NodeMessage::AppendRes(_) => vec![],
				}
			}
			NodeType::Leader(lead) => match msg {
				NodeMessage::AppendRes(res) => {
					if res.term < self.hard_state.curr_term {
						// in case we found an old message
						return vec![];
					}
					let other_idx: usize = self
						.other_addrs
						.iter()
						.position(|&a| a == res.from)
						.unwrap();

					match res.success {
						Ok(match_idx) => {
							if lead.match_index[other_idx] < match_idx {
								// node is up-to-date until given idx
								lead.match_index[other_idx] = match_idx;
								lead.next_index[other_idx] = match_idx + 1;

								let mut find_commit = lead.match_index.clone();
								find_commit.sort();
								// 4 other_addrs requires 2
								// 5 other_addrs requires 3
								let next_commit = find_commit[(self.other_addrs.len() + 1) / 2];
								if self.hard_state.get_term(next_commit).unwrap() == self.hard_state.curr_term {
									// only update our commit index when we've written something as leader
									self.soft_state.commit_index = next_commit;
									if self.hard_state.curr_term == res.term {
										// make sure response is from our term so no confusions about what reader index refers to
										if lead.reader_index[other_idx] < res.reader_idx {
											lead.reader_index[other_idx] = res.reader_idx;
										}
									}
								}
							}
						}
						Err(next_idx) => {
							// node needs entries at least starting from given idx
							if lead.match_index[other_idx] < next_idx && lead.next_index[other_idx] > next_idx {
								lead.next_index[other_idx] = next_idx;
							}
						}
					}
					match self._send_entries_to_node(self.other_addrs[other_idx].clone(), false) {
						Some(msg) => vec![msg],
						None => vec![],
					}
				}
				_ => vec![],
			},
		}
	}

	pub fn receive_client(
		&mut self,
		req: ClientRequest<CA, REQ, ENT>,
		at: Instant,
	) -> Vec<Message<NA, ENT, CA, RES>> {
		let mut to_ret = self._receive_client(req, at);
		let mut updates = self._update_statemachine();

		self.hard_state.flush();

		updates.append(&mut to_ret);
		updates
	}

	fn _receive_client(
		&mut self,
		req: ClientRequest<CA, REQ, ENT>,
		at: Instant,
	) -> Vec<Message<NA, ENT, CA, RES>> {
		println!("received at {:?} for {:?} -- message: {:?}", at, self, req);
		match &mut self.curr_type {
			NodeType::Candidate(_) => match self.last_known_leader {
				Some(node) => vec![Message::Client(
					req.get_addr(),
					ClientResponse::Redirect(node),
				)],
				None => vec![Message::Client(req.get_addr(), ClientResponse::TryAgain)],
			},
			NodeType::Follower => match self.last_known_leader {
				Some(node) => vec![Message::Client(
					req.get_addr(),
					ClientResponse::Redirect(node),
				)],
				None => vec![Message::Client(req.get_addr(), ClientResponse::TryAgain)],
			},

			NodeType::Leader(leader_data) => {
				match req {
					ClientRequest::Read(client, read_req) => {
						if self
							.hard_state
							.get_term(self.soft_state.commit_index)
							.unwrap() < self.hard_state.curr_term
						{
							// If something has not yet been committed from leader's term, reject
							// TODO we can still register the read request, and only respond once we get a commit in
							vec![Message::Client(client, ClientResponse::TryAgain)]
						} else {
							// store request in queue, we need to wait until this request is ACK'd (needs to be same term)
							leader_data.reader_next += 1;
							let my_idx = leader_data.reader_next;
							leader_data
								.reader_queue
								.push_back((my_idx, client, read_req));

							vec![]
						}
					}
					ClientRequest::Apply(client, new_entry) => {
						// append to local log
						let log_idx = self
							.hard_state
							.append_entry((self.hard_state.curr_term, Some(new_entry)));

						// add client addr to map in case we should return something
						self.client_addrs.insert(log_idx, client);
						self._send_entries(false)
					}
				}
			}
		}
	}

	pub fn tick(&mut self, at: Instant) -> Vec<Message<NA, ENT, CA, RES>> {
		let mut to_ret = self._tick(at);
		let mut updates = self._update_statemachine();
		updates.append(&mut to_ret);
		updates
	}

	fn _tick(&mut self, at: Instant) -> Vec<Message<NA, ENT, CA, RES>> {
		println!("ticking at {:?} -- {:?}", at, self);
		match at.checked_duration_since(self.next_deadline) {
			Some(_) => {
				match self.curr_type {
					NodeType::Follower => {
						// convert to candidate & start election
						self._start_election(at)
					}
					NodeType::Candidate(_) => {
						// start new election
						self._start_election(at)
					}
					NodeType::Leader(_) => {
						// send append entries to all nodes
						self.next_deadline = at + self.heartbeat_timeout;

						self._send_entries(true)
					}
				}
			}
			None => {
				println!("called tick when not yet past deadline");
				vec![]
			}
		}
	}

	pub fn get_next_deadline(&self) -> Instant {
		self.next_deadline
	}

	pub fn get_id(&self) -> NA {
		self.my_id
	}

	pub fn get_status(&self) -> NodeStatus<NA> {
		NodeStatus {
			id: self.my_id,
			term: self.hard_state.curr_term,
			role: match self.curr_type {
				NodeType::Follower => NodeRole::Follower,
				NodeType::Candidate(_) => NodeRole::Candidate,
				NodeType::Leader(_) => NodeRole::Leader,
			},
		}
	}

	fn _send_entries(&mut self, empty: bool) -> Vec<Message<NA, ENT, CA, RES>> {
		match &self.curr_type {
			NodeType::Leader(_) => self
				.other_addrs
				.iter()
				.filter_map(|other| self._send_entries_to_node(other.clone(), empty))
				.collect(),
			_ => unreachable!(),
		}
	}

	fn _send_entries_to_node(&self, addr: NA, empty: bool) -> Option<Message<NA, ENT, CA, RES>> {
		match &self.curr_type {
			NodeType::Leader(leader_data) => {
				let other_idx: usize = self._get_idx_of(addr);
				let next_index = leader_data.next_index[other_idx];
				if !empty && self.hard_state.last_entry() < next_index {
					None
				} else {
					Some(Message::Node(
						addr.clone(),
						NodeMessage::AppendReq(AppendEntries {
							term: self.hard_state.curr_term,
							leader_id: self.my_id,
							leader_commit: self.soft_state.commit_index,
							prev_log_index: next_index - 1,
							prev_log_term: self.hard_state.get_term(next_index - 1).unwrap(),
							entries: {
								if self.hard_state.last_entry() >= next_index {
									Vec::from(self.hard_state.get_entries(next_index))
								} else {
									vec![]
								}
							},
							reader_idx: leader_data.reader_next,
						}),
					))
				}
			}
			_ => unreachable!(),
		}
	}

	fn _get_idx_of(&self, addr: NA) -> usize {
		self.other_addrs.iter().position(|&a| a == addr).unwrap()
	}

	fn _handle_append_entries(&mut self, req: &AppendEntries<NA, ENT>) -> Message<NA, ENT, CA, RES> {
		if req.term < self.hard_state.curr_term
			|| !self
				.hard_state
				.has_entry_with(req.prev_log_index, req.prev_log_term)
		{
			Message::Node(
				req.leader_id,
				NodeMessage::AppendRes(AppendEntriesResponse {
					term: self.hard_state.curr_term,
					from: self.my_id,
					success: Err(req.prev_log_index),
					reader_idx: req.reader_idx,
				}),
			)
		} else {
			// delete conflicts & append new entries
			for (idx, (term, _)) in req.entries.iter().enumerate() {
				let start = req.prev_log_index + (idx as LogIndex) + 1;
				if start > self.hard_state.last_entry() {
					self.hard_state.append_entries(&req.entries[idx..]);
					break;
				} else if self.hard_state.get_entry(start).0 != *term {
					self
						.hard_state
						.update_entries(start, &req.entries[idx..])
						.unwrap();
					break;
				}
			}

			if req.leader_commit > self.soft_state.commit_index {
				self.soft_state.commit_index = min(req.leader_commit, self.hard_state.last_entry());
			}

			Message::Node(
				req.leader_id,
				NodeMessage::AppendRes(AppendEntriesResponse {
					term: self.hard_state.curr_term,
					from: self.my_id,
					success: Ok(self.hard_state.last_entry()),
					reader_idx: req.reader_idx,
				}),
			)
		}
	}

	fn _handle_request_vote(
		&mut self,
		req: &RequestVote<NA>,
		at: Instant,
	) -> Message<NA, ENT, CA, RES> {
		let mut vote_granted = false;

		if req.term >= self.hard_state.curr_term
			&& (self.hard_state.voted_for.is_none()
				|| self.hard_state.voted_for == Some(req.candidate_id))
			&& self
				.hard_state
				.is_up2date(req.last_log_index, req.last_log_term)
		{
			self.hard_state.voted_for = Some(req.candidate_id);
			self.last_known_leader = Some(req.candidate_id);
			vote_granted = true;
			self.next_deadline = at + self.election_timeout;
		}

		Message::Node(
			req.candidate_id,
			NodeMessage::VoteRes(RequestVoteResponse {
				term: self.hard_state.curr_term,
				from: self.my_id,
				vote_granted,
			}),
		)
	}

	fn _to_leader(&mut self, at: Instant) -> Vec<Message<NA, ENT, CA, RES>> {
		self.next_deadline = at + self.heartbeat_timeout;

		self.curr_type = NodeType::Leader(LeaderData::new(
			self.other_addrs.len(),
			self.hard_state.last_entry(),
		));
		// we're elected. commit a no-op to check what's been committed
		self
			.hard_state
			.append_entry((self.hard_state.curr_term, None));

		self._send_entries(true)
	}

	fn _start_election(&mut self, at: Instant) -> Vec<Message<NA, ENT, CA, RES>> {
		self.curr_type = NodeType::Candidate(CandidateData {
			votes: HashSet::from_iter(vec![self.my_id].iter().cloned()),
		});

		self.hard_state.curr_term += 1;
		self.hard_state.voted_for = Some(self.my_id);
		self.last_known_leader = Some(self.my_id);
		self.next_deadline = at + self.election_timeout;

		let my_last_idx = self.hard_state.last_entry();
		let my_last_term = self.hard_state.get_term(my_last_idx).unwrap();
		self
			.other_addrs
			.iter()
			.map(|addr| {
				Message::Node(
					addr.clone(),
					NodeMessage::VoteReq(RequestVote {
						term: self.hard_state.curr_term,
						candidate_id: self.my_id,
						last_log_index: my_last_idx,
						last_log_term: my_last_term,
					}),
				)
			})
			.collect()
	}

	fn _respond_to_reads(&mut self) -> Vec<Message<NA, ENT, CA, RES>> {
		match &mut self.curr_type {
			NodeType::Leader(lead) => {
				let mut find_readable = lead.reader_index.clone();
				find_readable.sort();
				let next_readable = find_readable[(self.other_addrs.len() + 1) / 2];
				let mut to_ret = vec![];
				loop {
					match lead.reader_queue.front() {
						Some(tup) => {
							if tup.0 > next_readable {
								break;
							}
							to_ret.push(Message::Client(
								tup.1.clone(),
								ClientResponse::Response(self.statemachine.read(&tup.2)),
							));
						}
						None => break,
					}
					lead.reader_queue.pop_front();
				}

				to_ret
			}
			_ => vec![],
		}
	}

	fn _update_statemachine(&mut self) -> Vec<Message<NA, ENT, CA, RES>> {
		let mut to_ret = vec![];
		while self.soft_state.commit_index > self.soft_state.last_applied {
			self.soft_state.last_applied += 1;
			let idx = self.soft_state.last_applied;
			if let Some(entry) = &self.hard_state.get_entry(idx).1 {
				let res = self.statemachine.apply(entry);
				match self.curr_type {
					NodeType::Leader(_) => {
						if self.client_addrs.contains_key(&idx) {
							to_ret.push(Message::Client(
								self.client_addrs[&idx].clone(),
								ClientResponse::Response(res),
							));
							self.client_addrs.remove_entry(&idx);
						}
					}
					_ => {}
				}
			}
		}
		to_ret
	}
}
