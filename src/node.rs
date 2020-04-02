use crate::message::{
	AppendEntries, AppendEntriesResponse, Message, RequestVote, RequestVoteResponse,
};
use crate::persistent::PersistentData;
use crate::types::{LogIndex, Term};

use std::cmp::min;
use std::fmt::{self, Debug};
use std::time::{Duration, Instant};

struct VolatileData {
	commit_index: LogIndex,
	last_applied: LogIndex,
}

struct LeaderData {
	next_index: Vec<LogIndex>,
	match_index: Vec<LogIndex>,
}

struct CandidateData {
	votes: usize,
}

enum NodeType {
	Leader(LeaderData),
	Candidate(CandidateData),
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

pub struct Node<'a, A, E> {
	curr_type: NodeType,
	my_id: A,
	other_addrs: Vec<A>,

	election_timeout: Duration,
	heartbeat_timeout: Duration,
	next_deadline: Instant,

	last_known_leader: Option<A>,

	hard_state: PersistentData<'a, A, E>,
	soft_state: VolatileData,
}

impl<'a, A, E> Debug for Node<'a, A, E>
where
	A: Debug,
	E: Debug,
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

impl LeaderData {
	fn new(num_others: usize, last_log_index: LogIndex) -> Self {
		LeaderData {
			next_index: vec![last_log_index + 1; num_others],
			match_index: vec![0; num_others],
		}
	}
}

// Assumes that {receive, propose, tick} will be called with an `at` that monotonically increases
impl<'a, A, E> Node<'a, A, E>
where
	A: Eq + Copy + Clone + Debug,
	E: Debug,
{
	pub fn new(
		my_id: A,
		other_addrs: Vec<A>,
		created_on: Instant,
		election_timeout: Duration,
		heartbeat_timeout: Duration,
		curr_state: PersistentData<'a, A, E>,
	) -> Self {
		Self {
			curr_type: NodeType::Follower,
			my_id,
			other_addrs,

			election_timeout,
			heartbeat_timeout,
			next_deadline: created_on + election_timeout,

			last_known_leader: None,

			hard_state: curr_state,
			soft_state: VolatileData::new(),
		}
	}

	pub fn is_other_node(&self, addr: &A) -> bool {
		self.other_addrs.contains(addr)
	}

	pub fn receive(&mut self, msg: &Message<A, E>, at: Instant) -> Vec<(A, Message<A, E>)> {
		let to_ret = self._receive(msg, at);
		// we just want to make sure the persistent data gets flushed before any messages get sent
		self.hard_state.flush();
		to_ret
	}

	fn _receive(&mut self, msg: &Message<A, E>, at: Instant) -> Vec<(A, Message<A, E>)> {
		println!("received at {:?} for {:?} -- message: {:?}", at, self, msg);

		if msg.get_term() > self.hard_state.curr_term {
			self.hard_state.curr_term = msg.get_term();
			self.hard_state.voted_for = None;
			self.curr_type = NodeType::Follower;
		}

		match &mut self.curr_type {
			NodeType::Follower => match msg {
				Message::AppendReq(req) => {
					if req.term >= self.hard_state.curr_term {
						// should be current leader
						self.next_deadline = at + self.election_timeout;
					}
					vec![self.handle_append_entries(req)]
				}
				Message::VoteReq(req) => vec![self.handle_request_vote(req)],
				_ => vec![],
			},
			NodeType::Candidate(cand) => {
				match msg {
					Message::AppendReq(req) => {
						self.curr_type = NodeType::Follower;
						vec![self.handle_append_entries(req)]
					}
					// we know this is going to reject, but whatever
					Message::VoteReq(req) => vec![self.handle_request_vote(req)],
					Message::VoteRes(res) => {
						// TODO - we may want to use set of nodes to avoid replay
						// (that's not in our current failure model afaik)
						if res.vote_granted {
							cand.votes += 1;

							if cand.votes > (self.other_addrs.len() + 1) / 2 {
								return self.to_leader(at);
							}
						}
						vec![]
					}
					Message::AppendRes(_) => vec![],
				}
			}
			NodeType::Leader(_) => match msg {
				_ => {
					// TODO - implement leader logic
					vec![]
				}
			},
		}
	}

	pub fn propose(&mut self, entry: E, at: Instant) -> Result<Vec<(A, Message<A, E>)>, Option<A>> {
		match self.curr_type {
			NodeType::Candidate(_) => Err(self.last_known_leader.clone()),
			NodeType::Follower => Err(self.last_known_leader.clone()),
			NodeType::Leader(_) => {
				// send append entries, or store it to be sent on heartbeat timeout
				unimplemented!()
			}
		}
	}

	pub fn tick(&mut self, at: Instant) -> Vec<(A, Message<A, E>)> {
		println!("ticking at {:?} -- {:?}", at, self);
		match at.checked_duration_since(self.next_deadline) {
			Some(_) => {
				match self.curr_type {
					NodeType::Follower => {
						// convert to candidate & start election
						self.start_election(at)
					}
					NodeType::Candidate(_) => {
						// start new election
						self.start_election(at)
					}
					NodeType::Leader(_) => {
						// send append entries to all nodes
						self.next_deadline = at + self.heartbeat_timeout;

						// TODO adapt message with entry log logic later
						self
							.other_addrs
							.iter()
							.map(|addr| {
								(
									addr.clone(),
									Message::AppendReq(AppendEntries {
										term: self.hard_state.curr_term,
										leader_id: self.my_id,
										leader_commit: self.soft_state.commit_index,
										prev_log_index: 0,
										prev_log_term: 0,
										entries: vec![],
									}),
								)
							})
							.collect()
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

	pub fn get_id(&self) -> A {
		self.my_id
	}

	pub fn get_status(&self) -> NodeStatus<A> {
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

	fn handle_append_entries(&mut self, req: &AppendEntries<A, E>) -> (A, Message<A, E>) {
		if req.term < self.hard_state.curr_term
			|| !self
				.hard_state
				.has_entry_with(req.prev_log_index, req.prev_log_term)
		{
			(
				req.leader_id,
				Message::AppendRes(AppendEntriesResponse {
					term: self.hard_state.curr_term,
					success: false,
				}),
			)
		} else {
			// TODO - conflicts should be deleted
			// TODO - append any new entries

			if req.leader_commit > self.soft_state.commit_index {
				self.soft_state.commit_index = min(req.leader_commit, self.hard_state.last_entry());
			}

			(
				req.leader_id,
				Message::AppendRes(AppendEntriesResponse {
					term: self.hard_state.curr_term,
					success: true,
				}),
			)
		}
	}

	fn handle_request_vote(&mut self, req: &RequestVote<A>) -> (A, Message<A, E>) {
		let mut vote_granted = false;

		if req.term >= self.hard_state.curr_term
			&& (self.hard_state.voted_for.is_none()
				|| self.hard_state.voted_for == Some(req.candidate_id))
			&& self
				.hard_state
				.is_up2date(req.last_log_index, req.last_log_term)
		{
			self.hard_state.voted_for = Some(req.candidate_id);
			vote_granted = true;
		}

		(
			req.candidate_id,
			Message::VoteRes(RequestVoteResponse {
				term: self.hard_state.curr_term,
				vote_granted,
			}),
		)
	}

	fn to_leader(&mut self, at: Instant) -> Vec<(A, Message<A, E>)> {
		self.next_deadline = at + self.heartbeat_timeout;

		self.curr_type = NodeType::Leader(LeaderData::new(
			self.other_addrs.len(),
			self.hard_state.last_entry(),
		));

		// TODO adapt message with correct prev_log_*
		self
			.other_addrs
			.iter()
			.map(|addr| {
				(
					addr.clone(),
					Message::AppendReq(AppendEntries {
						term: self.hard_state.curr_term,
						leader_id: self.my_id,
						leader_commit: self.soft_state.commit_index,
						prev_log_index: 0,
						prev_log_term: 0,
						entries: vec![],
					}),
				)
			})
			.collect()
	}

	fn start_election(&mut self, at: Instant) -> Vec<(A, Message<A, E>)> {
		self.curr_type = NodeType::Candidate(CandidateData { votes: 1 });

		self.hard_state.curr_term += 1;
		self.hard_state.voted_for = Some(self.my_id);
		self.next_deadline = at + self.election_timeout;

		self
			.other_addrs
			.iter()
			.map(|addr| {
				(
					addr.clone(),
					Message::VoteReq(RequestVote {
						term: self.hard_state.curr_term,
						candidate_id: self.my_id,
						last_log_index: 0,
						last_log_term: 0,
					}),
				)
			})
			.collect()
	}
}
