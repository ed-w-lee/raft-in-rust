use crate::types::{LogIndex, Term};

#[derive(Debug)]
pub struct PersistentData<A, E> {
	pub current_term: Term,
	pub voted_for: Option<A>,
	pub log: Vec<(Term, E)>,
}

pub struct VolatileData {
	pub commit_index: LogIndex,
	pub last_applied: LogIndex,
}

pub struct VolatileLeaderData {
	pub next_index: Vec<LogIndex>,
	pub match_index: Vec<LogIndex>,
}

pub enum NodeType {
	Leader,
	Candidate,
	Follower,
}

pub struct Node<A, E> {
	curr_type: NodeType,
	my_id: A,
	other_addrs: Vec<A>,

	hard_state: PersistentData<A, E>,
	soft_state: VolatileData,
	leader_state: Option<VolatileLeaderData>,
}

impl<A, E> PersistentData<A, E> {
	fn new() -> Self {
		Self {
			current_term: 0,
			voted_for: None,
			log: vec![],
		}
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

impl VolatileLeaderData {
	fn new(num_others: usize, last_log_index: LogIndex) -> Self {
		Self {
			next_index: vec![last_log_index + 1; num_others],
			match_index: vec![0; num_others],
		}
	}
}

impl<A, E> Node<A, E>
where
	A: PartialEq,
{
	pub fn new(my_id: A, other_addrs: Vec<A>, hard_state: Option<PersistentData<A, E>>) -> Self {
		Self {
			curr_type: NodeType::Follower,
			my_id,
			other_addrs,

			hard_state: match hard_state {
				Some(h_s) => h_s,
				None => PersistentData::new(),
			},
			soft_state: VolatileData::new(),
			leader_state: None,
		}
	}

	pub fn is_other_node(&self, addr: &A) -> bool {
		self.other_addrs.contains(addr)
	}

	pub fn get_hard_state(&self) -> &PersistentData<A, E> {
		&self.hard_state
	}
}
