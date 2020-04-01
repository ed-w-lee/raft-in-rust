use crate::types::{LogIndex, Term};

#[derive(Debug, PartialEq)]
pub struct AppendEntries<A, E> {
	pub term: Term,
	pub leader_id: A,
	pub leader_commit: LogIndex,
	pub prev_log_index: LogIndex,
	pub prev_log_term: Term,
	pub entries: Vec<E>,
}

#[derive(Debug, PartialEq)]
pub struct AppendEntriesResponse {
	pub term: Term,
	pub success: bool,
}

#[derive(Debug, PartialEq)]
pub struct RequestVote<A> {
	pub term: Term,
	pub candidate_id: A,
	pub last_log_index: LogIndex,
	pub last_log_term: Term,
}

#[derive(Debug, PartialEq)]
pub struct RequestVoteResponse {
	pub term: Term,
	pub vote_granted: bool,
}

#[derive(Debug, PartialEq)]
pub enum Message<A, E> {
	AppendReq(AppendEntries<A, E>),
	AppendRes(AppendEntriesResponse),
	VoteReq(RequestVote<A>),
	VoteRes(RequestVoteResponse),
}

impl<A, E> Message<A, E> {
	pub fn get_term(&self) -> Term {
		match self {
			Message::AppendReq(a_req) => a_req.term,
			Message::AppendRes(a_res) => a_res.term,
			Message::VoteReq(v_req) => v_req.term,
			Message::VoteRes(v_res) => v_res.term,
		}
	}
}
