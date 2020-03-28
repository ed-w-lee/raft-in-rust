use crate::types::{LogIndex, NodeId, Term};

#[derive(Debug, PartialEq)]
pub struct AppendEntries<E> {
	pub term: Term,
	pub leader_id: NodeId,
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
pub struct RequestVote {
	pub term: Term,
	pub candidate_id: NodeId,
	pub last_log_index: LogIndex,
	pub last_log_term: Term,
}

#[derive(Debug, PartialEq)]
pub struct RequestVoteResponse {
	pub term: Term,
	pub vote_granted: bool,
}

#[derive(Debug, PartialEq)]
pub enum Message<E> {
	AppendReq(AppendEntries<E>),
	AppendRes(AppendEntriesResponse),
	VoteReq(RequestVote),
	VoteRes(RequestVoteResponse),
}
