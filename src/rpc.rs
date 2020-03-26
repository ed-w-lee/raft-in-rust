use crate::types::{LogIndex, NodeId, Term};

pub struct AppendEntries {
	term: Term,
	leader_id: NodeId,
	leader_commit: LogIndex,
	prev_log_index: LogIndex,
	prev_log_term: Term,
	entries: Vec<u64>,
}

pub struct AppendEntriesResponse {
	term: Term,
	success: bool,
}

pub struct RequestVote {
	term: Term,
	candidate_id: NodeId,
	last_log_index: LogIndex,
	last_log_term: Term,
}

pub struct RequestVoteResponse {
	term: Term,
	vote_granted: bool,
}

pub enum Message {
	AppendReq(AppendEntries),
	AppendRes(AppendEntriesResponse),
	VoteReq(RequestVote),
	VoteRes(RequestVoteResponse),
}
