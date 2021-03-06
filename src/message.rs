use crate::types::{LogIndex, ReaderIndex, Term};

#[derive(Debug, PartialEq, Clone)]
pub struct AppendEntries<NA, ENT> {
	pub term: Term,
	pub leader_id: NA,
	pub leader_commit: LogIndex,
	pub prev_log_index: LogIndex,
	pub prev_log_term: Term,
	pub reader_idx: ReaderIndex,
	pub entries: Vec<(Term, Option<ENT>)>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct AppendEntriesResponse<NA> {
	pub term: Term,
	pub from: NA,
	pub success: Result<LogIndex, LogIndex>,
	pub reader_idx: ReaderIndex,
}

#[derive(Debug, PartialEq, Clone)]
pub struct RequestVote<NA> {
	pub term: Term,
	pub candidate_id: NA,
	pub last_log_index: LogIndex,
	pub last_log_term: Term,
}

#[derive(Debug, PartialEq, Clone)]
pub struct RequestVoteResponse<NA> {
	pub term: Term,
	pub from: NA,
	pub vote_granted: bool,
}

#[derive(Debug, PartialEq, Clone)]
pub enum NodeMessage<NA, ENT> {
	AppendReq(AppendEntries<NA, ENT>),
	AppendRes(AppendEntriesResponse<NA>),
	VoteReq(RequestVote<NA>),
	VoteRes(RequestVoteResponse<NA>),
}

impl<NA, ENT> NodeMessage<NA, ENT> {
	pub fn get_term(&self) -> Term {
		match self {
			NodeMessage::AppendReq(a_req) => a_req.term,
			NodeMessage::AppendRes(a_res) => a_res.term,
			NodeMessage::VoteReq(v_req) => v_req.term,
			NodeMessage::VoteRes(v_res) => v_res.term,
		}
	}
}

#[derive(Debug)]
pub enum ClientRequest<CA, REQ, ENT> {
	Read(CA, REQ),
	Apply(CA, ENT),
}

impl<CA, REQ, ENT> ClientRequest<CA, REQ, ENT>
where
	CA: Clone,
{
	pub fn get_addr(&self) -> CA {
		match self {
			ClientRequest::Read(a, _) => a.clone(),
			ClientRequest::Apply(a, _) => a.clone(),
		}
	}
}

#[derive(Debug, PartialEq, Clone)]
pub enum ClientResponse<NA, RES> {
	Response(RES),
	Redirect(NA),
	TryAgain,
}

/// Messages from a node to either another node or a client
#[derive(Clone)]
pub enum Message<NA, ENT, CA, RES> {
	Node(NA, NodeMessage<NA, ENT>),
	Client(CA, ClientResponse<NA, RES>),
}
