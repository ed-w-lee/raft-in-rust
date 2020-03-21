use crate::types::{LogIndex, NodeId, Term};

use std::marker::PhantomData;
use std::net::IpAddr;

struct PersistentData {
  my_id: NodeId,
  current_term: Term,
  voted_for: Option<NodeId>,
  log: Vec<u64>,
  addrs: Vec<IpAddr>, // generalize addressing scheme, probably
}

struct VolatileData {
  commit_index: LogIndex,
  last_applied: LogIndex,
}

struct VolatileLeaderData {
  next_index: Vec<LogIndex>,
  match_index: Vec<LogIndex>,
}

pub struct Leader {
  p_data: PersistentData,
  v_data: VolatileData,
  l_data: VolatileLeaderData,
}

pub struct Candidate;
pub struct Follower;

pub struct NonLeader<S> {
  p_data: PersistentData,
  v_data: VolatileData,
  _marker: PhantomData<S>,
}

pub enum Node {
  Leader(Leader),
  Candidate(NonLeader<Candidate>),
  Follower(NonLeader<Follower>),
}

impl NonLeader<Follower> {
  pub fn new(my_id: NodeId, other_addrs: Vec<IpAddr>) -> NonLeader<Follower> {
    NonLeader {
      p_data: PersistentData {
        // need some way of persisting + retrieving these
        my_id,
        current_term: 0,
        addrs: other_addrs,
        log: Vec::new(),
        voted_for: None,
      },
      v_data: VolatileData {
        commit_index: 0,
        last_applied: 0,
      },
      _marker: PhantomData,
    }
  }

  fn to_candidate(self) -> NonLeader<Candidate> {
    let candidate = NonLeader {
      p_data: self.p_data,
      v_data: self.v_data,
      _marker: PhantomData,
    };

    candidate
  }
}

impl NonLeader<Candidate> {
  fn start_election(&mut self) {
    self.p_data.current_term += 1;
    self.p_data.voted_for = Some(self.p_data.my_id);
    // reset election timer
    // send RequestVote to other servers
  }

  fn to_leader(self) -> Leader {
    let next_index = vec![self.p_data.log.len() as u64; self.p_data.addrs.len()];
    let match_index = vec![0; self.p_data.addrs.len()];
    let mut to_ret = Leader {
      p_data: self.p_data,
      v_data: self.v_data,
      l_data: VolatileLeaderData {
        next_index,
        match_index,
      },
    };

    to_ret.sendEntries();
    to_ret
  }

  fn to_follower(self) -> NonLeader<Follower> {
    NonLeader {
      p_data: self.p_data,
      v_data: self.v_data,
      _marker: PhantomData,
    }
  }
}

impl Leader {
  fn sendEntries(&mut self) {}
}
