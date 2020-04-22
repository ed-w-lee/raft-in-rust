use rafted::message::{AppendEntries, AppendEntriesResponse, RequestVote, RequestVoteResponse};
use rafted::{LogIndex, ReaderIndex, Term};

use std::convert::TryInto;
use std::mem::size_of;
use std::net::IpAddr;
use std::str;

#[derive(Debug, PartialEq)]
pub enum SerialStatus {
	Incomplete,
	Error,
}

/**
 * Custom serialize trait since I don't want to import serde.
 * I want to try doing everything from scratch.
 *
 * Several hours later and I regret everything.
 */
pub trait Serialize {
	fn to_bytes(&self) -> Vec<u8>;
	fn from_bytes(buf: &[u8]) -> Result<(usize, Box<Self>), SerialStatus>;
}

impl<A, E> Serialize for AppendEntries<A, E>
where
	A: Serialize,
	E: Serialize,
{
	fn to_bytes(&self) -> Vec<u8> {
		let mut serial = vec![];

		serial.extend_from_slice(&self.term.to_bytes());

		serial.extend_from_slice(&self.leader_id.to_bytes());

		serial.extend_from_slice(&self.leader_commit.to_bytes());
		serial.extend_from_slice(&self.prev_log_index.to_bytes());
		serial.extend_from_slice(&self.prev_log_term.to_bytes());

		serial.extend_from_slice(&(self.entries.len() as u64).to_bytes());
		for entry in &self.entries {
			serial.extend_from_slice(&entry.to_bytes());
		}
		serial.extend_from_slice(&self.reader_idx.to_bytes());

		let total_len = serial.len();
		let mut to_ret = vec![];
		to_ret.extend_from_slice(&(total_len as u64).to_bytes());

		to_ret.append(&mut serial);
		to_ret
	}

	fn from_bytes(buf: &[u8]) -> Result<(usize, Box<Self>), SerialStatus> {
		let (to_shift, total_len) = u64::from_bytes(buf)?;
		let mut buf = shift(buf, to_shift);

		if buf.len() < (*total_len as usize) {
			return Err(SerialStatus::Incomplete);
		}

		let (to_shift, term) = Term::from_bytes(buf)?;
		buf = shift(buf, to_shift);

		let (to_shift, leader_id) = A::from_bytes(buf)?;
		buf = shift(buf, to_shift);

		let (to_shift, leader_commit) = LogIndex::from_bytes(buf)?;
		buf = shift(buf, to_shift);

		let (to_shift, prev_log_index) = LogIndex::from_bytes(buf)?;
		buf = shift(buf, to_shift);

		let (to_shift, prev_log_term) = Term::from_bytes(buf)?;
		buf = shift(buf, to_shift);

		let (to_shift, num_entries) = u64::from_bytes(buf)?;
		buf = shift(buf, to_shift);
		let num_entries = *num_entries as usize;

		let mut entries = vec![];
		println!("{}", num_entries);
		for _i in 0..num_entries {
			let (to_shift, ent) = <(Term, Option<E>)>::from_bytes(buf)?;
			entries.push(*ent);
			buf = shift(buf, to_shift);
		}

		let (_, reader_idx) = ReaderIndex::from_bytes(buf)?;

		Ok((
			(*total_len as usize) + size_of::<u64>(),
			Box::new(Self {
				term: *term,
				leader_id: *leader_id,
				leader_commit: *leader_commit,
				prev_log_index: *prev_log_index,
				prev_log_term: *prev_log_term,
				entries,
				reader_idx: *reader_idx,
			}),
		))
	}
}

impl<NA> Serialize for AppendEntriesResponse<NA>
where
	NA: Serialize,
{
	fn to_bytes(&self) -> Vec<u8> {
		let mut serial = vec![];

		serial.extend_from_slice(&self.term.to_bytes());
		serial.extend_from_slice(&self.success.to_bytes());
		serial.extend_from_slice(&self.from.to_bytes());
		serial.extend_from_slice(&self.reader_idx.to_bytes());

		let mut to_ret = vec![];
		to_ret.extend_from_slice(&(serial.len() as usize).to_be_bytes());
		to_ret.append(&mut serial);

		to_ret
	}

	fn from_bytes(buf: &[u8]) -> Result<(usize, Box<Self>), SerialStatus> {
		let (to_shift, total_len) = u64::from_bytes(buf)?;
		let mut buf = shift(buf, to_shift);

		if buf.len() < (*total_len as usize) {
			return Err(SerialStatus::Incomplete);
		}

		let (to_shift, term) = Term::from_bytes(buf)?;
		buf = shift(buf, to_shift);

		let (to_shift, success) = Option::<LogIndex>::from_bytes(buf)?;
		buf = shift(buf, to_shift);

		let (to_shift, from) = NA::from_bytes(buf)?;
		buf = shift(buf, to_shift);

		let (_, reader_idx) = ReaderIndex::from_bytes(buf)?;

		Ok((
			(*total_len as usize) + size_of::<u64>(),
			Box::new(Self {
				term: *term,
				from: *from,
				success: *success,
				reader_idx: *reader_idx,
			}),
		))
	}
}

impl<A> Serialize for RequestVote<A>
where
	A: Serialize,
{
	fn to_bytes(&self) -> Vec<u8> {
		let mut serial = vec![];

		serial.extend_from_slice(&self.term.to_be_bytes());

		serial.extend_from_slice(&self.candidate_id.to_bytes());

		serial.extend_from_slice(&self.last_log_index.to_bytes());
		serial.extend_from_slice(&self.last_log_term.to_bytes());

		let mut to_ret = vec![];
		to_ret.extend_from_slice(&(serial.len() as u64).to_bytes());
		to_ret.append(&mut serial);

		to_ret
	}

	fn from_bytes(buf: &[u8]) -> Result<(usize, Box<Self>), SerialStatus> {
		let (to_shift, total_len) = u64::from_bytes(buf)?;
		let mut buf = shift(buf, to_shift);

		if buf.len() < (*total_len as usize) {
			return Err(SerialStatus::Incomplete);
		}

		let (to_shift, term) = Term::from_bytes(buf)?;
		buf = shift(buf, to_shift);

		let (to_shift, candidate) = A::from_bytes(buf)?;
		buf = shift(buf, to_shift);

		let (to_shift, last_log_index) = LogIndex::from_bytes(buf)?;
		buf = shift(buf, to_shift);

		let (_, last_log_term) = LogIndex::from_bytes(buf)?;

		Ok((
			(*total_len as usize) + size_of::<u64>(),
			Box::new(Self {
				term: *term,
				candidate_id: *candidate,
				last_log_index: *last_log_index,
				last_log_term: *last_log_term,
			}),
		))
	}
}

impl<A> Serialize for RequestVoteResponse<A>
where
	A: Serialize,
{
	fn to_bytes(&self) -> Vec<u8> {
		let mut serial = vec![];

		serial.append(&mut self.from.to_bytes());
		serial.extend_from_slice(&self.term.to_be_bytes());
		serial.push(if self.vote_granted { 1u8 } else { 0u8 });

		let mut to_ret = vec![];
		to_ret.extend_from_slice(&(serial.len() as u64).to_be_bytes());
		to_ret.append(&mut serial);

		to_ret
	}

	fn from_bytes(buf: &[u8]) -> Result<(usize, Box<Self>), SerialStatus> {
		let total_len = size_of::<u64>()
			+ match into_u64(buf) {
				Some(v) => v,
				None => return Err(SerialStatus::Incomplete),
			} as usize;

		if buf.len() < total_len {
			return Err(SerialStatus::Incomplete);
		}

		let mut buf = shift(buf, size_of::<u64>());

		let tup = A::from_bytes(buf)?;
		let (to_shift, from) = tup;
		buf = shift(buf, to_shift);

		let term = check(into_term(buf))?;
		buf = shift(buf, size_of::<Term>());

		let vote_granted: bool = if buf[0] > 0 { true } else { false };

		Ok((
			total_len,
			Box::new(Self {
				term,
				from: *from,
				vote_granted,
			}),
		))
	}
}

// Mostly for testing purposes
impl Serialize for u64 {
	fn to_bytes(&self) -> Vec<u8> {
		let mut to_ret = Vec::new();
		to_ret.extend_from_slice(&self.to_be_bytes());

		to_ret
	}

	fn from_bytes(buf: &[u8]) -> Result<(usize, Box<Self>), SerialStatus> {
		let res = check(into_u64(buf))?;
		Ok((size_of::<Self>(), Box::new(res)))
	}
}

impl Serialize for IpAddr {
	fn to_bytes(&self) -> Vec<u8> {
		let mut buf = vec![];

		let addr_str = self.to_string();
		let addr_bytes = addr_str.as_bytes();
		buf.extend_from_slice(&(addr_bytes.len() as u64).to_be_bytes());
		buf.extend_from_slice(addr_bytes);

		buf
	}

	fn from_bytes(buf: &[u8]) -> Result<(usize, Box<Self>), SerialStatus> {
		let addr_len = check(into_u64(buf))? as usize;
		let buf = shift(buf, size_of::<u64>());
		let addr_bytes = check(buf.get(0..addr_len))?;
		let addr_str = check(str::from_utf8(addr_bytes).ok())?;
		Ok((
			size_of::<u64>() + addr_len,
			Box::new(check(addr_str.parse().ok())?),
		))
	}
}

impl<T> Serialize for Option<T>
where
	T: Serialize,
{
	fn to_bytes(&self) -> Vec<u8> {
		let mut buf = vec![];
		match self {
			Some(obj) => {
				buf.push(1u8);
				buf.extend_from_slice(&obj.to_bytes());
			}
			None => buf.push(0u8),
		}
		buf
	}

	fn from_bytes(buf: &[u8]) -> Result<(usize, Box<Self>), SerialStatus> {
		let is_some = buf[0] > 0;
		if is_some {
			let slice = &buf[1..];
			let tup = T::from_bytes(slice)?;
			let (n_read, ret) = tup;

			Ok((n_read + 1, Box::new(Some(*ret))))
		} else {
			Ok((1, Box::new(None)))
		}
	}
}

impl<A, B> Serialize for (A, B)
where
	A: Serialize,
	B: Serialize,
{
	fn to_bytes(&self) -> Vec<u8> {
		let mut app = vec![];
		app.extend_from_slice(&self.0.to_bytes());
		app.extend_from_slice(&self.1.to_bytes());
		let mut to_ret = vec![];
		to_ret.extend_from_slice(&((app.len()) as u64).to_bytes());
		to_ret.extend_from_slice(&app);
		to_ret
	}

	fn from_bytes(buf: &[u8]) -> Result<(usize, Box<Self>), SerialStatus> {
		let (u64_len, total_len) = u64::from_bytes(buf)?;
		let buf = shift(buf, u64_len);

		if (buf.len() as u64) < *total_len {
			Err(SerialStatus::Incomplete)
		} else {
			let (to_shift, tup_0) = A::from_bytes(buf)?;
			let buf = shift(buf, to_shift);
			let (_, tup_1) = B::from_bytes(buf)?;

			Ok((u64_len + (*total_len as usize), Box::new((*tup_0, *tup_1))))
		}
	}
}

fn into_term(buf: &[u8]) -> Option<Term> {
	let bytes = buf.get(0..size_of::<Term>())?;
	let arr = bytes.try_into().ok()?;
	Some(u64::from_be_bytes(arr))
}

fn into_index(buf: &[u8]) -> Option<LogIndex> {
	let bytes = buf.get(0..size_of::<LogIndex>())?;
	let arr = bytes.try_into().ok()?;
	Some(u64::from_be_bytes(arr))
}

fn into_u64(buf: &[u8]) -> Option<u64> {
	let bytes = buf.get(0..size_of::<u64>())?;
	let arr = bytes.try_into().ok()?;
	Some(u64::from_be_bytes(arr))
}

fn check<T>(opt: Option<T>) -> Result<T, SerialStatus> {
	match opt {
		Some(v) => Ok(v),
		None => Err(SerialStatus::Error),
	}
}

fn shift(buf: &[u8], len: usize) -> &[u8] {
	&buf[len..]
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_append_entries_convert() {
		let ae: AppendEntries<IpAddr, u64> = AppendEntries {
			term: 10,
			leader_id: "127.0.0.1".parse().unwrap(),
			leader_commit: 20,
			prev_log_index: 30,
			prev_log_term: 16,
			entries: vec![
				(10, Some(120)),
				(20, Some(1525)),
				(25, None),
				(30, Some(480848)),
			]
			.to_vec(),
			reader_idx: 10,
		};
		let bytes = ae.to_bytes();
		println!("{:?}", bytes);
		match AppendEntries::<IpAddr, u64>::from_bytes(&bytes) {
			Ok(tup) => {
				let len = tup.0;
				let ae_new = tup.1;
				assert_eq!(len, bytes.len());
				assert_eq!(ae, *ae_new);
			}
			Err(_) => assert!(false),
		}
	}

	#[test]
	fn test_append_entries_incomplete() {
		let ae: AppendEntries<IpAddr, u64> = AppendEntries {
			term: 10,
			leader_id: "127.0.0.1".parse().unwrap(),
			leader_commit: 20,
			prev_log_index: 30,
			prev_log_term: 16,
			entries: vec![
				(10, Some(120)),
				(20, Some(1525)),
				(25, None),
				(30, Some(480848)),
			],
			reader_idx: 10,
		};
		let bytes = ae.to_bytes();
		assert_eq!(
			Err(SerialStatus::Incomplete),
			AppendEntries::<IpAddr, u64>::from_bytes(&bytes[..bytes.len() - 10])
		);
	}

	#[test]
	fn test_append_entries_error() {
		let ae: AppendEntries<IpAddr, u64> = AppendEntries {
			term: 10,
			leader_id: "127.0.0.1".parse().unwrap(),
			leader_commit: 20,
			prev_log_index: 30,
			prev_log_term: 16,
			entries: vec![
				(10, Some(120)),
				(20, Some(1525)),
				(25, None),
				(30, Some(480848)),
			],
			reader_idx: 10,
		};
		let mut bytes = ae.to_bytes();
		let mut orig_bytes = bytes.clone();
		bytes.truncate(20);
		bytes.append(&mut orig_bytes);
		assert_eq!(
			Err(SerialStatus::Error),
			AppendEntries::<IpAddr, u64>::from_bytes(&bytes[..bytes.len()])
		);
	}

	#[test]
	fn test_append_entries_extend() {
		let ae: AppendEntries<IpAddr, u64> = AppendEntries {
			term: 10,
			leader_id: "127.0.0.1".parse().unwrap(),
			leader_commit: 20,
			prev_log_index: 30,
			prev_log_term: 16,
			entries: vec![
				(10, Some(120)),
				(20, Some(1525)),
				(25, None),
				(30, Some(480848)),
			],
			reader_idx: 10,
		};
		let mut bytes = ae.to_bytes();
		let orig_len = bytes.len();
		bytes.append(&mut vec![12, 34, 56, 78, 90]);
		match AppendEntries::<IpAddr, u64>::from_bytes(&bytes) {
			Ok(tup) => {
				let len = tup.0;
				let ae_new = tup.1;
				assert_eq!(len, orig_len);
				assert_eq!(ae, *ae_new);
			}
			Err(_) => assert!(false),
		}
	}

	#[test]
	fn test_append_res_convert() {
		let ae: AppendEntriesResponse<u64> = AppendEntriesResponse {
			term: 10,
			from: 15,
			success: Some(10),
			reader_idx: 10,
		};
		let bytes = ae.to_bytes();
		match AppendEntriesResponse::<u64>::from_bytes(&bytes) {
			Ok(tup) => {
				let len = tup.0;
				let ae_new = tup.1;
				assert_eq!(len, bytes.len());
				assert_eq!(ae, *ae_new);
			}
			Err(_) => assert!(false),
		}
	}

	#[test]
	fn test_append_res_none_convert() {
		let ae: AppendEntriesResponse<u64> = AppendEntriesResponse {
			term: 10,
			from: 15,
			success: None,
			reader_idx: 10,
		};
		let bytes = ae.to_bytes();
		match AppendEntriesResponse::<u64>::from_bytes(&bytes) {
			Ok(tup) => {
				let len = tup.0;
				let ae_new = tup.1;
				assert_eq!(len, bytes.len());
				assert_eq!(ae, *ae_new);
			}
			Err(_) => assert!(false),
		}
	}

	#[test]
	fn test_append_res_incomplete() {
		let ae: AppendEntriesResponse<u64> = AppendEntriesResponse {
			term: 10,
			from: 15,
			success: Some(10),
			reader_idx: 10,
		};
		let bytes = ae.to_bytes();
		assert_eq!(
			Err(SerialStatus::Incomplete),
			AppendEntriesResponse::<u64>::from_bytes(&bytes[..bytes.len() - 3])
		);
	}

	#[test]
	fn test_append_res_extend() {
		let ae: AppendEntriesResponse<u64> = AppendEntriesResponse {
			term: 10,
			from: 15,
			success: Some(10),
			reader_idx: 10,
		};
		let mut bytes = ae.to_bytes();
		let orig_len = bytes.len();
		bytes.append(&mut vec![12, 34, 56, 78, 90]);
		match AppendEntriesResponse::<u64>::from_bytes(&bytes) {
			Ok(tup) => {
				let len = tup.0;
				let ae_new = tup.1;
				assert_eq!(len, orig_len);
				assert_eq!(ae, *ae_new);
			}
			Err(_) => assert!(false),
		}
	}

	#[test]
	fn test_request_vote_convert() {
		let ae: RequestVote<IpAddr> = RequestVote {
			candidate_id: "192.168.1.1".parse().unwrap(),
			term: 10,
			last_log_index: 20,
			last_log_term: 500,
		};
		let bytes = ae.to_bytes();
		match RequestVote::<IpAddr>::from_bytes(&bytes) {
			Ok(tup) => {
				let len = tup.0;
				let ae_new = tup.1;
				assert_eq!(len, bytes.len());
				assert_eq!(ae, *ae_new);
			}
			Err(_) => assert!(false),
		}
	}

	#[test]
	fn test_request_vote_incomplete() {
		let ae: RequestVote<IpAddr> = RequestVote {
			candidate_id: "192.168.1.1".parse().unwrap(),
			term: 10,
			last_log_index: 20,
			last_log_term: 500,
		};
		let bytes = ae.to_bytes();
		assert_eq!(
			Err(SerialStatus::Incomplete),
			RequestVote::<IpAddr>::from_bytes(&bytes[..bytes.len() - 10])
		);
	}

	#[test]
	fn test_request_vote_error() {
		let ae: RequestVote<IpAddr> = RequestVote {
			candidate_id: "192.168.1.1".parse().unwrap(),
			term: 10,
			last_log_index: 20,
			last_log_term: 500,
		};
		let mut bytes = ae.to_bytes();
		bytes.truncate(bytes.len() / 2 + 1);
		bytes.append(&mut bytes.clone());
		assert_eq!(
			Err(SerialStatus::Error),
			RequestVote::<IpAddr>::from_bytes(&bytes[..bytes.len()])
		);
	}

	#[test]
	fn test_request_vote_extend() {
		let ae: RequestVote<IpAddr> = RequestVote {
			candidate_id: "192.168.1.1".parse().unwrap(),
			term: 10,
			last_log_index: 20,
			last_log_term: 500,
		};
		let mut bytes = ae.to_bytes();
		let orig_len = bytes.len();
		bytes.append(&mut vec![12, 34, 56, 78, 90]);
		match RequestVote::<IpAddr>::from_bytes(&bytes) {
			Ok(tup) => {
				let len = tup.0;
				let ae_new = tup.1;
				assert_eq!(len, orig_len);
				assert_eq!(ae, *ae_new);
			}
			Err(_) => assert!(false),
		}
	}

	#[test]
	fn test_vote_res_convert() {
		let ae: RequestVoteResponse<u64> = RequestVoteResponse {
			term: 10,
			from: 15,
			vote_granted: true,
		};
		let bytes = ae.to_bytes();
		match RequestVoteResponse::<u64>::from_bytes(&bytes) {
			Ok(tup) => {
				let len = tup.0;
				let ae_new = tup.1;
				assert_eq!(len, bytes.len());
				assert_eq!(ae, *ae_new);
			}
			Err(_) => assert!(false),
		}
	}

	#[test]
	fn test_vote_res_incomplete() {
		let ae: RequestVoteResponse<u64> = RequestVoteResponse {
			term: 10,
			from: 15,
			vote_granted: true,
		};
		let bytes = ae.to_bytes();
		assert_eq!(
			Err(SerialStatus::Incomplete),
			RequestVoteResponse::<u64>::from_bytes(&bytes[..bytes.len() - 3])
		);
	}

	#[test]
	fn test_vote_res_extend() {
		let ae: RequestVoteResponse<u64> = RequestVoteResponse {
			term: 10,
			from: 15,
			vote_granted: true,
		};
		let mut bytes = ae.to_bytes();
		let orig_len = bytes.len();
		bytes.append(&mut vec![12, 34, 56, 78, 90]);
		match RequestVoteResponse::<u64>::from_bytes(&bytes) {
			Ok(tup) => {
				let len = tup.0;
				let ae_new = tup.1;
				assert_eq!(len, orig_len);
				assert_eq!(ae, *ae_new);
			}
			Err(_) => assert!(false),
		}
	}
}
