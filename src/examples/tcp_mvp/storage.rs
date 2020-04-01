use crate::serialize::Serialize;
use rafted::{LogIndex, PersistentData, Storage, Term};

use std::fmt::{Debug, Display};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::marker::PhantomData;

#[derive(Debug)]
pub struct FileStorage<A, E> {
	state_file: File,
	entries_file: File,
	_addr: PhantomData<A>,
	_entry: PhantomData<E>,
}

impl<A, E> FileStorage<A, E>
where
	A: Display,
{
	pub fn new(addr: A) -> Self {
		Self {
			state_file: OpenOptions::new()
				.read(true)
				.write(true)
				.create(true)
				.open(format!("/tmp/rafted_tcpmvp_state_{}", addr))
				.unwrap(),
			entries_file: OpenOptions::new()
				.read(true)
				.write(true)
				.create(true)
				.open(format!("/tmp/rafted_tcpmvp_entries_{}", addr))
				.unwrap(),
			_addr: PhantomData,
			_entry: PhantomData,
		}
	}
}

impl<'a, A, E> FileStorage<A, E>
where
	A: Serialize + 'a,
	E: Serialize + 'a,
{
	fn _get_data(&mut self) -> Option<(Term, Option<A>, LogIndex, Term, Vec<(Term, E)>)> {
		// --- read state ---
		self
			.state_file
			.seek(SeekFrom::Start(0))
			.expect("couldn't seek to start");

		let mut state_buf = vec![];
		self
			.state_file
			.read_to_end(&mut state_buf)
			.expect("couldn't read to end");

		let mut slice = state_buf.as_slice();
		// read term
		let tup = Term::from_bytes(slice).ok()?;
		let (to_shift, curr_term) = tup;
		slice = &slice[to_shift..];

		// read option<a>
		let tup = Option::<A>::from_bytes(slice).ok()?;
		let (to_shift, voted_for) = tup;
		slice = &slice[to_shift..];

		// read first_index
		let tup = LogIndex::from_bytes(slice).ok()?;
		let (to_shift, first_index) = tup;
		slice = &slice[to_shift..];

		// read first_term
		let tup = Term::from_bytes(slice).ok()?;
		let (_, first_term) = tup;

		// ---- read log ----
		self
			.entries_file
			.seek(SeekFrom::Start(0))
			.expect("couldn't seek to start");

		let mut entries_buf = vec![];
		self
			.entries_file
			.read_to_end(&mut entries_buf)
			.expect("couldn't read to end");
		let mut slice = entries_buf.as_slice();

		// read log_len
		let tup = u64::from_bytes(slice).ok()?;
		let (to_shift, log_len) = tup;
		slice = &slice[to_shift..];

		let mut log = Vec::with_capacity(*log_len as usize);

		for _ in 0..*log_len {
			let tup = Term::from_bytes(slice).ok()?;
			let (to_shift, my_term) = tup;
			slice = &slice[to_shift..];

			let tup = E::from_bytes(slice).ok()?;
			let (to_shift, my_entry) = tup;
			slice = &slice[to_shift..];
			log.push((*my_term, *my_entry));
		}

		Some((*curr_term, *voted_for, *first_index, *first_term, log))
	}
}

impl<'a, A, E> Storage<'a, A, E> for FileStorage<A, E>
where
	A: Debug + Serialize + 'a,
	E: Debug + Serialize + 'a,
{
	fn get_data(mut self) -> PersistentData<'a, A, E> {
		match self._get_data() {
			Some(tup) => {
				let (curr_term, voted_for, first_index, first_term, entries) = tup;
				println!("file storage read correctly, curr_term: {}", curr_term);
				PersistentData::from_existing(
					Box::new(self),
					curr_term,
					voted_for,
					first_index,
					first_term,
					entries,
				)
			}
			None => {
				// write initialized data
				println!("failed to read from file storage");
				let mut to_ret = PersistentData::init(Box::new(self));
				to_ret.flush();
				to_ret
			}
		}
	}

	fn update_state(
		&mut self,
		term: Term,
		voted_for: &Option<A>,
		first_index: LogIndex,
		first_term: Term,
	) {
		let mut buf = vec![];

		buf.append(&mut term.to_bytes());
		buf.append(&mut voted_for.to_bytes());
		buf.append(&mut first_index.to_bytes());
		buf.append(&mut first_term.to_bytes());

		self
			.state_file
			.seek(SeekFrom::Start(0))
			.expect("couldn't seek to start");
		self
			.state_file
			.write_all(&buf)
			.expect("couldn't update state");
		println!("file storage updated with term: {}", term);
	}

	fn update_entries(&mut self, start: LogIndex, entries: &[(Term, E)]) {
		self
			.entries_file
			.seek(SeekFrom::Start(0))
			.expect("couldn't seek to start");

		if entries.is_empty() && start == 0 {
			// handle empty case
			self
				.entries_file
				.write_all(&start.to_bytes())
				.expect("fuck couldn't write");
			return;
		}

		// read log_len
		let mut entries_buf = vec![];
		self
			.entries_file
			.read_to_end(&mut entries_buf)
			.expect("couldn't read to end");
		let mut slice = entries_buf.as_slice();
		let total_len = slice.len();

		let tup = u64::from_bytes(slice).expect("test");
		let (to_shift, log_len) = tup;
		slice = &slice[to_shift..];

		if entries.is_empty() {
			assert!(start <= *log_len);
			self
				.entries_file
				.seek(SeekFrom::Start(0))
				.expect("couldn't seek to start");
			self
				.entries_file
				.write_all(&start.to_bytes())
				.expect("fuck couldn't write");

			return;
		}

		if *log_len >= start {
			// we are overwriting other entries, find where to seek to
			for _ in 0..(start - 1) {
				let tup = Term::from_bytes(slice).expect("blah");
				let (to_shift, _) = tup;
				slice = &slice[to_shift..];

				let tup = E::from_bytes(slice).expect("fuck");
				let (to_shift, _) = tup;
				slice = &slice[to_shift..];
			}

			let did_read = total_len - slice.len();
			self
				.entries_file
				.seek(SeekFrom::Start(did_read as u64))
				.expect("couldn't seek to start");
		} else {
			// we are appending to log. don't seek anywhere
			assert_eq!(start, *log_len + 1);
		}
		// write log entries
		let mut to_write = vec![];
		for tup in entries {
			let (term, entry) = tup;
			to_write.append(&mut term.to_bytes());
			to_write.append(&mut entry.to_bytes());
		}
		self
			.entries_file
			.write_all(&to_write)
			.expect("couldn't write oof");

		// write final log length
		self
			.entries_file
			.seek(SeekFrom::Start(0))
			.expect("couldn't seek to start");
		self
			.entries_file
			.write_all(&(start + (entries.len() as u64) - 1).to_bytes())
			.expect("no write rip");
	}
}
