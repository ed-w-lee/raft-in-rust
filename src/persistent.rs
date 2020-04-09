use crate::types::{LogIndex, Term};

use std::cmp::min;
use std::fmt::Debug;

#[derive(Debug)]
pub struct PersistentData<'a, A, E> {
	storage: Box<dyn Storage<'a, A, E> + 'a>,

	pub curr_term: Term,
	pub voted_for: Option<A>,

	first_index: LogIndex,
	first_term: Term,
	dirty_begin: Option<LogIndex>,
	entries: Vec<(Term, E)>,
}

pub trait Storage<'a, A, E>: Debug {
	fn get_data(self) -> PersistentData<'a, A, E>;

	fn update_state(
		&mut self,
		term: Term,
		voted_for: &Option<A>,
		first_index: LogIndex,
		first_term: Term,
	);

	/// Clear the storage of all log entries whose index >= start
	/// Then append entries in their place
	fn update_entries(&mut self, start: LogIndex, entries: &[(Term, E)]);
}

impl<'a, A, E> PersistentData<'a, A, E>
where
	E: Clone + Debug,
{
	pub fn init(storage: Box<dyn Storage<'a, A, E> + 'a>) -> Self {
		Self {
			storage,
			curr_term: 0,
			voted_for: None,
			first_index: 0,
			first_term: 0,
			dirty_begin: Some(0),
			entries: vec![],
		}
	}

	pub fn from_existing(
		storage: Box<dyn Storage<'a, A, E> + 'a>,
		curr_term: Term,
		voted_for: Option<A>,
		first_index: LogIndex,
		first_term: Term,
		entries: Vec<(Term, E)>,
	) -> Self {
		Self {
			storage,

			curr_term,
			voted_for,

			first_index,
			first_term,
			dirty_begin: None,
			entries,
		}
	}

	pub fn flush(&mut self) {
		self.storage.update_state(
			self.curr_term,
			&self.voted_for,
			self.first_index,
			self.first_term,
		);
		if let Some(db) = self.dirty_begin {
			if db == self.first_index {
				assert!(self.entries.is_empty());
				self.storage.update_entries(db, &[]);
			} else {
				assert!(db > self.first_index);
				self
					.storage
					.update_entries(db, &self.entries.get(((db - 1) as usize)..).unwrap_or(&[]));
			}
		}
		self.dirty_begin = None;
	}

	/* Log related stuff */
	pub fn get_entry(&self, index: LogIndex) -> &(Term, E) {
		if index <= self.first_index {
			panic!("bad index query");
		} else {
			&self.entries[(index - self.first_index - 1) as usize]
		}
	}

	pub fn get_entries(&self, start: LogIndex) -> &[(Term, E)] {
		if start < self.first_index {
			panic!("bad index query");
		} else if start == self.first_index {
			&[]
		} else {
			let offs = (start - self.first_index - 1) as usize;
			&self.entries[offs..]
		}
	}

	pub fn append_entry(&mut self, new_entry: (Term, E)) -> LogIndex {
		self.append_entries(&mut vec![new_entry]);
		self.last_entry()
	}

	pub fn append_entries(&mut self, new_entries: &[(Term, E)]) {
		if !new_entries.is_empty() {
			self.dirty_begin = Some(match self.dirty_begin {
				Some(db) => min(db, self.last_entry() + 1),
				None => self.last_entry() + 1,
			});
			self.entries.extend_from_slice(new_entries);
		}
	}

	pub fn update_entries(&mut self, start: LogIndex, new_entries: &[(Term, E)]) -> Result<(), ()> {
		if start > self.last_entry() + 1 {
			Err(())
		} else if start == self.last_entry() + 1 {
			self.append_entries(new_entries);
			Ok(())
		} else {
			let remaining = start - self.first_index;
			self.entries.truncate(remaining as usize);
			self.dirty_begin = Some(self.last_entry() + 1);
			self.append_entries(new_entries);
			Ok(())
		}
	}

	/// Checks if there is an entry in the log with the given log index and term
	/// **Does not** check if the provided index and term are before the data
	pub fn has_entry_with(&self, index: LogIndex, term: Term) -> bool {
		if index < self.first_index {
			panic!("bad index query of the existing log");
		} else if index == self.first_index {
			term == self.first_term
		} else {
			let offset = (index - self.first_index - 1) as usize;
			match self.entries.get(offset) {
				Some(t) => t.0 == term,
				None => false,
			}
		}
	}

	/// Checks if the **inputted** (index, term) are at least as up-to-date as the log.
	pub fn is_up2date(&self, index: LogIndex, term: Term) -> bool {
		let (my_ind, my_term) = if self.entries.is_empty() {
			(self.first_index, self.first_term)
		} else {
			(self.last_entry(), self.entries.last().unwrap().0)
		};

		term > my_term || (term == my_term && index >= my_ind)
	}

	pub fn last_entry(&self) -> LogIndex {
		self.first_index + (self.entries.len() as LogIndex)
	}

	pub fn get_term(&self, idx: LogIndex) -> Result<Term, ()> {
		if idx < self.first_index {
			Err(())
		} else if idx == self.first_index {
			Ok(self.first_term)
		} else if idx > self.last_entry() {
			Err(())
		} else {
			let offs = idx - self.first_index - 1;
			Ok(self.entries[offs as usize].0)
		}
	}
}
