use crate::serialize::Serialize;
use rafted::{LogIndex, PersistentData, Storage, Term};

use std::fmt::{Debug, Display};
use std::fs::{copy, create_dir_all, rename, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::marker::PhantomData;

const TMP_SUFFIX: &'static str = ".tmp";

#[derive(Debug)]
pub struct FileStorage<A, E> {
	my_dir: String,
	state_file: String,
	entries_file: String,
	state_back: String,
	entries_back: String,
	entries_end: u64,
	_addr: PhantomData<A>,
	_entry: PhantomData<E>,
}

impl<A, E> FileStorage<A, E>
where
	A: Display,
{
	pub fn new(addr: A) -> Self {
		let my_dir = format!("/tmp/rafted_tcpmvp_{}", addr);
		create_dir_all(&my_dir).expect("failed to create required directory");
		let state_file = format!("{}/state", &my_dir);
		let state_back = state_file.clone() + TMP_SUFFIX;
		let entries_file = format!("{}/entries", &my_dir);
		let entries_back = entries_file.clone() + TMP_SUFFIX;

		Self {
			my_dir,
			state_file,
			entries_file,
			state_back,
			entries_back,
			entries_end: 0,
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
	fn _get_data(&mut self) -> Option<(Term, Option<A>, LogIndex, Term, Vec<(Term, Option<E>)>)> {
		// --- read state ---
		let mut state_reader = OpenOptions::new()
			.read(true)
			.write(true)
			.create(true)
			.open(&self.state_file)
			.expect("Unable to open state file");
		state_reader
			.seek(SeekFrom::Start(0))
			.expect("couldn't seek to start");

		println!("reading state");
		let mut state_buf = vec![];
		state_reader
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
		println!("state read completed successfully");

		// ---- read log ----
		let mut log_reader = OpenOptions::new()
			.read(true)
			.write(true)
			.create(true)
			.open(&self.entries_file)
			.expect("Unable to open log file");
		log_reader
			.seek(SeekFrom::Start(0))
			.expect("couldn't seek to start");

		println!("starting log read");
		let mut entries_buf = vec![];
		log_reader
			.read_to_end(&mut entries_buf)
			.expect("couldn't read to end");
		let mut slice = entries_buf.as_slice();

		// read log_len
		let tup = u64::from_bytes(slice).ok()?;
		let (to_shift, log_len) = tup;
		slice = &slice[to_shift..];

		let mut log = Vec::with_capacity(*log_len as usize);

		println!("reading {} entries", *log_len);
		for _ in 0..*log_len {
			let tup = Term::from_bytes(slice).ok()?;
			let (to_shift, my_term) = tup;
			slice = &slice[to_shift..];

			let tup = Option::<E>::from_bytes(slice).ok()?;
			let (to_shift, my_entry) = tup;
			slice = &slice[to_shift..];
			log.push((*my_term, *my_entry));
		}

		self.entries_end = (entries_buf.len() - slice.len()) as u64;
		println!("finished reading");

		Some((*curr_term, *voted_for, *first_index, *first_term, log))
	}
}

impl<'a, A, E> Storage<'a, A, E> for FileStorage<A, E>
where
	A: Debug + Serialize + 'a,
	E: Debug + Serialize + Clone + 'a,
{
	fn get_data(mut self) -> PersistentData<'a, A, E> {
		match self._get_data() {
			Some(tup) => {
				let (curr_term, voted_for, first_index, first_term, entries) = tup;
				println!("file storage read correctly");
				println!("curr_term: {:?}", curr_term);
				println!("voted_for: {:?}", voted_for);
				println!("first_index: {:?}", first_index);
				println!("first_term: {:?}", first_term);
				println!("past entries: {:?}", entries);
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

		let mut state_writer = OpenOptions::new()
			.write(true)
			.create(true)
			.open(&self.state_back)
			.expect("Unable to open state file writer");

		state_writer
			.seek(SeekFrom::Start(0))
			.expect("couldn't seek to start");
		state_writer.write_all(&buf).expect("couldn't update state");
		state_writer
			.sync_data()
			.expect("couldn't sync state_file backup successfully");

		rename(&self.state_back, &self.state_file)
			.expect("Unable to rename state_file backup to state_file");

		let dir = File::open(&self.my_dir).expect("Failed to open dir");
		dir
			.sync_data()
			.expect("Couldn't sync directory successfully");
		println!("file storage updated with term: {}", term);
	}

	fn update_entries(&mut self, start: LogIndex, entries: &[(Term, Option<E>)]) {
		println!(
			"updating file storage starting at: {} with {:?}",
			start, entries
		);

		let copied = copy(&self.entries_file, &self.entries_back).is_ok();
		let mut log_writer = OpenOptions::new()
			.read(true)
			.write(true)
			.create(!copied)
			.open(&self.entries_back)
			.expect("Unable to open log writer");

		if start == 0 {
			assert!(entries.is_empty());
			// handle empty case
			let to_write = start.to_bytes();
			log_writer
				.write_all(&to_write)
				.expect("fuck couldn't write");
			self.entries_end = to_write.len() as u64;
		} else {
			// read log_len
			let mut entries_buf = vec![];
			log_writer
				.read_to_end(&mut entries_buf)
				.expect("couldn't read to end");
			let mut slice = entries_buf.as_slice();
			let total_len = slice.len();

			let tup = u64::from_bytes(slice).expect("test");
			let (to_shift, log_len) = tup;
			slice = &slice[to_shift..];

			if entries.is_empty() {
				// case where truncating log
				assert!(start <= *log_len);
				let to_write = start.to_bytes();
				log_writer
					.seek(SeekFrom::Start(0))
					.expect("couldn't seek to start");
				log_writer
					.write_all(&to_write)
					.expect("fuck couldn't write");
				self.entries_end = to_write.len() as u64;
			}

			let curr_pos = {
				if *log_len >= start {
					// we are overwriting other entries, find where to seek to
					for _ in 0..(start - 1) {
						let tup = Term::from_bytes(slice).expect("blah");
						let (to_shift, term) = tup;
						slice = &slice[to_shift..];

						let tup = Option::<E>::from_bytes(slice).expect("fuck");
						let (to_shift, opt) = tup;
						slice = &slice[to_shift..];

						println!("iterating... read: {:?}", (term, opt));
					}

					let did_read = total_len - slice.len();
					log_writer
						.seek(SeekFrom::Start(did_read as u64))
						.expect("couldn't seek to overwrite location")
				} else {
					// we are appending to log. seek to end
					assert_eq!(start, *log_len + 1);
					log_writer
						.seek(SeekFrom::Start(self.entries_end))
						.expect("couldn't seek to append location")
				}
			};
			println!("starting write from {}", curr_pos);
			// write log entries
			let mut to_write = vec![];
			for tup in entries {
				let (term, entry) = tup;
				to_write.append(&mut term.to_bytes());
				to_write.append(&mut entry.to_bytes());
			}
			log_writer.write_all(&to_write).expect("couldn't write oof");

			// write final log length
			log_writer
				.seek(SeekFrom::Start(0))
				.expect("couldn't seek to start");
			log_writer
				.write_all(&(start + (entries.len() as u64) - 1).to_bytes())
				.expect("no write rip");

			self.entries_end = curr_pos + to_write.len() as u64;
		}
		log_writer.sync_data().expect("Couldn't sync successfully");
		rename(&self.entries_back, &self.entries_file)
			.expect("Failed to rename entries backup to actual file");

		let dir = File::open(&self.my_dir).expect("Failed to open dir");
		dir
			.sync_data()
			.expect("Couldn't sync directory successfully");
	}
}
