use crate::serialize::Serialize;
use rafted::{PersistentData, Term};

use std::convert::TryInto;
use std::fmt::Display;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::marker::PhantomData;
use std::mem::size_of;

pub struct Storage<A, E> {
	file: File,
	_addr: PhantomData<A>,
	_entry: PhantomData<E>,
}

impl<A, E> Storage<A, E>
where
	A: Display + Serialize,
	E: Serialize,
{
	pub fn new(addr: A) -> Self {
		Self {
			file: OpenOptions::new()
				.read(true)
				.write(true)
				.create(true)
				.open(format!("/tmp/rafted_tcpmvp_{}", addr))
				.unwrap(),
			_addr: PhantomData,
			_entry: PhantomData,
		}
	}

	pub fn get_hard_state(&mut self) -> Option<PersistentData<A, E>> {
		let mut buf = vec![];
		self
			.file
			.read_to_end(&mut buf)
			.expect("couldn't read to end");

		let mut slice = buf.as_slice();
		// read term
		let bytes = slice.get(0..size_of::<Term>())?;
		let arr = bytes.try_into().ok()?;
		let current_term = u64::from_be_bytes(arr);
		slice = &slice[size_of::<Term>()..];

		// read option<a>
		let is_some: bool = if slice[0] > 0 { true } else { false };
		slice = &slice[1..];
		let voted_for = match is_some {
			true => {
				let tup = A::from_bytes(slice).ok()?;
				let (num_bytes, addr) = tup;
				slice = &slice[num_bytes..];
				Some(*addr)
			}
			false => None,
		};

		// read log
		let bytes = slice.get(0..size_of::<u64>())?;
		let arr = bytes.try_into().ok()?;
		let log_len = u64::from_be_bytes(arr);
		let mut log = Vec::with_capacity(log_len as usize);

		for _ in 0..log_len {
			let bytes = slice.get(0..size_of::<Term>())?;
			let arr = bytes.try_into().ok()?;
			let my_term = Term::from_be_bytes(arr);

			match E::from_bytes(slice) {
				Ok(tup) => {
					let (num_bytes, entry) = tup;
					slice = &slice[num_bytes..];
					log.push((my_term, *entry));
				}
				Err(_) => return None,
			}
		}

		Some(PersistentData {
			current_term,
			voted_for,
			log,
		})
	}

	pub fn store_hard_state(&mut self, hard_state: PersistentData<A, E>) -> Result<(), ()> {
		let mut buf = vec![];

		buf.extend_from_slice(&hard_state.current_term.to_be_bytes());
		match hard_state.voted_for {
			Some(addr) => {
				buf.push(1u8);
				buf.extend_from_slice(&addr.to_bytes());
			}
			None => buf.push(0u8),
		}

		buf.extend_from_slice(&(hard_state.log.len() as u64).to_be_bytes());
		for tup in hard_state.log {
			let (term, entry) = tup;
			buf.extend_from_slice(&term.to_be_bytes());
			buf.extend_from_slice(&entry.to_bytes());
		}

		self
			.file
			.seek(SeekFrom::Start(0))
			.expect("couldn't seek to beginning");
		match self.file.write_all(&buf) {
			Ok(_) => Ok(()),
			Err(_) => Err(()),
		}
	}
}
