use crate::serialize::{SerialStatus, Serialize};

pub struct Entry {
	pub key: u64,
	pub val: u64,
}

impl Serialize for Entry {
	fn to_bytes(&self) -> Vec<u8> {
		unimplemented!()
	}

	fn from_bytes(buf: &[u8]) -> Result<(usize, Box<Self>), SerialStatus> {
		unimplemented!()
	}
}
