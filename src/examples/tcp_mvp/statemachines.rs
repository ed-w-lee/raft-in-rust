use rafted::statemachine::StateMachine;

pub struct BasicStateMachine {
	my_num: u64,
}

impl StateMachine<(), u64, u64> for BasicStateMachine {
	fn new() -> Self {
		Self { my_num: 1 }
	}

	fn read(&self, _req: &()) -> u64 {
		self.my_num
	}

	fn apply(&mut self, ent: &u64) -> u64 {
		if self.my_num >= *ent {
			0
		} else {
			self.my_num = *ent;
			self.my_num
		}
	}
}
