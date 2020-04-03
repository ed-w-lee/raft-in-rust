pub trait StateMachine<Req, Step, Res> {
	fn new() -> Self;

	fn read(&self, req: &Req) -> Res;

	fn apply(&mut self, step: &Step) -> Res;
}
