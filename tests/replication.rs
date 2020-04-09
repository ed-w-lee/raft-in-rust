use rafted::message::ClientRequest;
use rafted::{NodeRole, NodeStatus};

mod simulation;
use simulation::{ClientAddr, Entry, NodeId, Simulation, SimulationOpts};

use std::convert::TryFrom;

#[test]
fn it_replicates_correctly() {
	let mut sim = Simulation::try_from(SimulationOpts {
		num_nodes: 5,
		election_timeout: None,
		heartbeat_timeout: None,
		msg_delay: None,
		seed: None,
	})
	.expect("couldn't convert from given options");

	let mut curr_leader: Option<NodeId> = None;
	sim.run_for(3000, &mut |statuses| {
		let leaders: Vec<NodeStatus<NodeId>> = statuses
			.into_iter()
			.filter(|status| status.is_some())
			.map(|status| status.unwrap())
			.filter(|status| status.role == NodeRole::Leader)
			.collect();

		if leaders.is_empty() {
			curr_leader = None
		} else {
			curr_leader = Some(leaders[0].id)
		}
	});
	assert!(curr_leader.is_some());
	let leader = curr_leader.unwrap();
	sim.client_msg(leader, ClientRequest::<ClientAddr, (), Entry>::Apply(0, 10));
	sim.client_msg(leader, ClientRequest::<ClientAddr, (), Entry>::Apply(0, 20));
	sim.client_msg(leader, ClientRequest::<ClientAddr, (), Entry>::Apply(0, 30));

	sim.run_for(300, &mut |statuses| {
		let leaders: Vec<NodeStatus<NodeId>> = statuses
			.into_iter()
			.filter(|status| status.is_some())
			.map(|status| status.unwrap())
			.filter(|status| status.role == NodeRole::Leader)
			.collect();

		if leaders.is_empty() {
			curr_leader = None
		} else {
			curr_leader = Some(leaders[0].id)
		}
	});
}
