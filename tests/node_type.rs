use rafted::{NodeRole, NodeStatus, Term};

mod simulation;
use simulation::{NodeId, Simulation, SimulationOpts};

use std::collections::HashMap;
use std::convert::TryFrom;

#[test]
fn it_has_at_most_one_leader_when_no_drop() {
	let mut sim = Simulation::try_from(SimulationOpts {
		num_nodes: 5,
		election_timeout: None,
		heartbeat_timeout: None,
		msg_delay: None,
		seed: None,
	})
	.expect("couldn't convert from given options");

	sim.run_for(3000, &mut |statuses: Vec<NodeStatus<NodeId>>| {
		assert!(
			1 >= statuses
				.iter()
				.filter(|status| status.role == NodeRole::Leader)
				.count()
		);
	})
}

#[test]
fn it_has_at_most_one_leader_for_each_term() {
	let mut sim = Simulation::try_from(SimulationOpts {
		num_nodes: 5,
		election_timeout: None,
		heartbeat_timeout: None,
		msg_delay: None,
		seed: None,
	})
	.expect("couldn't convert from given options");

	sim.set_drop_rate(20);

	let mut leader_for_term: HashMap<Term, NodeId> = HashMap::new();
	sim.run_for(100000, &mut |statuses: Vec<NodeStatus<NodeId>>| {
		let leaders: Vec<NodeStatus<NodeId>> = statuses
			.into_iter()
			.filter(|status| status.role == NodeRole::Leader)
			.collect();

		for leader in leaders {
			if leader_for_term.contains_key(&leader.term) {
				assert_eq!(
					leader.id,
					leader_for_term.get(&leader.term).unwrap().clone()
				);
			} else {
				leader_for_term.insert(leader.term, leader.id);
			}
		}
	})
}
