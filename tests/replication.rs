use rafted::{NodeRole, NodeStatus, Term};

mod simulation;
use simulation::{NodeId, Simulation, SimulationOpts};

use std::collections::HashMap;
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

	sim.run_for(3000, &mut |_statuses| {})
}
