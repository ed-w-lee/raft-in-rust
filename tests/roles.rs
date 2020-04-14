use rafted::{NodeRole, NodeStatus, Term};

mod test_utils;
pub use test_utils::simulation::{NodeId, Simulation, SimulationOpts};
pub use test_utils::{partition, run_for_and_get_leader};

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

	sim.run_for(3000, &mut |statuses: Vec<Option<NodeStatus<NodeId>>>| {
		assert!(
			1 >= statuses
				.iter()
				.filter(|status| status.is_some())
				.map(|status| status.unwrap())
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
	sim.run_for(100000, &mut |statuses: Vec<Option<NodeStatus<NodeId>>>| {
		let leaders: Vec<NodeStatus<NodeId>> = statuses
			.into_iter()
			.filter(|status| status.is_some())
			.map(|status| status.unwrap())
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

#[test]
fn it_has_no_leaders_when_no_majority() {
	let mut sim = Simulation::try_from(SimulationOpts {
		num_nodes: 5,
		election_timeout: None,
		heartbeat_timeout: None,
		msg_delay: None,
		seed: None,
	})
	.expect("couldn't convert from given options");

	sim.stop_node(0);
	sim.stop_node(1);
	sim.stop_node(2);

	sim.run_for(10000, &mut |statuses: Vec<Option<NodeStatus<NodeId>>>| {
		assert_eq!(
			0,
			statuses
				.into_iter()
				.filter(|status| status.is_some())
				.map(|status| status.unwrap())
				.filter(|status| status.role == NodeRole::Leader)
				.count()
		);
	})
}

#[test]
fn it_chooses_new_leader_when_leader_stops() {
	let mut sim = Simulation::try_from(SimulationOpts {
		num_nodes: 5,
		election_timeout: None,
		heartbeat_timeout: None,
		msg_delay: None,
		seed: None,
	})
	.expect("couldn't convert from given options");

	let leader = run_for_and_get_leader(&mut sim, 1000);

	sim.stop_node(leader);

	let new_leader = run_for_and_get_leader(&mut sim, 1000);

	sim.start_node(leader, sim.next_event_time());

	let new_new_leader = run_for_and_get_leader(&mut sim, 1000);

	assert_ne!(leader, new_leader);
	assert_eq!(new_leader, new_new_leader);
}

#[test]
fn it_chooses_new_leader_after_partition() {
	let mut sim = Simulation::try_from(SimulationOpts {
		num_nodes: 5,
		election_timeout: None,
		heartbeat_timeout: None,
		msg_delay: None,
		seed: None,
	})
	.expect("couldn't convert from given options");

	let curr_leader = run_for_and_get_leader(&mut sim, 1000);

	partition(&mut sim, true, 5, vec![curr_leader, (curr_leader + 1) % 5]);

	sim.run_for(1000, &mut |_statuses: Vec<Option<NodeStatus<NodeId>>>| {});

	let mut new_leader: Option<NodeId> = None;
	let mut new_leader_term: Option<Term> = None;
	sim.run_for(1000, &mut |statuses: Vec<Option<NodeStatus<NodeId>>>| {
		let leaders: Vec<NodeStatus<NodeId>> = statuses
			.into_iter()
			.filter(|status| status.is_some())
			.map(|status| status.unwrap())
			.filter(|status| status.role == NodeRole::Leader)
			.collect();

		assert!(leaders.len() >= 1);

		if leaders.len() > 1 {
			assert_eq!(leaders.len(), 2);
			let tup = {
				if leaders[0].term > leaders[1].term {
					(Some(leaders[0].id), Some(leaders[0].term))
				} else {
					(Some(leaders[1].id), Some(leaders[1].term))
				}
			};
			new_leader = tup.0;
			new_leader_term = tup.1;
		}
	});

	assert!(new_leader.is_some());
	assert!(new_leader_term.is_some());
	let new_leader = new_leader.unwrap();
	let new_leader_term = new_leader_term.unwrap();
	assert_ne!(curr_leader, new_leader);

	// heal partition
	partition(&mut sim, false, 5, vec![curr_leader, (curr_leader + 1) % 5]);

	let mut final_leader: Option<NodeId> = None;
	sim.run_for(1000, &mut |statuses: Vec<Option<NodeStatus<NodeId>>>| {
		let leaders: Vec<NodeStatus<NodeId>> = statuses
			.into_iter()
			.filter(|status| status.is_some())
			.map(|status| status.unwrap())
			.filter(|status| status.role == NodeRole::Leader)
			.collect();

		assert!(leaders.len() >= 1);

		if leaders.len() == 1 {
			final_leader = Some(leaders[0].id);
			assert_eq!(leaders[0].id, new_leader);
			assert_eq!(leaders[0].term, new_leader_term);
		}
	});

	assert!(final_leader.is_some());
}
