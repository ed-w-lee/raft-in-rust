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

	let mut curr_leader: Option<NodeId> = None;
	sim.run_for(1000, &mut |statuses: Vec<Option<NodeStatus<NodeId>>>| {
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
	sim.stop_node(curr_leader.unwrap());

	sim.run_for(1000, &mut |_statuses: Vec<Option<NodeStatus<NodeId>>>| {});

	sim.start_node(curr_leader.unwrap(), sim.next_event_time());
	let mut new_leader: Option<NodeId> = None;
	sim.run_for(1000, &mut |statuses: Vec<Option<NodeStatus<NodeId>>>| {
		let leaders: Vec<NodeStatus<NodeId>> = statuses
			.into_iter()
			.filter(|status| status.is_some())
			.map(|status| status.unwrap())
			.filter(|status| status.role == NodeRole::Leader)
			.collect();

		if leaders.is_empty() {
			new_leader = None
		} else {
			new_leader = Some(leaders[0].id)
		}
	});

	assert!(new_leader.is_some());
	assert_ne!(curr_leader.unwrap(), new_leader.unwrap());
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

	let mut curr_leader: Option<NodeId> = None;
	sim.run_for(1000, &mut |statuses: Vec<Option<NodeStatus<NodeId>>>| {
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
	let curr_leader = curr_leader.unwrap();
	let conns_to_drop = [
		(curr_leader, (curr_leader + 2) % 5),
		(curr_leader, (curr_leader + 3) % 5),
		(curr_leader, (curr_leader + 4) % 5),
		((curr_leader + 1) % 5, (curr_leader + 2) % 5),
		((curr_leader + 1) % 5, (curr_leader + 3) % 5),
		((curr_leader + 1) % 5, (curr_leader + 4) % 5),
	];
	conns_to_drop.iter().for_each(|tup| {
		sim.drop_conn(tup.0, tup.1);
		sim.drop_conn(tup.1, tup.0);
	});

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

	conns_to_drop.iter().for_each(|tup| {
		sim.enable_conn(tup.0, tup.1);
		sim.enable_conn(tup.1, tup.0);
	});

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
