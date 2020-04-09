use super::simulation::{NodeId, Simulation};

use rafted::{NodeRole, NodeStatus};

pub fn run_for_and_get_leader(sim: &mut Simulation, iters: u64) -> NodeId {
	let mut curr_leader: Option<NodeId> = None;
	sim.run_for(iters, &mut |statuses| {
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

	curr_leader.unwrap()
}

pub fn partition(sim: &mut Simulation, split: bool, num_nodes: u64, nodes: Vec<NodeId>) {
	for node1 in 0..num_nodes {
		for node2 in 0..num_nodes {
			if node1 == node2 {
				continue;
			}
			if nodes.contains(&node1) != nodes.contains(&node2) {
				if split {
					sim.drop_conn(node1, node2);
				} else {
					sim.enable_conn(node1, node2);
				}
			}
		}
	}
}
