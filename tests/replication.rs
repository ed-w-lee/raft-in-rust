use rafted::message::{ClientRequest, ClientResponse};

mod test_utils;
pub use test_utils::run_for_and_get_leader;
pub use test_utils::simulation::{Simulation, SimulationOpts};

use std::convert::TryFrom;

#[test]
fn it_commits_correctly() {
	let mut sim = Simulation::try_from(SimulationOpts {
		num_nodes: 5,
		election_timeout: None,
		heartbeat_timeout: None,
		msg_delay: None,
		seed: None,
	})
	.expect("couldn't convert from given options");

	let leader = run_for_and_get_leader(&mut sim, 3000);
	sim.client_msg(leader, ClientRequest::Apply(0, 10));
	sim.client_msg(leader, ClientRequest::Apply(0, 20));
	sim.client_msg(leader, ClientRequest::Apply(0, 30));

	sim.run_for(300, &mut |_| {});

	let response = sim.recv_client_res(&0).unwrap();
	assert_eq!(response, ClientResponse::Response(10u64));
	let response = sim.recv_client_res(&0).unwrap();
	assert_eq!(response, ClientResponse::Response(20u64));
	let response = sim.recv_client_res(&0).unwrap();
	assert_eq!(response, ClientResponse::Response(30u64));
}

#[test]
fn it_commits_correctly_after_leader_down() {
	let mut sim = Simulation::try_from(SimulationOpts {
		num_nodes: 5,
		election_timeout: None,
		heartbeat_timeout: None,
		msg_delay: None,
		seed: None,
	})
	.expect("couldn't convert from given options");

	let leader = run_for_and_get_leader(&mut sim, 3000);
	sim.client_msg(leader, ClientRequest::Apply(0, 10));
	sim.client_msg(leader, ClientRequest::Apply(0, 20));
	sim.client_msg(leader, ClientRequest::Apply(0, 30));

	sim.run_for(300, &mut |_| {});

	let response = sim.recv_client_res(&0).unwrap();
	assert_eq!(response, ClientResponse::Response(10u64));
	let response = sim.recv_client_res(&0).unwrap();
	assert_eq!(response, ClientResponse::Response(20u64));
	let response = sim.recv_client_res(&0).unwrap();
	assert_eq!(response, ClientResponse::Response(30u64));

	sim.run_for(300, &mut |_| {});

	sim.stop_node(leader);

	let int_leader = run_for_and_get_leader(&mut sim, 400);

	sim.stop_node(int_leader);

	let new_leader = run_for_and_get_leader(&mut sim, 500);
	sim.client_msg(new_leader, ClientRequest::Apply(0, 40));
	sim.client_msg(new_leader, ClientRequest::Apply(0, 50));
	sim.client_msg(new_leader, ClientRequest::Apply(0, 60));

	sim.run_for(300, &mut |_| {});
	let response = sim.recv_client_res(&0).unwrap();
	assert_eq!(response, ClientResponse::Response(40u64));
	let response = sim.recv_client_res(&0).unwrap();
	assert_eq!(response, ClientResponse::Response(50u64));
	let response = sim.recv_client_res(&0).unwrap();
	assert_eq!(response, ClientResponse::Response(60u64));
}

#[test]
fn it_redirects_if_not_leader() {
	let mut sim = Simulation::try_from(SimulationOpts {
		num_nodes: 5,
		election_timeout: None,
		heartbeat_timeout: None,
		msg_delay: None,
		seed: None,
	})
	.expect("couldn't convert from given options");

	let leader = run_for_and_get_leader(&mut sim, 3000);
	let follow = (leader + 1) % 5;
	sim.client_msg(follow, ClientRequest::Apply(0, 10));

	sim.run_for(300, &mut |_| {});

	let response = sim.recv_client_res(&0).unwrap();
	assert_eq!(response, ClientResponse::Redirect(leader));
}
