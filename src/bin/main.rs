use rafted::NodeId;

use libc::{self, POLLIN};
use std::cmp::min;
use std::collections::HashMap;
use std::io;
use std::io::prelude::*;
use std::io::Error;
use std::mem::MaybeUninit;
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::os::unix::io::*;
use std::ptr;
use std::thread;
use std::time::{Duration, Instant};

const ELECTION_TIMEOUT: u64 = 5000;
const MAX_FDS: usize = 900;

fn get_all_pollfds(
    pollfds: &mut [MaybeUninit<libc::pollfd>; MAX_FDS],
    curr_len: usize,
    other_fds: &HashMap<RawFd, NodeId>,
    client_fds: &HashMap<RawFd, (bool, TcpStream)>,
) -> (usize, usize) {
    // TODO: maybe make this into a struct for simpler update logic and whatnot
    for i in 1..curr_len {
        unsafe {
            ptr::drop_in_place(pollfds[i].as_mut_ptr());
        }
    }

    let other_pollfds: Vec<libc::pollfd> = other_fds
        .iter()
        .map(|(k, _v)| k)
        .map(|&k| libc::pollfd {
            fd: k,
            events: POLLIN,
            revents: 0,
        })
        .collect();
    let client_pollfds: Vec<libc::pollfd> = client_fds
        .iter()
        .filter(|(_k, v)| v.0)
        .map(|(k, _v)| k)
        .map(|&k| libc::pollfd {
            fd: k,
            events: POLLIN,
            revents: 0,
        })
        .collect();

    let total_len = 1 + other_pollfds.len() + client_pollfds.len();
    // check isn't really needed, since array accesses are bounded
    if total_len > MAX_FDS {
        panic!("hit limit on file descriptors");
    }

    for i in 0..other_pollfds.len() {
        println!("{} {}", i + 1, other_pollfds[i].fd);
        *(&mut pollfds[i + 1]) = MaybeUninit::new(other_pollfds[i]);
    }
    for i in 0..client_pollfds.len() {
        println!("{} {}", i + other_pollfds.len() + 1, client_pollfds[i].fd);
        *(&mut pollfds[i + other_pollfds.len() + 1]) = MaybeUninit::new(client_pollfds[i]);
    }

    let other_end = 1 + other_pollfds.len();
    (other_end, total_len)
}

fn pollfd_readable(pollfd: &libc::pollfd) -> bool {
    println!("revents: {}", pollfd.revents);
    pollfd.revents & POLLIN != 0
}

fn main() {
    let other_addrs = [SocketAddr::from(([127, 0, 0, 1], 8484))];
    let addrs = [SocketAddr::from(([127, 0, 0, 1], 4242))];
    let listener = TcpListener::bind(&addrs[..]).unwrap();

    listener
        .set_nonblocking(true)
        .expect("Cannot set non-blocking");

    let listener_fd = libc::pollfd {
        fd: listener.as_raw_fd(),
        events: POLLIN,
        revents: 0,
    };
    let mut other_fds: HashMap<RawFd, NodeId> = HashMap::new();
    let mut other_streams: HashMap<NodeId, Option<TcpStream>> = HashMap::new();
    let mut client_fds: HashMap<RawFd, (bool, TcpStream)> = HashMap::new();

    let mut pollfds: [MaybeUninit<libc::pollfd>; MAX_FDS] =
        unsafe { MaybeUninit::uninit().assume_init() };
    let mut is_pollfd_changed = true;
    let mut curr_len = 1;
    let mut other_end = 1;

    *(&mut pollfds[0]) = MaybeUninit::new(listener_fd);

    let mut deadline = Instant::now()
        .checked_add(Duration::from_millis(ELECTION_TIMEOUT))
        .unwrap();

    loop {
        if is_pollfd_changed {
            let tup = get_all_pollfds(&mut pollfds, curr_len, &other_fds, &client_fds);
            other_end = tup.0;
            curr_len = tup.1;
            is_pollfd_changed = false;
        }
        println!("curr_lens: {}, {}", other_end, curr_len);
        thread::sleep(Duration::from_secs(1));

        match deadline.checked_duration_since(Instant::now()) {
            Some(timeout) => {
                println!("time til election timeout: {:?}", timeout);
                let result = unsafe {
                    libc::poll(
                        pollfds[0].as_mut_ptr(),
                        curr_len as u64,
                        min(ELECTION_TIMEOUT as i32, timeout.as_millis() as i32),
                    )
                };
                println!("result: {}", result);
                if result < 0 {
                    panic!("poll error: {}", Error::last_os_error());
                } else if result > 0 {
                    // listener
                    match listener.accept() {
                        Ok((stream, addr)) => {
                            stream
                                .set_nonblocking(true)
                                .expect("stream.set_nonblocking failed");
                            println!("{}", addr);
                            is_pollfd_changed = true;
                            if other_addrs.contains(&addr) {
                                println!("another node: {}", stream.as_raw_fd());
                                other_fds.insert(stream.as_raw_fd(), addr);
                                other_streams.insert(addr, Some(stream));
                            } else {
                                println!("client: {}", stream.as_raw_fd());
                                client_fds.insert(stream.as_raw_fd(), (true, stream));
                            }
                        }
                        Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                            println!("nothing!");
                        }
                        Err(e) => panic!("encountered IO error: {}", e),
                    }

                    // other nodes
                    for other_pollfd in pollfds[1..other_end].iter() {
                        let other_pollfd = unsafe { &*other_pollfd.as_ptr() };
                        if pollfd_readable(other_pollfd) {
                            let mut buf: Vec<u8> = vec![];
                            let addr = other_fds.get(&other_pollfd.fd).unwrap();
                            if let Some(ref mut stream) = other_streams.get_mut(addr).unwrap() {
                                match stream.read_to_end(&mut buf) {
                                    Ok(_) => {
                                        // delete nod
                                    }
                                    Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                                        println!("other node --\n {:?}", buf);
                                    }
                                    Err(e) => panic!("encountered IO error: {}", e),
                                }
                                println!("other node --\n {:?}", buf);
                            }
                        }
                    }

                    // client nodes
                    for client_pollfd in pollfds[other_end..curr_len].iter() {
                        let client_pollfd = unsafe { &*client_pollfd.as_ptr() };
                        if pollfd_readable(client_pollfd) {
                            let mut buf: Vec<u8> = vec![];
                            if let Some(ref mut stream) = client_fds.get_mut(&client_pollfd.fd) {
                                match stream.1.read_to_end(&mut buf) {
                                    Ok(_) => {
                                        println!("Watashi wa done-desu");
                                        stream.1.shutdown(Shutdown::Read).expect("shutdown failed");
                                        stream.0 = false;
                                        is_pollfd_changed = true;
                                    }
                                    Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {}
                                    Err(e) => panic!("encountered IO error: {}", e),
                                }
                                println!("client -- \n{:?}", buf);
                            }
                        }
                    }
                }
                deadline = Instant::now()
                    .checked_add(Duration::from_millis(ELECTION_TIMEOUT))
                    .unwrap();
            }
            None => {
                // election timeout
                panic!("election timeout!");
            }
        }
    }
}
