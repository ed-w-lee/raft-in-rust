use rafted::NodeId;

use libc::{c_int, nfds_t, poll, pollfd, POLLIN};
use std::io;
use std::io::prelude::*;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::os::unix::io::*;

fn rpoll(fds: &mut [libc::pollfd], timeout: libc::c_int) -> Option<i32> {
    let result = unsafe {
        libc::poll(
            &mut fds[0] as *mut libc::pollfd,
            fds.len() as libc::nfds_t,
            timeout,
        )
    };
    if result >= 0 {
        Some(result)
    } else {
        None
    }
}

fn main() {
    let x: NodeId = 10;
    println!("{}", x);

    let addrs = [
        SocketAddr::from(([127, 0, 0, 1], 4242)),
        // SocketAddr::from(([127, 0, 0, 1], 8282)),
    ];

    let listener = TcpListener::bind(&addrs[..]).unwrap();
    listener
        .set_nonblocking(true)
        .expect("Cannot set non-blocking");
    let mypollfd = libc::pollfd {
        fd: listener.as_raw_fd(),
        events: POLLIN,
        revents: 0,
    };

    for stream in listener.incoming() {
        match stream {
            Ok(mut s) => {
                let mut buf = String::new();
                s.read_to_string(&mut buf).unwrap();
                println!("{}", buf);
            }
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                match rpoll(&mut [mypollfd], 5000) {
                    Some(n) => {
                        if n == 0 {
                            println!("didn't receive anything");
                        } else {
                            continue;
                        }
                    }
                    None => panic!("encountered some error with poll"),
                }
            }
            Err(e) => panic!("encountered IO error: {}", e),
        }
    }
}
