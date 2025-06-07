use std::io;

use liburing_rs::*;

macro_rules! check_err {
    ($status:expr) => {{
        if $status < 0 {
            return Err(io::Error::from_raw_os_error(-$status));
        }
    }};
}

struct IoRing {
    ring: io_uring,
    closed: bool,
}

impl IoRing {
    fn new(queue_size: u32, flags: u32) -> io::Result<Self> {
        let probe = super::ops::RingProbe::new()?;

        let mut ring = unsafe { std::mem::zeroed::<io_uring>() };

        let result = unsafe { io_uring_queue_init(queue_size, &raw mut ring, flags) };
        check_err!(result);

        let result = unsafe { io_uring_register_ring_fd(&raw mut ring) };
        check_err!(result);

        Ok(Self {
            ring,
            closed: false,
        })
    }

    fn get_available_sqe(&mut self) -> Option<&mut io_uring_sqe> {
        unsafe {
            let sqe = io_uring_get_sqe(&raw mut self.ring);
            sqe.as_mut()
        }
    }
}

impl Drop for IoRing {
    fn drop(&mut self) {
        if !self.closed {
            let result = unsafe { io_uring_close_ring_fd(&raw mut self.ring) };
            if result < 0 {
                let err = io::Error::from_raw_os_error(-result);
                tracing::error!(error = %err, "cannot shutdown ring");
            } else {
                self.closed = true;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_ring_creation() {
        let _ring = IoRing::new(64, 0).expect("ring should be created ok");
    }
}
