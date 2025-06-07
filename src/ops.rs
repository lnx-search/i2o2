use std::io;
use std::io::ErrorKind;

use liburing_rs::*;

/// The [RingProbe] allows you to see what operations are currently supported
/// by the kernel running.
pub struct RingProbe {
    probe: *mut io_uring_probe,
}

impl RingProbe {
    /// Create a new probe.
    pub fn new() -> io::Result<Self> {
        let probe = unsafe { io_uring_get_probe() };

        if probe.is_null() {
            Err(io::Error::new(
                ErrorKind::Unsupported,
                "cannot probe kernel for io_uring features",
            ))
        } else {
            Ok(Self { probe })
        }
    }

    /// Returns if the op is supported or not with the current kernel.
    pub fn is_supported(&self, op: u8) -> bool {
        unsafe { io_uring_opcode_supported(self.probe, op as i32) == 1 }
    }
}

impl Drop for RingProbe {
    fn drop(&mut self) {
        if !self.probe.is_null() {
            unsafe { io_uring_free_probe(self.probe) };
        }
    }
}

pub(super) mod sealed {
    use liburing_rs::{IOSQE_FIXED_FILE, io_uring_sqe, iovec};

    pub trait RegisterOp {
        fn register_with_sqe(&self, sqe: &mut io_uring_sqe);
    }

    pub trait IntoFdTarget {
        fn into_fd_target(self) -> FixedOrFd;
    }

    pub trait IntoBufTarget {
        fn into_buf_target(self) -> FixedOrBuf;
    }

    pub enum FixedOrFd {
        Fd(std::os::fd::RawFd),
        Fixed(u16),
    }

    impl FixedOrFd {
        pub(crate) fn as_fd(&self) -> libc::c_int {
            match self {
                FixedOrFd::Fd(fd) => *fd,
                FixedOrFd::Fixed(idx) => *idx as _,
            }
        }

        pub(crate) fn sqe_flags(&self) -> u8 {
            match self {
                FixedOrFd::Fd(_) => 0,
                FixedOrFd::Fixed(_) => IOSQE_FIXED_FILE as u8,
            }
        }
    }

    pub enum FixedOrBuf {
        Buf(iovec),
        Fixed(u16),
    }
}

pub trait RingOp: sealed::RegisterOp {
    /// The op code which can be used to probe the availability of the op
    /// with the kernel.
    const CODE: io_uring_op;

    /// Configure the IO priority the op should have on the ring.
    fn set_io_priority(&mut self, priority: u16);

    /// Informs io_uring that the operation will always (or nearly always) block,
    /// and it should not attempt to complete the operation in a non-blocking way first.
    ///
    /// Internally this sets `IOSQE_ASYNC`.
    ///
    /// > Normal operation for io_uring is to try and issue an sqe as
    /// > non-blocking first, and if that fails, execute it in an
    /// > async manner. To support more efficient overlapped
    /// > operation of requests that the application knows/assumes
    /// > will always (or most of the time) block, the application
    /// > can ask for an sqe to be issued async from the start. Note
    /// > that this flag immediately causes the SQE to be offloaded
    /// > to an async helper thread with no initial non-blocking
    /// > attempt.  This may be less efficient and should not be used
    /// > liberally or without understanding the performance and
    /// > efficiency tradeoffs.
    fn will_block(&mut self);
}

/// Describes the resource being affected by the IO op.
pub mod target {
    /// A standard file descriptor.
    pub struct Fd(pub std::os::fd::RawFd);

    /// A previously registered buffer or file descriptor index.
    pub struct Fixed(pub u16);

    /// A standard buffer pointer.
    pub struct Buf {
        ptr: *mut u8,
        len: usize,
    }
}

macro_rules! impl_op_boilerplate {
    (
        code = $code:ident,
        $op:ident
    ) => {
        impl RingOp for $op {
            const CODE: io_uring_op = $code;

            fn set_io_priority(&mut self, priority: u16) {
                self.io_prio = priority;
            }

            fn will_block(&mut self) {
                self.additional_flags |= IOSQE_ASYNC as u8;
            }
        }
    };
}

/// Invokes [IORING_OP_NOP](https://man.archlinux.org/man/io_uring_enter.2.en#IORING_OP_NOP).
pub struct Nop {
    io_prio: u16,
    additional_flags: u8,
}

impl sealed::RegisterOp for Nop {
    fn register_with_sqe(&self, sqe: &mut io_uring_sqe) {
        unsafe { io_uring_prep_nop(sqe) };
        sqe.flags |= self.additional_flags;
        sqe.ioprio = self.io_prio;
    }
}

impl_op_boilerplate!(code = IORING_OP_NOP, Nop);

/// Invokes [IORING_OP_FADVISE](https://man.archlinux.org/man/io_uring_enter.2.en#IORING_OP_FADVISE).
pub struct Fadvise {
    fd: sealed::FixedOrFd,
    offset: u64,
    len: u64,
    advice: i32,
    io_prio: u16,
    additional_flags: u8,
}

impl sealed::RegisterOp for Fadvise {
    fn register_with_sqe(&self, sqe: &mut io_uring_sqe) {
        unsafe {
            io_uring_prep_fadvise64(
                sqe,
                self.fd.as_fd(),
                self.offset,
                self.len as i64,
                self.advice,
            )
        };
        sqe.flags |= self.fd.sqe_flags();
        sqe.flags |= self.additional_flags;
        sqe.ioprio = self.io_prio;
    }
}

impl_op_boilerplate!(code = IORING_OP_FADVISE, Fadvise);

/// Invokes [IORING_OP_FALLOCATE](https://man.archlinux.org/man/io_uring_enter.2.en#IORING_OP_FALLOCATE).
pub struct Fallocate {
    fd: sealed::FixedOrFd,
    mode: i32,
    offset: u64,
    len: u64,
    io_prio: u16,
    additional_flags: u8,
}

impl sealed::RegisterOp for Fallocate {
    fn register_with_sqe(&self, sqe: &mut io_uring_sqe) {
        unsafe {
            io_uring_prep_fallocate(
                sqe,
                self.fd.as_fd(),
                self.mode,
                self.offset,
                self.len,
            )
        };
        sqe.flags |= self.fd.sqe_flags();
        sqe.flags |= self.additional_flags;
        sqe.ioprio = self.io_prio;
    }
}

impl_op_boilerplate!(code = IORING_OP_FALLOCATE, Fallocate);

pub enum FsyncMode {}

/// Invokes [IORING_OP_FSYNC](https://man.archlinux.org/man/io_uring_enter.2.en#IORING_OP_FSYNC).
///
/// The `fsync_mode` determines if this operation performs an full fsync or fdatasync equivalent
/// operations.
///
/// See the descriptions of `O_SYNC` and `O_DSYNC` in the open(2) manual page for more information.
pub struct Fsync {
    fd: sealed::FixedOrFd,
    mode: u32,
    io_prio: u16,
    additional_flags: u8,
}

impl Fsync {
    pub const MODE_FSYNC: u32 = 0;
    pub const MODE_FDATASYNC: u32 = IORING_FSYNC_DATASYNC;
}

impl sealed::RegisterOp for Fsync {
    fn register_with_sqe(&self, sqe: &mut io_uring_sqe) {
        unsafe { io_uring_prep_fsync(sqe, self.fd.as_fd(), self.mode) };
        sqe.flags |= self.fd.sqe_flags();
        sqe.flags |= self.additional_flags;
        sqe.ioprio = self.io_prio;
    }
}

impl_op_boilerplate!(code = IORING_OP_FSYNC, Fsync);

/// Invokes [IORING_OP_FTRUNCATE](https://man.archlinux.org/man/io_uring_enter.2.en#IORING_OP_FTRUNCATE).
pub struct Ftruncate {
    fd: sealed::FixedOrFd,
    len: u64,
    io_prio: u16,
    additional_flags: u8,
}

impl sealed::RegisterOp for Ftruncate {
    fn register_with_sqe(&self, sqe: &mut io_uring_sqe) {
        unsafe { io_uring_prep_ftruncate(sqe, self.fd.as_fd(), self.len as _) };
        sqe.flags |= self.fd.sqe_flags();
        sqe.flags |= self.additional_flags;
        sqe.ioprio = self.io_prio;
    }
}

impl_op_boilerplate!(code = IORING_OP_FTRUNCATE, Ftruncate);

/// Invokes [IORING_OP_MADVISE](https://man.archlinux.org/man/io_uring_enter.2.en#IORING_OP_MADVISE).
pub struct Madvise {
    addr: *mut u8,
    len: u64,
    advice: i32,
    io_prio: u16,
    additional_flags: u8,
}

impl sealed::RegisterOp for Madvise {
    fn register_with_sqe(&self, sqe: &mut io_uring_sqe) {
        unsafe {
            io_uring_prep_madvise64(
                sqe,
                self.addr as *mut _,
                self.len as i64,
                self.advice,
            )
        };
        sqe.flags |= self.additional_flags;
        sqe.ioprio = self.io_prio;
    }
}

impl_op_boilerplate!(code = IORING_OP_MADVISE, Madvise);

/// Invokes [IORING_OP_READ](https://man.archlinux.org/man/io_uring_enter.2.en#IORING_OP_READ).
pub struct Read {
    fd: sealed::FixedOrFd,
    buf_ptr: *mut u8,
    buf_len: u32,
    offset: u64,
    io_prio: u16,
    additional_flags: u8,
}

impl sealed::RegisterOp for Read {
    fn register_with_sqe(&self, sqe: &mut io_uring_sqe) {
        unsafe {
            io_uring_prep_read(
                sqe,
                self.fd.as_fd(),
                self.buf_ptr as *mut _,
                self.buf_len,
                self.offset,
            )
        };
        sqe.flags |= self.fd.sqe_flags();
        sqe.flags |= self.additional_flags;
        sqe.ioprio = self.io_prio;
    }
}

impl_op_boilerplate!(code = IORING_OP_READ, Read);

/// Invokes [IORING_OP_READ_FIXED](https://man.archlinux.org/man/io_uring_enter.2.en#IORING_OP_READ_FIXED).
pub struct ReadFixed {
    fd: sealed::FixedOrFd,
    buf_addr: *mut u8,
    buf_len: u32,
    buf_index: u32,
    offset: u64,
    io_prio: u16,
    additional_flags: u8,
}

impl sealed::RegisterOp for ReadFixed {
    fn register_with_sqe(&self, sqe: &mut io_uring_sqe) {
        unsafe {
            io_uring_prep_read_fixed(
                sqe,
                self.fd.as_fd(),
                self.buf_addr as *mut _,
                self.buf_len,
                self.offset,
                self.buf_index as i32,
            )
        };
        sqe.flags |= self.fd.sqe_flags();
        sqe.flags |= self.additional_flags;
        sqe.ioprio = self.io_prio;
    }
}

impl_op_boilerplate!(code = IORING_OP_READ_FIXED, ReadFixed);

/// Invokes [IORING_OP_WRITE](https://man.archlinux.org/man/io_uring_enter.2.en#IORING_OP_WRITE).
pub struct Write {
    fd: sealed::FixedOrFd,
    buf_addr: *mut u8,
    buf_len: u32,
    offset: u64,
    io_prio: u16,
    additional_flags: u8,
}

impl sealed::RegisterOp for Write {
    fn register_with_sqe(&self, sqe: &mut io_uring_sqe) {
        unsafe {
            io_uring_prep_write(
                sqe,
                self.fd.as_fd(),
                self.buf_addr as *mut _,
                self.buf_len,
                self.offset,
            )
        };
        sqe.flags |= self.fd.sqe_flags();
        sqe.flags |= self.additional_flags;
        sqe.ioprio = self.io_prio;
    }
}

impl_op_boilerplate!(code = IORING_OP_WRITE, Write);

/// Invokes [IORING_OP_WRITE_FIXED](https://man.archlinux.org/man/io_uring_enter.2.en#IORING_OP_WRITE_FIXED).
pub struct WriteFixed {
    fd: sealed::FixedOrFd,
    buf_addr: *mut u8,
    buf_len: u32,
    buf_index: u32,
    offset: u64,
    io_prio: u16,
    additional_flags: u8,
}

impl sealed::RegisterOp for WriteFixed {
    fn register_with_sqe(&self, sqe: &mut io_uring_sqe) {
        unsafe {
            io_uring_prep_write_fixed(
                sqe,
                self.fd.as_fd(),
                self.buf_addr as *mut _,
                self.buf_len,
                self.offset,
                self.buf_index as i32,
            )
        };
        sqe.flags |= self.fd.sqe_flags();
        sqe.flags |= self.additional_flags;
        sqe.ioprio = self.io_prio;
    }
}

impl_op_boilerplate!(code = IORING_OP_WRITE_FIXED, WriteFixed);
