use std::fmt::Formatter;
use std::io;
use std::io::ErrorKind;

pub use liburing_rs::iovec;
use liburing_rs::*;

use crate::opcode::sealed::IntoFdTarget;

macro_rules! validate_flag {
    ($flags:expr, $flag:expr, $check:expr, $version:expr) => {
        if ($flags & $flag) != 0 && !$check() {
            return Err(io::Error::new(
                ErrorKind::Unsupported,
                format!(
                    "kernel version v{}+ is required to use {}",
                    $version,
                    stringify!($flag),
                ),
            ));
        }
    };
}

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
    pub fn is_supported(&self, op: u32) -> bool {
        unsafe { io_uring_opcode_supported(self.probe, op as i32) == 1 }
    }

    /// Validated the provided set of ring setup flags is supported by the current kernel.
    pub fn validate_ring_setup_flags(&self, flags: u32) -> io::Result<()> {
        validate_flag!(
            flags,
            IORING_SETUP_SUBMIT_ALL,
            || self.is_kernel_v5_18_or_newer(),
            "5.18"
        );
        validate_flag!(
            flags,
            IORING_SETUP_COOP_TASKRUN,
            || self.is_kernel_v5_19_or_newer(),
            "5.19"
        );
        validate_flag!(
            flags,
            IORING_SETUP_SINGLE_ISSUER,
            || self.is_kernel_v6_0_or_newer(),
            "6.0"
        );
        validate_flag!(
            flags,
            IORING_SETUP_DEFER_TASKRUN,
            || self.is_kernel_v6_1_or_newer(),
            "6.1"
        );
        Ok(())
    }

    /// Returns if the current kernel is v5.15+
    pub fn is_kernel_v5_15_or_newer(&self) -> bool {
        #[cfg(test)]
        fail::fail_point!("kernel_v5_13", |_| false);
        self.is_supported(IORING_OP_OPENAT2)
    }

    /// Returns if the current kernel is v5.18+
    pub fn is_kernel_v5_18_or_newer(&self) -> bool {
        #[cfg(test)]
        {
            fail::fail_point!("kernel_v5_13", |_| false);
            fail::fail_point!("kernel_v5_15", |_| false);
        }
        self.is_supported(IORING_OP_MSG_RING)
    }

    /// Returns if the current kernel is v5.19+
    pub fn is_kernel_v5_19_or_newer(&self) -> bool {
        #[cfg(test)]
        {
            eprintln!("{:?}", std::env::var("FAILPOINTS"));
            fail::fail_point!("kernel_v5_13", |_| false);
            fail::fail_point!("kernel_v5_15", |_| false);
            fail::fail_point!("kernel_v5_18", |_| false);
        }
        self.is_supported(IORING_OP_SOCKET)
    }

    /// Returns if the current kernel is v6.0+
    pub fn is_kernel_v6_0_or_newer(&self) -> bool {
        #[cfg(test)]
        {
            eprintln!("{:?}", std::env::var("FAILPOINTS"));
            fail::fail_point!("kernel_v5_13", |_| false);
            fail::fail_point!("kernel_v5_15", |_| false);
            fail::fail_point!("kernel_v5_18", |_| false);
            fail::fail_point!("kernel_v5_19", |_| false);
        }
        self.is_supported(IORING_OP_SEND_ZC)
    }

    /// Returns if the current kernel is v6.1+
    pub fn is_kernel_v6_1_or_newer(&self) -> bool {
        #[cfg(test)]
        {
            eprintln!("{:?}", std::env::var("FAILPOINTS"));
            fail::fail_point!("kernel_v5_13", |_| false);
            fail::fail_point!("kernel_v5_15", |_| false);
            fail::fail_point!("kernel_v5_18", |_| false);
            fail::fail_point!("kernel_v5_19", |_| false);
            fail::fail_point!("kernel_v6_0", |_| false);
        }
        self.is_supported(IORING_OP_SENDMSG_ZC)
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
    use liburing_rs::{IOSQE_FIXED_FILE, io_uring_sqe};

    pub trait RegisterOp {
        fn register_with_sqe(&self, sqe: &mut io_uring_sqe);
    }

    pub trait IntoFdTarget {
        fn into_fd_target(self) -> FixedOrFd;
    }

    pub enum FixedOrFd {
        Fd(std::os::fd::RawFd),
        Fixed(u32),
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
    use crate::opcode::sealed::FixedOrFd;

    /// A standard file descriptor.
    pub struct Fd(pub std::os::fd::RawFd);

    impl super::sealed::IntoFdTarget for Fd {
        fn into_fd_target(self) -> FixedOrFd {
            FixedOrFd::Fd(self.0)
        }
    }

    /// A previously registered buffer or file descriptor index.
    pub struct Fixed(pub u32);

    impl super::sealed::IntoFdTarget for Fixed {
        fn into_fd_target(self) -> FixedOrFd {
            FixedOrFd::Fixed(self.0)
        }
    }
}

macro_rules! impl_op_boilerplate {
    (
        code = $code:ident,
        $op:ident
    ) => {
        impl std::fmt::Debug for $op {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                write!(
                    f,
                    "Op(code={}, io_prio={}, flags={})",
                    $code, self.io_prio, self.additional_flags
                )
            }
        }

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

/// An enum of all supported IO ops.
pub enum AnyOp {
    Nop(Nop),
    Fadvise(Fadvise),
    Fallocate(Fallocate),
    Fsync(Fsync),
    Ftruncate(Ftruncate),
    Read(Read),
    ReadFixed(ReadFixed),
    Write(Write),
    WriteFixed(WriteFixed),
    Madvise(Madvise),
    Cmd80(UringCmd80),
}

impl AnyOp {
    #[inline]
    pub(super) fn requires_size128(&self) -> bool {
        matches!(self, AnyOp::Cmd80(_))
    }
}

impl sealed::RegisterOp for AnyOp {
    fn register_with_sqe(&self, sqe: &mut io_uring_sqe) {
        match self {
            AnyOp::Nop(op) => op.register_with_sqe(sqe),
            AnyOp::Fadvise(op) => op.register_with_sqe(sqe),
            AnyOp::Fallocate(op) => op.register_with_sqe(sqe),
            AnyOp::Fsync(op) => op.register_with_sqe(sqe),
            AnyOp::Ftruncate(op) => op.register_with_sqe(sqe),
            AnyOp::Read(op) => op.register_with_sqe(sqe),
            AnyOp::ReadFixed(op) => op.register_with_sqe(sqe),
            AnyOp::Write(op) => op.register_with_sqe(sqe),
            AnyOp::WriteFixed(op) => op.register_with_sqe(sqe),
            AnyOp::Madvise(op) => op.register_with_sqe(sqe),
            AnyOp::Cmd80(op) => op.register_with_sqe(sqe),
        }
    }
}

unsafe impl Send for AnyOp {}

macro_rules! impl_from {
    ($t:ident, $op:ident) => {
        impl From<$t> for $op {
            fn from(value: $t) -> Self {
                Self::$t(value)
            }
        }
    };
}

impl_from!(Nop, AnyOp);
impl_from!(Fadvise, AnyOp);
impl_from!(Fallocate, AnyOp);
impl_from!(Fsync, AnyOp);
impl_from!(Ftruncate, AnyOp);
impl_from!(Read, AnyOp);
impl_from!(ReadFixed, AnyOp);
impl_from!(Write, AnyOp);
impl_from!(WriteFixed, AnyOp);
impl_from!(Madvise, AnyOp);

/// Invokes [IORING_OP_NOP](https://man.archlinux.org/man/io_uring_enter.2.en#IORING_OP_NOP).
pub struct Nop {
    io_prio: u16,
    additional_flags: u8,
}

impl Nop {
    /// Creates a new [Nop] operation.
    pub fn new() -> Self {
        Self {
            io_prio: 0,
            additional_flags: 0,
        }
    }
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

impl Fadvise {
    /// Creates a new [Fadvise] op.
    pub fn new(fd: impl IntoFdTarget, offset: u64, len: u64, advice: i32) -> Self {
        Self {
            fd: fd.into_fd_target(),
            offset,
            len,
            advice,
            io_prio: 0,
            additional_flags: 0,
        }
    }
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

impl Fallocate {
    /// Creates a new [Fallocate] op.
    pub fn new(fd: impl IntoFdTarget, mode: i32, offset: u64, len: u64) -> Self {
        Self {
            fd: fd.into_fd_target(),
            mode,
            offset,
            len,
            io_prio: 0,
            additional_flags: 0,
        }
    }
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

/// The [Fsync] op mode to use.
pub enum FSyncMode {
    /// Equivalent of [File::sync_all](std::fs::File::sync_all).
    Full,
    /// Equivalent of [File::sync_data](std::fs::File::sync_data).
    Data,
}

/// Invokes [IORING_OP_FSYNC](https://man.archlinux.org/man/io_uring_enter.2.en#IORING_OP_FSYNC).
///
/// The `fsync_mode` determines if this operation performs a full fsync or fdatasync equivalent
/// operations.
///
/// See the descriptions of `O_SYNC` and `O_DSYNC` in the open(2) manual page for more information.
pub struct Fsync {
    fd: sealed::FixedOrFd,
    mode: FSyncMode,
    io_prio: u16,
    additional_flags: u8,
}

impl Fsync {
    /// Creates a new [Fsync] op.
    pub fn new(fd: impl IntoFdTarget, mode: FSyncMode) -> Self {
        Self {
            fd: fd.into_fd_target(),
            mode,
            io_prio: 0,
            additional_flags: 0,
        }
    }
}

impl sealed::RegisterOp for Fsync {
    fn register_with_sqe(&self, sqe: &mut io_uring_sqe) {
        let mode = match self.mode {
            FSyncMode::Full => 0,
            FSyncMode::Data => IORING_FSYNC_DATASYNC,
        };

        unsafe { io_uring_prep_fsync(sqe, self.fd.as_fd(), mode) };
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

impl Ftruncate {
    /// Creates a new [Ftruncate] op.
    pub fn new(fd: impl IntoFdTarget, len: u64) -> Self {
        Self {
            fd: fd.into_fd_target(),
            len,
            io_prio: 0,
            additional_flags: 0,
        }
    }
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

impl Madvise {
    /// Creates a new [Madvise] op.
    pub fn new(addr: *mut u8, len: u64, advice: i32) -> Self {
        Self {
            addr,
            len,
            advice,
            io_prio: 0,
            additional_flags: 0,
        }
    }
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
    buf_addr: *mut u8,
    buf_len: u32,
    offset: u64,
    io_prio: u16,
    additional_flags: u8,
}

impl Read {
    /// Creates a new [Read] op.
    pub fn new(
        fd: impl IntoFdTarget,
        buf_addr: *mut u8,
        buf_len: usize,
        offset: u64,
    ) -> Self {
        Self {
            fd: fd.into_fd_target(),
            buf_addr,
            buf_len: buf_len as u32,
            offset,
            io_prio: 0,
            additional_flags: 0,
        }
    }
}

impl sealed::RegisterOp for Read {
    fn register_with_sqe(&self, sqe: &mut io_uring_sqe) {
        unsafe {
            io_uring_prep_read(
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

impl ReadFixed {
    /// Creates a new [ReadFixed] op.
    ///
    /// The provided `buf_addr` must be the same buffer that was registered
    /// with the ring  with the provided `buf_index`.
    pub fn new(
        fd: impl IntoFdTarget,
        buf_addr: *mut u8,
        buf_len: usize,
        buf_index: u32,
        offset: u64,
    ) -> Self {
        Self {
            fd: fd.into_fd_target(),
            buf_addr,
            buf_len: buf_len as u32,
            buf_index,
            offset,
            io_prio: 0,
            additional_flags: 0,
        }
    }
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
    buf_addr: *const u8,
    buf_len: u32,
    offset: u64,
    io_prio: u16,
    additional_flags: u8,
}

impl Write {
    /// Creates a new [Write] op.
    pub fn new(
        fd: impl IntoFdTarget,
        buf_addr: *const u8,
        buf_len: usize,
        offset: u64,
    ) -> Self {
        Self {
            fd: fd.into_fd_target(),
            buf_addr,
            buf_len: buf_len as u32,
            offset,
            io_prio: 0,
            additional_flags: 0,
        }
    }
}

impl sealed::RegisterOp for Write {
    fn register_with_sqe(&self, sqe: &mut io_uring_sqe) {
        unsafe {
            io_uring_prep_write(
                sqe,
                self.fd.as_fd(),
                self.buf_addr as *const _,
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
    buf_addr: *const u8,
    buf_len: u32,
    buf_index: u32,
    offset: u64,
    io_prio: u16,
    additional_flags: u8,
}

impl WriteFixed {
    /// Creates a new [WriteFixed] op.
    ///
    /// The provided `buf_addr` must be the same buffer that was registered
    /// with the ring  with the provided `buf_index`.
    pub fn new(
        fd: impl IntoFdTarget,
        buf_addr: *const u8,
        buf_len: usize,
        buf_index: u32,
        offset: u64,
    ) -> Self {
        Self {
            fd: fd.into_fd_target(),
            buf_addr,
            buf_len: buf_len as u32,
            buf_index,
            offset,
            io_prio: 0,
            additional_flags: 0,
        }
    }
}

impl sealed::RegisterOp for WriteFixed {
    fn register_with_sqe(&self, sqe: &mut io_uring_sqe) {
        unsafe {
            io_uring_prep_write_fixed(
                sqe,
                self.fd.as_fd(),
                self.buf_addr as *const _,
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

/// Invokes [IORING_OP_URING_CMD](https://man.archlinux.org/man/io_uring_enter.2.en#IORING_OP_URING_CMD).
pub struct UringCmd80 {
    fd: sealed::FixedOrFd,
    cmd_op: u32,
    cmd: [u8; 80],
    io_prio: u16,
    additional_flags: u8,
}

impl UringCmd80 {
    /// Creates a new [UringCmd80] op.
    pub fn new(fd: impl IntoFdTarget, cmd_op: u32, cmd: [u8; 80]) -> Self {
        Self {
            fd: fd.into_fd_target(),
            cmd_op,
            cmd,
            io_prio: 0,
            additional_flags: 0,
        }
    }
}

impl sealed::RegisterOp for UringCmd80 {
    fn register_with_sqe(&self, sqe: &mut io_uring_sqe) {
        sqe.opcode = IORING_OP_URING_CMD as u8;
        sqe.fd = self.fd.as_fd();

        let cmd1: [u8; 16] = self.cmd[..16].try_into().unwrap();
        let cmd2: [u8; 64] = self.cmd[16..].try_into().unwrap();

        unsafe {
            sqe.__liburing_anon_1.__liburing_anon_1.cmd_op = self.cmd_op;
            *sqe.__liburing_anon_6
                .cmd
                .as_mut()
                .as_mut_ptr()
                .cast::<[u8; 16]>() = cmd1;
        }

        sqe.flags |= self.fd.sqe_flags();
        sqe.flags |= self.additional_flags;
        sqe.ioprio = self.io_prio;

        // Write the command bytes in the 64-byte void space between the current and next SQE.
        unsafe {
            let data_ptr = (sqe as *mut io_uring_sqe)
                .add(size_of::<io_uring_sqe>())
                .cast::<[u8; 64]>();
            data_ptr.write(cmd2);
        }
    }
}

impl_op_boilerplate!(code = IORING_OP_URING_CMD, UringCmd80);
