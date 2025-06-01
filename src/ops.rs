use io_uring::opcode;

use crate::mode;

/// A [RingOp] is a [opcode] that is valid to use via the I2o2 interface.
///
/// This trait mostly exists to prevent foot guns around registered buffers, etc...
pub trait RingOp<M: mode::RingMode = mode::EntrySize64>: sealed::Sealed {
    /// Consume `self` and return the raw submission queue entry.
    fn into_entry(self) -> M::SQEntry;

    /// Consume `self` and return a [AnyOpcode] entry.
    fn into_any_opcode(self) -> AnyOpcode<M>;
}

mod sealed {
    pub trait Sealed {}
}

macro_rules! impl_ring_op {
    ($t:ty) => {
        impl sealed::Sealed for $t {}

        impl<M: mode::RingMode> RingOp<M> for $t {
            #[inline]
            fn into_entry(self) -> M::SQEntry {
                self.build().into()
            }

            #[inline]
            fn into_any_opcode(self) -> AnyOpcode<M> {
                AnyOpcode(<Self as RingOp<M>>::into_entry(self))
            }
        }
    };
}

impl_ring_op!(opcode::Nop);

impl_ring_op!(opcode::Accept);
impl_ring_op!(opcode::AcceptMulti);
impl_ring_op!(opcode::AsyncCancel);
impl_ring_op!(opcode::AsyncCancel2);

impl_ring_op!(opcode::Bind);

impl_ring_op!(opcode::Close);
impl_ring_op!(opcode::Connect);

impl_ring_op!(opcode::EpollCtl);

// BAD! breaks the scheduler -- impl_ring_op!(opcode::FilesUpdate);
impl_ring_op!(opcode::Fsync);
impl_ring_op!(opcode::Fadvise);
impl_ring_op!(opcode::Fallocate);
impl_ring_op!(opcode::Ftruncate);
impl_ring_op!(opcode::FixedFdInstall);
impl_ring_op!(opcode::FutexWait);
impl_ring_op!(opcode::FutexWaitV);
impl_ring_op!(opcode::FutexWake);

impl_ring_op!(opcode::LinkAt);
impl_ring_op!(opcode::Listen);
impl_ring_op!(opcode::LinkTimeout);

impl_ring_op!(opcode::Madvise);
impl_ring_op!(opcode::MsgRingData);
impl_ring_op!(opcode::MkDirAt);
impl_ring_op!(opcode::MsgRingSendFd);

impl_ring_op!(opcode::OpenAt);
impl_ring_op!(opcode::OpenAt2);

// BAD! has some dangerous requirements to make work, not sure if we want to support currently
// -- impl_ring_op!(opcode::ProvideBuffers);
impl_ring_op!(opcode::PollAdd);
impl_ring_op!(opcode::PollRemove);

impl_ring_op!(opcode::Read);
impl_ring_op!(opcode::Readv);
impl_ring_op!(opcode::ReadFixed);
impl_ring_op!(opcode::Recv);
impl_ring_op!(opcode::RecvMsg);
impl_ring_op!(opcode::RecvBundle);
impl_ring_op!(opcode::RecvMulti);
impl_ring_op!(opcode::RecvMsgMulti);
impl_ring_op!(opcode::RecvMultiBundle);
impl_ring_op!(opcode::RenameAt);

impl_ring_op!(opcode::Socket);
impl_ring_op!(opcode::Send);
impl_ring_op!(opcode::SendZc);
impl_ring_op!(opcode::SendMsgZc);
impl_ring_op!(opcode::SendMsg);
impl_ring_op!(opcode::SendBundle);

impl_ring_op!(opcode::Shutdown);
impl_ring_op!(opcode::SetSockOpt);
impl_ring_op!(opcode::Splice);
impl_ring_op!(opcode::Statx);
impl_ring_op!(opcode::SymlinkAt);
impl_ring_op!(opcode::SyncFileRange);

impl_ring_op!(opcode::Tee);
impl_ring_op!(opcode::Timeout);
impl_ring_op!(opcode::TimeoutRemove);
impl_ring_op!(opcode::TimeoutUpdate);

impl_ring_op!(opcode::UnlinkAt);

impl_ring_op!(opcode::Write);
impl_ring_op!(opcode::Writev);
impl_ring_op!(opcode::WriteFixed);
impl_ring_op!(opcode::WaitId);

impl_ring_op!(opcode::UringCmd16);

impl sealed::Sealed for opcode::UringCmd80 {}

impl RingOp<mode::EntrySize128> for opcode::UringCmd80 {
    fn into_entry(self) -> <mode::EntrySize128 as mode::RingMode>::SQEntry {
        self.build()
    }

    fn into_any_opcode(self) -> AnyOpcode<mode::EntrySize128> {
        AnyOpcode(self.into_entry())
    }
}


/// A helper type that allows you to mix and match ops as part of the bulk API
/// without having to box.
pub struct AnyOpcode<M: mode::RingMode = mode::EntrySize64>(M::SQEntry);

impl<M: mode::RingMode> sealed::Sealed for AnyOpcode<M> {}

impl<M: mode::RingMode> RingOp<M> for AnyOpcode<M> {
    #[inline]
    fn into_entry(self) -> M::SQEntry {
        self.0
    }

    #[inline]
    fn into_any_opcode(self) -> Self {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_any_opcode() {
        let mut foo: AnyOpcode = opcode::Nop::new().into_any_opcode();
        foo = foo.into_any_opcode();
        let _entry = foo.into_entry();

        let mut foo: AnyOpcode<mode::EntrySize128> =
            opcode::Nop::new().into_any_opcode();
        foo = foo.into_any_opcode();
        let _entry = foo.into_entry();
    }
}
