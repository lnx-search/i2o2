/// The [RingMode] determines the operating size of the IO ring.
///
/// Normally you want [EntrySize64] which is usable for all IO operations
/// _except_ NVME pass through currently.
pub trait RingMode: sealed::Sealed {
    type SQEntry: SQEntryOptions;
    type CQEntry: CQEntryOptions;
}

mod sealed {
    pub trait Sealed {}
}

/// The default 64 byte entry size.
///
/// If you're ever unsure, this is the one to pick.
pub struct EntrySize64;

impl sealed::Sealed for EntrySize64 {}

impl RingMode for EntrySize64 {
    type SQEntry = io_uring::squeue::Entry;
    type CQEntry = io_uring::cqueue::Entry;
}

/// Wider 128 byte entry size for NVME commands.
///
/// If you're ever unsure, you do not want this.
pub struct EntrySize128;

impl sealed::Sealed for EntrySize128 {}

impl RingMode for EntrySize128 {
    type SQEntry = io_uring::squeue::Entry128;
    type CQEntry = io_uring::cqueue::Entry32;
}

pub trait SQEntryOptions:
    io_uring::squeue::EntryMarker + From<io_uring::squeue::Entry>
{
    fn user_data(self, data: u64) -> Self;
}

impl SQEntryOptions for io_uring::squeue::Entry {
    fn user_data(self, data: u64) -> Self {
        io_uring::squeue::Entry::user_data(self, data)
    }
}

impl SQEntryOptions for io_uring::squeue::Entry128 {
    fn user_data(self, data: u64) -> Self {
        io_uring::squeue::Entry128::user_data(self, data)
    }
}

pub trait CQEntryOptions: io_uring::cqueue::EntryMarker {
    fn user_data(&self) -> u64;

    fn result(&self) -> i32;
}

impl CQEntryOptions for io_uring::cqueue::Entry {
    fn user_data(&self) -> u64 {
        io_uring::cqueue::Entry::user_data(self)
    }

    fn result(&self) -> i32 {
        io_uring::cqueue::Entry::result(self)
    }
}

impl CQEntryOptions for io_uring::cqueue::Entry32 {
    fn user_data(&self) -> u64 {
        io_uring::cqueue::Entry32::user_data(self)
    }

    fn result(&self) -> i32 {
        io_uring::cqueue::Entry32::result(self)
    }
}
