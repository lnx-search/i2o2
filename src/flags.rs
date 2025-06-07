use std::process::abort;

pub const MAX_SAFE_IDX: u32 = 0x3FFF_FFFF;
const FLAGS_MASK: u64 = 0xF000_0000_0000_0000;
pub const UNGUARDED: u64 = 0x0000_0000_0000_0000;
pub const EVENT_FD_WAKER: u64 = 0x1000_0000_0000_0000;
pub const GUARDED: u64 = 0x2000_0000_0000_0000;
pub const GUARDED_RESOURCE_BUFFER: u64 = 0x3000_0000_0000_0000;
pub const GUARDED_RESOURCE_FILE: u64 = 0x4000_0000_0000_0000;

#[repr(u64)]
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
/// The possible flags that can be set.
pub enum Flag {
    /// The event is coming from the event FD waker.
    EventFdWaker = EVENT_FD_WAKER,
    /// The event has a guard value.
    Guarded = GUARDED,
    /// The event has no special properties and has no guard value.
    Unguarded = UNGUARDED,
    /// The event is tied to a registered resource buffer which is now unregistered
    /// _and_ no longer used by any operation in the ring.
    GuardedResourceBuffer = GUARDED_RESOURCE_BUFFER,
    /// The event is tied to a registered resource file which is now unregistered
    /// _and_ no longer used by any operation in the ring.
    GuardedResourceFile = GUARDED_RESOURCE_FILE,
}

/// Packs the 4 bit `flag` with the 30 bit `reply_idx` and `guard_idx`.
pub fn pack(flag: Flag, reply_idx: u32, guard_idx: u32) -> u64 {
    // If a program has *somehow* managed to enqueue 1,073,741,823
    // they are doing something *very* wrong, if the system is even still alive
    // we don't care to support that sort of behaviour so will abort to prevent
    // wraps or corrupting of the packed value.
    if reply_idx > MAX_SAFE_IDX || guard_idx > MAX_SAFE_IDX {
        abort_insane_program();
    }

    let reply_idx = (reply_idx as u64) << 30;
    let guard_idx = guard_idx as u64;
    let flag = flag as u64;
    flag | reply_idx | guard_idx
}

/// Unpacks the 4 bit `flag` and 30 bit `reply_idx` and `guard_idx` from
/// the provided value.
pub fn unpack(packed_value: u64) -> (Flag, u32, u32) {
    const REPLY_IDX_MASK: u64 = 0x0FFF_FFFF_C000_0000;
    const GUARD_IDX_MASK: u64 = 0x0000_0000_3FFF_FFFF;

    let guard_idx = (packed_value & GUARD_IDX_MASK) as u32;
    let reply_idx = ((packed_value & REPLY_IDX_MASK) >> 30) as u32;
    let flag = match packed_value & FLAGS_MASK {
        EVENT_FD_WAKER => Flag::EventFdWaker,
        GUARDED => Flag::Guarded,
        UNGUARDED => Flag::Unguarded,
        GUARDED_RESOURCE_BUFFER => Flag::GuardedResourceBuffer,
        GUARDED_RESOURCE_FILE => Flag::GuardedResourceFile,
        // This should **never** happen, if this occurs the system has
        // already entered a UB state since we have to assume that any or all of
        // our prior event reads and unpacking are invalid; which means we have
        // wrongly freed guards we shouldn't have and all guarantees are now gone.
        #[cfg(debug_assertions)]
        _ => unreachable!(
            "retrieved completion flag should never be unknown without being UB!"
        ),
        #[cfg(not(debug_assertions))]
        _ => abort_system_fail(),
    };

    (flag, reply_idx, guard_idx)
}

#[inline(never)]
fn abort_insane_program() {
    eprintln!(
        "billions of operations have been enqueued and not completed, program should abort"
    );
    abort();
}

#[cfg(not(debug_assertions))]
#[inline(never)]
fn abort_system_fail() -> ! {
    eprintln!(
        "the system is aborting due to I2o2 witnessing a unknown flag in the IO ring completion \
            events, either this is a bug or you have done something _very_ wrong."
    );
    abort()
}
