#![allow(unused)]

use std::{
    cell::UnsafeCell,
    ops::{Deref, DerefMut},
};

use crate::sync::{
    atomic::{AtomicU64, Ordering},
    cooperative_yield,
};

const OBSOLETE: u64 = 0b01;
const LOCKED: u64 = 0b10;
const UNLOCK_INCREMENT: u64 = 0b10;
const UNLOCK_OBSOLETE_INCREMENT: u64 = 0b11;
const MAX_WRITE_SPINS: usize = 64;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Restart;

#[repr(transparent)]
pub(crate) struct VersionLock(AtomicU64);

impl VersionLock {
    pub(crate) fn new() -> Self {
        Self(AtomicU64::new(0))
    }

    #[inline(always)]
    pub(crate) fn read_optimistic<T>(&self, f: impl FnOnce() -> T) -> Result<T, Restart> {
        let version = self.0.load(Ordering::Acquire);
        if !state_is_readable(version) {
            return Err(Restart);
        }

        let result = f();
        let observed = self.0.load(Ordering::Acquire);
        if observed != version {
            return Err(Restart);
        }

        Ok(result)
    }

    #[inline]
    pub(crate) fn write_lock(&self) -> Result<LockGuard<'_>, Restart> {
        for _ in 0..MAX_WRITE_SPINS {
            let version = self.0.load(Ordering::Relaxed);
            if !state_is_writable(version) {
                wait_for_retry();
                continue;
            }

            if self
                .0
                .compare_exchange_weak(
                    version,
                    version | LOCKED,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                return Ok(LockGuard {
                    lock: self,
                    version: version | LOCKED,
                });
            }

            wait_for_retry();
        }

        Err(Restart)
    }

    #[inline]
    pub(crate) fn write_lock_for<'a, T>(
        &'a self,
        cell: &'a UnsafeCell<T>,
    ) -> Result<WriteGuard<'a, T>, Restart> {
        let lock_guard = self.write_lock()?;
        Ok(WriteGuard { lock_guard, cell })
    }

    #[cfg(test)]
    fn raw_state(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

impl Default for VersionLock {
    fn default() -> Self {
        Self::new()
    }
}

pub(crate) struct LockGuard<'a> {
    lock: &'a VersionLock,
    version: u64,
}

impl LockGuard<'_> {
    #[inline]
    pub(crate) fn unlock_obsolete(self) {
        self.lock
            .0
            .store(self.version + UNLOCK_OBSOLETE_INCREMENT, Ordering::Release);
        std::mem::forget(self);
    }
}

impl Drop for LockGuard<'_> {
    fn drop(&mut self) {
        self.lock
            .0
            .store(self.version + UNLOCK_INCREMENT, Ordering::Release);
    }
}

pub(crate) struct WriteGuard<'a, T> {
    lock_guard: LockGuard<'a>,
    cell: &'a UnsafeCell<T>,
}

impl<T> WriteGuard<'_, T> {
    #[inline]
    pub(crate) fn unlock_obsolete(self) {
        self.lock_guard.unlock_obsolete();
    }
}

impl<T> Deref for WriteGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.cell.get() }
    }
}

impl<T> DerefMut for WriteGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *self.cell.get() }
    }
}

#[inline(always)]
fn state_is_readable(version: u64) -> bool {
    version & (OBSOLETE | LOCKED) == 0
}

#[inline(always)]
fn state_is_writable(version: u64) -> bool {
    version & (OBSOLETE | LOCKED) == 0
}

#[inline(always)]
fn wait_for_retry() {
    std::hint::spin_loop();
    cooperative_yield();
}

#[cfg(all(test, not(feature = "shuttle")))]
mod tests {
    use std::{cell::UnsafeCell, sync::Arc};

    use super::{Restart, VersionLock};
    use crate::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn optimistic_read_returns_value_when_version_is_stable() {
        let lock = VersionLock::new();

        let observed = lock.read_optimistic(|| 7_u64);

        assert_eq!(observed, Ok(7));
        assert_eq!(lock.raw_state(), 0);
    }

    #[test]
    fn optimistic_read_restarts_while_locked() {
        let lock = VersionLock::new();
        let _guard = lock.write_lock().expect("write lock");

        let observed = lock.read_optimistic(|| 7_u64);

        assert_eq!(observed, Err(Restart));
        assert_eq!(lock.raw_state(), 0b10);
    }

    #[test]
    fn optimistic_read_restarts_after_obsolete_unlock() {
        let lock = VersionLock::new();
        lock.write_lock().expect("write lock").unlock_obsolete();

        let observed = lock.read_optimistic(|| 7_u64);

        assert_eq!(observed, Err(Restart));
        assert_eq!(lock.raw_state(), 0b101);
    }

    #[test]
    fn write_lock_drop_clears_lock_and_bumps_version() {
        let lock = VersionLock::new();

        {
            let _guard = lock.write_lock().expect("write lock");
            assert_eq!(lock.raw_state(), 0b10);
        }

        assert_eq!(lock.raw_state(), 0b100);
    }

    #[test]
    fn write_guard_mutates_value_and_unlocks() {
        let lock = VersionLock::new();
        let value = UnsafeCell::new(3_u64);

        {
            let mut guard = lock.write_lock_for(&value).expect("write guard");
            *guard = *guard + 4;
            assert_eq!(*guard, 7);
            assert_eq!(lock.raw_state(), 0b10);
        }

        let observed = lock
            .read_optimistic(|| unsafe { *value.get() })
            .expect("stable read");
        assert_eq!(observed, 7);
        assert_eq!(lock.raw_state(), 0b100);
    }

    #[test]
    fn write_guard_unlock_obsolete_marks_lock_obsolete() {
        let lock = VersionLock::new();
        let value = UnsafeCell::new(3_u64);

        {
            let mut guard = lock.write_lock_for(&value).expect("write guard");
            *guard = 11;
            guard.unlock_obsolete();
        }

        assert_eq!(lock.raw_state(), 0b101);
        assert_eq!(
            lock.read_optimistic(|| unsafe { *value.get() }),
            Err(Restart)
        );
    }

    #[test]
    fn concurrent_writers_can_both_make_progress() {
        let lock = Arc::new(VersionLock::new());
        let wins = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::new();
        for _ in 0..2 {
            let lock = lock.clone();
            let wins = wins.clone();
            handles.push(std::thread::spawn(move || {
                loop {
                    if let Ok(_guard) = lock.write_lock() {
                        wins.fetch_add(1, Ordering::Relaxed);
                        break;
                    }
                }
            }));
        }

        for handle in handles {
            handle.join().expect("thread");
        }

        assert_eq!(wins.load(Ordering::Relaxed), 2);
        assert_eq!(lock.raw_state(), 0b1000);
    }
}
