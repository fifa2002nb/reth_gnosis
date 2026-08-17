use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use tokio::sync::Notify;

/// Local next-block fill from a revm pack of `pool.best_transactions()`.
///
/// `packed_gas` is cumulative **gasUsed** after executing txs in tip order against
/// head state (same packing rule as the payload builder: `cum + gas_limit` must
/// fit, then add actual `gasUsed`). Capped at one block (`<= gas_limit`).
///
/// The 200ms WS poll only reads this cache. A dedicated pack task reruns revm
/// when [`Self::mark_dirty`] is set (pending add/replace/mined/discard, or new head).
#[derive(Debug)]
pub struct GasPressureTracker {
    packed_gas: AtomicU64,
    block_gas_limit: AtomicU64,
    head_block_number: AtomicU64,
    dirty: AtomicBool,
    notify: Notify,
}

impl GasPressureTracker {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            packed_gas: AtomicU64::new(0),
            block_gas_limit: AtomicU64::new(0),
            head_block_number: AtomicU64::new(0),
            dirty: AtomicBool::new(true),
            notify: Notify::new(),
        })
    }

    pub fn set_block_gas_state(&self, gas_limit: u64, head_block: u64) {
        let prev = self.head_block_number.load(Ordering::Acquire);
        self.head_block_number.store(head_block, Ordering::Release);
        self.block_gas_limit.store(gas_limit, Ordering::Relaxed);
        if prev == 0 {
            self.mark_dirty();
            return;
        }
        if head_block != prev {
            // New head: drop the previous window's fill so subscribers can disarm
            // before the next pack completes.
            self.packed_gas.store(0, Ordering::Relaxed);
            self.mark_dirty();
        }
    }

    pub fn block_gas_limit(&self) -> u64 {
        self.block_gas_limit.load(Ordering::Relaxed)
    }

    pub fn head_block_number(&self) -> u64 {
        self.head_block_number.load(Ordering::Acquire)
    }

    /// Pending set changed: coalesce into the next pack run.
    pub fn mark_dirty(&self) {
        self.dirty.store(true, Ordering::Release);
        self.notify.notify_one();
    }

    pub fn notify(&self) -> &Notify {
        &self.notify
    }

    /// Swap dirty to false. Returns whether a pack is needed.
    pub fn take_dirty(&self) -> bool {
        self.dirty.swap(false, Ordering::AcqRel)
    }

    pub fn store_pack(&self, gas_used: u64, gas_limit: u64, head_block: u64) {
        self.packed_gas.store(gas_used.min(gas_limit), Ordering::Relaxed);
        self.block_gas_limit.store(gas_limit, Ordering::Relaxed);
        self.head_block_number.store(head_block, Ordering::Release);
    }

    /// Returns `(packed gasUsed, current block gas limit)`.
    ///
    /// `ceiling_wei` is kept for the WS filter signature. Packing includes the
    /// full local pending set in tip order (high-tip txs take space first); a
    /// 10k gwei ceiling already covers essentially every tx on Gnosis.
    pub fn water_level(&self, _ceiling_wei: u64) -> (u64, u64) {
        let limit = self.block_gas_limit.load(Ordering::Relaxed);
        let packed = self.packed_gas.load(Ordering::Relaxed);
        (packed.min(limit), limit)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tracker() -> Arc<GasPressureTracker> {
        GasPressureTracker::new()
    }

    #[test]
    fn new_head_zeros_packed_gas() {
        let t = tracker();
        t.set_block_gas_state(17_000_000, 100);
        t.store_pack(8_000_000, 17_000_000, 100);
        let (sum, _) = t.water_level(0);
        assert_eq!(sum, 8_000_000);
        t.set_block_gas_state(17_000_000, 101);
        let (sum_next, limit) = t.water_level(0);
        assert_eq!(sum_next, 0);
        assert_eq!(limit, 17_000_000);
        assert!(t.take_dirty());
    }

    #[test]
    fn same_head_does_not_dirty_or_zero() {
        let t = tracker();
        t.set_block_gas_state(17_000_000, 100);
        assert!(t.take_dirty());
        t.store_pack(1_000_000, 17_000_000, 100);
        t.set_block_gas_state(17_000_000, 100);
        let (sum, _) = t.water_level(0);
        assert_eq!(sum, 1_000_000);
        assert!(!t.take_dirty());
    }

    #[test]
    fn store_pack_caps_at_gas_limit() {
        let t = tracker();
        t.store_pack(30_000_000, 17_000_000, 1);
        let (sum, limit) = t.water_level(0);
        assert_eq!(sum, 17_000_000);
        assert_eq!(limit, 17_000_000);
        assert_eq!(t.block_gas_limit(), 17_000_000);
    }

    #[test]
    fn take_dirty_clears() {
        let t = tracker();
        assert!(t.take_dirty()); // new() starts dirty
        assert!(!t.take_dirty());
        t.mark_dirty();
        assert!(t.take_dirty());
        assert!(!t.take_dirty());
    }
}
