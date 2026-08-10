use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use alloy_primitives::B256;
use reth_transaction_pool::{PoolTransaction, ValidPoolTransaction};

/// Bucket width: 0.1 gwei.
const BUCKET_WEI: u64 = 100_000_000;
/// Covers tip ceilings up to 20,000 gwei; ~1.6MB resident (200_000 * 8 bytes).
const MAX_BUCKETS: usize = 200_000;

/// Tracks pending-pool gas per 0.1gwei tip bucket across the **full** mempool
/// (no blacklist filter — unlike `MempoolArbHub`, which only tracks txs shaped
/// like arbitrage swaps). Answers: "if I'm only willing to pay up to `ceiling`,
/// how much of the next block's gas is already claimed by txs bidding at or
/// below that tip?" — used to detect the next block filling up before our
/// own `rbf_start_at_window_ms` t0 would otherwise fire.
///
/// Known approximation (shared with `MempoolArbHub`): `effective_tip_per_gas`
/// is computed against `base_fee` at insert time and not recomputed as
/// `base_fee` drifts block to block. Acceptable given Gnosis's bounded
/// (±12.5%/block) base fee movement and that most pool entries get replaced
/// or mined within a few blocks anyway.
#[derive(Debug)]
pub struct GasPressureTracker {
    buckets: Vec<AtomicU64>,
    index: RwLock<HashMap<B256, (usize, u64)>>,
    block_gas_limit: AtomicU64,
    head_block_number: AtomicU64,
}

impl GasPressureTracker {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            buckets: (0..MAX_BUCKETS).map(|_| AtomicU64::new(0)).collect(),
            index: RwLock::new(HashMap::new()),
            block_gas_limit: AtomicU64::new(0),
            head_block_number: AtomicU64::new(0),
        })
    }

    pub fn set_block_gas_state(&self, gas_limit: u64, head_block: u64) {
        self.block_gas_limit.store(gas_limit, Ordering::Relaxed);
        self.head_block_number.store(head_block, Ordering::Relaxed);
    }

    pub fn block_gas_limit(&self) -> u64 {
        self.block_gas_limit.load(Ordering::Relaxed)
    }

    pub fn head_block_number(&self) -> u64 {
        self.head_block_number.load(Ordering::Relaxed)
    }

    /// Full-mempool ingest: intentionally skips `filter::should_track` — a plain
    /// transfer eats real block gas same as an arb swap does, and excluding it
    /// would understate how full the next block actually is.
    pub fn on_added<T: PoolTransaction>(&self, vtx: &ValidPoolTransaction<T>, base_fee: u64) {
        let tip = vtx.effective_tip_per_gas(base_fee).unwrap_or(0);
        let tip = u64::try_from(tip).unwrap_or(u64::MAX);
        self.record_add(*vtx.hash(), tip, vtx.gas_limit());
    }

    pub fn on_removed(&self, hash: B256) {
        self.record_remove(hash);
    }

    fn record_add(&self, hash: B256, tip_wei: u64, gas: u64) {
        let bucket_idx = Self::bucket_index(tip_wei);
        let mut index = self.index.write().expect("gas pressure index lock");
        // Same hash re-added (e.g. duplicate "added" notification): reverse the
        // old bucket contribution first so it's never double-counted.
        if let Some((old_idx, old_gas)) = index.remove(&hash) {
            self.buckets[old_idx].fetch_sub(old_gas, Ordering::Relaxed);
        }
        self.buckets[bucket_idx].fetch_add(gas, Ordering::Relaxed);
        index.insert(hash, (bucket_idx, gas));
    }

    fn record_remove(&self, hash: B256) {
        let mut index = self.index.write().expect("gas pressure index lock");
        if let Some((idx, gas)) = index.remove(&hash) {
            self.buckets[idx].fetch_sub(gas, Ordering::Relaxed);
        }
    }

    fn bucket_index(tip_wei: u64) -> usize {
        ((tip_wei / BUCKET_WEI) as usize).min(MAX_BUCKETS - 1)
    }

    /// Returns `(gas summed over buckets [0, ceiling_wei], current block gas limit)`.
    pub fn water_level(&self, ceiling_wei: u64) -> (u64, u64) {
        let ceiling_idx = Self::bucket_index(ceiling_wei);
        let gas_sum: u64 = self.buckets[..=ceiling_idx]
            .iter()
            .map(|b| b.load(Ordering::Relaxed))
            .sum();
        (gas_sum, self.block_gas_limit())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tracker() -> Arc<GasPressureTracker> {
        GasPressureTracker::new()
    }

    #[test]
    fn add_accumulates_into_bucket() {
        let t = tracker();
        t.record_add(B256::with_last_byte(1), 5 * BUCKET_WEI, 100_000);
        t.record_add(B256::with_last_byte(2), 5 * BUCKET_WEI + 1, 50_000);
        t.set_block_gas_state(30_000_000, 1);
        let (sum, limit) = t.water_level(6 * BUCKET_WEI);
        assert_eq!(sum, 150_000);
        assert_eq!(limit, 30_000_000);
    }

    #[test]
    fn remove_reverses_bucket() {
        let t = tracker();
        let h = B256::with_last_byte(1);
        t.record_add(h, 2 * BUCKET_WEI, 21_000);
        t.record_remove(h);
        let (sum, _) = t.water_level(10 * BUCKET_WEI);
        assert_eq!(sum, 0);
    }

    #[test]
    fn readd_same_hash_overwrites_not_double_counts() {
        let t = tracker();
        let h = B256::with_last_byte(9);
        t.record_add(h, BUCKET_WEI, 21_000);
        t.record_add(h, BUCKET_WEI, 42_000); // e.g. duplicate "added" notification
        let (sum, _) = t.water_level(10 * BUCKET_WEI);
        assert_eq!(sum, 42_000);
    }

    #[test]
    fn ceiling_excludes_higher_buckets() {
        let t = tracker();
        t.record_add(B256::with_last_byte(1), BUCKET_WEI, 21_000);
        t.record_add(B256::with_last_byte(2), 50 * BUCKET_WEI, 21_000);
        let (sum, _) = t.water_level(2 * BUCKET_WEI);
        assert_eq!(sum, 21_000);
    }

    #[test]
    fn tip_beyond_max_clamps_into_last_bucket() {
        let t = tracker();
        t.record_add(B256::with_last_byte(1), u64::MAX, 21_000);
        let (sum, _) = t.water_level(u64::MAX);
        assert_eq!(sum, 21_000);
    }
}
