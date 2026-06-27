use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use alloy_primitives::{B256, U256};
use reth_transaction_pool::{PoolTransaction, ValidPoolTransaction};
use tokio::sync::broadcast;

use super::types::{
    PendingArbSnapshot, PendingArbSnapshotEntry, PendingArbSnapshotFilter, PendingArbTxEvent,
};

const MAX_INDEX_ENTRIES: usize = 1024;
const BROADCAST_CAP: usize = 4096;

fn pending_block_number(head_block: u64) -> u64 {
    head_block.saturating_add(1)
}

/// Shared mempool tip state: in-memory index + changelog broadcast.
#[derive(Debug)]
pub struct MempoolArbHub {
    index: RwLock<HashMap<B256, PendingArbTxEvent>>,
    head_block_number: AtomicU64,
    tx: broadcast::Sender<PendingArbTxEvent>,
}

impl MempoolArbHub {
    pub fn new() -> Arc<Self> {
        let (tx, _) = broadcast::channel(BROADCAST_CAP);
        Arc::new(Self {
            index: RwLock::new(HashMap::new()),
            head_block_number: AtomicU64::new(0),
            tx,
        })
    }

    pub fn set_head_block_number(&self, block_number: u64) {
        self.head_block_number
            .store(block_number, Ordering::Relaxed);
    }

    pub fn head_block_number(&self) -> u64 {
        self.head_block_number.load(Ordering::Relaxed)
    }

    pub fn subscribe(&self) -> broadcast::Receiver<PendingArbTxEvent> {
        self.tx.subscribe()
    }

    pub fn on_pending_added<T: PoolTransaction>(
        &self,
        vtx: &ValidPoolTransaction<T>,
        base_fee: u64,
        head_block: u64,
        action: &str,
    ) {
        if !super::filter::should_track(vtx) {
            return;
        }
        let event = build_event(vtx, base_fee, pending_block_number(head_block), action);
        self.upsert(event);
    }

    pub fn on_removed(&self, tx_hash: B256, head_block: u64) {
        {
            let mut index = self.index.write().expect("mempool tip index lock");
            index.remove(&tx_hash);
        }
        let _ = self.tx.send(PendingArbTxEvent::removed(
            tx_hash,
            pending_block_number(head_block),
            now_ms(),
        ));
    }

    fn upsert(&self, event: PendingArbTxEvent) {
        {
            let mut index = self.index.write().expect("mempool tip index lock");
            index.insert(event.tx_hash, event.clone());
            if index.len() > MAX_INDEX_ENTRIES {
                let mut entries: Vec<(B256, U256)> = index
                    .iter()
                    .map(|(h, e)| (*h, e.effective_tip_per_gas))
                    .collect();
                entries.sort_by_key(|(_, tip)| *tip);
                for (hash, _) in entries.into_iter().take(index.len() - MAX_INDEX_ENTRIES) {
                    index.remove(&hash);
                }
            }
        }
        let _ = self.tx.send(event);
    }

    pub fn snapshot(&self, filter: PendingArbSnapshotFilter) -> PendingArbSnapshot {
        let block_number = pending_block_number(self.head_block_number());
        let index = self.index.read().expect("mempool tip index lock");
        let min_tip = filter
            .min_effective_tip_per_gas
            .map(U256::from)
            .unwrap_or(U256::ZERO);
        let top_n = filter.top_n.unwrap_or(32).max(1) as usize;

        let mut all: Vec<PendingArbTxEvent> = index
            .values()
            .filter(|evt| evt.effective_tip_per_gas >= min_tip)
            .cloned()
            .collect();
        all.sort_by(|a, b| b.effective_tip_per_gas.cmp(&a.effective_tip_per_gas));

        let max_tip = all.first().map(|e| e.effective_tip_per_gas).unwrap_or(U256::ZERO);
        let top_by_tip: Vec<PendingArbSnapshotEntry> = all
            .into_iter()
            .take(top_n)
            .map(|evt| PendingArbSnapshotEntry {
                tx_hash: evt.tx_hash,
                from: evt.from,
                to: evt.to,
                gas: evt.gas,
                max_fee_per_gas: evt.max_fee_per_gas,
                max_priority_fee_per_gas: evt.max_priority_fee_per_gas,
                effective_tip_per_gas: evt.effective_tip_per_gas,
                priority_cost: evt.priority_cost,
                block_number: evt.block_number,
            })
            .collect();

        PendingArbSnapshot {
            block_number,
            pending_count: index.len() as u64,
            max_effective_tip_per_gas: max_tip,
            top_by_tip,
            snapshot_at_ms: now_ms(),
        }
    }
}

fn build_event<T: PoolTransaction>(
    vtx: &ValidPoolTransaction<T>,
    base_fee: u64,
    block_number: u64,
    action: &str,
) -> PendingArbTxEvent {
    let max_fee = U256::from(vtx.max_fee_per_gas());
    let max_priority = U256::from(vtx.max_priority_fee_per_gas().unwrap_or(vtx.max_fee_per_gas()));
    let effective = vtx
        .effective_tip_per_gas(base_fee)
        .map(U256::from)
        .unwrap_or(U256::ZERO);
    let gas = U256::from(vtx.gas_limit());
    let priority_cost = effective.saturating_mul(gas);

    PendingArbTxEvent {
        kind: "tx".to_string(),
        action: action.to_string(),
        tx_hash: *vtx.hash(),
        from: vtx.sender(),
        to: vtx.to(),
        nonce: vtx.nonce(),
        gas: vtx.gas_limit(),
        max_fee_per_gas: max_fee,
        max_priority_fee_per_gas: max_priority,
        effective_tip_per_gas: effective,
        priority_cost,
        block_number,
        received_at_ms: now_ms(),
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
