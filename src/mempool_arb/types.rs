use alloy_primitives::{Address, Bytes, B256, U256};
use serde::{Deserialize, Serialize};

/// Skip-empty serializer helper for `Bytes` (works around ambiguous method resolution).
fn bytes_is_empty(b: &Bytes) -> bool {
    b.as_ref().is_empty()
}

/// Changelog event pushed over `reth_subscribePendingArbTx`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct PendingArbTxEvent {
    pub kind: String,
    pub action: String,
    pub tx_hash: B256,
    pub from: Address,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub to: Option<Address>,
    #[serde(with = "alloy_serde::quantity")]
    pub nonce: u64,
    #[serde(with = "alloy_serde::quantity")]
    pub gas: u64,
    pub max_fee_per_gas: U256,
    pub max_priority_fee_per_gas: U256,
    pub effective_tip_per_gas: U256,
    pub priority_cost: U256,
    /// EIP-2718 transaction type (0 = legacy, 1 = 2930, 2 = 1559, ...).
    #[serde(with = "alloy_serde::quantity")]
    pub tx_type: u8,
    /// Full transaction calldata (hex 0x...).
    #[serde(default, skip_serializing_if = "bytes_is_empty")]
    pub input: Bytes,
    /// Target block these pending txs compete for (`head + 1`).
    #[serde(with = "alloy_serde::quantity")]
    pub block_number: u64,
    #[serde(with = "alloy_serde::quantity")]
    pub received_at_ms: u64,
}

impl PendingArbTxEvent {
    pub fn removed(tx_hash: B256, block_number: u64, received_at_ms: u64) -> Self {
        Self {
            kind: "tx".to_string(),
            action: "removed".to_string(),
            tx_hash,
            from: Address::ZERO,
            to: None,
            nonce: 0,
            gas: 0,
            max_fee_per_gas: U256::ZERO,
            max_priority_fee_per_gas: U256::ZERO,
            effective_tip_per_gas: U256::ZERO,
            priority_cost: U256::ZERO,
            tx_type: 0,
            input: Bytes::default(),
            block_number,
            received_at_ms,
        }
    }
}

/// Optional filter for WS subscription.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PendingArbFilter {
    #[serde(default, with = "alloy_serde::quantity::opt")]
    pub min_effective_tip_per_gas: Option<u128>,
}

/// HTTP snapshot query.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PendingArbSnapshotFilter {
    #[serde(default, with = "alloy_serde::quantity::opt")]
    pub min_effective_tip_per_gas: Option<u128>,
    #[serde(default, with = "alloy_serde::quantity::opt")]
    pub top_n: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct PendingArbSnapshotEntry {
    pub tx_hash: B256,
    pub from: Address,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub to: Option<Address>,
    #[serde(with = "alloy_serde::quantity")]
    pub gas: u64,
    pub max_fee_per_gas: U256,
    pub max_priority_fee_per_gas: U256,
    pub effective_tip_per_gas: U256,
    pub priority_cost: U256,
    /// EIP-2718 transaction type (0 = legacy, 1 = 2930, 2 = 1559, ...).
    #[serde(with = "alloy_serde::quantity")]
    pub tx_type: u8,
    /// Full transaction calldata (hex 0x...).
    #[serde(default, skip_serializing_if = "bytes_is_empty")]
    pub input: Bytes,
    #[serde(with = "alloy_serde::quantity")]
    pub block_number: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct PendingArbSnapshot {
    /// Target pending block at snapshot time (`head + 1`).
    #[serde(with = "alloy_serde::quantity")]
    pub block_number: u64,
    pub pending_count: u64,
    pub max_effective_tip_per_gas: U256,
    pub top_by_tip: Vec<PendingArbSnapshotEntry>,
    #[serde(with = "alloy_serde::quantity")]
    pub snapshot_at_ms: u64,
}
