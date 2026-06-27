use alloy_primitives::Bytes;
use reth_transaction_pool::{PoolTransaction, ValidPoolTransaction};

use super::config::{is_blacklisted_selector, PLAIN_TRANSFER_GAS};

/// Returns true when the tx should be tracked for tip intelligence.
pub fn should_track<T: PoolTransaction>(tx: &ValidPoolTransaction<T>) -> bool {
    if tx.to().is_none() {
        return false;
    }
    let input = tx.transaction.input();
    if input.is_empty() && tx.gas_limit() <= PLAIN_TRANSFER_GAS {
        return false;
    }
    if input.len() >= 4 && is_blacklisted_selector(read_selector(input)) {
        return false;
    }
    true
}

fn read_selector(input: &Bytes) -> [u8; 4] {
    [input[0], input[1], input[2], input[3]]
}
