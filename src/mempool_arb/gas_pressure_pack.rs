//! Dry-run pack of the local pending pool: execute in tip order, sum gasUsed.

use alloy_consensus::BlockHeader;
use alloy_primitives::{Bytes, B256};
use gnosis_primitives::header::GnosisHeader;
use reth_errors::{BlockExecutionError, BlockValidationError};
use reth_evm::{execute::BlockBuilder, ConfigureEvm, Evm, NextBlockEnvAttributes};
use reth_primitives_traits::SealedHeader;
use reth_provider::{BlockNumReader, HeaderProvider, StateProviderFactory};
use reth_revm::{database::StateProviderDatabase, db::State};
use reth_transaction_pool::{
    error::InvalidPoolTransactionError, BestTransactions, BestTransactionsAttributes,
    PoolTransaction, TransactionPool,
};
use revm::context::Block;
use tracing::{debug, warn};

use crate::evm_config::GnosisEvmConfig;
use crate::primitives::block::TransactionSigned;

/// Gnosis slot time. Next-block env timestamp = parent + this.
const SLOT_SECONDS: u64 = 5;
/// Safety cap: never execute more txs than this in one pack.
const MAX_PACK_TXS: usize = 1024;

#[derive(Debug, Clone, Copy)]
pub struct PackedWaterLevel {
    pub gas_used: u64,
    pub gas_limit: u64,
    pub head_block: u64,
    pub packed_txs: u32,
}

/// Pack one next block from `pool.best_transactions()` using revm gasUsed.
///
/// Same inclusion rule as the payload builder: skip a tx when
/// `cum + tx.gas_limit > gas_limit`, then `cum += execute.gasUsed`.
pub fn pack_local_next_block<Pool, Provider>(
    pool: &Pool,
    provider: &Provider,
    evm_config: &GnosisEvmConfig,
) -> Result<PackedWaterLevel, String>
where
    Pool: TransactionPool<Transaction: PoolTransaction<Consensus = TransactionSigned>>,
    Provider: HeaderProvider<Header = GnosisHeader> + BlockNumReader + StateProviderFactory,
{
    let head_block = provider
        .best_block_number()
        .map_err(|e| format!("best_block_number: {e}"))?;
    let parent = provider
        .sealed_header(head_block)
        .map_err(|e| format!("sealed_header: {e}"))?
        .ok_or_else(|| format!("missing sealed header {head_block}"))?;

    pack_against_parent(pool, provider, evm_config, &parent)
}

fn pack_against_parent<Pool, Provider>(
    pool: &Pool,
    provider: &Provider,
    evm_config: &GnosisEvmConfig,
    parent: &SealedHeader<GnosisHeader>,
) -> Result<PackedWaterLevel, String>
where
    Pool: TransactionPool<Transaction: PoolTransaction<Consensus = TransactionSigned>>,
    Provider: StateProviderFactory,
{
    let started = std::time::Instant::now();
    let state_provider = provider
        .state_by_block_hash(parent.hash())
        .map_err(|e| format!("state_by_block_hash: {e}"))?;
    let mut db = State::builder()
        .with_database(StateProviderDatabase::new(&state_provider))
        .with_bundle_update()
        .build();

    let attributes = NextBlockEnvAttributes {
        timestamp: parent.timestamp().saturating_add(SLOT_SECONDS),
        suggested_fee_recipient: parent.beneficiary(),
        prev_randao: parent.mix_hash().unwrap_or(B256::ZERO),
        gas_limit: parent.gas_limit(),
        parent_beacon_block_root: parent.parent_beacon_block_root(),
        withdrawals: None,
        extra_data: Bytes::new(),
        slot_number: parent.slot_number().map(|s| s.saturating_add(1)),
    };

    let mut builder = evm_config
        .builder_for_next_block(&mut db, parent, attributes)
        .map_err(|e| format!("builder_for_next_block: {e}"))?;

    let block_gas_limit: u64 = builder.evm_mut().block().gas_limit();
    let base_fee = builder.evm_mut().block().basefee();
    let blob_fee = builder
        .evm_mut()
        .block()
        .blob_gasprice()
        .map(|p| p as u64);

    builder
        .apply_pre_execution_changes()
        .map_err(|e| format!("pre_execution: {e}"))?;

    let mut best_txs = pool.best_transactions_with_attributes(BestTransactionsAttributes::new(
        base_fee, blob_fee,
    ));
    best_txs.no_updates();
    best_txs.skip_blobs();

    let mut cumulative_gas_used = 0u64;
    let mut packed_txs = 0u32;
    let mut considered = 0usize;

    while let Some(pool_tx) = best_txs.next() {
        considered += 1;
        if considered > MAX_PACK_TXS {
            break;
        }
        if cumulative_gas_used + pool_tx.gas_limit() > block_gas_limit {
            best_txs.mark_invalid(
                &pool_tx,
                &InvalidPoolTransactionError::ExceedsGasLimit(
                    pool_tx.gas_limit(),
                    block_gas_limit,
                ),
            );
            continue;
        }

        let tx = pool_tx.to_consensus();
        match builder.execute_transaction(tx) {
            Ok(gas_output) => {
                cumulative_gas_used = cumulative_gas_used.saturating_add(gas_output.tx_gas_used());
                packed_txs = packed_txs.saturating_add(1);
                if cumulative_gas_used >= block_gas_limit {
                    break;
                }
            }
            Err(BlockExecutionError::Validation(BlockValidationError::InvalidTx {
                error, ..
            })) => {
                if !error.is_nonce_too_low() {
                    best_txs.mark_invalid(
                        &pool_tx,
                        &InvalidPoolTransactionError::ExceedsGasLimit(
                            pool_tx.gas_limit(),
                            block_gas_limit,
                        ),
                    );
                }
            }
            Err(err) => {
                warn!(
                    target: "rpc::reth",
                    %err,
                    tx=?pool_tx.hash(),
                    "gas pressure pack skipped tx"
                );
                best_txs.mark_invalid(
                    &pool_tx,
                    &InvalidPoolTransactionError::ExceedsGasLimit(
                        pool_tx.gas_limit(),
                        block_gas_limit,
                    ),
                );
            }
        }
    }

    let gas_used = cumulative_gas_used.min(block_gas_limit);
    debug!(
        target: "rpc::reth",
        head_block = parent.number(),
        gas_used,
        gas_limit = block_gas_limit,
        packed_txs,
        considered,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "gas pressure packed local next block"
    );

    Ok(PackedWaterLevel {
        gas_used,
        gas_limit: block_gas_limit,
        head_block: parent.number(),
        packed_txs,
    })
}
