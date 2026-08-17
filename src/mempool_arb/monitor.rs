use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt;
use gnosis_primitives::header::GnosisHeader;
use reth_chain_state::CanonStateSubscriptions;
use reth_provider::{BlockNumReader, HeaderProvider, StateProviderFactory};
use reth_tasks::TaskExecutor;
use reth_transaction_pool::{
    FullTransactionEvent, NewSubpoolTransactionStream, PoolTransaction, SubPool,
    TransactionListenerKind, TransactionPool,
};
use tokio_stream::wrappers::BroadcastStream;
use tracing::{debug, warn};

use super::gas_pressure::GasPressureTracker;
use super::gas_pressure_pack::pack_local_next_block;
use super::hub::MempoolArbHub;
use crate::evm_config::GnosisEvmConfig;
use crate::primitives::block::TransactionSigned;
use crate::primitives::GnosisNodePrimitives;

/// Background task: watch txpool pending txs and pack local next-block gasUsed.
pub fn spawn_monitor<Pool, Provider>(
    executor: &TaskExecutor,
    pool: Pool,
    provider: Provider,
    evm_config: GnosisEvmConfig,
    hub: Arc<MempoolArbHub>,
    gas_pressure: Arc<GasPressureTracker>,
) where
    Pool: TransactionPool<Transaction: PoolTransaction<Consensus = TransactionSigned>>
        + Clone
        + Send
        + Sync
        + 'static,
    Provider: HeaderProvider<Header = GnosisHeader>
        + BlockNumReader
        + StateProviderFactory
        + CanonStateSubscriptions<Primitives = GnosisNodePrimitives>
        + Clone
        + Send
        + Sync
        + 'static,
{
    let pack_pool = pool.clone();
    let pack_provider = provider.clone();
    let pack_evm = evm_config.clone();
    let pack_tracker = gas_pressure.clone();
    executor.spawn_task(async move {
        pack_loop(pack_pool, pack_provider, pack_evm, pack_tracker).await;
    });
    executor.spawn_task(async move {
        monitor_loop(pool, provider, hub, gas_pressure).await;
    });
}

async fn pack_loop<Pool, Provider>(
    pool: Pool,
    provider: Provider,
    evm_config: GnosisEvmConfig,
    tracker: Arc<GasPressureTracker>,
) where
    Pool: TransactionPool<Transaction: PoolTransaction<Consensus = TransactionSigned>>
        + Clone
        + Send
        + Sync
        + 'static,
    Provider: HeaderProvider<Header = GnosisHeader>
        + BlockNumReader
        + StateProviderFactory
        + Clone
        + Send
        + Sync
        + 'static,
{
    let mut interval = tokio::time::interval(Duration::from_millis(200));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = tracker.notify().notified() => {
                // Coalesce a burst of add/replace/mined into one pack.
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            _ = interval.tick() => {}
        }
        if !tracker.take_dirty() {
            continue;
        }
        let pool = pool.clone();
        let provider = provider.clone();
        let evm_config = evm_config.clone();
        match tokio::task::spawn_blocking(move || {
            pack_local_next_block(&pool, &provider, &evm_config)
        })
        .await
        {
            Ok(Ok(packed)) => {
                tracker.store_pack(packed.gas_used, packed.gas_limit, packed.head_block);
                debug!(
                    target: "rpc::reth",
                    gas_used = packed.gas_used,
                    gas_limit = packed.gas_limit,
                    head_block = packed.head_block,
                    packed_txs = packed.packed_txs,
                    "gas pressure cache updated"
                );
            }
            Ok(Err(err)) => {
                warn!(target: "rpc::reth", %err, "gas pressure pack failed");
            }
            Err(err) => {
                warn!(target: "rpc::reth", %err, "gas pressure pack task join failed");
            }
        }
    }
}

async fn monitor_loop<Pool, Provider>(
    pool: Pool,
    provider: Provider,
    hub: Arc<MempoolArbHub>,
    gas_pressure: Arc<GasPressureTracker>,
) where
    Pool: TransactionPool + Clone + Send + Sync + 'static,
    Pool::Transaction: PoolTransaction + 'static,
    Provider: HeaderProvider<Header = GnosisHeader>
        + BlockNumReader
        + CanonStateSubscriptions<Primitives = GnosisNodePrimitives>
        + Clone
        + Send
        + Sync
        + 'static,
{
    let mut pending_stream = NewSubpoolTransactionStream::new(
        pool.new_transactions_listener_for(TransactionListenerKind::All),
        SubPool::Pending,
    );
    let mut all_events = pool.all_transactions_event_listener();
    let (mut base_fee, mut gas_limit, mut head_block) =
        fetch_head_state(&provider).unwrap_or((0, 0, 0));
    hub.set_head_block_number(head_block);
    gas_pressure.set_block_gas_state(gas_limit, head_block);
    let mut head_tick = tokio::time::interval(Duration::from_secs(1));
    let mut canon = BroadcastStream::new(provider.subscribe_to_canonical_state());

    debug!(
        target: "rpc::reth",
        head_block,
        pending_block = head_block.saturating_add(1),
        "mempool tip monitor started"
    );

    loop {
        tokio::select! {
            Some(notification) = canon.next() => {
                if notification.is_err() {
                    continue;
                }
                if let Some((bf, gl, bn)) = fetch_head_state(&provider) {
                    if bn == head_block {
                        continue;
                    }
                    base_fee = bf;
                    gas_limit = gl;
                    head_block = bn;
                    hub.set_head_block_number(head_block);
                    gas_pressure.set_block_gas_state(gas_limit, head_block);
                }
            }
            _ = head_tick.tick() => {
                if let Some((bf, gl, bn)) = fetch_head_state(&provider) {
                    if bn == head_block {
                        continue;
                    }
                    base_fee = bf;
                    gas_limit = gl;
                    head_block = bn;
                    hub.set_head_block_number(head_block);
                    gas_pressure.set_block_gas_state(gas_limit, head_block);
                }
            }
            Some(evt) = pending_stream.next() => {
                hub.on_pending_added(&evt.transaction, base_fee, head_block, "added");
                gas_pressure.mark_dirty();
            }
            Some(evt) = all_events.next() => {
                match evt {
                    FullTransactionEvent::Discarded(hash) | FullTransactionEvent::Invalid(hash) => {
                        hub.on_removed(hash, head_block);
                        gas_pressure.mark_dirty();
                    }
                    FullTransactionEvent::Mined { tx_hash, .. } => {
                        hub.on_removed(tx_hash, head_block);
                        gas_pressure.mark_dirty();
                    }
                    FullTransactionEvent::Replaced { transaction, replaced_by } => {
                        hub.on_removed(*transaction.hash(), head_block);
                        if let Some(new_tx) = pool.get(&replaced_by) {
                            hub.on_pending_added(&new_tx, base_fee, head_block, "replaced");
                        }
                        // Pool already dropped the old hash; next pack sees only the replacement.
                        gas_pressure.mark_dirty();
                    }
                    _ => {}
                }
            }
        }
    }
}

fn fetch_head_state<Provider>(provider: &Provider) -> Option<(u64, u64, u64)>
where
    Provider: HeaderProvider<Header = GnosisHeader> + BlockNumReader,
{
    let num = provider.best_block_number().ok()?;
    let header = provider.header_by_number(num).ok().flatten()?;
    Some((header.base_fee_per_gas.unwrap_or(0), header.gas_limit, num))
}
