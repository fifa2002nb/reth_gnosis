use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt;
use gnosis_primitives::header::GnosisHeader;
use reth_chain_state::CanonStateSubscriptions;
use reth_provider::{BlockNumReader, HeaderProvider};
use reth_tasks::TaskExecutor;
use reth_transaction_pool::{
    FullTransactionEvent, NewSubpoolTransactionStream, PoolTransaction, SubPool,
    TransactionListenerKind, TransactionPool,
};
use tokio_stream::wrappers::BroadcastStream;
use tracing::debug;

use super::gas_pressure::GasPressureTracker;
use super::hub::MempoolArbHub;
use crate::primitives::GnosisNodePrimitives;

/// Background task: watch txpool pending txs and push tip changelog events.
pub fn spawn_monitor<Pool, Provider>(
    executor: &TaskExecutor,
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
    executor.spawn_task(async move {
        monitor_loop(pool, provider, hub, gas_pressure).await;
    });
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
    // Fallback if a canon notification is lagged/dropped. Primary reset is the
    // canon-state stream below — same moment as reth_subscribeBlockEndLogs.
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
                    base_fee = bf;
                    gas_limit = gl;
                    head_block = bn;
                    hub.set_head_block_number(head_block);
                    gas_pressure.set_block_gas_state(gas_limit, head_block);
                }
            }
            Some(evt) = pending_stream.next() => {
                hub.on_pending_added(&evt.transaction, base_fee, head_block, "added");
                // Full-mempool ingest, no blacklist filter — see gas_pressure.rs.
                gas_pressure.on_added(&evt.transaction, base_fee);
            }
            Some(evt) = all_events.next() => {
                match evt {
                    FullTransactionEvent::Discarded(hash) | FullTransactionEvent::Invalid(hash) => {
                        hub.on_removed(hash, head_block);
                        gas_pressure.on_removed(hash);
                    }
                    FullTransactionEvent::Mined { tx_hash, .. } => {
                        hub.on_removed(tx_hash, head_block);
                        gas_pressure.on_removed(tx_hash);
                    }
                    FullTransactionEvent::Replaced { transaction, replaced_by } => {
                        hub.on_removed(*transaction.hash(), head_block);
                        gas_pressure.on_removed(*transaction.hash());
                        if let Some(new_tx) = pool.get(&replaced_by) {
                            hub.on_pending_added(&new_tx, base_fee, head_block, "replaced");
                            gas_pressure.on_added(&new_tx, base_fee);
                        }
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
