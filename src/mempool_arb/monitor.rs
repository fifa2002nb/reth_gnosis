use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt;
use gnosis_primitives::header::GnosisHeader;
use reth_provider::{BlockNumReader, HeaderProvider};
use reth_tasks::TaskExecutor;
use reth_transaction_pool::{
    FullTransactionEvent, NewSubpoolTransactionStream, PoolTransaction, SubPool,
    TransactionListenerKind, TransactionPool,
};
use tracing::debug;

use super::hub::MempoolArbHub;

/// Background task: watch txpool pending txs and push tip changelog events.
pub fn spawn_monitor<Pool, Provider>(
    executor: &TaskExecutor,
    pool: Pool,
    provider: Provider,
    hub: Arc<MempoolArbHub>,
) where
    Pool: TransactionPool + Clone + Send + Sync + 'static,
    Pool::Transaction: PoolTransaction + 'static,
    Provider: HeaderProvider<Header = GnosisHeader> + BlockNumReader + Clone + Send + Sync + 'static,
{
    executor.spawn_task(async move {
        monitor_loop(pool, provider, hub).await;
    });
}

async fn monitor_loop<Pool, Provider>(
    pool: Pool,
    provider: Provider,
    hub: Arc<MempoolArbHub>,
) where
    Pool: TransactionPool + Clone + Send + Sync + 'static,
    Pool::Transaction: PoolTransaction + 'static,
    Provider: HeaderProvider<Header = GnosisHeader> + BlockNumReader + Clone + Send + Sync + 'static,
{
    let mut pending_stream = NewSubpoolTransactionStream::new(
        pool.new_transactions_listener_for(TransactionListenerKind::All),
        SubPool::Pending,
    );
    let mut all_events = pool.all_transactions_event_listener();
    let (mut base_fee, mut head_block) = fetch_head_state(&provider).unwrap_or((0, 0));
    hub.set_head_block_number(head_block);
    let mut head_tick = tokio::time::interval(Duration::from_secs(1));

    debug!(
        target: "rpc::reth",
        head_block,
        pending_block = head_block.saturating_add(1),
        "mempool tip monitor started"
    );

    loop {
        tokio::select! {
            _ = head_tick.tick() => {
                if let Some((bf, bn)) = fetch_head_state(&provider) {
                    base_fee = bf;
                    head_block = bn;
                    hub.set_head_block_number(head_block);
                }
            }
            Some(evt) = pending_stream.next() => {
                hub.on_pending_added(&evt.transaction, base_fee, head_block, "added");
            }
            Some(evt) = all_events.next() => {
                match evt {
                    FullTransactionEvent::Discarded(hash) | FullTransactionEvent::Invalid(hash) => {
                        hub.on_removed(hash, head_block);
                    }
                    FullTransactionEvent::Mined { tx_hash, .. } => {
                        hub.on_removed(tx_hash, head_block);
                    }
                    FullTransactionEvent::Replaced { transaction, replaced_by } => {
                        hub.on_removed(*transaction.hash(), head_block);
                        if let Some(new_tx) = pool.get(&replaced_by) {
                            hub.on_pending_added(&new_tx, base_fee, head_block, "replaced");
                        }
                    }
                    _ => {}
                }
            }
        }
    }
}

fn fetch_head_state<Provider>(provider: &Provider) -> Option<(u64, u64)>
where
    Provider: HeaderProvider<Header = GnosisHeader> + BlockNumReader,
{
    let num = provider.best_block_number().ok()?;
    let header = provider.header_by_number(num).ok().flatten()?;
    Some((header.base_fee_per_gas.unwrap_or(0), num))
}
