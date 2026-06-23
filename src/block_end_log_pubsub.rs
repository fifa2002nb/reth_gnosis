//! Reth extended log subscription: `reth_subscribeBlockEndLogs`.
//!
//! Emits standard matching logs per canonical block, then a `blockEnd` event so
//! clients can seal a block without waiting for `newHead(N+1)`.

use alloy_consensus::TxReceipt;
use alloy_primitives::B256;
use alloy_rpc_types_eth::Filter;
use futures_util::{Stream, StreamExt};
use jsonrpsee::{
    core::SubscriptionResult,
    proc_macros::rpc,
    server::SubscriptionMessage,
    PendingSubscriptionSink, SubscriptionSink,
};
use reth_chain_state::CanonStateSubscriptions;
use reth_execution_types::BlockReceipts;
use reth_primitives_traits::NodePrimitives;
use reth_rpc_eth_types::logs_utils::matching_block_logs_with_tx_hashes;
use reth_rpc_server_types::result::internal_rpc_err;
use serde::Serialize;
use tokio_stream::wrappers::BroadcastStream;

use crate::primitives::GnosisNodePrimitives;

/// WS payload: `{ "kind": "log", ...standard log fields... }` or `{ "kind": "blockEnd", ... }`.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum BlockEndLogEvent {
    Log {
        #[serde(flatten)]
        log: alloy_rpc_types_eth::Log,
    },
    BlockEnd {
        #[serde(rename = "blockNumber", with = "alloy_serde::quantity")]
        block_number: u64,
        block_hash: B256,
        #[serde(rename = "blockTimestamp", with = "alloy_serde::quantity")]
        block_timestamp: u64,
        #[serde(rename = "matchedLogCount", with = "alloy_serde::quantity")]
        matched_log_count: u64,
        #[serde(default, skip_serializing_if = "is_false")]
        removed: bool,
    },
}

fn is_false(v: &bool) -> bool {
    !*v
}

/// RPC trait: `reth_subscribeBlockEndLogs(filter)`.
#[rpc(server, namespace = "reth")]
pub trait BlockEndLogPubSubApi {
    #[subscription(name = "subscribeBlockEndLogs", item = BlockEndLogEvent)]
    fn subscribe_block_end_logs(&self, filter: Filter) -> SubscriptionResult;
}

/// Block-end log pub-sub backed by the node's canonical state stream.
pub struct BlockEndLogPubSub<Provider> {
    provider: Provider,
}

impl<Provider> BlockEndLogPubSub<Provider> {
    pub const fn new(provider: Provider) -> Self {
        Self { provider }
    }
}

impl<Provider> BlockEndLogPubSubApiServer for BlockEndLogPubSub<Provider>
where
    Provider: CanonStateSubscriptions<Primitives = GnosisNodePrimitives> + Clone + Send + Sync + 'static,
{
    fn subscribe_block_end_logs(
        &self,
        pending: PendingSubscriptionSink,
        filter: Filter,
    ) -> SubscriptionResult {
        let provider = self.provider.clone();
        tokio::spawn(async move {
            let sink = match pending.accept().await {
                Ok(sink) => sink,
                Err(err) => {
                    tracing::warn!(target: "rpc::reth", %err, "reth_subscribeBlockEndLogs accept failed");
                    return;
                }
            };
            let stream = block_end_log_event_stream(provider, filter);
            let _ = pipe_subscription(sink, stream).await;
        });
        Ok(())
    }
}

fn block_end_log_event_stream<Provider>(
    provider: Provider,
    filter: Filter,
) -> impl Stream<Item = BlockEndLogEvent> + Unpin
where
    Provider: CanonStateSubscriptions<Primitives = GnosisNodePrimitives>,
{
    BroadcastStream::new(provider.subscribe_to_canonical_state()).flat_map(move |notification| {
        let notification = match notification {
            Ok(n) => n,
            Err(_) => return futures_util::stream::iter(Vec::new()),
        };
        let events = block_receipts_to_events::<GnosisNodePrimitives>(&filter, notification.block_receipts());
        futures_util::stream::iter(events)
    })
}

fn block_receipts_to_events<N>(
    filter: &Filter,
    block_receipts: Vec<(BlockReceipts<N::Receipt>, bool)>,
) -> Vec<BlockEndLogEvent>
where
    N: NodePrimitives,
    N::Receipt: TxReceipt<Log = alloy_primitives::Log>,
{
    let mut out = Vec::new();
    for (block_receipts, removed) in block_receipts {
        let block = block_receipts.block;
        let timestamp = block_receipts.timestamp;
        let logs = matching_block_logs_with_tx_hashes(
            filter,
            block,
            timestamp,
            block_receipts.tx_receipts.iter().map(|(tx, receipt)| (*tx, receipt)),
            removed,
        );
        let matched_log_count = logs.len() as u64;
        out.extend(logs.into_iter().map(|log| BlockEndLogEvent::Log { log }));
        out.push(BlockEndLogEvent::BlockEnd {
            block_number: block.number,
            block_hash: block.hash,
            block_timestamp: timestamp,
            matched_log_count,
            removed,
        });
    }
    out
}

#[derive(Debug, thiserror::Error)]
#[error("failed to serialize blockEnd log subscription item: {0}")]
struct SubscriptionSerializeError(#[from] serde_json::Error);

impl From<SubscriptionSerializeError> for jsonrpsee::types::ErrorObject<'static> {
    fn from(value: SubscriptionSerializeError) -> Self {
        internal_rpc_err(value.to_string())
    }
}

async fn pipe_subscription<T, St>(sink: SubscriptionSink, mut stream: St) -> Result<(), jsonrpsee::types::ErrorObject<'static>>
where
    St: Stream<Item = T> + Unpin,
    T: Serialize,
{
    loop {
        tokio::select! {
            _ = sink.closed() => break Ok(()),
            maybe_item = stream.next() => {
                let item = match maybe_item {
                    Some(item) => item,
                    None => break Ok(()),
                };
                let msg = SubscriptionMessage::new(
                    sink.method_name(),
                    sink.subscription_id(),
                    &item,
                )
                .map_err(SubscriptionSerializeError::new)?;
                if sink.send(msg).await.is_err() {
                    break Ok(());
                }
            }
        }
    }
}

impl SubscriptionSerializeError {
    const fn new(err: serde_json::Error) -> Self {
        Self(err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_eips::BlockNumHash;
    use alloy_primitives::Bytes;
    use alloy_primitives::{Address, Log as PrimitiveLog, LogData, B256};
    use alloy_rpc_types_eth::Filter;
    use reth_execution_types::BlockReceipts;
    use reth_ethereum_primitives::Receipt;

    #[test]
    fn block_end_serializes_kind() {
        let ev = BlockEndLogEvent::BlockEnd {
            block_number: 42,
            block_hash: B256::ZERO,
            block_timestamp: 1_700_000_000,
            matched_log_count: 0,
            removed: false,
        };
        let v = serde_json::to_value(&ev).unwrap();
        assert_eq!(v["kind"], "blockEnd");
        assert_eq!(v["blockNumber"].as_str().unwrap(), "0x2a");
        assert_eq!(v["matchedLogCount"].as_str().unwrap(), "0x0");
        assert!(v.get("removed").is_none());
    }

    #[test]
    fn block_receipts_empty_still_emits_block_end() {
        let filter = Filter::default();
        let block = BlockReceipts {
            block: BlockNumHash { number: 100, hash: B256::ZERO },
            tx_receipts: vec![],
            timestamp: 123,
        };
        let events = block_receipts_to_events::<GnosisNodePrimitives>(&filter, vec![(block, false)]);
        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], BlockEndLogEvent::BlockEnd { matched_log_count: 0, .. }));
    }

    #[test]
    fn log_then_block_end_order() {
        let filter = Filter::default();
        let receipt = Receipt {
            logs: vec![PrimitiveLog {
                address: Address::ZERO,
                data: LogData::new_unchecked(vec![], Bytes::new()),
            }],
            ..Default::default()
        };
        let block = BlockReceipts {
            block: BlockNumHash { number: 7, hash: B256::ZERO },
            tx_receipts: vec![(B256::ZERO, receipt)],
            timestamp: 999,
        };
        let events = block_receipts_to_events::<GnosisNodePrimitives>(&filter, vec![(block, false)]);
        assert_eq!(events.len(), 2);
        assert!(matches!(events[0], BlockEndLogEvent::Log { .. }));
        assert!(matches!(events[1], BlockEndLogEvent::BlockEnd { block_number: 7, matched_log_count: 1, .. }));
    }
}
