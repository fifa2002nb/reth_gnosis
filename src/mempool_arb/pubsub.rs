use std::sync::Arc;

use futures_util::{future::ready, Stream, StreamExt};
use jsonrpsee::{
    core::SubscriptionResult,
    proc_macros::rpc,
    server::SubscriptionMessage,
    PendingSubscriptionSink, SubscriptionSink,
};
use reth_rpc_server_types::result::internal_rpc_err;
use serde::Serialize;
use tokio_stream::wrappers::BroadcastStream;

use super::hub::MempoolArbHub;
use super::types::{PendingArbFilter, PendingArbTxEvent};

#[derive(Debug, thiserror::Error)]
#[error("failed to serialize pending arb subscription item: {0}")]
struct SubscriptionSerializeError(#[from] serde_json::Error);

impl From<SubscriptionSerializeError> for jsonrpsee::types::ErrorObject<'static> {
    fn from(value: SubscriptionSerializeError) -> Self {
        internal_rpc_err(value.to_string())
    }
}

/// WS: `reth_subscribePendingArbTx(filter?)` — realtime pending tip changelog.
#[rpc(server, namespace = "reth")]
pub trait MempoolArbPubSubApi {
    #[subscription(name = "subscribePendingArbTx", item = PendingArbTxEvent)]
    fn subscribe_pending_arb_tx(
        &self,
        filter: PendingArbFilter,
    ) -> SubscriptionResult;
}

pub struct MempoolArbPubSub {
    hub: Arc<MempoolArbHub>,
}

impl MempoolArbPubSub {
    pub const fn new(hub: Arc<MempoolArbHub>) -> Self {
        Self { hub }
    }
}

impl MempoolArbPubSubApiServer for MempoolArbPubSub {
    fn subscribe_pending_arb_tx(
        &self,
        pending: PendingSubscriptionSink,
        filter: PendingArbFilter,
    ) -> SubscriptionResult {
        let hub = self.hub.clone();
        tokio::spawn(async move {
            let sink = match pending.accept().await {
                Ok(sink) => sink,
                Err(err) => {
                    tracing::warn!(target: "rpc::reth", %err, "reth_subscribePendingArbTx accept failed");
                    return;
                }
            };
            let stream = filtered_event_stream(hub, filter);
            let _ = pipe_subscription(sink, stream).await;
        });
        Ok(())
    }
}

fn filtered_event_stream(
    hub: Arc<MempoolArbHub>,
    filter: PendingArbFilter,
) -> impl Stream<Item = PendingArbTxEvent> + Unpin {
    BroadcastStream::new(hub.subscribe()).filter_map(move |msg| {
        ready(match msg {
            Ok(evt) if matches_filter(&filter, &evt) => Some(evt),
            _ => None,
        })
    })
}

fn matches_filter(filter: &PendingArbFilter, evt: &PendingArbTxEvent) -> bool {
    if evt.action == "removed" {
        return true;
    }
    if let Some(min) = filter.min_effective_tip_per_gas {
        if evt.effective_tip_per_gas < alloy_primitives::U256::from(min) {
            return false;
        }
    }
    true
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
                .map_err(SubscriptionSerializeError)?;
                if sink.send(msg).await.is_err() {
                    break Ok(());
                }
            }
        }
    }
}
