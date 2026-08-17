use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use jsonrpsee::{
    core::SubscriptionResult, proc_macros::rpc, server::SubscriptionMessage,
    PendingSubscriptionSink, SubscriptionSink,
};
use serde::{Deserialize, Serialize};

use super::gas_pressure::GasPressureTracker;

fn default_arm_threshold_permille() -> u32 {
    900
}
fn default_disarm_threshold_permille() -> u32 {
    800
}
fn default_poll_ms() -> u64 {
    200
}

/// Params for `reth_subscribeBlockGasPressure`.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GasPressureFilter {
    /// Caller's own max willingness-to-pay tip (wei/gas) — matches goodboy's
    /// `competitive_tip_max_wei_per_gas`. The water level only counts pending
    /// gas at or below this tip; bids above it will claim block space
    /// regardless of what we do, so they sit outside our decision space.
    #[serde(default, with = "alloy_serde::quantity")]
    pub ceiling_wei_per_gas: u64,
    /// Per-mille (out of 1000) fill ratio at which an `armed:true` event fires.
    #[serde(default = "default_arm_threshold_permille")]
    pub arm_threshold_permille: u32,
    /// Fill ratio must drop back below this before re-arming (hysteresis) —
    /// keeps a ratio oscillating near the arm threshold from spamming events.
    #[serde(default = "default_disarm_threshold_permille")]
    pub disarm_threshold_permille: u32,
    /// Poll interval (ms) for recomputing the water level.
    #[serde(default = "default_poll_ms")]
    pub poll_ms: u64,
}

/// Pushed over `reth_subscribeBlockGasPressure` on each armed/disarmed edge
/// (not every poll tick — only when crossing a threshold).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct GasPressureEvent {
    pub armed: bool,
    pub ratio_permille: u32,
    #[serde(with = "alloy_serde::quantity")]
    pub gas_sum: u64,
    #[serde(with = "alloy_serde::quantity")]
    pub gas_limit: u64,
    #[serde(with = "alloy_serde::quantity")]
    pub head_block: u64,
    #[serde(with = "alloy_serde::quantity")]
    pub at_ms: u64,
}

/// WS: `reth_subscribeBlockGasPressure(filter)` — edge-triggered signal for
/// "the next block's gas is about to fill up at or below my max tip". Lets a
/// subscriber (goodboy's RBF defender) skip a fixed time-window wait and
/// react to real mempool congestion instead.
#[rpc(server, namespace = "reth")]
pub trait GasPressurePubSubApi {
    #[subscription(name = "subscribeBlockGasPressure", item = GasPressureEvent)]
    fn subscribe_block_gas_pressure(&self, filter: GasPressureFilter) -> SubscriptionResult;
}

pub struct GasPressurePubSub {
    tracker: Arc<GasPressureTracker>,
}

impl GasPressurePubSub {
    pub const fn new(tracker: Arc<GasPressureTracker>) -> Self {
        Self { tracker }
    }
}

impl GasPressurePubSubApiServer for GasPressurePubSub {
    fn subscribe_block_gas_pressure(
        &self,
        pending: PendingSubscriptionSink,
        filter: GasPressureFilter,
    ) -> SubscriptionResult {
        let tracker = self.tracker.clone();
        tokio::spawn(async move {
            let sink = match pending.accept().await {
                Ok(sink) => sink,
                Err(err) => {
                    tracing::warn!(target: "rpc::reth", %err, "reth_subscribeBlockGasPressure accept failed");
                    return;
                }
            };
            poll_and_push(sink, tracker, filter).await;
        });
        Ok(())
    }
}

/// Owns per-subscription armed/disarmed state — each caller gets its own
/// hysteresis, independent of any other subscriber's ceiling/thresholds.
async fn poll_and_push(sink: SubscriptionSink, tracker: Arc<GasPressureTracker>, filter: GasPressureFilter) {
    let poll_ms = filter.poll_ms.max(20);
    let mut interval = tokio::time::interval(Duration::from_millis(poll_ms));
    let mut armed = false;
    let mut last_head = 0u64;
    loop {
        tokio::select! {
            _ = sink.closed() => return,
            _ = interval.tick() => {
                let (gas_sum, gas_limit) = tracker.water_level(filter.ceiling_wei_per_gas);
                if gas_limit == 0 {
                    continue;
                }
                let head_block = tracker.head_block_number();
                let ratio_permille = ((gas_sum as u128 * 1000) / gas_limit as u128).min(u32::MAX as u128) as u32;
                let Some(armed_now) = decide_push(
                    &mut armed,
                    &mut last_head,
                    head_block,
                    ratio_permille,
                    filter.arm_threshold_permille,
                    filter.disarm_threshold_permille,
                ) else {
                    continue;
                };

                let event = GasPressureEvent {
                    armed: armed_now,
                    ratio_permille,
                    gas_sum,
                    gas_limit,
                    head_block,
                    at_ms: now_ms(),
                };
                let msg = match SubscriptionMessage::new(sink.method_name(), sink.subscription_id(), &event) {
                    Ok(msg) => msg,
                    Err(err) => {
                        tracing::warn!(target: "rpc::reth", %err, "gas pressure event serialize failed");
                        continue;
                    }
                };
                if sink.send(msg).await.is_err() {
                    return;
                }
            }
        }
    }
}

/// Arm/disarm edge, plus a forced snapshot when `head_block` advances so
/// subscribers drop the previous window's ratio instead of keeping a stale
/// armed cache across the block boundary.
fn decide_push(
    armed: &mut bool,
    last_head: &mut u64,
    head_block: u64,
    ratio_permille: u32,
    arm_threshold: u32,
    disarm_threshold: u32,
) -> Option<bool> {
    let head_changed = *last_head != 0 && head_block != *last_head;
    if head_changed {
        *armed = false;
    }
    *last_head = head_block;

    if !*armed && ratio_permille >= arm_threshold {
        *armed = true;
        Some(true)
    } else if *armed && ratio_permille <= disarm_threshold {
        *armed = false;
        Some(false)
    } else if head_changed {
        Some(false)
    } else {
        None
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::decide_push;

    #[test]
    fn new_head_forces_disarm_snapshot() {
        let mut armed = true;
        let mut last_head = 100;
        // Previous window was armed; new head with empty buckets must push armed=false
        // so the subscriber drops the stale high ratio.
        assert_eq!(decide_push(&mut armed, &mut last_head, 101, 0, 940, 840), Some(false));
        assert!(!armed);
        assert_eq!(last_head, 101);
        // Same head, still empty: no more events.
        assert_eq!(decide_push(&mut armed, &mut last_head, 101, 0, 940, 840), None);
    }

    #[test]
    fn same_head_only_emits_on_edges() {
        let mut armed = false;
        let mut last_head = 50;
        assert_eq!(decide_push(&mut armed, &mut last_head, 50, 900, 940, 840), None);
        assert_eq!(decide_push(&mut armed, &mut last_head, 50, 950, 940, 840), Some(true));
        assert!(armed);
        assert_eq!(decide_push(&mut armed, &mut last_head, 50, 900, 940, 840), None);
        assert_eq!(decide_push(&mut armed, &mut last_head, 50, 800, 940, 840), Some(false));
        assert!(!armed);
    }

    #[test]
    fn first_head_does_not_force_snapshot() {
        let mut armed = false;
        let mut last_head = 0;
        assert_eq!(decide_push(&mut armed, &mut last_head, 42, 100, 940, 840), None);
        assert_eq!(last_head, 42);
    }
}
