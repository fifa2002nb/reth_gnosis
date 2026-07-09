//! 快路径交易广播：让本地 caller 通过 `arb_sendRawTransactionFast` 立即向全网
//! 强制广播一笔签名交易，跳过 txpool 的验证/排队/入池。
//!
//! 完全不入本地池，因此 txpool API 查不到、本节点不打包、无防重复——仅适用于
//! 「默认本地 tx 有效、只靠全网广播」的套利发单场景。
//!
//! ## 为何不走 `TransactionsHandle::broadcast_transactions`
//!
//! reth 的 `BroadcastTransactions` → `propagate_transactions(Forced)` **并不会**对所有
//! peer 发完整 tx：`Forced` 只跳过 `seen_transactions` 过滤，full/hash 分流仍由
//! `TransactionPropagationMode`（默认 `Sqrt`）决定——只对 √N 个 peer 发 full，其余只
//! announce hash。对端再用 `GetPooledTransactions` 来拉 body 时，本节点 pool 里没有这
//! 笔 tx，应答为空 → hash-only 那批 peer 永远拿不到交易体。
//!
//! 因此本路径改为：
//! 1. 用 [`TransactionsHandle::get_active_peers`] 拿到当前可收 tx gossip 的 peer 集合；
//! 2. 对每个 peer 直接 [`NetworkHandle::send_transactions`] 发完整 tx（不经 hash announce、
//!    不经 pool）。
//!
//! 句柄由 [`crate::network::GnosisNetworkBuilder`] 在节点启动时取得并填充到进程级单例。
//! 采用全局 [`OnceLock`] 是因为：泛型 `Node::Network: FullNetwork` 看不到
//! `NetworkHandle` 的 inherent `transactions_handle()`，而 `build_network` 内部持有的是
//! 具体类型，可在那里取得句柄并填充本单例。单节点进程，单例可接受。

use std::sync::{Arc, OnceLock};

use alloy_primitives::{B256, Bytes};
use jsonrpsee::{
    core::{async_trait, RpcResult},
    proc_macros::rpc,
};
use reth::network::{transactions::TransactionsHandle, NetworkHandle};
use reth_rpc_eth_types::utils::recover_raw_transaction;

use crate::network::GnosisNetworkPrimitives;
use crate::primitives::block::TransactionSigned;

/// 进程级单例：P2P 网络句柄（用于对每个 peer 发完整 Transactions 消息）。
static FAST_TX_NETWORK: OnceLock<NetworkHandle<GnosisNetworkPrimitives>> = OnceLock::new();
/// 进程级单例：TransactionsManager 句柄（用于枚举当前 active peers）。
static FAST_TX_HANDLE: OnceLock<TransactionsHandle<GnosisNetworkPrimitives>> = OnceLock::new();

/// 由 [`crate::network::GnosisNetworkBuilder`] 在节点启动时调用，缓存广播所需句柄。
/// 重复设置会被忽略并告警（正常只设置一次）。
pub(crate) fn set_fast_tx_handles(
    network: NetworkHandle<GnosisNetworkPrimitives>,
    tx_handle: TransactionsHandle<GnosisNetworkPrimitives>,
) {
    if FAST_TX_NETWORK.set(network).is_err() {
        tracing::warn!(target: "fast_tx", "FAST_TX_NETWORK already set; ignoring duplicate");
    }
    if FAST_TX_HANDLE.set(tx_handle).is_err() {
        tracing::warn!(target: "fast_tx", "FAST_TX_HANDLE already set; ignoring duplicate");
    }
}

/// 立即向所有 active peer 发送完整签名交易（不验证、不入池、不走 hash announce）。
/// 句柄未就绪时返回 `false`。
async fn try_fast_broadcast(tx: TransactionSigned) -> bool {
    let (Some(network), Some(tx_handle)) = (FAST_TX_NETWORK.get(), FAST_TX_HANDLE.get()) else {
        return false;
    };

    if network.tx_gossip_disabled() {
        tracing::warn!(target: "fast_tx", "tx gossip disabled; fast broadcast skipped");
        return false;
    }

    let peers = match tx_handle.get_active_peers().await {
        Ok(peers) => peers,
        Err(err) => {
            tracing::warn!(target: "fast_tx", %err, "get_active_peers failed");
            return false;
        }
    };

    let peer_count = peers.len();
    let shared = Arc::new(tx);
    for peer_id in peers {
        network.send_transactions(peer_id, vec![Arc::clone(&shared)]);
    }
    tracing::debug!(target: "fast_tx", peer_count, "fast broadcast full tx to all active peers");
    true
}

/// 快路径交易广播 RPC（namespace = `arb`）。
#[rpc(server, namespace = "arb")]
pub trait FastTxApi {
    /// 立即广播 raw transaction，跳过 txpool 验证/排队/入池，返回交易哈希。
    ///
    /// 与标准 `eth_sendRawTransaction` 的区别：
    /// - 不调用 `pool.add_transaction`，因此不 await 任何验证 task；
    /// - 对所有 active peer 直接发完整 `Transactions` 消息（不经 Sqrt hash announce）；
    /// - 不入本地池（txpool API 查不到、本节点不打包、无防重）。
    ///
    /// 句柄未就绪（节点启动初期）时返回 `-38001`，caller 可重试或 fallback
    /// 到标准 `eth_sendRawTransaction`。
    #[method(name = "sendRawTransactionFast")]
    async fn send_raw_transaction_fast(&self, bytes: Bytes) -> RpcResult<B256>;
}

/// [`FastTxApi`] 的服务端实现。
#[derive(Debug, Clone, Default)]
pub struct FastTxRpc;

#[async_trait]
impl FastTxApiServer for FastTxRpc {
    async fn send_raw_transaction_fast(&self, bytes: Bytes) -> RpcResult<B256> {
        // 1. decode + ecrecover sender（必须：广播与计算 hash 都需要 recovered tx）
        let recovered = recover_raw_transaction::<TransactionSigned>(&bytes).map_err(|e| {
            jsonrpsee::types::ErrorObject::owned(
                -32000,
                format!("recover raw transaction failed: {e:?}"),
                None::<()>,
            )
        })?;

        let hash: B256 = *recovered.hash();

        // 2. 取回签名交易本体（广播需要 TransactionSigned = N::BroadcastedTransaction）
        let signed = recovered.into_inner();

        // 3. 向所有 active peer 发完整 tx；句柄未就绪时返回错误，caller 可重试或 fallback。
        if !try_fast_broadcast(signed).await {
            return Err(jsonrpsee::types::ErrorObject::owned(
                -38001,
                "fast broadcast handle not ready (node still starting)",
                None::<()>,
            ));
        }

        Ok(hash)
    }
}
