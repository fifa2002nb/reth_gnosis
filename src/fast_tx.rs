//! 快路径交易广播：让本地 caller 通过 `arb_sendRawTransactionFast` 立即向全网
//! 强制广播一笔签名交易，跳过 txpool 的验证/排队/入池。
//!
//! 完全不入本地池，因此 txpool API 查不到、本节点不打包、无防重复——仅适用于
//! 「默认本地 tx 有效、只靠全网广播」的套利发单场景。
//!
//! 实现复用 reth 原生的同步强制广播入口 [`TransactionsHandle::broadcast_transactions`]
//! (→ `TransactionsCommand::BroadcastTransactions` → `propagate_transactions(Forced)`)：
//! 立即把完整 tx 强制广播给所有 peer，不经 pool、不等验证。
//!
//! 广播句柄由 [`crate::network::GnosisNetworkBuilder`] 在节点启动时取得并填充到
//! 进程级单例 [`FAST_TX_HANDLE`]。采用全局 [`OnceLock`] 是因为：泛型
//! `Node::Network: FullNetwork` 看不到 `NetworkHandle` 的 inherent
//! `transactions_handle()`，而 `build_network` 内部持有的是具体类型
//! `NetworkHandle<GnosisNetworkPrimitives>`，可在那里取得句柄并填充本单例，从而
//! 绕过泛型限制把广播能力暴露给 RPC 层。单节点进程，单例可接受（与现有
//! [`crate::mempool_arb`] 的 in-memory 状态风格一致）。

use std::sync::OnceLock;

use alloy_primitives::{B256, Bytes};
use jsonrpsee::{core::RpcResult, proc_macros::rpc};
use reth::network::transactions::TransactionsHandle;
use reth_rpc_eth_types::utils::recover_raw_transaction;

use crate::network::GnosisNetworkPrimitives;
use crate::primitives::block::TransactionSigned;

/// 进程级单例：缓存的强制广播句柄，由 network builder 在启动时填充。
static FAST_TX_HANDLE: OnceLock<TransactionsHandle<GnosisNetworkPrimitives>> = OnceLock::new();

/// 由 [`crate::network::GnosisNetworkBuilder`] 在节点启动时调用，缓存强制广播句柄。
/// 重复设置会被忽略并告警（正常只设置一次）。
pub(crate) fn set_fast_tx_handle(handle: TransactionsHandle<GnosisNetworkPrimitives>) {
    if FAST_TX_HANDLE.set(handle).is_err() {
        tracing::warn!(target: "fast_tx", "FAST_TX_HANDLE already set; ignoring duplicate");
    }
}

/// 立即强制广播一笔签名交易（不验证、不入池）。句柄未就绪时返回 `false`。
fn try_fast_broadcast(tx: TransactionSigned) -> bool {
    if let Some(handle) = FAST_TX_HANDLE.get() {
        handle.broadcast_transactions(std::iter::once(tx));
        true
    } else {
        false
    }
}

/// 快路径交易广播 RPC（namespace = `arb`）。
#[rpc(server, namespace = "arb")]
pub trait FastTxApi {
    /// 立即广播 raw transaction，跳过 txpool 验证/排队/入池，返回交易哈希。
    ///
    /// 与标准 `eth_sendRawTransaction` 的区别：
    /// - 不调用 `pool.add_transaction`，因此不 await 任何验证 task；
    /// - 直接走 `TransactionsHandle::broadcast_transactions` 强制广播完整 tx；
    /// - 不入本地池（txpool API 查不到、本节点不打包、无防重）。
    ///
    /// 句柄未就绪（节点启动初期）时返回 `-38001`，caller 可重试或 fallback
    /// 到标准 `eth_sendRawTransaction`。
    #[method(name = "sendRawTransactionFast")]
    fn send_raw_transaction_fast(&self, bytes: Bytes) -> RpcResult<B256>;
}

/// [`FastTxApi`] 的服务端实现。
#[derive(Debug, Clone, Default)]
pub struct FastTxRpc;

impl FastTxApiServer for FastTxRpc {
    fn send_raw_transaction_fast(&self, bytes: Bytes) -> RpcResult<B256> {
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

        // 3. 立即强制广播；句柄未就绪时返回错误，caller 可重试或 fallback。
        if !try_fast_broadcast(signed) {
            return Err(jsonrpsee::types::ErrorObject::owned(
                -38001,
                "fast broadcast handle not ready (node still starting)",
                None::<()>,
            ));
        }

        Ok(hash)
    }
}
