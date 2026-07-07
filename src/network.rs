use reth::{
    api::{FullNodeTypes, NodeTypes, TxTy},
    builder::{components::NetworkBuilder, BuilderContext},
    network::{NetworkHandle, NetworkManager, PeersInfo},
};
use reth_chainspec::EthChainSpec;
use reth_eth_wire_types::{BasicNetworkPrimitives, UnifiedStatus};
use reth_ethereum_primitives::PooledTransactionVariant;
use reth_transaction_pool::{PoolTransaction, TransactionPool};
use tracing::info;

use crate::{primitives::GnosisNodePrimitives, spec::gnosis_spec::GnosisChainSpec};

pub type GnosisNetworkPrimitives =
    BasicNetworkPrimitives<GnosisNodePrimitives, PooledTransactionVariant>;

/// A basic ethereum payload service.
#[derive(Debug, Default, Clone, Copy)]
pub struct GnosisNetworkBuilder {
    // TODO add closure to modify network
}

impl<Node, Pool> NetworkBuilder<Node, Pool> for GnosisNetworkBuilder
where
    Node: FullNodeTypes<
        Types: NodeTypes<ChainSpec = GnosisChainSpec, Primitives = GnosisNodePrimitives>,
    >,
    Pool: TransactionPool<
            Transaction: PoolTransaction<
                Consensus = TxTy<Node::Types>,
                Pooled = PooledTransactionVariant,
            >,
        > + Unpin
        + 'static,
{
    type Network = NetworkHandle<GnosisNetworkPrimitives>;

    async fn build_network(
        self,
        ctx: &BuilderContext<Node>,
        pool: Pool,
    ) -> eyre::Result<NetworkHandle<GnosisNetworkPrimitives>> {
        let mut network_config = ctx.network_config()?;

        let spec = ctx.chain_spec();
        let head = &ctx.head();

        // using actual genesis hash for mainnet and chiado
        let genesis_hash = spec.genesis_hash();

        // lookup_head() returns total_difficulty=0 for all blocks. On pre-merge
        // AuRa chains this causes peers to reject us (TD=0 at block N>0 is invalid).
        // Use the chain spec's terminal total difficulty as a reasonable value —
        // it tells peers we've completed the pre-merge chain.
        let total_difficulty = if head.total_difficulty.is_zero() && head.number > 0 {
            spec.final_paris_total_difficulty()
                .unwrap_or(head.total_difficulty)
        } else {
            head.total_difficulty
        };

        let status = UnifiedStatus::builder()
            .chain(spec.chain())
            .genesis(genesis_hash)
            .blockhash(head.hash)
            .total_difficulty(Some(total_difficulty))
            .forkid(network_config.fork_filter.current())
            .build();
        network_config.status = status;

        let network = NetworkManager::builder(network_config).await?;
        let handle = ctx.start_network(network, pool);
        info!(target: "reth::cli", enode=%handle.local_node_record(), "P2P networking initialized");

        // 取得 TransactionsHandle（async oneshot 到 NetworkManager），缓存到进程级单例，
        // 供 `arb_sendRawTransactionFast` 快路径强制广播使用。NetworkManager 就绪后才会
        // 返回 Some；启动初期可能返回 None，此时快路径不可用（RPC 返回 -38001）。
        let handle_for_tx = handle.clone();
        ctx.task_executor().spawn_drop(async move {
            match handle_for_tx.transactions_handle().await {
                Some(tx_handle) => {
                    crate::fast_tx::set_fast_tx_handle(tx_handle);
                    info!(target: "reth::cli", "fast-tx broadcast handle ready");
                }
                None => {
                    tracing::warn!(
                        target: "reth::cli",
                        "transactions_handle unavailable; arb_sendRawTransactionFast disabled"
                    );
                }
            }
        });

        Ok(handle)
    }
}
