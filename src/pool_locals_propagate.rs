//! 仅允许 `--txpool.locals` 中地址的交易向 P2P 网络广播。
//!
//! 其他交易仍正常入池（供本地 mempool 监控 / 打包），但 `propagate=false`，
//! 不会触发 pending listener gossip，也不会在 `GetPooledTransactions` 中返回。
//!
//! [`crate::fast_tx::FastTxRpc`] 快路径绕过 txpool，不受此逻辑影响。

use std::fmt::{self, Debug};

use reth_primitives_traits::SealedBlock;
use reth_transaction_pool::{
    PoolTransaction, TransactionOrigin,
    LocalTransactionConfig, TransactionValidationOutcome, TransactionValidator,
};

/// Wraps an inner validator and only marks txs as propagatable when their sender
/// is listed in `--txpool.locals`.
#[derive(Clone)]
pub struct LocalsOnlyPropagateValidator<V> {
    inner: V,
    local_config: LocalTransactionConfig,
}

impl<V: Debug> Debug for LocalsOnlyPropagateValidator<V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LocalsOnlyPropagateValidator")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

impl<V> LocalsOnlyPropagateValidator<V> {
    pub fn new(inner: V, local_config: LocalTransactionConfig) -> Self {
        Self { inner, local_config }
    }

    fn adjust_propagate<T: PoolTransaction>(
        outcome: TransactionValidationOutcome<T>,
        local_config: &LocalTransactionConfig,
    ) -> TransactionValidationOutcome<T> {
        match outcome {
            TransactionValidationOutcome::Valid {
                balance,
                state_nonce,
                bytecode_hash,
                transaction,
                propagate: _,
                authorities,
            } => {
                let propagate =
                    local_config.contains_local_address(transaction.transaction().sender_ref());
                TransactionValidationOutcome::Valid {
                    balance,
                    state_nonce,
                    bytecode_hash,
                    transaction,
                    propagate,
                    authorities,
                }
            }
            other => other,
        }
    }
}

impl<V> TransactionValidator for LocalsOnlyPropagateValidator<V>
where
    V: TransactionValidator + Send + Sync,
{
    type Transaction = V::Transaction;
    type Block = V::Block;

    async fn validate_transaction(
        &self,
        origin: TransactionOrigin,
        transaction: Self::Transaction,
    ) -> TransactionValidationOutcome<Self::Transaction> {
        let outcome = self.inner.validate_transaction(origin, transaction).await;
        Self::adjust_propagate(outcome, &self.local_config)
    }

    fn on_new_head_block(&self, new_tip_block: &SealedBlock<Self::Block>) {
        self.inner.on_new_head_block(new_tip_block);
    }
}
