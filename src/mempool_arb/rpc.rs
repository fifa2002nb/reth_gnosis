use std::sync::Arc;

use jsonrpsee::{core::RpcResult, proc_macros::rpc};

use super::hub::MempoolArbHub;
use super::types::{PendingArbSnapshot, PendingArbSnapshotFilter};

/// HTTP: `reth_getPendingArbSnapshot(filter?)` — synchronous pending tip snapshot.
#[rpc(server, namespace = "reth")]
pub trait MempoolArbApi {
    #[method(name = "getPendingArbSnapshot")]
    fn get_pending_arb_snapshot(
        &self,
        filter: PendingArbSnapshotFilter,
    ) -> RpcResult<PendingArbSnapshot>;
}

pub struct MempoolArbRpc {
    hub: Arc<MempoolArbHub>,
}

impl MempoolArbRpc {
    pub const fn new(hub: Arc<MempoolArbHub>) -> Self {
        Self { hub }
    }
}

impl MempoolArbApiServer for MempoolArbRpc {
    fn get_pending_arb_snapshot(
        &self,
        filter: PendingArbSnapshotFilter,
    ) -> RpcResult<PendingArbSnapshot> {
        Ok(self.hub.snapshot(filter))
    }
}
