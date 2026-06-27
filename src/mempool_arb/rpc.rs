use std::sync::Arc;

use jsonrpsee::{core::RpcResult, proc_macros::rpc};
use serde::{Deserialize, Serialize};

use super::hub::MempoolArbHub;
use super::types::{PendingArbSnapshot, PendingArbSnapshotFilter};

/// Request body for blacklist add/remove — selectors as hex strings ("0x91252c55").
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BlacklistUpdate {
    pub selectors: Vec<String>,
}

/// HTTP RPC: snapshot + dynamic blacklist management.
#[rpc(server, namespace = "reth")]
pub trait MempoolArbApi {
    #[method(name = "getPendingArbSnapshot")]
    fn get_pending_arb_snapshot(
        &self,
        filter: PendingArbSnapshotFilter,
    ) -> RpcResult<PendingArbSnapshot>;

    /// Add selectors to the runtime blacklist.
    #[method(name = "addArbBlacklist")]
    fn add_arb_blacklist(&self, update: BlacklistUpdate) -> RpcResult<Vec<String>>;

    /// Remove selectors from the runtime blacklist.
    #[method(name = "removeArbBlacklist")]
    fn remove_arb_blacklist(&self, update: BlacklistUpdate) -> RpcResult<Vec<String>>;

    /// List current blacklist selectors.
    #[method(name = "getArbBlacklist")]
    fn get_arb_blacklist(&self) -> RpcResult<Vec<String>>;

    /// Add selectors to the runtime whitelist.
    #[method(name = "addArbWhitelist")]
    fn add_arb_whitelist(&self, update: BlacklistUpdate) -> RpcResult<Vec<String>>;

    /// Remove selectors from the runtime whitelist.
    #[method(name = "removeArbWhitelist")]
    fn remove_arb_whitelist(&self, update: BlacklistUpdate) -> RpcResult<Vec<String>>;

    /// List current whitelist selectors.
    #[method(name = "getArbWhitelist")]
    fn get_arb_whitelist(&self) -> RpcResult<Vec<String>>;
}

pub struct MempoolArbRpc {
    hub: Arc<MempoolArbHub>,
}

impl MempoolArbRpc {
    pub const fn new(hub: Arc<MempoolArbHub>) -> Self {
        Self { hub }
    }
}

fn parse_selectors(hex_strs: &[String]) -> Result<Vec<[u8; 4]>, String> {
    hex_strs
        .iter()
        .map(|s| {
            let s = s.strip_prefix("0x").unwrap_or(s);
            let bytes = hex::decode(s).map_err(|e| format!("invalid hex {s}: {e}"))?;
            if bytes.len() != 4 {
                return Err(format!("selector must be 4 bytes, got {} for {s}", bytes.len()));
            }
            let mut arr = [0u8; 4];
            arr.copy_from_slice(&bytes);
            Ok(arr)
        })
        .collect()
}

fn selectors_to_hex(selectors: &[[u8; 4]]) -> Vec<String> {
    selectors
        .iter()
        .map(|s| format!("0x{}", hex::encode(s)))
        .collect()
}

impl MempoolArbApiServer for MempoolArbRpc {
    fn get_pending_arb_snapshot(
        &self,
        filter: PendingArbSnapshotFilter,
    ) -> RpcResult<PendingArbSnapshot> {
        Ok(self.hub.snapshot(filter))
    }

    fn add_arb_blacklist(&self, update: BlacklistUpdate) -> RpcResult<Vec<String>> {
        let selectors = parse_selectors(&update.selectors)
            .map_err(|msg| jsonrpsee::types::ErrorObject::owned(-32602, msg, None::<()>))?;
        self.hub.blacklist().add(&selectors);
        Ok(selectors_to_hex(&self.hub.blacklist().snapshot()))
    }

    fn remove_arb_blacklist(&self, update: BlacklistUpdate) -> RpcResult<Vec<String>> {
        let selectors = parse_selectors(&update.selectors)
            .map_err(|msg| jsonrpsee::types::ErrorObject::owned(-32602, msg, None::<()>))?;
        self.hub.blacklist().remove(&selectors);
        Ok(selectors_to_hex(&self.hub.blacklist().snapshot()))
    }

    fn get_arb_blacklist(&self) -> RpcResult<Vec<String>> {
        Ok(selectors_to_hex(&self.hub.blacklist().snapshot()))
    }

    fn add_arb_whitelist(&self, update: BlacklistUpdate) -> RpcResult<Vec<String>> {
        let selectors = parse_selectors(&update.selectors)
            .map_err(|msg| jsonrpsee::types::ErrorObject::owned(-32602, msg, None::<()>))?;
        self.hub.whitelist().add(&selectors);
        Ok(selectors_to_hex(&self.hub.whitelist().snapshot()))
    }

    fn remove_arb_whitelist(&self, update: BlacklistUpdate) -> RpcResult<Vec<String>> {
        let selectors = parse_selectors(&update.selectors)
            .map_err(|msg| jsonrpsee::types::ErrorObject::owned(-32602, msg, None::<()>))?;
        self.hub.whitelist().remove(&selectors);
        Ok(selectors_to_hex(&self.hub.whitelist().snapshot()))
    }

    fn get_arb_whitelist(&self) -> RpcResult<Vec<String>> {
        Ok(selectors_to_hex(&self.hub.whitelist().snapshot()))
    }
}
