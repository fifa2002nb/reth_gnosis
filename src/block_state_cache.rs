//! Short-TTL pool of resolved historical state providers, shared by the fork/arb
//! simulation RPCs (`eth_callAtBlock`, `eth_callScriptAtBlock`,
//! `arb_simulateArbitrageAtBlock`).
//!
//! A single candidate-evaluation burst from an arb bot fires many of these calls
//! against the *same* `block_number`. Each call used to independently resolve
//! `block_number -> block_hash -> header -> StateProviderBox` via
//! `StateProviderFactory::history_by_block_hash`, which opens a fresh DB read
//! transaction every time. This cache keeps a small pool of idle
//! `StateProviderBox`es per block hash so repeat calls within `TTL` reuse one
//! instead of opening a new transaction.
//!
//! State for a given block hash is immutable forever, so reuse is never a
//! staleness risk — the TTL only bounds how long we keep tracking a block
//! that's no longer being queried, and `MAX_POOLED_PER_BLOCK` bounds how many
//! idle transactions we keep open per block. Providers are checked out (not
//! shared by reference), so concurrent calls against the same block still run
//! their EVM execution fully in parallel instead of serializing on a lock.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use alloy_primitives::B256;
use gnosis_primitives::header::GnosisHeader;
use jsonrpsee::types::ErrorObjectOwned;
use reth_provider::{BlockHashReader, HeaderProvider, StateProviderFactory};
use reth_storage_api::StateProviderBox;

/// How long a resolved block stays tracked after its last use.
const TTL: Duration = Duration::from_secs(3);
/// Cap on distinct blocks tracked at once (bots target head/head-1 in practice).
const MAX_TRACKED_BLOCKS: usize = 8;
/// Cap on idle `StateProviderBox`es kept per block (bounds open read-tx count).
const MAX_POOLED_PER_BLOCK: usize = 32;

struct BlockEntry {
    header: GnosisHeader,
    last_used: Mutex<Instant>,
    idle: Mutex<Vec<StateProviderBox>>,
}

/// Shared cache mapping `block_number -> (header, pool of StateProviderBox)`.
pub struct BlockStateCache {
    entries: Mutex<HashMap<B256, Arc<BlockEntry>>>,
}

/// A checked-out state provider for one call. Returns itself to the pool on drop.
pub struct CheckedOutState {
    entry: Arc<BlockEntry>,
    provider: Option<StateProviderBox>,
}

impl CheckedOutState {
    pub fn header(&self) -> &GnosisHeader {
        &self.entry.header
    }

    pub fn provider(&self) -> &StateProviderBox {
        self.provider.as_ref().expect("provider taken before drop")
    }
}

impl Drop for CheckedOutState {
    fn drop(&mut self) {
        if let Some(provider) = self.provider.take() {
            let mut idle = self.entry.idle.lock().expect("block state pool lock");
            if idle.len() < MAX_POOLED_PER_BLOCK {
                idle.push(provider);
            }
        }
    }
}

impl BlockStateCache {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            entries: Mutex::new(HashMap::new()),
        })
    }

    /// Resolves `block_number` to its header + a state provider, reusing an idle
    /// pooled provider for the same block hash when available, otherwise
    /// resolving a fresh one via `StateProviderFactory::history_by_block_hash`.
    pub fn checkout<Provider>(
        &self,
        provider: &Provider,
        block_number: u64,
    ) -> Result<CheckedOutState, ErrorObjectOwned>
    where
        Provider: BlockHashReader + HeaderProvider<Header = GnosisHeader> + StateProviderFactory,
    {
        let block_hash = provider
            .block_hash(block_number)
            .map_err(|e| ErrorObjectOwned::owned(-32000, format!("Provider error: {}", e), None::<()>))?
            .ok_or_else(|| ErrorObjectOwned::owned(-32000, "Block not found", None::<()>))?;

        self.evict_expired();

        let entry = self.entry_for(provider, block_hash)?;
        *entry.last_used.lock().expect("block state pool lock") = Instant::now();

        let pooled = entry.idle.lock().expect("block state pool lock").pop();
        let state_provider = match pooled {
            Some(p) => p,
            None => provider.history_by_block_hash(block_hash).map_err(|e| {
                ErrorObjectOwned::owned(-32000, format!("State not available: {}", e), None::<()>)
            })?,
        };

        Ok(CheckedOutState {
            entry,
            provider: Some(state_provider),
        })
    }

    fn entry_for<Provider>(
        &self,
        provider: &Provider,
        block_hash: B256,
    ) -> Result<Arc<BlockEntry>, ErrorObjectOwned>
    where
        Provider: HeaderProvider<Header = GnosisHeader>,
    {
        let mut entries = self.entries.lock().expect("block state cache lock");
        if let Some(entry) = entries.get(&block_hash) {
            return Ok(entry.clone());
        }

        let header = provider
            .header(block_hash)
            .map_err(|e| ErrorObjectOwned::owned(-32000, format!("Provider error: {}", e), None::<()>))?
            .ok_or_else(|| ErrorObjectOwned::owned(-32000, "Block not found", None::<()>))?;

        if entries.len() >= MAX_TRACKED_BLOCKS {
            if let Some(oldest_hash) = entries
                .iter()
                .min_by_key(|(_, e)| *e.last_used.lock().expect("block state pool lock"))
                .map(|(hash, _)| *hash)
            {
                entries.remove(&oldest_hash);
            }
        }

        let entry = Arc::new(BlockEntry {
            header,
            last_used: Mutex::new(Instant::now()),
            idle: Mutex::new(Vec::new()),
        });
        entries.insert(block_hash, entry.clone());
        Ok(entry)
    }

    fn evict_expired(&self) {
        let mut entries = self.entries.lock().expect("block state cache lock");
        entries.retain(|_, e| e.last_used.lock().expect("block state pool lock").elapsed() < TTL);
    }
}
