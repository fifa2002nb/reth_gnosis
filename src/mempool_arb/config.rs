use std::collections::HashSet;
use std::sync::Arc;

use arc_swap::ArcSwap;

/// Gas limit of a plain ETH transfer.
pub const PLAIN_TRANSFER_GAS: u64 = 21_000;

/// Default blacklist seeded at startup.
const DEFAULT_BLACKLIST: &[[u8; 4]] = &[
    // ERC-20 / ERC-721
    [0xa9, 0x05, 0x9c, 0xbb], // transfer(address,uint256)
    [0x09, 0x5e, 0xa7, 0xb3], // approve(address,uint256)
    [0x23, 0xb8, 0x72, 0xdd], // transferFrom(address,address,uint256)
    [0x39, 0x51, 0x21, 0x51], // increaseAllowance(address,uint256)
    [0xa4, 0x57, 0xc2, 0xd7], // decreaseAllowance(address,uint256)
    [0xd5, 0x05, 0xac, 0xcf], // permit(address,address,uint256,uint256,uint8,bytes32,bytes32)
    [0x8f, 0xcb, 0xaf, 0x0c], // permit(address,address,uint256,uint256,bool,uint8,bytes32,bytes32)
    [0x42, 0x84, 0x2e, 0x0e], // safeTransferFrom(address,address,uint256) ERC-721
    [0xa2, 0x2c, 0xb4, 0x65], // setApprovalForAll(address,bool)
    [0x40, 0xc1, 0x0f, 0x19], // mint(address,uint256)
    [0x9d, 0xc2, 0x9f, 0xac], // burn(address,uint256)
    // WETH / native wrap
    [0xd0, 0xe3, 0x0d, 0xb0], // deposit()
    [0x2e, 0x1a, 0x7d, 0x4d], // withdraw(uint256)
    // Uniswap V2-style swaps
    [0x38, 0xed, 0x17, 0x39], // swapExactTokensForTokens
    [0x7f, 0xf3, 0x6a, 0xb5], // swapExactETHForTokens
    [0x18, 0xcb, 0xaf, 0xe5], // swapExactTokensForETH
    [0x5c, 0x11, 0xd7, 0x95], // swapExactTokensForTokensSupportingFeeOnTransferTokens
    [0x88, 0x03, 0xdb, 0xee], // swapTokensForExactTokens
    [0xb6, 0xf9, 0xde, 0x95], // swapExactETHForTokensSupportingFeeOnTransferTokens
    [0x4a, 0x25, 0xd9, 0x4a], // swapTokensForExactETH
    [0xfb, 0x3b, 0xdb, 0x41], // swapETHForExactTokens
    // Uniswap V3-style swaps
    [0x41, 0x4b, 0xf3, 0x89], // exactInputSingle(...)
    [0xc0, 0x4b, 0x8d, 0x59], // exactInput(...)
    // Multicall / batch (routers, aggregators)
    [0xac, 0x96, 0x50, 0xd8], // multicall(bytes[])
    [0x82, 0xad, 0x56, 0xcb], // aggregate3((address,bool,bytes)[])
    // ERC-4337 EntryPoint
    [0x76, 0x5e, 0x82, 0x7f], // handleOps v0.7
    [0x1f, 0xad, 0x94, 0x8c], // handleOps v0.6
    // Gnosis Safe
    [0x6a, 0x76, 0x12, 0x02], // execTransaction(...)
    [0x46, 0x87, 0x21, 0xa7], // execTransactionFromModule(...)
    [0x61, 0xff, 0x12, 0x3c], // proposeTransaction(...)
    // Shutter / threshold crypto (high freq on Gnosis)
    [0x23, 0xc6, 0x40, 0xe7], // sendMessage(bytes32,bytes)
    [0x52, 0x7b, 0xdd, 0xe9], // signRevealNonces(...)
    [0x95, 0xb5, 0x7d, 0x9d], // signShareWithCallback(...)
    // Gnosis Pay / payment rails
    [0x8a, 0x32, 0x02, 0x55], // spend(address,address,address,uint256,bytes32)
    // CoW Protocol / settlement
    [0x13, 0xd7, 0x9a, 0x0b], // settle(...)
    [0x54, 0x1e, 0x34, 0x15], // performSettlement(...)
    [0x61, 0x52, 0xbe, 0x48], // high-freq on 0xa44466f1… (block 46909625)
    [0x0e, 0x49, 0xda, 0x11], // high-freq on 0xa44466f1… (block 46909625)
    // Oracles / keepers / infra
    [0x31, 0x61, 0xb7, 0xf6], // setPrice(...)
    [0x2b, 0x28, 0xb3, 0x4e], // setFares(...)
    [0xb1, 0xdc, 0x65, 0xa4], // transmit(...) Chainlink OCR
    [0x1d, 0x86, 0xd4, 0x4b], // sealBatch(...)
    [0x60, 0xa8, 0x93, 0x6d], // publishChunk(...)
    [0x9f, 0xe6, 0x03, 0xe8], // redeemToBase(...)
    [0x9d, 0x9c, 0xa9, 0xf9], // submitProof(...)
    [0xcf, 0x49, 0x2d, 0x01], // assignBlob()
    [0xe9, 0x9f, 0x7f, 0x3a], // mintTokunFlameGuardToken(...)
    // Other high-freq Gnosis sample (blocks ~46909525–46909625)
    [0x41, 0x26, 0x58, 0xe5], // frequent, contract-specific
    [0x5f, 0xb4, 0x20, 0xce], // frequent, contract-specific
];

/// Runtime-mutable selector blacklist, shared between monitor and RPC.
#[derive(Debug, Clone)]
pub struct Blacklist {
    inner: Arc<ArcSwap<HashSet<[u8; 4]>>>,
}

impl Blacklist {
    pub fn with_defaults() -> Self {
        let set: HashSet<[u8; 4]> = DEFAULT_BLACKLIST.iter().copied().collect();
        Self {
            inner: Arc::new(ArcSwap::from_pointee(set)),
        }
    }

    pub fn contains(&self, selector: [u8; 4]) -> bool {
        self.inner.load().contains(&selector)
    }

    pub fn add(&self, selectors: &[[u8; 4]]) {
        let mut next = (**self.inner.load()).clone();
        next.extend(selectors.iter().copied());
        self.inner.store(Arc::new(next));
    }

    pub fn remove(&self, selectors: &[[u8; 4]]) {
        let mut next = (**self.inner.load()).clone();
        for s in selectors {
            next.remove(s);
        }
        self.inner.store(Arc::new(next));
    }

    pub fn snapshot(&self) -> Vec<[u8; 4]> {
        let mut v: Vec<[u8; 4]> = self.inner.load().iter().copied().collect();
        v.sort_unstable();
        v
    }
}

/// Selector whitelist: when non-empty, only txs whose selector is in the
/// whitelist are pushed to the whitelist broadcast channel.
#[derive(Debug, Clone, Default)]
pub struct Whitelist {
    inner: Arc<ArcSwap<HashSet<[u8; 4]>>>,
}

impl Whitelist {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(ArcSwap::from_pointee(HashSet::new())),
        }
    }

    pub fn contains(&self, selector: [u8; 4]) -> bool {
        self.inner.load().contains(&selector)
    }

    pub fn is_empty(&self) -> bool {
        self.inner.load().is_empty()
    }

    pub fn add(&self, selectors: &[[u8; 4]]) {
        let mut next = (**self.inner.load()).clone();
        next.extend(selectors.iter().copied());
        self.inner.store(Arc::new(next));
    }

    pub fn remove(&self, selectors: &[[u8; 4]]) {
        let mut next = (**self.inner.load()).clone();
        for s in selectors {
            next.remove(s);
        }
        self.inner.store(Arc::new(next));
    }

    pub fn snapshot(&self) -> Vec<[u8; 4]> {
        let mut v: Vec<[u8; 4]> = self.inner.load().iter().copied().collect();
        v.sort_unstable();
        v
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blacklist_default_and_mutation() {
        let bl = Blacklist::with_defaults();
        assert!(bl.contains([0xa9, 0x05, 0x9c, 0xbb]));
        assert!(bl.contains([0x38, 0xed, 0x17, 0x39]));
        assert!(!bl.contains([0x91, 0x25, 0x2c, 0x55])); // executePath

        bl.add(&[[0x91, 0x25, 0x2c, 0x55]]);
        assert!(bl.contains([0x91, 0x25, 0x2c, 0x55]));

        bl.remove(&[[0xa9, 0x05, 0x9c, 0xbb]]);
        assert!(!bl.contains([0xa9, 0x05, 0x9c, 0xbb]));
    }

    #[test]
    fn whitelist_starts_empty_and_matches() {
        let wl = Whitelist::new();
        assert!(wl.is_empty());
        assert!(!wl.contains([0x91, 0x25, 0x2c, 0x55]));

        wl.add(&[[0x91, 0x25, 0x2c, 0x55]]);
        assert!(!wl.is_empty());
        assert!(wl.contains([0x91, 0x25, 0x2c, 0x55]));

        wl.remove(&[[0x91, 0x25, 0x2c, 0x55]]);
        assert!(wl.is_empty());
    }
}
