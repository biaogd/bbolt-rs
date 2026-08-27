//! Stats and inspect structures (upstream `Stats`, `Info`, `BucketStructure`).

use crate::page::Pgid;

/// Database statistics snapshot.
#[derive(Clone, Debug, Default)]
pub struct Stats {
    pub free_page_n: usize,
    pub pending_page_n: usize,
    pub free_alloc: usize,
    pub freelist_inuse: usize,
    pub tx_n: usize,
    pub open_tx_n: usize,
    pub tx_stats: TxStats,
}

impl Stats {
    /// Difference between two snapshots (counters only; freelist fields take `self`).
    pub fn sub(&self, other: &Stats) -> Stats {
        Stats {
            free_page_n: self.free_page_n,
            pending_page_n: self.pending_page_n,
            free_alloc: self.free_alloc,
            freelist_inuse: self.freelist_inuse,
            tx_n: self.tx_n.saturating_sub(other.tx_n),
            open_tx_n: self.open_tx_n,
            tx_stats: self.tx_stats.sub(&other.tx_stats),
        }
    }
}

/// Per-transaction performance counters (subset of upstream `TxStats`).
#[derive(Clone, Debug, Default)]
pub struct TxStats {
    pub page_count: i64,
    pub page_alloc: i64,
    pub cursor_count: i64,
    pub node_count: i64,
    pub node_deref: i64,
    pub rebalance: i64,
    pub rebalance_time_ns: i64,
    pub split: i64,
    pub spill: i64,
    pub spill_time_ns: i64,
    pub write: i64,
    pub write_time_ns: i64,
}

impl TxStats {
    pub fn sub(&self, other: &TxStats) -> TxStats {
        TxStats {
            page_count: self.page_count - other.page_count,
            page_alloc: self.page_alloc - other.page_alloc,
            cursor_count: self.cursor_count - other.cursor_count,
            node_count: self.node_count - other.node_count,
            node_deref: self.node_deref - other.node_deref,
            rebalance: self.rebalance - other.rebalance,
            rebalance_time_ns: self.rebalance_time_ns - other.rebalance_time_ns,
            split: self.split - other.split,
            spill: self.spill - other.spill,
            spill_time_ns: self.spill_time_ns - other.spill_time_ns,
            write: self.write - other.write,
            write_time_ns: self.write_time_ns - other.write_time_ns,
        }
    }
}

/// [`Db::info`](crate::Db::info) payload.
#[derive(Clone, Debug)]
pub struct Info {
    pub page_size: usize,
}

/// Nested bucket tree from [`crate::Tx::inspect`].
#[derive(Clone, Debug, Default)]
pub struct BucketStructure {
    pub name: String,
    pub key_n: usize,
    pub children: Vec<BucketStructure>,
}

/// Lightweight page listing entry for CLI / debugging.
#[derive(Clone, Debug)]
pub struct PageInfo {
    pub id: Pgid,
    pub page_type: String,
    pub count: u16,
    pub overflow: u32,
}
