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
    pub fn inc_page_count(&mut self, n: i64) {
        self.page_count += n;
    }
    pub fn get_page_count(&self) -> i64 {
        self.page_count
    }
    pub fn inc_page_alloc(&mut self, n: i64) {
        self.page_alloc += n;
    }
    pub fn get_page_alloc(&self) -> i64 {
        self.page_alloc
    }
    pub fn inc_cursor_count(&mut self, n: i64) {
        self.cursor_count += n;
    }
    pub fn get_cursor_count(&self) -> i64 {
        self.cursor_count
    }
    pub fn inc_node_count(&mut self, n: i64) {
        self.node_count += n;
    }
    pub fn get_node_count(&self) -> i64 {
        self.node_count
    }
    pub fn inc_node_deref(&mut self, n: i64) {
        self.node_deref += n;
    }
    pub fn get_node_deref(&self) -> i64 {
        self.node_deref
    }
    pub fn inc_rebalance(&mut self, n: i64) {
        self.rebalance += n;
    }
    pub fn get_rebalance(&self) -> i64 {
        self.rebalance
    }
    pub fn inc_rebalance_time_ns(&mut self, n: i64) {
        self.rebalance_time_ns += n;
    }
    pub fn get_rebalance_time_ns(&self) -> i64 {
        self.rebalance_time_ns
    }
    pub fn inc_split(&mut self, n: i64) {
        self.split += n;
    }
    pub fn get_split(&self) -> i64 {
        self.split
    }
    pub fn inc_spill(&mut self, n: i64) {
        self.spill += n;
    }
    pub fn get_spill(&self) -> i64 {
        self.spill
    }
    pub fn inc_spill_time_ns(&mut self, n: i64) {
        self.spill_time_ns += n;
    }
    pub fn get_spill_time_ns(&self) -> i64 {
        self.spill_time_ns
    }
    pub fn inc_write(&mut self, n: i64) {
        self.write += n;
    }
    pub fn get_write(&self) -> i64 {
        self.write
    }
    pub fn inc_write_time_ns(&mut self, n: i64) {
        self.write_time_ns += n;
    }
    pub fn get_write_time_ns(&self) -> i64 {
        self.write_time_ns
    }

    /// Accumulate another stats snapshot into `self` (upstream `TxStats.add`).
    pub fn add(&mut self, other: &TxStats) {
        self.page_count += other.page_count;
        self.page_alloc += other.page_alloc;
        self.cursor_count += other.cursor_count;
        self.node_count += other.node_count;
        self.node_deref += other.node_deref;
        self.rebalance += other.rebalance;
        self.rebalance_time_ns += other.rebalance_time_ns;
        self.split += other.split;
        self.spill += other.spill;
        self.spill_time_ns += other.spill_time_ns;
        self.write += other.write;
        self.write_time_ns += other.write_time_ns;
    }

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

/// Per-bucket page statistics (upstream `BucketStats`).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BucketStats {
    pub branch_page_n: usize,
    pub branch_overflow_n: usize,
    pub leaf_page_n: usize,
    pub leaf_overflow_n: usize,
    pub key_n: usize,
    pub depth: usize,
    pub branch_alloc: usize,
    pub branch_inuse: usize,
    pub leaf_alloc: usize,
    pub leaf_inuse: usize,
    pub bucket_n: usize,
    pub inline_bucket_n: usize,
    pub inline_bucket_inuse: usize,
}

impl BucketStats {
    pub fn add(&mut self, other: &BucketStats) {
        self.branch_page_n += other.branch_page_n;
        self.branch_overflow_n += other.branch_overflow_n;
        self.leaf_page_n += other.leaf_page_n;
        self.leaf_overflow_n += other.leaf_overflow_n;
        self.key_n += other.key_n;
        if other.depth > self.depth {
            self.depth = other.depth;
        }
        self.branch_alloc += other.branch_alloc;
        self.branch_inuse += other.branch_inuse;
        self.leaf_alloc += other.leaf_alloc;
        self.leaf_inuse += other.leaf_inuse;
        self.bucket_n += other.bucket_n;
        self.inline_bucket_n += other.inline_bucket_n;
        self.inline_bucket_inuse += other.inline_bucket_inuse;
    }
}

/// Lightweight page listing entry for CLI / debugging.
#[derive(Clone, Debug)]
pub struct PageInfo {
    pub id: Pgid,
    pub page_type: String,
    pub count: u16,
    pub overflow: u32,
}
