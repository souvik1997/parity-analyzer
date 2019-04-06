extern crate structopt;
extern crate serde;
extern crate serde_json;
extern crate rayon;
#[macro_use]
extern crate derive_more;

use std::path::PathBuf;
use std::collections::{HashMap, BTreeMap};
use std::fs::File;
use std::io::{BufReader, BufRead};

use structopt::StructOpt;
use serde::{Serialize, Deserialize};
use rayon::prelude::*;

#[derive(Default, Clone, Copy, Eq, Debug, PartialEq, Serialize, Deserialize, Add, AddAssign, Mul, Sub, Div)]
pub struct DBStatsEntry {
    pub ops: usize,
    pub cached_ops: usize,
    pub bytes: usize,
    pub cached_bytes: usize,
}

#[derive(Default, Clone, Copy, Eq, Debug, PartialEq, Serialize, Deserialize, Add, AddAssign, Mul, Sub, Div)]
struct DBStats {
    read: DBStatsEntry,
    write: DBStatsEntry,
    delete: DBStatsEntry,
}

#[derive(Default, Clone, Copy, Eq, Debug, PartialEq, Serialize, Deserialize, Add, AddAssign, Mul, Sub, Div)]
struct UnifiedStats {
    journal_stats: DBStats,
    db_stats: DBStats,
}


#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
enum TxType {
    SimpleTransaction,
    Contract
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct TxStats {
    stats: UnifiedStats,
    unique_accounts_touched: Vec<String>,
    transaction_type: TxType
}

#[derive(Default, Clone, Eq, Debug, PartialEq, Serialize, Deserialize)]
struct StatsRecord {
    initial_stats: UnifiedStats,
    final_stats: UnifiedStats,
    tx_stats: HashMap<String, TxStats>,
    gas_used: String,
    on_disk_size: Option<u64>,
    miner: String
}

#[derive(Default, Clone, Copy, Eq, Debug, PartialEq)]
struct WitnessRecord {
    pub bytes: usize,
    pub block_bytes: usize
}

#[derive(Clone, Eq, Debug, PartialEq)]
enum ParsedLine {
    Witness(usize, WitnessRecord),
    Stats(usize, StatsRecord)
}

fn parse_line(line: &str) -> Option<ParsedLine> {
    if let Some(index) = line.find("TRACE stateless  ") {
        let line_part = &line[(index + 7)..];
        let first_space_index = line_part.find(" ").unwrap();
        let block_num: usize = line_part[..first_space_index].parse().unwrap();

        let bytes_index = line_part.find("bytes: ").unwrap();
        let bytes_part = &line_part[(bytes_index + 7)..];
        let bytes_sep_index = bytes_part.find(",").unwrap();
        let bytes: usize = bytes_part[..bytes_sep_index].parse().unwrap();


        let block_bytes_index = line_part.find("block_bytes: ").unwrap();
        let block_bytes_part = &line_part[(block_bytes_index + 7)..];
        let block_bytes_sep_index = block_bytes_part.find(",").unwrap();
        let block_bytes: usize = block_bytes_part[..block_bytes_sep_index].parse().unwrap();
        Some(ParsedLine::Witness(block_num, WitnessRecord { bytes: bytes, block_bytes: block_bytes }))
    } else if let Some(index) = line.find("TRACE stats  RECORD") {
        let line_part = &line[(index + 27)..];
        let first_space_index = line_part.find(" ").unwrap();
        let block_num: usize = line_part[..first_space_index].parse().unwrap();
        let stats_part = &line_part[(first_space_index + 12)..];
        let stats: StatsRecord = serde_json::from_str(stats_part).unwrap();
        Some(ParsedLine::Stats(block_num, stats))
    } else {
        None
    }
}



#[derive(Debug, StructOpt)]
#[structopt(name = "parity-analyzer", about = "An example of StructOpt usage.")]
struct Opt {
    #[structopt(name = "FILE", parse(from_os_str))]
    files: Vec<PathBuf>,
}

#[derive(Default, Clone, Copy, Eq, Debug, PartialEq)]
struct BlockStats {
    pub block_size: usize,
    pub witness_size: usize,
    pub num_transfers: usize,
    pub num_contracts: usize,
    pub unique_accounts_touched_by_transfers: usize,
    pub unique_accounts_touched_by_contracts: usize,
    pub total_transfer_db_stats: UnifiedStats,
    pub total_contract_db_stats: UnifiedStats,
    pub total_db_stats: UnifiedStats,
    pub on_disk_size: Option<u64>,
    pub added_witness_data: bool,
    pub added_stats_data: bool,
}

impl BlockStats {
    fn add_witness_data(&mut self, w: &WitnessRecord) {
        self.block_size = w.block_bytes;
        self.witness_size = w.bytes;
        self.added_witness_data = true;
    }

    fn add_stats_data(&mut self, s: &StatsRecord) {
        let mut num_transfers = 0;
        let mut num_contracts = 0;
        let mut total_transfer_db_stats = UnifiedStats::default();
        let mut total_contract_db_stats = UnifiedStats::default();
        let mut total_transfer_unique_accounts = 0;
        let mut total_contract_unique_accounts = 0;

        for (_, ref tx) in &s.tx_stats {
            match tx.transaction_type {
                TxType::SimpleTransaction => {
                    num_transfers += 1;
                    total_transfer_db_stats += tx.stats;
                    total_transfer_unique_accounts += tx.unique_accounts_touched.len();
                },
                TxType::Contract => {
                    num_contracts += 1;
                    total_contract_db_stats += tx.stats;
                    total_contract_unique_accounts += tx.unique_accounts_touched.len();
                }
            }
        }

        self.num_transfers = num_transfers;
        self.num_contracts = num_contracts;
        self.unique_accounts_touched_by_transfers = total_transfer_unique_accounts;
        self.unique_accounts_touched_by_contracts = total_contract_unique_accounts;
        self.total_transfer_db_stats = total_transfer_db_stats;
        self.total_contract_db_stats = total_contract_db_stats;
        self.total_db_stats = s.final_stats - s.initial_stats;
        self.on_disk_size = s.on_disk_size;
        self.added_stats_data = true;
    }
}

struct ParityStats {
    block_stats: Vec<BlockStats>,
}

impl ParityStats {
    pub fn from_iter<I>(it: I) -> Self where I: Iterator<Item = Option<ParsedLine>> {
        let mut block_stats: BTreeMap<usize, BlockStats> = BTreeMap::new();
        let mut index = 0;
        for i in it {
            match i {
                None => {}
                Some(ref pl) => {
                    match pl {
                        ParsedLine::Witness(block_num, ref w) => {
                            block_stats.entry(*block_num).or_default().add_witness_data(&w);
                        }
                        ParsedLine::Stats(block_num, ref s) => {
                            block_stats.entry(*block_num).or_default().add_stats_data(&s);
                        }
                    }
                }
            }

            index += 1;
            if index % 100000 == 0 {
                println!("Parsed {} records", index);
            }
        }

        let mut bv = Vec::new();
        for (k, v) in block_stats {
            if k != bv.len() + 1 {
                panic!("error: skipped blocks {}..{}", bv.len() + 1, k);
            }

            if !(v.added_stats_data && v.added_witness_data) {
                panic!("error: incomplete data for block {}", k);
            }

            bv.push(v);
        }

        Self {
            block_stats: bv
        }
    }
}

fn main() {
    let opt = Opt::from_args();
    dbg!(::std::mem::size_of::<Option<ParsedLine>>());
    dbg!(::std::mem::size_of::<BlockStats>());
    println!("Parsing files: {:?}", opt.files);
    let ps = ParityStats::from_iter(opt.files.iter().flat_map(|f| BufReader::new(File::open(f).unwrap()).lines().map(|line| parse_line(&line.unwrap()))));
}
