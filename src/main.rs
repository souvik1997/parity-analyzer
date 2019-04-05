extern crate structopt;
extern crate serde;
extern crate serde_json;
extern crate rayon;

use std::path::PathBuf;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, BufRead};

use structopt::StructOpt;
use serde::{Serialize, Deserialize};
use rayon::prelude::*;

#[derive(Default, Clone, Copy, Eq, Debug, PartialEq, Serialize, Deserialize)]
pub struct DBStatsEntry {
    pub ops: usize,
    pub cached_ops: usize,
    pub bytes: usize,
    pub cached_bytes: usize,
}

#[derive(Default, Clone, Copy, Eq, Debug, PartialEq, Serialize, Deserialize)]
struct DBStats {
    read: DBStatsEntry,
    write: DBStatsEntry,
    delete: DBStatsEntry,
}

#[derive(Default, Clone, Copy, Eq, Debug, PartialEq, Serialize, Deserialize)]
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
    bytes: usize,
    block_bytes: usize
}

#[derive(Clone, Eq, Debug, PartialEq)]
enum ParsedLine {
    Witness(usize, WitnessRecord),
    Stats(usize, StatsRecord)
}

fn parse_line(line: &str) -> Option<ParsedLine> {
    if let Some(index) = line.find("TRACE stateless") {
        None
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

fn main() {
    let opt = Opt::from_args();
    println!("Parsing files: {:?}", opt.files);
    let parsed = opt.files.par_iter().map(|f| BufReader::new(File::open(f).unwrap()).lines().map(|line| parse_line(&line.unwrap())).collect::<Vec<_>>()).collect::<Vec<_>>();
    let parsed = parsed.into_iter().flat_map(|m| m.into_iter()).filter(|m| m.is_some()).map(|m| m.unwrap()).collect::<Vec<_>>();
    println!("parsed: {:?}", parsed);
}
