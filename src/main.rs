extern crate structopt;
extern crate serde;
extern crate serde_json;
extern crate rayon;
#[macro_use]
extern crate derive_more;
extern crate csv;
extern crate gnuplot;

use std::path::PathBuf;
use std::collections::{HashMap, BTreeMap};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufRead, BufWriter};
use gnuplot::Figure;

use structopt::StructOpt;
use serde::{Serialize, Deserialize};

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
        let line_part = &line[(index + 24)..];
        let first_space_index = line_part.find(" ").unwrap();
        let block_num: usize = line_part[..first_space_index].parse().unwrap();

        let bytes_index = line_part.find("bytes: ").unwrap();
        let bytes_part = &line_part[(bytes_index + 7)..];
        let bytes_sep_index = bytes_part.find(",").unwrap();
        let bytes: usize = bytes_part[..bytes_sep_index].parse().unwrap();


        let block_bytes_index = line_part.find("block_bytes: ").unwrap();
        let block_bytes_part = &line_part[(block_bytes_index + 13)..];
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
    #[structopt(short = "-o", long="--output", name = "output", parse(from_os_str))]
    output_file: Option<PathBuf>,

    #[structopt(short = "-i", long="--input", name = "input", parse(from_os_str))]
    input_file: Option<PathBuf>,

    #[structopt(long="--min", name = "min")]
    min_block_num: Option<usize>,

    #[structopt(long="--max", name = "max")]
    max_block_num: Option<usize>,

    #[structopt(long="--plot-witness-size-output", name="plot witness size output file", parse(from_os_str))]
    plot_witness_size_output: Option<PathBuf>,

    #[structopt(long="--plot-block-size-output", name="plot block size output file", parse(from_os_str))]
    plot_block_size_output: Option<PathBuf>,

    #[structopt(long="--plot-db-ops-output", name="plot db ops output file", parse(from_os_str))]
    plot_db_ops_output: Option<PathBuf>,

    #[structopt(long="--plot-db-bytes-output", name="plot db bytes output file", parse(from_os_str))]
    plot_db_bytes_output: Option<PathBuf>,

    #[structopt(long="--plot-transfer-unique-accounts-output", name="plot transfer unique accounts output file", parse(from_os_str))]
    plot_transfer_unique_accounts_output: Option<PathBuf>,

    #[structopt(long="--plot-contract-unique-accounts-output", name="plot contract unique accounts output file", parse(from_os_str))]
    plot_contract_unique_accounts_output: Option<PathBuf>,

    #[structopt(long="--plot-on-disk-size-output", name="plot on disk size output file", parse(from_os_str))]
    plot_on_disk_size_output: Option<PathBuf>,


    #[structopt(name = "FILE", parse(from_os_str))]
    files: Vec<PathBuf>,
}

#[derive(Clone, Copy, Eq, Debug, PartialEq, Serialize, Deserialize)]
struct BlockStats {
    pub block_num: usize,
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
    fn new(block_num: usize) -> Self {
        Self {
            block_num: block_num,
            block_size: 0,
            witness_size: 0,
            num_transfers: 0,
            num_contracts: 0,
            unique_accounts_touched_by_transfers: 0,
            unique_accounts_touched_by_contracts: 0,
            total_transfer_db_stats: UnifiedStats::default(),
            total_contract_db_stats: UnifiedStats::default(),
            total_db_stats: UnifiedStats::default(),
            on_disk_size: None,
            added_witness_data: false,
            added_stats_data: false,
        }
    }

    fn add_witness_data(&mut self, w: &WitnessRecord) {
        if self.added_witness_data {
            return;
        }
        self.block_size = w.block_bytes;
        self.witness_size = w.bytes;
        self.added_witness_data = true;
    }

    fn add_stats_data(&mut self, s: &StatsRecord) {
        if self.added_stats_data {
            return;
        }

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

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq, Clone)]
struct ParityStats {
    block_stats: BTreeMap<usize, BlockStats>,
}

fn in_range(min_block_num: Option<usize>, max_block_num: Option<usize>, k: usize) -> bool {
    match (min_block_num, max_block_num) {
        (Some(min_block_num), Some(max_block_num)) => min_block_num <= k && k <= max_block_num,
        (Some(min_block_num), None) => min_block_num <= k,
        (None, Some(max_block_num)) => k <= max_block_num,
        (None, None) => true
    }
}

fn complete_block_stats<'a>(v: &(&'a usize, &'a BlockStats)) -> bool  {
    v.1.added_stats_data && v.1.added_witness_data
}

impl ParityStats {
    pub fn from_iter<I>(min_block_num: Option<usize>, max_block_num: Option<usize>, it: I) -> Self where I: Iterator<Item = Option<ParsedLine>> {
        let mut block_stats: BTreeMap<usize, BlockStats> = BTreeMap::new();
        let mut index = 0;
        for i in it {
            match i {
                None => {}
                Some(ref pl) => {
                    match pl {
                        ParsedLine::Witness(block_num, ref w) => {
                            if in_range(min_block_num, max_block_num, *block_num) {
                                block_stats.entry(*block_num).or_insert_with(|| BlockStats::new(*block_num)).add_witness_data(&w);
                            }

                        }
                        ParsedLine::Stats(block_num, ref s) => {
                            if in_range(min_block_num, max_block_num, *block_num) {
                                block_stats.entry(*block_num).or_insert_with(|| BlockStats::new(*block_num)).add_stats_data(&s);
                            }
                        }
                    }
                }
            }

            index += 1;
            if index % 100000 == 0 {
                eprintln!("Parsed {} records", index);
            }
        }
        Self {
            block_stats: block_stats
        }
    }

    pub fn merge(&mut self, min_block_num: Option<usize>, max_block_num: Option<usize>, other: Self) {
        self.block_stats.extend(other.block_stats.into_iter().filter(|(k, _)| {
            in_range(min_block_num, max_block_num, *k)
        }));
    }

    pub fn block_intervals(&self) -> Vec<(usize, usize)> {
        let mut intervals = Vec::new();
        for (k, _) in self.block_stats.iter().filter(|c| complete_block_stats(c)) {
            if intervals.len() == 0 {
                intervals.push((*k, *k));
            } else {
                let l = intervals.last_mut().unwrap();
                if k - 1 == l.1 {
                    *l = (l.0, *k);
                } else {
                    intervals.push((*k, *k));
                }
            }
        }
        intervals
    }

    pub fn print_statistics(&self) {
        // Average witness size per block
        {
            let (count, total)  = self.block_stats.iter().filter(|c| complete_block_stats(c)).map(|(_, v)| (1, v.witness_size)).fold((0, 0), |(a, b), (x, y)| {
                (a + x, b + y)
            });
            eprintln!("Average witness size for {} blocks: {}", count, (total as f64) / (count as f64));
        }

        {
            let (max_block_num, _)  = self.block_stats.iter().filter(|c| complete_block_stats(c)).map(|(k, v)| (*k, v.witness_size)).fold((0, 0), |(a, b), (x, y)| {
                if y > b {
                    (x, y)
                } else {
                    (a, b)
                }
            });
            let block = self.block_stats.get(&max_block_num).unwrap();
            eprintln!("Max witness size at #{} with {} bytes ({} block bytes)", max_block_num, block.witness_size, block.block_size);
        }

        // Average block size per block
        {
            let (count, total)  = self.block_stats.iter().filter(|c| complete_block_stats(c)).map(|(_, v)| (1, v.block_size)).fold((0, 0), |(a, b), (x, y)| {
                (a + x, b + y)
            });
            eprintln!("Average block size for {} blocks: {}", count, (total as f64) / (count as f64));
        }

        {
            let (max_block_num, _)  = self.block_stats.iter().filter(|c| complete_block_stats(c)).map(|(k, v)| (*k, v.block_size)).fold((0, 0), |(a, b), (x, y)| {
                if y > b {
                    (x, y)
                } else {
                    (a, b)
                }
            });
            let block = self.block_stats.get(&max_block_num).unwrap();
            eprintln!("Max block size at #{} with {} bytes ({} witness bytes)", max_block_num, block.block_size, block.witness_size);
        }
    }

    pub fn plot_witness_sizes<'a>(&self, fg: &'a mut Figure) -> &'a mut Figure {
        Self::plot_single_dimension(fg, "Witness size (bytes)", "Witness size", self.block_stats.iter().filter(|c| complete_block_stats(c)).map(|(k, v)| (*k, v.witness_size)))
    }

    pub fn plot_block_sizes<'a>(&self, fg: &'a mut Figure) -> &'a mut Figure {
        Self::plot_single_dimension(fg, "Block size (bytes)", "Block size", self.block_stats.iter().filter(|c| complete_block_stats(c)).map(|(k, v)| (*k, v.block_size)))
    }

    pub fn plot_on_disk_size<'a>(&self, fg: &'a mut Figure) -> &'a mut Figure {
        Self::plot_single_dimension(fg, "On disk size (bytes)", "On disk size", self.block_stats.iter()
                                    .filter(|c| complete_block_stats(c))
                                    .map(|(k, v)| (*k, v.on_disk_size))
                                    .filter(|(_, v)| v.is_some())
                                    .map(|(k, v)| (k, v.unwrap() as usize)))
    }

    fn plot_single_dimension<'a, I>(fg: &'a mut Figure, y_label: &str, caption_label: &str, it: I) -> &'a mut Figure where I: Iterator<Item = (usize, usize)> + Clone {
        use gnuplot::*;

        fg.axes2d()
            .points(it.clone().map(|s| s.0), it.map(|s| s.1), &[Caption(caption_label), Color("black")])
            .set_x_label("Block number", &[])
            .set_y_label(y_label, &[]);
        fg
    }

    fn plot_database<'a, I>(fg: &'a mut Figure, y_label: &str, it: I) -> &'a mut Figure where I: Iterator<Item = (usize, usize, usize, usize)> + Clone {
        use gnuplot::*;
        fg.axes2d()
            .points(it.clone().map(|s| s.0), it.clone().map(|s| s.1), &[Caption("Reads"), Color("#55FF0000")])
            .points(it.clone().map(|s| s.0), it.clone().map(|s| s.2), &[Caption("Writes"), Color("#55FF00")])
            .points(it.clone().map(|s| s.0), it.map(|s| s.3), &[Caption("Deletes"), Color("#550000FF")])
            .set_x_label("Block number", &[])
            .set_y_label(y_label, &[]);
        fg
    }

    pub fn plot_database_ops<'a>(&self, fg: &'a mut Figure) -> &'a mut Figure {
        Self::plot_database(fg, "Operations", self.block_stats.iter().filter(|c| complete_block_stats(c)).map(|(k, v)| {
            (*k, v.total_db_stats.journal_stats.read.ops, v.total_db_stats.journal_stats.write.ops, v.total_db_stats.journal_stats.delete.ops)
        }))
    }

    pub fn plot_database_bytes<'a>(&self, fg: &'a mut Figure) -> &'a mut Figure {
        Self::plot_database(fg, "Bytes", self.block_stats.iter().filter(|c| complete_block_stats(c)).map(|(k, v)| {
            (*k, v.total_db_stats.journal_stats.read.bytes, v.total_db_stats.journal_stats.write.bytes, v.total_db_stats.journal_stats.delete.bytes)
        }))
    }

    pub fn plot_transfer_unique_accounts<'a>(&self, fg: &'a mut Figure) -> &'a mut Figure {
        Self::plot_single_dimension(fg, "Number of unique accounts", "Number of unique accounts touched by transfers", self.block_stats.iter().filter(|c| complete_block_stats(c)).map(|(k, v)| (*k, v.unique_accounts_touched_by_transfers)))
    }

    pub fn plot_contract_unique_accounts<'a>(&self, fg: &'a mut Figure) -> &'a mut Figure {
        Self::plot_single_dimension(fg, "Number of unique accounts", "Number of unique accounts touched by contracts", self.block_stats.iter().filter(|c| complete_block_stats(c)).map(|(k, v)| (*k, v.unique_accounts_touched_by_contracts)))
    }
}

fn main() {
    let opt = Opt::from_args();
    eprintln!("Options: {:#?}", opt);
    let mut ps = ParityStats::from_iter(opt.min_block_num, opt.max_block_num, opt.files.iter().flat_map(|f| BufReader::new(File::open(f).unwrap()).lines().map(|line| parse_line(&line.unwrap()))));

    match opt.input_file {
        Some(input_file) => {
            let input_file = BufReader::new(OpenOptions::new().read(true).open(input_file).expect("Failed to open input file"));
            let input_stats: ParityStats = serde_json::from_reader(input_file).expect("Input file is not valid");
            ps.merge(opt.min_block_num, opt.max_block_num, input_stats);
        }
        None => {}
    }

    eprintln!("Block intervals: {:?}", ps.block_intervals());
    ps.print_statistics();

    const TERMINAL: &str = "pngcairo size 1000, 1000";

    match opt.plot_witness_size_output {
        Some(plot_witness_size_output) => {
            let mut fg = Figure::new();
            fg.set_terminal(TERMINAL, plot_witness_size_output.to_str().unwrap());
            ps.plot_witness_sizes(&mut fg).show();
            fg.close();
        }
        None => {}
    }


    match opt.plot_block_size_output {
        Some(plot_block_size_output) => {
            let mut fg = Figure::new();
            fg.set_terminal(TERMINAL, plot_block_size_output.to_str().unwrap());
            ps.plot_block_sizes(&mut fg).show();
            fg.close();
        }
        None => {}
    }

    match opt.plot_db_ops_output {
        Some(plot_db_ops_output) => {
            let mut fg = Figure::new();
            fg.set_terminal(TERMINAL, plot_db_ops_output.to_str().unwrap());
            ps.plot_database_ops(&mut fg).show();
            fg.close();
        }
        None => {}
    }

    match opt.plot_db_bytes_output {
        Some(plot_db_bytes_output) => {
            let mut fg = Figure::new();
            fg.set_terminal(TERMINAL, plot_db_bytes_output.to_str().unwrap());
            ps.plot_database_bytes(&mut fg).show();
            fg.close();
        }
        None => {}
    }

    match opt.plot_transfer_unique_accounts_output {
        Some(plot_transfer_unique_accounts_output) => {
            let mut fg = Figure::new();
            fg.set_terminal(TERMINAL, plot_transfer_unique_accounts_output.to_str().unwrap());
            ps.plot_transfer_unique_accounts(&mut fg).show();
            fg.close();
        }
        None => {}
    }

    match opt.plot_contract_unique_accounts_output {
        Some(plot_contract_unique_accounts_output) => {
            let mut fg = Figure::new();
            fg.set_terminal(TERMINAL, plot_contract_unique_accounts_output.to_str().unwrap());
            ps.plot_contract_unique_accounts(&mut fg).show();
            fg.close();
        }
        None => {}
    }

    match opt.plot_on_disk_size_output {
        Some(plot_on_disk_size_output) => {
            let mut fg = Figure::new();
            fg.set_terminal(TERMINAL, plot_on_disk_size_output.to_str().unwrap());
            ps.plot_on_disk_size(&mut fg).show();
            fg.close();
        }
        None => {}
    }

    match opt.output_file {
        Some(output_file) => {
            let output_file = BufWriter::new(OpenOptions::new().create(true).write(true).open(output_file).expect("Failed to open output file"));
            serde_json::to_writer(output_file, &ps).expect("Failed to write to output file");
        }
        None => {}
    }
}
