use chrono::NaiveDateTime;
use clap::Parser;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::PathBuf;
use tabled::settings::object::{Columns, Object, Rows};
use tabled::settings::{Alignment, Modify};
use tabled::Tabled;

#[derive(Debug, Parser)]
struct Args {
    /// Path to the transactions in JSON format.
    transactions: PathBuf,

    /// A file that contains records of address => alias.
    #[clap(long)]
    aliases: Option<PathBuf>,

    /// The output file. Optional, by default will output to STDOUT.
    output: Option<PathBuf>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawDuplicatedTransaction {
    orig_time: String,
    max_time: String,
    method: String,
    height: String,
    hash: String,
    argument: Option<String>,
    // count: String,
    neighborhood: String,
}

#[derive(Debug)]
struct DuplicatedTransaction {
    orig_time: NaiveDateTime,
    max_time: NaiveDateTime,
    method: String,
    heights: Vec<u64>,
    hash: bytes::Bytes,
    argument: Option<String>,
    neighborhood: u64,
}

impl From<RawDuplicatedTransaction> for DuplicatedTransaction {
    fn from(value: RawDuplicatedTransaction) -> Self {
        Self {
            orig_time: NaiveDateTime::parse_from_str(&value.orig_time, "%Y-%m-%d %H:%M:%S")
                .unwrap(),
            max_time: NaiveDateTime::parse_from_str(&value.max_time, "%Y-%m-%d %H:%M:%S").unwrap(),
            method: value.method,
            heights: value
                .height
                .trim_matches(|x| x == '{' || x == '}')
                .split(',')
                .map(|x| x.parse().unwrap())
                .collect(),
            hash: hex::decode(&value.hash[2..]).unwrap().into(),
            argument: value.argument,
            neighborhood: value.neighborhood.parse().unwrap(),
        }
    }
}

type AliasMap = std::collections::BTreeMap<String, String>;

#[derive(Default, Debug, Serialize)]
struct TransposedMintTable(BTreeMap<u64, BTreeMap<String, u64>>);

impl TransposedMintTable {
    pub fn insert(&mut self, entry: DuplicatedTransaction) {
        if entry.method != "tokens.mint" {
            return;
        }

        let argument = entry.argument.as_ref().unwrap();
        let argument: BTreeMap<String, String> = serde_json::from_str(argument).unwrap();

        // We ignore the first one as it is the only _valid_ transaction.
        for (address, amount) in &argument {
            for height in &entry.heights[1..] {
                let inner = self.0.entry(*height).or_default();
                *inner.entry(address.clone()).or_default() += amount.parse::<u64>().unwrap();
            }
        }
    }
}

#[derive(Deserialize)]
struct LedgerSendArgument {
    from: String,
    to: String,
    amount: u64,
    // Ignore symbol, it doesn't matter.
}

#[derive(Default, Debug, Serialize)]
struct TransposedSendTable(pub BTreeMap<(String, String), u64>);

impl TransposedSendTable {
    pub fn insert(&mut self, entry: DuplicatedTransaction) {
        if entry.method != "ledger.send" {
            return;
        }

        let argument = entry.argument.as_ref().unwrap();
        let LedgerSendArgument { from, to, amount } = serde_json::from_str(argument).unwrap();
        *self.0.entry((from, to)).or_default() += amount;
    }
}

#[derive(Tabled)]
struct SummaryRow {
    address: String,
    alias: String,
    total: u64,
}

fn main() {
    let Args {
        transactions,
        aliases,
        output,
    } = Args::parse();
    eprintln!("args: {:?}", Args::parse());

    // Load transactions.
    let transactions = std::fs::read_to_string(transactions).unwrap();
    let transactions: Vec<RawDuplicatedTransaction> = serde_json::from_str(&transactions).unwrap();
    let transactions: Vec<DuplicatedTransaction> =
        transactions.into_iter().map(|x| x.into()).collect();

    // Load aliases.
    let aliases = aliases.map(|x| std::fs::read_to_string(x).unwrap());
    let aliases: AliasMap = aliases
        .map(|x| serde_json::from_str(&x).unwrap())
        .unwrap_or_default();

    // Filter transactions we're not interested in.
    let transactions: Vec<DuplicatedTransaction> = transactions.into_iter().collect();

    let mut mint_table = TransposedMintTable::default();
    let mut send_table = TransposedSendTable::default();

    for t in transactions {
        if t.method == "tokens.mint" {
            mint_table.insert(t);
        } else if t.method == "ledger.send" {
            eprintln!("...");
            send_table.insert(t);
        }
    }

    let mut output_csv = csv::Writer::from_writer(vec![]);
    let mut totals: BTreeMap<String, u64> = BTreeMap::new();
    for (height, entry) in mint_table.0 {
        for (address, amount) in entry {
            let alias = aliases.get(&address).cloned().unwrap_or_default();
            *totals.entry(address.clone()).or_default() += amount;
            output_csv
                .write_record(&[height.to_string(), address, alias, amount.to_string()])
                .unwrap();
        }
    }
    let data = String::from_utf8(output_csv.into_inner().unwrap()).unwrap();

    let summary = totals
        .iter()
        .map(|(address, total)| SummaryRow {
            address: address.clone(),
            alias: aliases.get(address).cloned().unwrap_or_default(),
            total: *total,
        })
        .collect::<Vec<_>>();

    if let Some(x) = output {
        std::fs::write(x, data).unwrap();
    } else {
        println!("{}", data);
    }

    eprintln!("# Summary");
    eprintln!(
        "{}",
        tabled::Table::new(summary)
            .with(tabled::settings::Style::markdown())
            .with(Modify::new(Rows::new(1..).and(Columns::last())).with(Alignment::right()))
    );

    eprintln!("\nTotal: {}", totals.values().sum::<u64>());

    eprintln!("Send txs:");
    for ((from, to), amount) in send_table.0.iter() {
        eprintln!(
            "{}{} => {}{}    ++ {}",
            from,
            aliases
                .get(from)
                .map(|a| format!(" ({a})"))
                .unwrap_or("".to_string()),
            to,
            aliases
                .get(to)
                .map(|a| format!(" ({a})"))
                .unwrap_or("".to_string()),
            amount
        );
    }
}
