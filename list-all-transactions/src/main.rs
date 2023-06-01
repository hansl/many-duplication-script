use chrono::NaiveDateTime;
use clap::Parser;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::PathBuf;

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
    count: String,
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

#[derive(Debug, Serialize)]
struct TransposedEntry(Option<String>, u64);

#[derive(Default, Debug, Serialize)]
struct TransposedTable(BTreeMap<u64, BTreeMap<String, TransposedEntry>>);

impl TransposedTable {
    pub fn insert(&mut self, entry: DuplicatedTransaction, aliases: &AliasMap) {
        if entry.method != "tokens.mint" {
            return;
        }

        let argument = entry.argument.as_ref().unwrap();
        let argument: BTreeMap<String, String> = serde_json::from_str(argument).unwrap();

        // We ignore the first one as it is the only _valid_ transaction.
        for (address, amount) in &argument {
            let alias = aliases.get(address).cloned();
            for height in &entry.heights[1..] {
                let mut inner = self.0.entry(*height).or_default();
                let mut entry = inner
                    .entry(address.clone())
                    .or_insert(TransposedEntry(alias.clone(), 0));
                entry.1 += amount.parse::<u64>().unwrap();
            }
        }
    }
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
    let transactions: Vec<DuplicatedTransaction> = transactions
        .into_iter()
        .filter(|x| x.method == "tokens.mint")
        .collect();

    let mut table = TransposedTable::default();
    for t in transactions {
        table.insert(t, &aliases);
    }

    let mut output_csv = csv::Writer::from_writer(vec![]);
    for (height, entry) in table.0 {
        for (address, TransposedEntry(alias, amount)) in entry {
            output_csv
                .write_record(&[
                    height.to_string(),
                    address,
                    alias.unwrap_or_default(),
                    amount.to_string(),
                ])
                .unwrap();
        }
    }
    let data = String::from_utf8(output_csv.into_inner().unwrap()).unwrap();

    // let output_json = serde_json::to_string(&table).unwrap();
    if let Some(x) = output {
        std::fs::write(x, data).unwrap();
    } else {
        println!("{}", data);
    }
}
