use anyhow::{bail, Context, Result};
use clap::{App, Arg};
use data_encoding::HEXUPPER;
use rayon::prelude::*;
use ring::digest::SHA256;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::io::{BufReader, Read, Write};
use std::path::Path;
use walkdir::WalkDir;

#[derive(Serialize, Deserialize, Debug)]
struct Record {
    name: String,
    hash: Option<String>,
}

// process reference directory and save state
fn save_state(reference_directory: &str, state: &str) -> Result<()> {
    let entries = map_directory(reference_directory)?;

    // save directory map to file
    let state_file =
        File::create(state).with_context(|| format!("Failed to save state to {}", state))?;
    bincode::serialize_into(state_file, &entries)?;

    Ok(())
}

fn map_directory(directory: &str) -> Result<HashMap<String, Vec<Record>>> {
    let mut records: HashMap<String, Vec<Record>> = HashMap::new();
    let mut files: Vec<(String, String, String)> = Vec::new();
    records.insert("".to_string(), Vec::new());
    let base = Path::new(directory);
    for entry in WalkDir::new(directory).into_iter().skip(1) {
        let entry = entry?;
        let name = String::from(entry.file_name().to_string_lossy());
        let path = entry.path();
        let relative_parent = path.parent().unwrap().strip_prefix(base)?;
        let relative_self = relative_parent.join(&name);

        let relative_parent_string = String::from(relative_parent.to_string_lossy());
        let path_string = String::from(path.to_string_lossy());

        if entry.file_type().is_dir() {
            records.insert(String::from(relative_self.to_string_lossy()), Vec::new());
            records
                .get_mut(&String::from(relative_parent.to_string_lossy()))
                .unwrap()
                .push(Record { name, hash: None });
        } else {
            files.push((relative_parent_string, name, path_string));
        }
    }

    // calculate hashes in parallel
    let file_records: Vec<_> = files
        .par_iter()
        .map(|(relative_parent, name, path)| {
            let file_record = Record {
                name: name.to_string(),
                hash: Some(calculate_hash(Path::new(path)).unwrap()),
            };
            (relative_parent, file_record)
        })
        .collect();

    for file_record in file_records {
        records.get_mut(file_record.0).unwrap().push(file_record.1);
    }

    Ok(records)
}

fn calculate_hash(path: &Path) -> Result<String> {
    let input = File::open(path)?;
    let mut reader = BufReader::new(input);
    let mut context = ring::digest::Context::new(&SHA256);
    let mut buffer = [0; 1024];

    loop {
        let count = reader.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        context.update(&buffer[..count]);
    }

    Ok(HEXUPPER.encode(context.finish().as_ref()))
}

// load saved state, process target directory and output the diff
fn sync_directory(
    target_directory: &str,
    state: &str,
    mut out_writer: std::boxed::Box<dyn std::io::Write>,
) -> Result<()> {
    let state_file =
        File::open(state).with_context(|| format!("Failed to open state in {}", state))?;
    let entries: HashMap<String, Vec<Record>> = bincode::deserialize_from(state_file)?;
    let mut operations = Vec::new();
    let base = Path::new(target_directory);
    let mut processed_parents: HashMap<String, bool> = HashMap::new();

    let target_entries = map_directory(target_directory)?;

    for (relative_parent, records) in &target_entries {
        for target_record in records {
            let relative_self = Path::new(relative_parent).join(&target_record.name);
            match entries.get(relative_parent) {
                Some(records) => {
                    // copy reference records that do not exist in target
                    if !processed_parents.contains_key(relative_parent) {
                        processed_parents.insert(relative_parent.to_string(), true);
                        for record in records {
                            let path = Path::new(base);
                            let relative_self = path.join(relative_parent).join(&record.name);
                            if !relative_self.exists() {
                                let mut copy_operations =
                                    copy_record(record, &relative_parent, &entries)?;
                                copy_operations.reverse();
                                operations.append(&mut copy_operations);
                            }
                        }
                    }

                    if let Some(record) = records
                        .iter()
                        .find(|&record| record.name == target_record.name)
                    {
                        let target_is_dir = target_record.hash.is_none();
                        // delete record if the type doesn't match
                        let record_is_dir = record.hash.is_none();
                        if target_is_dir != record_is_dir {
                            operations.push(format!("delete `{}`", relative_self.display()))
                        }
                        // do nothing for directories
                        else if !target_is_dir {
                            // copy from source if hashes do not match
                            if target_record.hash.as_ref().unwrap() != record.hash.as_ref().unwrap()
                            {
                                operations.push(format!("copy `{}`", relative_self.display()))
                            }
                        }
                    } else {
                        operations.push(format!("delete `{}`", relative_self.display()));
                    }
                }
                None => operations.push(format!("delete `{}`", relative_self.display())),
            }
        }
    }

    for operation in operations.iter().rev() {
        writeln!(out_writer, "{}", operation)?;
    }

    Ok(())
}

fn copy_record(
    record: &Record,
    relative_parent: &str,
    entries: &HashMap<String, Vec<Record>>,
) -> Result<Vec<String>> {
    let mut operations = Vec::new();
    let relative_self = String::from(
        Path::new(relative_parent)
            .join(&record.name)
            .to_string_lossy(),
    );
    if record.hash.is_none() {
        operations.push(format!("create `{}`", relative_self));
        for record in entries.get(&relative_self).unwrap() {
            operations.append(&mut copy_record(record, &relative_self, &entries)?);
        }
    } else {
        operations.push(format!("copy `{}`", relative_self));
    }
    Ok(operations)
}

// nice-to-haves:
// - tests
// - parallelism
// - structopt
fn main() -> Result<()> {
    let matches = App::new("S¥nc 10000")
        .version("0.1.0")
        .author("Dmitry Mamchur <metaphysical.intoxication@gmail.com>")
        .about("A tool that can display a list of *operations* that *would* be needed to sync a target directory to a reference directory.")
        .arg(
            Arg::with_name("directory")
                .index(1)
                .required(true)
                .help("A directory that is used either as a reference or a sync target."),
        )
        .arg(
            Arg::with_name("sync")
                .short("s")
                .long("sync")
                .help("When present, a directory will be synced with the reference state."),
        )
        .arg(
            Arg::with_name("output")
                .short("o")
                .long("output")
                .takes_value(true)
                .help("A file that will be used to output sync commands; defaults to stdio."),
        )
        .arg(
            Arg::with_name("reference-state")
                .short("r")
                .long("reference-state")
                .takes_value(true)
                .help("A file with a reference state. Defauls to 'state' in the current directory."),
        )
        .get_matches();

    let mut default_state_path = env::current_dir()?;
    default_state_path.push("state");
    let mut state_file_path = default_state_path.to_str().unwrap();

    if let Some(state_parameter) = matches.value_of("state") {
        state_file_path = state_parameter;
    }

    let directory = matches.value_of("directory").unwrap();

    if !Path::new(directory).is_dir() {
        bail!("{} is not a valid directory", directory)
    }

    if matches.is_present("sync") {
        let out_writer = match matches.value_of("output") {
            Some(output) => {
                let path = Path::new(output);
                Box::new(File::create(&path)?) as Box<dyn Write>
            }
            None => Box::new(std::io::stdout()) as Box<dyn Write>,
        };
        sync_directory(directory, state_file_path, out_writer)?;
    } else {
        save_state(directory, state_file_path)?;
    }

    Ok(())
}
