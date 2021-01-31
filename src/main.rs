use anyhow::{bail, Context, Result};
use clap::{App, Arg};
use data_encoding::HEXUPPER;
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
    let mut entries: HashMap<String, Vec<Record>> = HashMap::new();
    entries.insert("".to_string(), Vec::new());
    let base = Path::new(reference_directory);
    for entry in WalkDir::new(reference_directory).into_iter().skip(1) {
        let dir_entry = entry?;
        let name = dir_entry.file_name().to_str().unwrap();
        let path = dir_entry.path();
        let relative_parent = path
            .parent()
            .unwrap()
            .strip_prefix(base)
            .unwrap()
            .to_str()
            .unwrap();
        let relative_self = path
            .strip_prefix(base)
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();

        let mut record = Record {
            name: name.to_string(),
            hash: None,
        };

        if dir_entry.file_type().is_dir() {
            entries.insert(relative_self, Vec::new());
        } else {
            record.hash = Some(calculate_hash(&path).unwrap());
        }

        entries.get_mut(relative_parent).unwrap().push(record);
    }

    // save directory map to file
    let state_file =
        File::create(state).with_context(|| format!("Failed to save state to {}", state))?;
    bincode::serialize_into(state_file, &entries)?;

    Ok(())
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
    for entry in WalkDir::new(target_directory).into_iter().skip(1) {
        let dir_entry = entry?;
        let name = dir_entry.file_name().to_str().unwrap();
        let path = dir_entry.path();
        let relative_parent = path
            .parent()
            .unwrap()
            .strip_prefix(base)
            .unwrap()
            .to_str()
            .unwrap();
        let relative_self = path
            .strip_prefix(base)
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();

        let is_dir = dir_entry.file_type().is_dir();

        match entries.get(relative_parent) {
            Some(records) => {
                // copy reference records that do not exist in target
                if !processed_parents.contains_key(relative_parent) {
                    processed_parents.insert(relative_parent.to_string(), true);
                    for record in records {
                        let path = Path::new(base);
                        let relative_self = path
                            .join(relative_parent)
                            .join(&record.name)
                            .into_os_string()
                            .into_string()
                            .unwrap();
                        if !Path::new(&relative_self).exists() {
                            let mut copy_operations =
                                copy_record(record, &relative_parent, &entries)?;
                            copy_operations.reverse();
                            operations.append(&mut copy_operations);
                        }
                    }
                }

                if let Some(record) = records.iter().find(|&record| record.name == name) {
                    let record_is_dir = record.hash.is_none();
                    // delete record if the type doesn't match
                    if is_dir != record_is_dir {
                        operations.push(format!("delete `{}`", relative_self))
                    }
                    // do nothing for directories
                    else if !is_dir {
                        // copy from source if hashes do not match
                        let hash = calculate_hash(&path)?;
                        if &hash != record.hash.as_ref().unwrap() {
                            operations.push(format!("copy `{}`", relative_self))
                        }
                    }
                } else {
                    operations.push(format!("delete `{}`", relative_self));
                }
            }
            None => operations.push(format!("delete `{}`", relative_self)),
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
    let path = Path::new(relative_parent);
    let relative_self = path
        .join(&record.name)
        .into_os_string()
        .into_string()
        .unwrap();
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
