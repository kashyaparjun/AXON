use axon::archive::{
    add_file, apply_batch_mutations, gc_checkpoint_with_options, init_empty_archive, list_files,
    patch_file_with_expected_version, read_archive_info, read_file, read_file_history,
    read_file_version, read_root_manifest, remove_file_with_expected_version, search_files,
    verify_archive, wal_status, BatchMutation, GcOptions,
};
use axon::AxonError;
use clap::{Parser, Subcommand};
use serde::{Deserialize, Serialize};
use std::io::Write;
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(name = "axon")]
#[command(about = "AXON archive CLI (foundation)")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Create an empty AXON archive with valid header + root manifest.
    Init {
        archive: PathBuf,
        #[arg(long, default_value_t = false)]
        force: bool,
    },
    /// Read AXON archive metadata from header.
    Info {
        archive: PathBuf,
        #[arg(long, default_value_t = false)]
        pretty: bool,
    },
    /// Read root manifest from archive.
    Peek {
        archive: PathBuf,
        #[arg(long, default_value_t = false)]
        pretty: bool,
    },
    /// Add a file to archive from local source path.
    Add {
        archive: PathBuf,
        file: String,
        source: PathBuf,
    },
    /// Replace existing file content from local source path.
    Patch {
        archive: PathBuf,
        file: String,
        source: PathBuf,
        #[arg(long)]
        expected_version: Option<u32>,
    },
    /// Mark a file as removed via manifest tombstone.
    Remove {
        archive: PathBuf,
        file: String,
        #[arg(long)]
        expected_version: Option<u32>,
    },
    /// Search file paths from manifest without touching data blocks.
    Search {
        archive: PathBuf,
        query: String,
        #[arg(long, default_value_t = false)]
        include_tombstoned: bool,
        #[arg(long, default_value_t = false)]
        pretty: bool,
    },
    /// List file paths from manifests with optional prefix and pagination.
    List {
        archive: PathBuf,
        #[arg(long, default_value = "")]
        prefix: String,
        #[arg(long, default_value_t = 0)]
        offset: usize,
        #[arg(long)]
        limit: Option<usize>,
        #[arg(long, default_value_t = false)]
        include_tombstoned: bool,
        #[arg(long, default_value_t = false)]
        pretty: bool,
    },
    /// WAL inspection commands.
    Wal {
        archive: PathBuf,
        #[arg(long, default_value_t = false)]
        status: bool,
        #[arg(long, default_value_t = false)]
        pretty: bool,
    },
    /// Verify archive integrity, including deep consistency checks.
    Verify {
        archive: PathBuf,
        #[arg(long, default_value_t = false)]
        pretty: bool,
    },
    /// Compact archive snapshots and fold WAL into a fresh checkpoint.
    Gc {
        archive: PathBuf,
        #[arg(long, default_value_t = false)]
        prune_tombstones: bool,
        #[arg(long, default_value_t = false)]
        pretty: bool,
    },
    /// Apply multiple mutations atomically with OCC checks.
    Batch {
        archive: PathBuf,
        plan: PathBuf,
        #[arg(long, default_value_t = false)]
        pretty: bool,
    },
    /// Read file content from archive.
    Read {
        archive: PathBuf,
        file: String,
        #[arg(long)]
        version: Option<u32>,
        #[arg(short = 'o', long)]
        output: Option<PathBuf>,
    },
    /// Show per-file version history metadata.
    Log {
        archive: PathBuf,
        file: String,
        #[arg(long, default_value_t = false)]
        pretty: bool,
    },
}

fn main() {
    if let Err(err) = run() {
        emit_error_json(&err);
        std::process::exit(exit_code_for_error(&err));
    }
}

fn run() -> axon::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Command::Init { archive, force } => {
            init_empty_archive(&archive, force)?;
            println!(
                "{}",
                serde_json::json!({
                    "ok": true,
                    "archive": archive,
                    "created": true
                })
            );
        }
        Command::Info { archive, pretty } => {
            let info = read_archive_info(&archive)?;
            let output = if pretty {
                serde_json::to_string_pretty(&info)?
            } else {
                serde_json::to_string(&info)?
            };
            println!("{output}");
        }
        Command::Peek { archive, pretty } => {
            let manifest = read_root_manifest(&archive)?;
            let output = if pretty {
                serde_json::to_string_pretty(&manifest)?
            } else {
                serde_json::to_string(&manifest)?
            };
            println!("{output}");
        }
        Command::Add {
            archive,
            file,
            source,
        } => {
            add_file(&archive, &file, &source)?;
            println!(
                "{}",
                serde_json::json!({
                    "ok": true,
                    "archive": archive,
                    "file": file,
                    "source": source
                })
            );
        }
        Command::Patch {
            archive,
            file,
            source,
            expected_version,
        } => {
            patch_file_with_expected_version(&archive, &file, &source, expected_version)?;
            println!(
                "{}",
                serde_json::json!({
                    "ok": true,
                    "archive": archive,
                    "file": file,
                    "source": source,
                    "patched": true
                })
            );
        }
        Command::Remove {
            archive,
            file,
            expected_version,
        } => {
            remove_file_with_expected_version(&archive, &file, expected_version)?;
            println!(
                "{}",
                serde_json::json!({
                    "ok": true,
                    "archive": archive,
                    "file": file,
                    "removed": true
                })
            );
        }
        Command::Read {
            archive,
            file,
            version,
            output,
        } => {
            let bytes = match version {
                Some(value) => read_file_version(&archive, &file, value)?,
                None => read_file(&archive, &file)?,
            };
            if let Some(dest) = output {
                std::fs::write(&dest, &bytes)?;
                println!(
                    "{}",
                    serde_json::json!({
                        "ok": true,
                        "archive": archive,
                        "file": file,
                        "version": version,
                        "output": dest,
                        "bytes": bytes.len()
                    })
                );
            } else {
                let mut stdout = std::io::stdout();
                stdout.write_all(&bytes)?;
            }
        }
        Command::Log {
            archive,
            file,
            pretty,
        } => {
            let report = read_file_history(&archive, &file)?;
            let output = if pretty {
                serde_json::to_string_pretty(&report)?
            } else {
                serde_json::to_string(&report)?
            };
            println!("{output}");
        }
        Command::Search {
            archive,
            query,
            include_tombstoned,
            pretty,
        } => {
            let matches = search_files(&archive, &query, include_tombstoned)?;
            let payload = serde_json::json!({
                "query": query,
                "include_tombstoned": include_tombstoned,
                "count": matches.len(),
                "matches": matches
            });
            let output = if pretty {
                serde_json::to_string_pretty(&payload)?
            } else {
                serde_json::to_string(&payload)?
            };
            println!("{output}");
        }
        Command::List {
            archive,
            prefix,
            offset,
            limit,
            include_tombstoned,
            pretty,
        } => {
            let items = list_files(&archive, &prefix, include_tombstoned, offset, limit)?;
            let payload = serde_json::json!({
                "prefix": prefix,
                "offset": offset,
                "limit": limit,
                "include_tombstoned": include_tombstoned,
                "count": items.len(),
                "items": items
            });
            let output = if pretty {
                serde_json::to_string_pretty(&payload)?
            } else {
                serde_json::to_string(&payload)?
            };
            println!("{output}");
        }
        Command::Wal {
            archive,
            status,
            pretty,
        } => {
            if !status {
                return Err(axon::AxonError::Unsupported(
                    "use --status (WAL mutation commands are not implemented)",
                ));
            }
            let report = wal_status(&archive)?;
            let output = if pretty {
                serde_json::to_string_pretty(&report)?
            } else {
                serde_json::to_string(&report)?
            };
            println!("{output}");
        }
        Command::Verify { archive, pretty } => {
            let report = verify_archive(&archive)?;
            let output = if pretty {
                serde_json::to_string_pretty(&report)?
            } else {
                serde_json::to_string(&report)?
            };
            println!("{output}");
        }
        Command::Gc {
            archive,
            prune_tombstones,
            pretty,
        } => {
            let report = gc_checkpoint_with_options(&archive, GcOptions { prune_tombstones })?;
            let output = if pretty {
                serde_json::to_string_pretty(&report)?
            } else {
                serde_json::to_string(&report)?
            };
            println!("{output}");
        }
        Command::Batch {
            archive,
            plan,
            pretty,
        } => {
            let request: BatchRequest = serde_json::from_slice(&std::fs::read(&plan)?)?;
            let mut mutations = Vec::with_capacity(request.mutations.len());
            for item in request.mutations {
                match item.op.as_str() {
                    "add" => {
                        let source = item
                            .source
                            .ok_or(AxonError::InvalidArchive("batch add requires source"))?;
                        mutations.push(BatchMutation::Add {
                            path: item.path,
                            source,
                        });
                    }
                    "patch" => {
                        let source = item
                            .source
                            .ok_or(AxonError::InvalidArchive("batch patch requires source"))?;
                        mutations.push(BatchMutation::Patch {
                            path: item.path,
                            source,
                            expected_version: item.expected_version,
                        });
                    }
                    "remove" => {
                        mutations.push(BatchMutation::Remove {
                            path: item.path,
                            expected_version: item.expected_version,
                        });
                    }
                    _ => return Err(AxonError::InvalidArchive("unknown batch operation")),
                }
            }
            let applied = apply_batch_mutations(&archive, &mutations)?;
            let payload = serde_json::json!({
                "ok": true,
                "archive": archive,
                "plan": plan,
                "applied": applied
            });
            let output = if pretty {
                serde_json::to_string_pretty(&payload)?
            } else {
                serde_json::to_string(&payload)?
            };
            println!("{output}");
        }
    }

    Ok(())
}

#[derive(Debug, Deserialize)]
struct BatchRequest {
    mutations: Vec<BatchMutationRequest>,
}

#[derive(Debug, Deserialize)]
struct BatchMutationRequest {
    op: String,
    path: String,
    source: Option<PathBuf>,
    expected_version: Option<u32>,
}

#[derive(Debug, Serialize)]
struct CliErrorPayload<'a> {
    ok: bool,
    error: CliErrorBody<'a>,
}

#[derive(Debug, Serialize)]
struct CliErrorBody<'a> {
    code: &'a str,
    exit_code: i32,
    message: String,
}

fn emit_error_json(err: &axon::AxonError) {
    let exit_code = exit_code_for_error(err);
    let payload = CliErrorPayload {
        ok: false,
        error: CliErrorBody {
            code: error_code_symbol(err),
            exit_code,
            message: err.to_string(),
        },
    };
    let output = serde_json::to_string(&payload).unwrap_or_else(|_| {
        format!(
            "{{\"ok\":false,\"error\":{{\"code\":\"ERR_INTERNAL\",\"exit_code\":70,\"message\":\"{}\"}}}}",
            err
        )
    });
    eprintln!("{output}");
}

fn error_code_symbol(err: &axon::AxonError) -> &'static str {
    match err {
        AxonError::AlreadyExists(_) => "ERR_ALREADY_EXISTS",
        AxonError::EntryExists(_) => "ERR_ENTRY_EXISTS",
        AxonError::NotFound(_) => "ERR_NOT_FOUND",
        AxonError::Conflict(_) => "ERR_CONFLICT",
        AxonError::Unsupported(_) => "ERR_UNSUPPORTED",
        AxonError::InvalidArchive(_) => "ERR_INVALID_ARCHIVE",
        AxonError::Json(_) => "ERR_JSON",
        AxonError::Io(_) => "ERR_IO",
    }
}

fn exit_code_for_error(err: &axon::AxonError) -> i32 {
    match err {
        AxonError::AlreadyExists(_) => 21,
        AxonError::EntryExists(_) => 22,
        AxonError::NotFound(_) => 23,
        AxonError::Conflict(_) => 24,
        AxonError::Unsupported(_) => 25,
        AxonError::InvalidArchive(_) => 26,
        AxonError::Json(_) => 27,
        AxonError::Io(_) => 28,
    }
}
