use crate::format::block::{StoredBlock, ALGO_NONE, BLOCK_HEADER_SIZE};
use crate::format::header::{Header, HEADER_SIZE};
use crate::format::index::{find_block, BlockIndexEntry, BLOCK_INDEX_ENTRY_SIZE};
use crate::format::manifest::{
    decode_root_manifest, decode_shard_manifest, encode_root_manifest, encode_shard_manifest,
    is_binary_root_manifest, is_binary_shard_manifest, FLAG_BINARY_MANIFESTS, FLAG_SHARDED_LAYOUT,
};
use crate::format::wal::{decode_wal, encode_wal, WalEntry, OP_ADD, OP_PATCH, OP_REMOVE};
use crate::manifest::{FileEntry, RootManifest, ShardDescriptor, ShardManifest};
use crate::{AxonError, Result};
use serde::Serialize;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

const SHARD_BUCKET_COUNT: u32 = 16;
const SHARD_SPLIT_MIN_FILES: usize = 8;

#[derive(Debug, Serialize)]
pub struct ArchiveInfo {
    pub version_major: u16,
    pub version_minor: u16,
    pub total_files: u64,
    pub wal_size: u64,
    pub block_index_count: u64,
    pub root_manifest_offset: u64,
    pub root_manifest_size: u32,
    pub data_region_offset: u64,
    pub target_shard_size: u32,
    pub min_shard_size: u32,
    pub created_at: u64,
}

#[derive(Debug, Serialize)]
pub struct WalStatus {
    pub wal_offset: u64,
    pub wal_size: u64,
    pub entry_count: usize,
}

#[derive(Debug, Serialize)]
pub struct VerifyReport {
    pub ok: bool,
    pub file_size: u64,
    pub total_files: u64,
    pub block_index_count: u64,
    pub wal_entry_count: usize,
}

#[derive(Debug, Serialize)]
pub struct GcReport {
    pub ok: bool,
    pub old_size: u64,
    pub new_size: u64,
    pub blocks_copied: usize,
    pub wal_entries_compacted: usize,
}

#[derive(Debug, Clone)]
pub enum BatchMutation {
    Add {
        path: String,
        source: PathBuf,
    },
    Patch {
        path: String,
        source: PathBuf,
        expected_version: Option<u32>,
    },
    Remove {
        path: String,
        expected_version: Option<u32>,
    },
}

pub fn init_empty_archive(path: &Path, force: bool) -> Result<()> {
    if path.exists() && !force {
        return Err(AxonError::AlreadyExists(path.display().to_string()));
    }

    let manifest = RootManifest::empty();
    let manifest_bytes = encode_root_manifest(&RootManifest {
        format_flags: FLAG_BINARY_MANIFESTS,
        ..manifest
    })?;
    let manifest_size = u32::try_from(manifest_bytes.len())
        .map_err(|_| AxonError::InvalidArchive("root manifest exceeds u32 size"))?;
    let header = Header::new_empty(manifest_size);

    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(path)?;
    file.write_all(&header.encode())?;
    file.write_all(&manifest_bytes)?;
    file.flush()?;
    Ok(())
}

pub fn add_file(archive_path: &Path, archive_file_path: &str, source: &Path) -> Result<()> {
    let mut header = read_header(archive_path)?;
    let mut wal_entries = read_wal_entries(archive_path, &header)?;
    let root_manifest = read_root_manifest(archive_path)?;
    let mut files = load_manifest_files(archive_path)?;
    if files.iter().any(|entry| entry.path == archive_file_path) {
        return Err(AxonError::EntryExists(archive_file_path.to_string()));
    }

    let mut index = read_block_index(archive_path, &header)?;
    let raw = std::fs::read(source)?;
    let new_block = StoredBlock::from_raw_base(&raw)?;
    if new_block.header.algo != ALGO_NONE {
        return Err(AxonError::Unsupported("only ALGO_NONE is implemented"));
    }

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(archive_path)?;
    let mut end = file.seek(SeekFrom::End(0))?;

    append_block_if_missing(&mut file, &mut end, &mut index, &new_block)?;

    files.push(FileEntry {
        path: archive_file_path.to_string(),
        block_id_hex: hex_encode(&new_block.header.block_id),
        raw_size: new_block.header.raw_size,
        stored_size: new_block.header.stored_size,
        algo: new_block.header.algo,
        version: 1,
        history_block_ids: Vec::new(),
        tombstoned: false,
    });
    wal_entries.push(WalEntry {
        op: OP_ADD,
        path: archive_file_path.to_string(),
        expected_version: None,
        resulting_version: 1,
        block_id: new_block.header.block_id,
        raw_size: new_block.header.raw_size,
        stored_size: new_block.header.stored_size,
        algo: new_block.header.algo,
        tombstoned: false,
    });
    files.sort_by(|a, b| a.path.cmp(&b.path));

    commit_snapshots(
        &mut file,
        &mut header,
        root_manifest.format_flags,
        &files,
        &wal_entries,
        &index,
        end,
    )?;
    Ok(())
}

pub fn patch_file(archive_path: &Path, archive_file_path: &str, source: &Path) -> Result<()> {
    patch_file_with_expected_version(archive_path, archive_file_path, source, None)
}

pub fn patch_file_with_expected_version(
    archive_path: &Path,
    archive_file_path: &str,
    source: &Path,
    expected_version: Option<u32>,
) -> Result<()> {
    let mut header = read_header(archive_path)?;
    let mut wal_entries = read_wal_entries(archive_path, &header)?;
    let root_manifest = read_root_manifest(archive_path)?;
    let mut files = load_manifest_files(archive_path)?;
    let mut index = read_block_index(archive_path, &header)?;

    let raw = std::fs::read(source)?;
    let new_block = StoredBlock::from_raw_base(&raw)?;
    if new_block.header.algo != ALGO_NONE {
        return Err(AxonError::Unsupported("only ALGO_NONE is implemented"));
    }

    let file_entry = files
        .iter_mut()
        .find(|entry| entry.path == archive_file_path && !entry.tombstoned)
        .ok_or_else(|| AxonError::NotFound(archive_file_path.to_string()))?;
    ensure_expected_version(file_entry, archive_file_path, expected_version)?;

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(archive_path)?;
    let mut end = file.seek(SeekFrom::End(0))?;
    append_block_if_missing(&mut file, &mut end, &mut index, &new_block)?;

    file_entry
        .history_block_ids
        .push(file_entry.block_id_hex.clone());
    file_entry.block_id_hex = hex_encode(&new_block.header.block_id);
    file_entry.raw_size = new_block.header.raw_size;
    file_entry.stored_size = new_block.header.stored_size;
    file_entry.algo = new_block.header.algo;
    file_entry.version = file_entry.version.saturating_add(1);
    wal_entries.push(WalEntry {
        op: OP_PATCH,
        path: archive_file_path.to_string(),
        expected_version,
        resulting_version: file_entry.version,
        block_id: new_block.header.block_id,
        raw_size: new_block.header.raw_size,
        stored_size: new_block.header.stored_size,
        algo: new_block.header.algo,
        tombstoned: false,
    });

    commit_snapshots(
        &mut file,
        &mut header,
        root_manifest.format_flags,
        &files,
        &wal_entries,
        &index,
        end,
    )?;
    Ok(())
}

pub fn remove_file(archive_path: &Path, archive_file_path: &str) -> Result<()> {
    remove_file_with_expected_version(archive_path, archive_file_path, None)
}

pub fn remove_file_with_expected_version(
    archive_path: &Path,
    archive_file_path: &str,
    expected_version: Option<u32>,
) -> Result<()> {
    let mut header = read_header(archive_path)?;
    let mut wal_entries = read_wal_entries(archive_path, &header)?;
    let root_manifest = read_root_manifest(archive_path)?;
    let mut files = load_manifest_files(archive_path)?;
    let index = read_block_index(archive_path, &header)?;

    let file_entry = files
        .iter_mut()
        .find(|entry| entry.path == archive_file_path && !entry.tombstoned)
        .ok_or_else(|| AxonError::NotFound(archive_file_path.to_string()))?;
    ensure_expected_version(file_entry, archive_file_path, expected_version)?;

    file_entry
        .history_block_ids
        .push(file_entry.block_id_hex.clone());
    file_entry.version = file_entry.version.saturating_add(1);
    file_entry.tombstoned = true;
    wal_entries.push(WalEntry {
        op: OP_REMOVE,
        path: archive_file_path.to_string(),
        expected_version,
        resulting_version: file_entry.version,
        block_id: hex_decode_32(&file_entry.block_id_hex)?,
        raw_size: file_entry.raw_size,
        stored_size: file_entry.stored_size,
        algo: file_entry.algo,
        tombstoned: true,
    });

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(archive_path)?;
    let end = file.seek(SeekFrom::End(0))?;
    commit_snapshots(
        &mut file,
        &mut header,
        root_manifest.format_flags,
        &files,
        &wal_entries,
        &index,
        end,
    )?;
    Ok(())
}

pub fn read_file(archive_path: &Path, archive_file_path: &str) -> Result<Vec<u8>> {
    let files = load_manifest_files(archive_path)?;
    let file = files
        .iter()
        .find(|entry| entry.path == archive_file_path && !entry.tombstoned)
        .ok_or_else(|| AxonError::NotFound(archive_file_path.to_string()))?;

    if file.algo != ALGO_NONE {
        return Err(AxonError::Unsupported("only ALGO_NONE is implemented"));
    }

    let block_id = hex_decode_32(&file.block_id_hex)?;
    let header = read_header(archive_path)?;
    let index = read_block_index(archive_path, &header)?;
    let entry = find_block(&index, &block_id)
        .ok_or_else(|| AxonError::NotFound(format!("block {}", file.block_id_hex)))?;

    let block = read_stored_block_at(archive_path, entry.offset, entry.stored_size)?;
    if block.header.block_id != block_id {
        return Err(AxonError::InvalidArchive("block id mismatch"));
    }
    Ok(block.body)
}

pub fn read_archive_info(path: &Path) -> Result<ArchiveInfo> {
    let header = read_header(path)?;
    Ok(ArchiveInfo {
        version_major: header.version_major,
        version_minor: header.version_minor,
        total_files: header.total_files,
        wal_size: header.wal_size,
        block_index_count: header.block_index_count,
        root_manifest_offset: header.root_manifest_offset,
        root_manifest_size: header.root_manifest_size,
        data_region_offset: header.data_region_offset,
        target_shard_size: header.target_shard_size,
        min_shard_size: header.min_shard_size,
        created_at: header.created_at,
    })
}

pub fn wal_status(path: &Path) -> Result<WalStatus> {
    let header = read_header(path)?;
    let entries = read_wal_entries(path, &header)?;
    Ok(WalStatus {
        wal_offset: header.wal_offset,
        wal_size: header.wal_size,
        entry_count: entries.len(),
    })
}

pub fn verify_archive(path: &Path) -> Result<VerifyReport> {
    let header = read_header(path)?;
    let file_size = std::fs::metadata(path)?.len();

    validate_region_bounds(
        header.root_manifest_offset,
        u64::from(header.root_manifest_size),
        file_size,
        "root manifest out of bounds",
    )?;
    if header.wal_size > 0 {
        validate_region_bounds(
            header.wal_offset,
            header.wal_size,
            file_size,
            "WAL out of bounds",
        )?;
    }
    if header.block_index_count > 0 {
        let index_bytes = header
            .block_index_count
            .checked_mul(BLOCK_INDEX_ENTRY_SIZE as u64)
            .ok_or(AxonError::InvalidArchive("block index range overflow"))?;
        validate_region_bounds(
            header.block_index_offset,
            index_bytes,
            file_size,
            "block index out of bounds",
        )?;
    }

    let wal_entries = read_wal_entries(path, &header)?;
    let manifest = read_root_manifest(path)?;
    let index = read_block_index(path, &header)?;
    for entry in &index {
        let total = u64::from(entry.stored_size)
            .checked_add(BLOCK_HEADER_SIZE as u64)
            .ok_or(AxonError::InvalidArchive("block region overflow"))?;
        validate_region_bounds(entry.offset, total, file_size, "block entry out of bounds")?;
    }

    Ok(VerifyReport {
        ok: true,
        file_size,
        total_files: manifest.total_file_count,
        block_index_count: header.block_index_count,
        wal_entry_count: wal_entries.len(),
    })
}

pub fn gc_checkpoint(path: &Path) -> Result<GcReport> {
    let old_size = std::fs::metadata(path)?.len();
    let header = read_header(path)?;
    let root = read_root_manifest(path)?;
    let files = load_manifest_files(path)?;
    let old_index = read_block_index(path, &header)?;
    let wal_entries = read_wal_entries(path, &header)?;

    let tmp = path.with_extension("axon.gc.tmp");
    let mut out = OpenOptions::new()
        .create(true)
        .truncate(true)
        .read(true)
        .write(true)
        .open(&tmp)?;

    let mut new_header = header.clone();
    out.write_all(&new_header.encode())?;
    let mut end = HEADER_SIZE as u64;
    let mut new_index = Vec::with_capacity(old_index.len());
    for entry in &old_index {
        let block = read_stored_block_at(path, entry.offset, entry.stored_size)?;
        out.seek(SeekFrom::Start(end))?;
        out.write_all(&block.encode())?;
        new_index.push(BlockIndexEntry {
            block_id: entry.block_id,
            offset: end,
            stored_size: entry.stored_size,
        });
        end += block.encoded_len();
    }

    commit_snapshots(
        &mut out,
        &mut new_header,
        root.format_flags,
        &files,
        &[],
        &new_index,
        end,
    )?;
    drop(out);

    let new_size = std::fs::metadata(&tmp)?.len();
    std::fs::rename(&tmp, path)?;
    Ok(GcReport {
        ok: true,
        old_size,
        new_size,
        blocks_copied: new_index.len(),
        wal_entries_compacted: wal_entries.len(),
    })
}

pub fn apply_batch_mutations(archive_path: &Path, mutations: &[BatchMutation]) -> Result<usize> {
    if mutations.is_empty() {
        return Ok(0);
    }

    enum PreparedMutation {
        Add {
            path: String,
            block: StoredBlock,
        },
        Patch {
            path: String,
            block: StoredBlock,
            expected_version: Option<u32>,
        },
        Remove {
            path: String,
            expected_version: Option<u32>,
        },
    }

    let mut prepared = Vec::with_capacity(mutations.len());
    for mutation in mutations {
        match mutation {
            BatchMutation::Add { path, source } => {
                let raw = std::fs::read(source)?;
                let block = StoredBlock::from_raw_base(&raw)?;
                if block.header.algo != ALGO_NONE {
                    return Err(AxonError::Unsupported("only ALGO_NONE is implemented"));
                }
                prepared.push(PreparedMutation::Add {
                    path: path.clone(),
                    block,
                });
            }
            BatchMutation::Patch {
                path,
                source,
                expected_version,
            } => {
                let raw = std::fs::read(source)?;
                let block = StoredBlock::from_raw_base(&raw)?;
                if block.header.algo != ALGO_NONE {
                    return Err(AxonError::Unsupported("only ALGO_NONE is implemented"));
                }
                prepared.push(PreparedMutation::Patch {
                    path: path.clone(),
                    block,
                    expected_version: *expected_version,
                });
            }
            BatchMutation::Remove {
                path,
                expected_version,
            } => {
                prepared.push(PreparedMutation::Remove {
                    path: path.clone(),
                    expected_version: *expected_version,
                });
            }
        }
    }

    let mut header = read_header(archive_path)?;
    let mut wal_entries = read_wal_entries(archive_path, &header)?;
    let root_manifest = read_root_manifest(archive_path)?;
    let mut files = load_manifest_files(archive_path)?;
    let mut index = read_block_index(archive_path, &header)?;

    for mutation in &prepared {
        match mutation {
            PreparedMutation::Add { path, block } => {
                if files.iter().any(|entry| entry.path == *path) {
                    return Err(AxonError::EntryExists(path.clone()));
                }
                files.push(FileEntry {
                    path: path.clone(),
                    block_id_hex: hex_encode(&block.header.block_id),
                    raw_size: block.header.raw_size,
                    stored_size: block.header.stored_size,
                    algo: block.header.algo,
                    version: 1,
                    history_block_ids: Vec::new(),
                    tombstoned: false,
                });
                wal_entries.push(WalEntry {
                    op: OP_ADD,
                    path: path.clone(),
                    expected_version: None,
                    resulting_version: 1,
                    block_id: block.header.block_id,
                    raw_size: block.header.raw_size,
                    stored_size: block.header.stored_size,
                    algo: block.header.algo,
                    tombstoned: false,
                });
            }
            PreparedMutation::Patch {
                path,
                block,
                expected_version,
            } => {
                let file_entry = files
                    .iter_mut()
                    .find(|entry| entry.path == *path && !entry.tombstoned)
                    .ok_or_else(|| AxonError::NotFound(path.clone()))?;
                ensure_expected_version(file_entry, path, *expected_version)?;
                file_entry
                    .history_block_ids
                    .push(file_entry.block_id_hex.clone());
                file_entry.block_id_hex = hex_encode(&block.header.block_id);
                file_entry.raw_size = block.header.raw_size;
                file_entry.stored_size = block.header.stored_size;
                file_entry.algo = block.header.algo;
                file_entry.version = file_entry.version.saturating_add(1);
                wal_entries.push(WalEntry {
                    op: OP_PATCH,
                    path: path.clone(),
                    expected_version: *expected_version,
                    resulting_version: file_entry.version,
                    block_id: block.header.block_id,
                    raw_size: block.header.raw_size,
                    stored_size: block.header.stored_size,
                    algo: block.header.algo,
                    tombstoned: false,
                });
            }
            PreparedMutation::Remove {
                path,
                expected_version,
            } => {
                let file_entry = files
                    .iter_mut()
                    .find(|entry| entry.path == *path && !entry.tombstoned)
                    .ok_or_else(|| AxonError::NotFound(path.clone()))?;
                ensure_expected_version(file_entry, path, *expected_version)?;
                file_entry
                    .history_block_ids
                    .push(file_entry.block_id_hex.clone());
                file_entry.version = file_entry.version.saturating_add(1);
                file_entry.tombstoned = true;
                wal_entries.push(WalEntry {
                    op: OP_REMOVE,
                    path: path.clone(),
                    expected_version: *expected_version,
                    resulting_version: file_entry.version,
                    block_id: hex_decode_32(&file_entry.block_id_hex)?,
                    raw_size: file_entry.raw_size,
                    stored_size: file_entry.stored_size,
                    algo: file_entry.algo,
                    tombstoned: true,
                });
            }
        }
    }

    files.sort_by(|a, b| a.path.cmp(&b.path));

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(archive_path)?;
    let mut end = file.seek(SeekFrom::End(0))?;
    for mutation in &prepared {
        match mutation {
            PreparedMutation::Add { block, .. } | PreparedMutation::Patch { block, .. } => {
                append_block_if_missing(&mut file, &mut end, &mut index, block)?;
            }
            PreparedMutation::Remove { .. } => {}
        }
    }

    commit_snapshots(
        &mut file,
        &mut header,
        root_manifest.format_flags,
        &files,
        &wal_entries,
        &index,
        end,
    )?;
    Ok(prepared.len())
}

pub fn read_header(path: &Path) -> Result<Header> {
    let mut file = File::open(path)?;
    let mut header_bytes = [0u8; HEADER_SIZE];
    file.read_exact(&mut header_bytes)?;
    Header::decode(header_bytes)
}

pub fn read_root_manifest(path: &Path) -> Result<RootManifest> {
    let header = read_header(path)?;
    let mut file = File::open(path)?;
    file.seek(SeekFrom::Start(header.root_manifest_offset))?;
    let mut bytes = vec![0u8; header.root_manifest_size as usize];
    file.read_exact(&mut bytes)?;
    if is_binary_root_manifest(&bytes) {
        return decode_root_manifest(&bytes);
    }
    Ok(serde_json::from_slice(&bytes)?)
}

pub fn search_files(
    archive_path: &Path,
    query: &str,
    include_tombstoned: bool,
) -> Result<Vec<FileEntry>> {
    Ok(load_manifest_files(archive_path)?
        .into_iter()
        .filter(|entry| include_tombstoned || !entry.tombstoned)
        .filter(|entry| query.is_empty() || entry.path.contains(query))
        .collect())
}

pub fn list_files(
    archive_path: &Path,
    prefix: &str,
    include_tombstoned: bool,
    offset: usize,
    limit: Option<usize>,
) -> Result<Vec<FileEntry>> {
    let mut files: Vec<FileEntry> = load_manifest_files(archive_path)?
        .into_iter()
        .filter(|entry| include_tombstoned || !entry.tombstoned)
        .filter(|entry| prefix.is_empty() || entry.path.starts_with(prefix))
        .collect();
    files.sort_by(|a, b| a.path.cmp(&b.path));
    let files = files.into_iter().skip(offset);
    Ok(match limit {
        Some(value) => files.take(value).collect(),
        None => files.collect(),
    })
}

fn load_manifest_files(path: &Path) -> Result<Vec<FileEntry>> {
    let manifest = read_root_manifest(path)?;
    let mut files = manifest.files;
    for descriptor in &manifest.shard_descriptors {
        let shard = read_shard_manifest(path, descriptor)?;
        files.extend(shard.files);
    }
    let header = read_header(path)?;
    let wal_entries = read_wal_entries(path, &header)?;
    apply_wal_entries(&mut files, &wal_entries)?;
    Ok(files)
}

fn read_shard_manifest(path: &Path, descriptor: &ShardDescriptor) -> Result<ShardManifest> {
    if descriptor.compressed {
        return Err(AxonError::Unsupported(
            "compressed shard manifests are not implemented",
        ));
    }
    let mut file = File::open(path)?;
    file.seek(SeekFrom::Start(descriptor.shard_offset))?;
    let mut bytes = vec![0u8; descriptor.shard_size as usize];
    file.read_exact(&mut bytes)?;
    if is_binary_shard_manifest(&bytes) {
        return decode_shard_manifest(&bytes);
    }
    Ok(serde_json::from_slice(&bytes)?)
}

fn read_wal_entries(path: &Path, header: &Header) -> Result<Vec<WalEntry>> {
    if header.wal_size == 0 {
        return Ok(Vec::new());
    }
    let size = usize::try_from(header.wal_size)
        .map_err(|_| AxonError::InvalidArchive("WAL too large to load"))?;
    let mut file = File::open(path)?;
    file.seek(SeekFrom::Start(header.wal_offset))?;
    let mut bytes = vec![0u8; size];
    file.read_exact(&mut bytes)?;
    decode_wal(&bytes)
}

fn apply_wal_entries(files: &mut Vec<FileEntry>, entries: &[WalEntry]) -> Result<()> {
    for entry in entries {
        if entry.op != OP_ADD && entry.op != OP_PATCH && entry.op != OP_REMOVE {
            return Err(AxonError::InvalidArchive("unknown WAL operation"));
        }
        let wal_block_hex = hex_encode(&entry.block_id);
        let pos = files.iter().position(|file| file.path == entry.path);
        match pos {
            Some(idx) => {
                let file = &mut files[idx];
                if file.version > entry.resulting_version {
                    continue;
                }
                if file.version == entry.resulting_version {
                    if file.block_id_hex == wal_block_hex && file.tombstoned == entry.tombstoned {
                        continue;
                    }
                    return Err(AxonError::InvalidArchive("WAL version state mismatch"));
                }
                if let Some(expected) = entry.expected_version {
                    if file.version != expected {
                        return Err(AxonError::Conflict(format!(
                            "{}: expected version {expected}, current version {}",
                            entry.path, file.version
                        )));
                    }
                }
                file.history_block_ids.push(file.block_id_hex.clone());
                file.block_id_hex = wal_block_hex;
                file.raw_size = entry.raw_size;
                file.stored_size = entry.stored_size;
                file.algo = entry.algo;
                file.version = entry.resulting_version;
                file.tombstoned = entry.tombstoned;
            }
            None => {
                if entry.op != OP_ADD {
                    return Err(AxonError::InvalidArchive(
                        "WAL update references missing file",
                    ));
                }
                files.push(FileEntry {
                    path: entry.path.clone(),
                    block_id_hex: wal_block_hex,
                    raw_size: entry.raw_size,
                    stored_size: entry.stored_size,
                    algo: entry.algo,
                    version: entry.resulting_version,
                    history_block_ids: Vec::new(),
                    tombstoned: entry.tombstoned,
                });
            }
        }
    }
    files.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(())
}

fn validate_region_bounds(
    offset: u64,
    size: u64,
    file_size: u64,
    message: &'static str,
) -> Result<()> {
    let end = offset
        .checked_add(size)
        .ok_or(AxonError::InvalidArchive(message))?;
    if end > file_size {
        return Err(AxonError::InvalidArchive(message));
    }
    Ok(())
}

fn read_block_index(path: &Path, header: &Header) -> Result<Vec<BlockIndexEntry>> {
    let mut file = File::open(path)?;
    file.seek(SeekFrom::Start(header.block_index_offset))?;

    let count = usize::try_from(header.block_index_count)
        .map_err(|_| AxonError::InvalidArchive("block index count too large"))?;
    let mut entries = Vec::with_capacity(count);
    for _ in 0..count {
        let mut buf = [0u8; BLOCK_INDEX_ENTRY_SIZE];
        file.read_exact(&mut buf)?;
        entries.push(BlockIndexEntry::decode(&buf)?);
    }

    entries.sort_by(|a, b| a.block_id.cmp(&b.block_id));
    Ok(entries)
}

fn read_stored_block_at(path: &Path, offset: u64, stored_size: u32) -> Result<StoredBlock> {
    let mut file = File::open(path)?;
    file.seek(SeekFrom::Start(offset))?;
    let mut bytes = vec![0u8; BLOCK_HEADER_SIZE + stored_size as usize];
    file.read_exact(&mut bytes)?;
    StoredBlock::decode(&bytes)
}

fn append_block_if_missing(
    file: &mut File,
    end: &mut u64,
    index: &mut Vec<BlockIndexEntry>,
    block: &StoredBlock,
) -> Result<()> {
    if find_block(index, &block.header.block_id).is_some() {
        return Ok(());
    }

    file.seek(SeekFrom::Start(*end))?;
    file.write_all(&block.encode())?;
    let offset = *end;
    *end += block.encoded_len();
    index.push(BlockIndexEntry {
        block_id: block.header.block_id,
        offset,
        stored_size: block.header.stored_size,
    });
    index.sort_by(|a, b| a.block_id.cmp(&b.block_id));
    Ok(())
}

fn commit_snapshots(
    file: &mut File,
    header: &mut Header,
    previous_format_flags: u64,
    all_files: &[FileEntry],
    wal_entries: &[WalEntry],
    index: &[BlockIndexEntry],
    mut end: u64,
) -> Result<()> {
    let wal_offset = end;
    let wal_bytes = encode_wal(wal_entries)?;
    file.seek(SeekFrom::Start(wal_offset))?;
    file.write_all(&wal_bytes)?;
    end += wal_bytes.len() as u64;

    let index_offset = end;
    file.seek(SeekFrom::Start(index_offset))?;
    for entry in index {
        file.write_all(&entry.encode())?;
    }
    end = index_offset + (index.len() as u64 * BLOCK_INDEX_ENTRY_SIZE as u64);

    let mut root_files = all_files.to_vec();
    let mut shard_descriptors = Vec::new();
    let mut format_flags = previous_format_flags | FLAG_BINARY_MANIFESTS;
    if all_files.len() >= SHARD_SPLIT_MIN_FILES {
        let mut buckets: Vec<Vec<FileEntry>> = vec![Vec::new(); SHARD_BUCKET_COUNT as usize];
        for entry in all_files {
            if let Some(shard_id) =
                crate::manifest::route_path_to_shard(&entry.path, SHARD_BUCKET_COUNT)
            {
                buckets[shard_id as usize].push(entry.clone());
            }
        }
        root_files.clear();
        format_flags |= FLAG_SHARDED_LAYOUT;
        for (shard_id, files) in buckets.into_iter().enumerate() {
            if files.is_empty() {
                continue;
            }
            let shard_manifest = ShardManifest { files };
            let shard_bytes = encode_shard_manifest(&shard_manifest)?;
            let shard_offset = end;
            file.seek(SeekFrom::Start(shard_offset))?;
            file.write_all(&shard_bytes)?;
            end += shard_bytes.len() as u64;
            shard_descriptors.push(ShardDescriptor {
                shard_id: shard_id as u32,
                shard_offset,
                shard_size: u32::try_from(shard_bytes.len())
                    .map_err(|_| AxonError::InvalidArchive("shard manifest exceeds u32 size"))?,
                compressed: false,
            });
        }
    } else {
        format_flags &= !FLAG_SHARDED_LAYOUT;
    }

    let root_manifest = RootManifest {
        shard_descriptors,
        total_file_count: all_files.len() as u64,
        format_flags,
        files: root_files,
    };
    let manifest_bytes = encode_root_manifest(&root_manifest)?;
    let manifest_size = u32::try_from(manifest_bytes.len())
        .map_err(|_| AxonError::InvalidArchive("root manifest exceeds u32 size"))?;
    let manifest_offset = end;
    file.seek(SeekFrom::Start(manifest_offset))?;
    file.write_all(&manifest_bytes)?;

    header.root_manifest_offset = manifest_offset;
    header.root_manifest_size = manifest_size;
    header.wal_offset = wal_offset;
    header.wal_size = wal_bytes.len() as u64;
    header.block_index_offset = index_offset;
    header.block_index_count = index.len() as u64;
    header.total_files = all_files.len() as u64;

    file.seek(SeekFrom::Start(0))?;
    file.write_all(&header.encode())?;
    file.flush()?;
    Ok(())
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn ensure_expected_version(
    file_entry: &FileEntry,
    archive_file_path: &str,
    expected_version: Option<u32>,
) -> Result<()> {
    if let Some(expected) = expected_version {
        if file_entry.version != expected {
            return Err(AxonError::Conflict(format!(
                "{archive_file_path}: expected version {expected}, current version {}",
                file_entry.version
            )));
        }
    }
    Ok(())
}

fn hex_decode_32(value: &str) -> Result<[u8; 32]> {
    if value.len() != 64 {
        return Err(AxonError::InvalidArchive("invalid block id hex length"));
    }

    let mut out = [0u8; 32];
    let bytes = value.as_bytes();
    for i in 0..32 {
        let hi = hex_val(bytes[2 * i])?;
        let lo = hex_val(bytes[2 * i + 1])?;
        out[i] = (hi << 4) | lo;
    }
    Ok(out)
}

fn hex_val(b: u8) -> Result<u8> {
    match b {
        b'0'..=b'9' => Ok(b - b'0'),
        b'a'..=b'f' => Ok(10 + b - b'a'),
        b'A'..=b'F' => Ok(10 + b - b'A'),
        _ => Err(AxonError::InvalidArchive("invalid hex character")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    static TEST_SEQ: AtomicU64 = AtomicU64::new(0);

    fn test_path(prefix: &str, ext: &str) -> std::path::PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_nanos();
        let seq = TEST_SEQ.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!("{prefix}-{nanos}-{seq}.{ext}"))
    }

    fn rewrite_header(path: &Path, mutate: impl FnOnce(&mut Header)) {
        let mut header = read_header(path).expect("read header");
        mutate(&mut header);

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .expect("open archive");
        file.seek(SeekFrom::Start(0)).expect("seek header");
        file.write_all(&header.encode()).expect("write header");
        file.flush().expect("flush header");
    }

    #[test]
    fn init_writes_valid_header_and_manifest() {
        let archive_path = test_path("axon-test-init", "axon");
        init_empty_archive(&archive_path, false).expect("init should succeed");

        let info = read_archive_info(&archive_path).expect("info should read");
        assert_eq!(info.version_major, 0);
        assert_eq!(info.version_minor, 1);
        assert_eq!(info.total_files, 0);
        assert_eq!(info.block_index_count, 0);

        let manifest = read_root_manifest(&archive_path).expect("manifest should read");
        assert_eq!(
            manifest,
            RootManifest {
                format_flags: FLAG_BINARY_MANIFESTS,
                ..RootManifest::empty()
            }
        );
        std::fs::remove_file(archive_path).expect("cleanup");
    }

    #[test]
    fn add_and_read_round_trip() {
        let archive_path = test_path("axon-test-add", "axon");
        let source_path = test_path("axon-source", "txt");
        init_empty_archive(&archive_path, false).expect("init should succeed");
        std::fs::write(&source_path, b"fn main() {}\n").expect("write source");

        add_file(&archive_path, "src/main.rs", &source_path).expect("add should succeed");
        let bytes = read_file(&archive_path, "src/main.rs").expect("read should succeed");

        assert_eq!(bytes, b"fn main() {}\n");
        let info = read_archive_info(&archive_path).expect("info");
        assert_eq!(info.total_files, 1);
        assert_eq!(info.block_index_count, 1);

        std::fs::remove_file(archive_path).expect("cleanup");
        std::fs::remove_file(source_path).expect("cleanup");
    }

    #[test]
    fn add_errors_when_path_already_exists() {
        let archive_path = test_path("axon-test-dup", "axon");
        let source_path = test_path("axon-source", "txt");
        init_empty_archive(&archive_path, false).expect("init should succeed");
        std::fs::write(&source_path, b"same").expect("write source");

        add_file(&archive_path, "dup.txt", &source_path).expect("first add");
        let err = add_file(&archive_path, "dup.txt", &source_path).expect_err("second add fails");
        assert!(matches!(err, AxonError::EntryExists(_)));

        std::fs::remove_file(archive_path).expect("cleanup");
        std::fs::remove_file(source_path).expect("cleanup");
    }

    #[test]
    fn deduplicates_identical_blocks() {
        let archive_path = test_path("axon-test-dedupe", "axon");
        let source_a = test_path("axon-source", "txt");
        let source_b = test_path("axon-source", "txt");
        init_empty_archive(&archive_path, false).expect("init should succeed");
        std::fs::write(&source_a, b"shared").expect("write source");
        std::fs::write(&source_b, b"shared").expect("write source");

        add_file(&archive_path, "a.txt", &source_a).expect("add a");
        add_file(&archive_path, "b.txt", &source_b).expect("add b");

        let info = read_archive_info(&archive_path).expect("info");
        assert_eq!(info.total_files, 2);
        assert_eq!(info.block_index_count, 1);

        std::fs::remove_file(archive_path).expect("cleanup");
        std::fs::remove_file(source_a).expect("cleanup");
        std::fs::remove_file(source_b).expect("cleanup");
    }

    #[test]
    fn multiple_adds_append_without_truncation() {
        let archive_path = test_path("axon-test-append", "axon");
        let source_a = test_path("axon-source", "txt");
        let source_b = test_path("axon-source", "txt");
        init_empty_archive(&archive_path, false).expect("init should succeed");
        std::fs::write(&source_a, b"first").expect("write source");
        std::fs::write(&source_b, b"second").expect("write source");

        let initial_size = std::fs::metadata(&archive_path).expect("meta").len();
        add_file(&archive_path, "a.txt", &source_a).expect("add a");
        let after_first = std::fs::metadata(&archive_path).expect("meta").len();
        add_file(&archive_path, "b.txt", &source_b).expect("add b");
        let after_second = std::fs::metadata(&archive_path).expect("meta").len();

        assert!(after_first > initial_size);
        assert!(after_second > after_first);
        assert_eq!(
            read_file(&archive_path, "a.txt").expect("read a"),
            b"first".to_vec()
        );
        assert_eq!(
            read_file(&archive_path, "b.txt").expect("read b"),
            b"second".to_vec()
        );

        std::fs::remove_file(archive_path).expect("cleanup");
        std::fs::remove_file(source_a).expect("cleanup");
        std::fs::remove_file(source_b).expect("cleanup");
    }

    #[test]
    fn patch_updates_content_and_version_history() {
        let archive_path = test_path("axon-test-patch", "axon");
        let source_v1 = test_path("axon-source", "txt");
        let source_v2 = test_path("axon-source", "txt");
        init_empty_archive(&archive_path, false).expect("init should succeed");
        std::fs::write(&source_v1, b"v1").expect("write source");
        std::fs::write(&source_v2, b"v2").expect("write source");

        add_file(&archive_path, "cfg/app.toml", &source_v1).expect("add should succeed");
        patch_file(&archive_path, "cfg/app.toml", &source_v2).expect("patch should succeed");

        assert_eq!(
            read_file(&archive_path, "cfg/app.toml").expect("read patched"),
            b"v2".to_vec()
        );

        let manifest = read_root_manifest(&archive_path).expect("manifest");
        let entry = manifest
            .files
            .iter()
            .find(|entry| entry.path == "cfg/app.toml")
            .expect("entry");
        assert_eq!(entry.version, 2);
        assert_eq!(entry.history_block_ids.len(), 1);
        assert_ne!(entry.history_block_ids[0], entry.block_id_hex);

        std::fs::remove_file(archive_path).expect("cleanup");
        std::fs::remove_file(source_v1).expect("cleanup");
        std::fs::remove_file(source_v2).expect("cleanup");
    }

    #[test]
    fn patch_missing_file_returns_not_found() {
        let archive_path = test_path("axon-test-patch-missing", "axon");
        let source = test_path("axon-source", "txt");
        init_empty_archive(&archive_path, false).expect("init should succeed");
        std::fs::write(&source, b"payload").expect("write source");

        let err = patch_file(&archive_path, "missing.txt", &source).expect_err("must fail");
        assert!(matches!(err, AxonError::NotFound(_)));

        std::fs::remove_file(archive_path).expect("cleanup");
        std::fs::remove_file(source).expect("cleanup");
    }

    #[test]
    fn patch_expected_version_conflict_returns_error() {
        let archive_path = test_path("axon-test-patch-conflict", "axon");
        let source_v1 = test_path("axon-source", "txt");
        let source_v2 = test_path("axon-source", "txt");
        init_empty_archive(&archive_path, false).expect("init should succeed");
        std::fs::write(&source_v1, b"v1").expect("write source");
        std::fs::write(&source_v2, b"v2").expect("write source");

        add_file(&archive_path, "cfg/app.toml", &source_v1).expect("add should succeed");
        let err =
            patch_file_with_expected_version(&archive_path, "cfg/app.toml", &source_v2, Some(7))
                .expect_err("patch must fail");
        assert!(matches!(err, AxonError::Conflict(_)));

        std::fs::remove_file(archive_path).expect("cleanup");
        std::fs::remove_file(source_v1).expect("cleanup");
        std::fs::remove_file(source_v2).expect("cleanup");
    }

    #[test]
    fn remove_marks_tombstone_and_blocks_reads() {
        let archive_path = test_path("axon-test-remove", "axon");
        let source_v1 = test_path("axon-source", "txt");
        let source_v2 = test_path("axon-source", "txt");
        init_empty_archive(&archive_path, false).expect("init should succeed");
        std::fs::write(&source_v1, b"v1").expect("write source");
        std::fs::write(&source_v2, b"v2").expect("write source");

        add_file(&archive_path, "cfg/app.toml", &source_v1).expect("add should succeed");
        patch_file(&archive_path, "cfg/app.toml", &source_v2).expect("patch should succeed");
        remove_file(&archive_path, "cfg/app.toml").expect("remove should succeed");

        let err = read_file(&archive_path, "cfg/app.toml").expect_err("read should fail");
        assert!(matches!(err, AxonError::NotFound(_)));

        let manifest = read_root_manifest(&archive_path).expect("manifest");
        let entry = manifest
            .files
            .iter()
            .find(|entry| entry.path == "cfg/app.toml")
            .expect("entry");
        assert!(entry.tombstoned);
        assert_eq!(entry.version, 3);
        assert_eq!(entry.history_block_ids.len(), 2);
        assert_eq!(manifest.total_file_count, 1);

        std::fs::remove_file(archive_path).expect("cleanup");
        std::fs::remove_file(source_v1).expect("cleanup");
        std::fs::remove_file(source_v2).expect("cleanup");
    }

    #[test]
    fn remove_missing_file_returns_not_found() {
        let archive_path = test_path("axon-test-remove-missing", "axon");
        init_empty_archive(&archive_path, false).expect("init should succeed");

        let err = remove_file(&archive_path, "missing.txt").expect_err("must fail");
        assert!(matches!(err, AxonError::NotFound(_)));

        std::fs::remove_file(archive_path).expect("cleanup");
    }

    #[test]
    fn remove_expected_version_conflict_returns_error() {
        let archive_path = test_path("axon-test-remove-conflict", "axon");
        let source = test_path("axon-source", "txt");
        init_empty_archive(&archive_path, false).expect("init should succeed");
        std::fs::write(&source, b"v1").expect("write source");
        add_file(&archive_path, "cfg/app.toml", &source).expect("add should succeed");

        let err = remove_file_with_expected_version(&archive_path, "cfg/app.toml", Some(2))
            .expect_err("remove must fail");
        assert!(matches!(err, AxonError::Conflict(_)));

        std::fs::remove_file(archive_path).expect("cleanup");
        std::fs::remove_file(source).expect("cleanup");
    }

    #[test]
    fn stale_manifest_pointer_returns_error() {
        let archive_path = test_path("axon-test-stale-manifest", "axon");
        init_empty_archive(&archive_path, false).expect("init should succeed");

        rewrite_header(&archive_path, |header| {
            header.root_manifest_offset = u64::MAX - 8;
            header.root_manifest_size = 64;
        });

        let err = read_root_manifest(&archive_path).expect_err("manifest read should fail");
        assert!(matches!(err, AxonError::Io(_)));

        std::fs::remove_file(archive_path).expect("cleanup");
    }

    #[test]
    fn stale_block_index_pointer_breaks_read() {
        let archive_path = test_path("axon-test-stale-index", "axon");
        let source = test_path("axon-source", "txt");
        init_empty_archive(&archive_path, false).expect("init should succeed");
        std::fs::write(&source, b"payload").expect("write source");
        add_file(&archive_path, "a.txt", &source).expect("add should succeed");

        rewrite_header(&archive_path, |header| {
            header.block_index_offset = u64::MAX - 16;
            header.block_index_count = 1;
        });

        let err = read_file(&archive_path, "a.txt").expect_err("read should fail");
        assert!(matches!(err, AxonError::Io(_)));

        std::fs::remove_file(archive_path).expect("cleanup");
        std::fs::remove_file(source).expect("cleanup");
    }

    #[test]
    fn stale_manifest_size_returns_error() {
        let archive_path = test_path("axon-test-stale-manifest-size", "axon");
        init_empty_archive(&archive_path, false).expect("init should succeed");

        rewrite_header(&archive_path, |header| {
            header.root_manifest_size = u32::MAX;
        });

        let err = read_root_manifest(&archive_path).expect_err("manifest read should fail");
        assert!(matches!(err, AxonError::Io(_)));

        std::fs::remove_file(archive_path).expect("cleanup");
    }

    #[test]
    fn search_filters_tombstoned_entries_by_default() {
        let archive_path = test_path("axon-test-search", "axon");
        let source_a = test_path("axon-source", "txt");
        let source_b = test_path("axon-source", "txt");
        init_empty_archive(&archive_path, false).expect("init should succeed");
        std::fs::write(&source_a, b"a").expect("write source");
        std::fs::write(&source_b, b"b").expect("write source");
        add_file(&archive_path, "docs/a.txt", &source_a).expect("add a");
        add_file(&archive_path, "docs/b.txt", &source_b).expect("add b");
        remove_file(&archive_path, "docs/b.txt").expect("remove b");

        let active = search_files(&archive_path, "docs/", false).expect("search active");
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].path, "docs/a.txt");

        let all = search_files(&archive_path, "docs/", true).expect("search all");
        assert_eq!(all.len(), 2);

        std::fs::remove_file(archive_path).expect("cleanup");
        std::fs::remove_file(source_a).expect("cleanup");
        std::fs::remove_file(source_b).expect("cleanup");
    }

    #[test]
    fn search_loads_entries_from_shard_manifests() {
        let archive_path = test_path("axon-test-search-shard", "axon");
        let source = test_path("axon-source", "txt");
        init_empty_archive(&archive_path, false).expect("init should succeed");
        std::fs::write(&source, b"root").expect("write source");
        add_file(&archive_path, "root.txt", &source).expect("add root");

        let header = read_header(&archive_path).expect("header");
        let mut root = read_root_manifest(&archive_path).expect("manifest");
        let shard = ShardManifest {
            files: vec![FileEntry {
                path: "shards/data.txt".to_string(),
                block_id_hex: "00".repeat(32),
                raw_size: 0,
                stored_size: 0,
                algo: ALGO_NONE,
                version: 1,
                history_block_ids: Vec::new(),
                tombstoned: false,
            }],
        };
        let shard_bytes = serde_json::to_vec(&shard).expect("serialize shard");

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&archive_path)
            .expect("open archive");
        let mut end = file.seek(SeekFrom::End(0)).expect("seek end");
        let shard_offset = end;
        file.write_all(&shard_bytes).expect("write shard");
        end += shard_bytes.len() as u64;

        root.shard_descriptors.push(ShardDescriptor {
            shard_id: 0,
            shard_offset,
            shard_size: u32::try_from(shard_bytes.len()).expect("u32 size"),
            compressed: false,
        });
        let root_bytes = serde_json::to_vec(&root).expect("serialize root");
        let root_offset = end;
        file.write_all(&root_bytes).expect("write root");

        rewrite_header(&archive_path, |current| {
            current.root_manifest_offset = root_offset;
            current.root_manifest_size = u32::try_from(root_bytes.len()).expect("u32 size");
            current.block_index_offset = header.block_index_offset;
            current.block_index_count = header.block_index_count;
        });

        let matches = search_files(&archive_path, "shards/", false).expect("search");
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].path, "shards/data.txt");

        std::fs::remove_file(archive_path).expect("cleanup");
        std::fs::remove_file(source).expect("cleanup");
    }

    #[test]
    fn search_rejects_compressed_shard_descriptor() {
        let archive_path = test_path("axon-test-search-shard-compressed", "axon");
        init_empty_archive(&archive_path, false).expect("init should succeed");
        let mut root = read_root_manifest(&archive_path).expect("manifest");
        root.shard_descriptors.push(ShardDescriptor {
            shard_id: 0,
            shard_offset: 0,
            shard_size: 0,
            compressed: true,
        });

        let root_bytes = serde_json::to_vec(&root).expect("serialize root");
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&archive_path)
            .expect("open archive");
        let root_offset = file.seek(SeekFrom::End(0)).expect("seek");
        file.write_all(&root_bytes).expect("write root");
        rewrite_header(&archive_path, |header| {
            header.root_manifest_offset = root_offset;
            header.root_manifest_size = u32::try_from(root_bytes.len()).expect("u32 size");
        });

        let err = search_files(&archive_path, "", false).expect_err("search should fail");
        assert!(matches!(err, AxonError::Unsupported(_)));

        std::fs::remove_file(archive_path).expect("cleanup");
    }

    #[test]
    fn list_applies_prefix_and_pagination() {
        let archive_path = test_path("axon-test-list", "axon");
        let a = test_path("axon-source", "txt");
        let b = test_path("axon-source", "txt");
        let c = test_path("axon-source", "txt");
        init_empty_archive(&archive_path, false).expect("init should succeed");
        std::fs::write(&a, b"a").expect("write source");
        std::fs::write(&b, b"b").expect("write source");
        std::fs::write(&c, b"c").expect("write source");

        add_file(&archive_path, "docs/a.txt", &a).expect("add a");
        add_file(&archive_path, "docs/b.txt", &b).expect("add b");
        add_file(&archive_path, "src/main.rs", &c).expect("add c");
        remove_file(&archive_path, "docs/b.txt").expect("remove b");

        let page = list_files(&archive_path, "docs/", false, 0, Some(10)).expect("list docs");
        assert_eq!(page.len(), 1);
        assert_eq!(page[0].path, "docs/a.txt");

        let all_docs = list_files(&archive_path, "docs/", true, 0, Some(10)).expect("list all");
        assert_eq!(all_docs.len(), 2);

        let paged = list_files(&archive_path, "", true, 1, Some(1)).expect("paged");
        assert_eq!(paged.len(), 1);
        assert_eq!(paged[0].path, "docs/b.txt");

        std::fs::remove_file(archive_path).expect("cleanup");
        std::fs::remove_file(a).expect("cleanup");
        std::fs::remove_file(b).expect("cleanup");
        std::fs::remove_file(c).expect("cleanup");
    }

    #[test]
    fn commit_routes_large_file_sets_into_shards() {
        let archive_path = test_path("axon-test-route-shards", "axon");
        init_empty_archive(&archive_path, false).expect("init should succeed");

        let mut sources = Vec::new();
        for i in 0..10 {
            let source = test_path("axon-source", "txt");
            std::fs::write(&source, format!("payload-{i}")).expect("write source");
            add_file(&archive_path, &format!("docs/file-{i}.txt"), &source).expect("add");
            sources.push(source);
        }

        let root = read_root_manifest(&archive_path).expect("root");
        assert_eq!(root.total_file_count, 10);
        assert!(root.format_flags & FLAG_SHARDED_LAYOUT != 0);
        assert!(!root.shard_descriptors.is_empty());
        assert!(root.files.is_empty());

        let bytes = read_file(&archive_path, "docs/file-7.txt").expect("read shard file");
        assert_eq!(bytes, b"payload-7".to_vec());

        std::fs::remove_file(archive_path).expect("cleanup");
        for source in sources {
            std::fs::remove_file(source).expect("cleanup");
        }
    }

    #[test]
    fn wal_status_tracks_mutation_entries() {
        let archive_path = test_path("axon-test-wal-status", "axon");
        let a = test_path("axon-source", "txt");
        let b = test_path("axon-source", "txt");
        init_empty_archive(&archive_path, false).expect("init should succeed");
        std::fs::write(&a, b"a").expect("write source");
        std::fs::write(&b, b"b").expect("write source");

        add_file(&archive_path, "docs/a.txt", &a).expect("add");
        patch_file(&archive_path, "docs/a.txt", &b).expect("patch");
        remove_file(&archive_path, "docs/a.txt").expect("remove");

        let status = wal_status(&archive_path).expect("wal status");
        assert_eq!(status.entry_count, 3);
        assert!(status.wal_size > 0);

        std::fs::remove_file(archive_path).expect("cleanup");
        std::fs::remove_file(a).expect("cleanup");
        std::fs::remove_file(b).expect("cleanup");
    }

    #[test]
    fn verify_succeeds_for_valid_archive() {
        let archive_path = test_path("axon-test-verify", "axon");
        let source = test_path("axon-source", "txt");
        init_empty_archive(&archive_path, false).expect("init should succeed");
        std::fs::write(&source, b"payload").expect("write source");
        add_file(&archive_path, "docs/a.txt", &source).expect("add");

        let report = verify_archive(&archive_path).expect("verify");
        assert!(report.ok);
        assert_eq!(report.total_files, 1);

        std::fs::remove_file(archive_path).expect("cleanup");
        std::fs::remove_file(source).expect("cleanup");
    }

    #[test]
    fn verify_detects_manifest_out_of_bounds() {
        let archive_path = test_path("axon-test-verify-bounds", "axon");
        init_empty_archive(&archive_path, false).expect("init should succeed");
        rewrite_header(&archive_path, |header| {
            header.root_manifest_offset = u64::MAX - 1;
            header.root_manifest_size = 8;
        });

        let err = verify_archive(&archive_path).expect_err("verify should fail");
        assert!(matches!(err, AxonError::InvalidArchive(_)));

        std::fs::remove_file(archive_path).expect("cleanup");
    }

    #[test]
    fn gc_checkpoint_compacts_wal_and_preserves_reads() {
        let archive_path = test_path("axon-test-gc", "axon");
        let a = test_path("axon-source", "txt");
        let b = test_path("axon-source", "txt");
        init_empty_archive(&archive_path, false).expect("init should succeed");
        std::fs::write(&a, b"a").expect("write source");
        std::fs::write(&b, b"b").expect("write source");
        add_file(&archive_path, "docs/a.txt", &a).expect("add");
        patch_file(&archive_path, "docs/a.txt", &b).expect("patch");

        let before = wal_status(&archive_path).expect("status before");
        assert!(before.entry_count >= 2);

        let report = gc_checkpoint(&archive_path).expect("gc");
        assert!(report.ok);
        assert!(report.wal_entries_compacted >= 2);
        assert_eq!(
            read_file(&archive_path, "docs/a.txt").expect("read"),
            b"b".to_vec()
        );

        let after = wal_status(&archive_path).expect("status after");
        assert_eq!(after.entry_count, 0);

        std::fs::remove_file(archive_path).expect("cleanup");
        std::fs::remove_file(a).expect("cleanup");
        std::fs::remove_file(b).expect("cleanup");
    }

    #[test]
    fn wal_replay_recovers_from_stale_manifest_pointer() {
        let archive_path = test_path("axon-test-wal-replay", "axon");
        let source = test_path("axon-source", "txt");
        init_empty_archive(&archive_path, false).expect("init should succeed");
        let init_header = read_header(&archive_path).expect("init header");
        std::fs::write(&source, b"payload").expect("write source");
        add_file(&archive_path, "docs/a.txt", &source).expect("add");

        rewrite_header(&archive_path, |header| {
            header.root_manifest_offset = init_header.root_manifest_offset;
            header.root_manifest_size = init_header.root_manifest_size;
        });

        let bytes = read_file(&archive_path, "docs/a.txt").expect("read via wal replay");
        assert_eq!(bytes, b"payload".to_vec());

        std::fs::remove_file(archive_path).expect("cleanup");
        std::fs::remove_file(source).expect("cleanup");
    }

    #[test]
    fn read_root_manifest_accepts_patch_only_wal_history() {
        let archive_path = test_path("axon-test-root-manifest-patch-only-wal", "axon");
        let source_v1 = test_path("axon-source", "txt");
        let source_v2 = test_path("axon-source", "txt");
        init_empty_archive(&archive_path, false).expect("init should succeed");
        std::fs::write(&source_v1, b"v1").expect("write source");
        std::fs::write(&source_v2, b"v2").expect("write source");
        add_file(&archive_path, "docs/a.txt", &source_v1).expect("add");
        patch_file(&archive_path, "docs/a.txt", &source_v2).expect("patch");

        let header = read_header(&archive_path).expect("header");
        let wal_entries = read_wal_entries(&archive_path, &header).expect("wal");
        let patch_only = vec![wal_entries[1].clone()];
        let wal_bytes = encode_wal(&patch_only).expect("encode wal");

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&archive_path)
            .expect("open archive");
        let wal_offset = file.seek(SeekFrom::End(0)).expect("seek end");
        file.write_all(&wal_bytes).expect("write wal");
        file.flush().expect("flush wal");

        rewrite_header(&archive_path, |current| {
            current.wal_offset = wal_offset;
            current.wal_size = wal_bytes.len() as u64;
        });

        let root = read_root_manifest(&archive_path).expect("read root manifest");
        assert_eq!(root.total_file_count, 1);
        assert_eq!(root.files[0].path, "docs/a.txt");

        std::fs::remove_file(archive_path).expect("cleanup");
        std::fs::remove_file(source_v1).expect("cleanup");
        std::fs::remove_file(source_v2).expect("cleanup");
    }

    #[test]
    fn batch_mutations_are_atomic_on_conflict() {
        let archive_path = test_path("axon-test-batch-conflict", "axon");
        let a = test_path("axon-source", "txt");
        let b = test_path("axon-source", "txt");
        init_empty_archive(&archive_path, false).expect("init should succeed");
        std::fs::write(&a, b"a").expect("write source");
        std::fs::write(&b, b"b").expect("write source");
        add_file(&archive_path, "docs/a.txt", &a).expect("seed add");

        let err = apply_batch_mutations(
            &archive_path,
            &[
                BatchMutation::Patch {
                    path: "docs/a.txt".to_string(),
                    source: b.clone(),
                    expected_version: Some(99),
                },
                BatchMutation::Add {
                    path: "docs/new.txt".to_string(),
                    source: b.clone(),
                },
            ],
        )
        .expect_err("batch should fail");
        assert!(matches!(err, AxonError::Conflict(_)));
        assert!(read_file(&archive_path, "docs/new.txt").is_err());
        assert_eq!(
            read_file(&archive_path, "docs/a.txt").expect("read"),
            b"a".to_vec()
        );

        std::fs::remove_file(archive_path).expect("cleanup");
        std::fs::remove_file(a).expect("cleanup");
        std::fs::remove_file(b).expect("cleanup");
    }

    #[test]
    fn batch_mutations_apply_in_single_commit() {
        let archive_path = test_path("axon-test-batch-ok", "axon");
        let a = test_path("axon-source", "txt");
        let b = test_path("axon-source", "txt");
        init_empty_archive(&archive_path, false).expect("init should succeed");
        std::fs::write(&a, b"a").expect("write source");
        std::fs::write(&b, b"b").expect("write source");
        add_file(&archive_path, "docs/a.txt", &a).expect("seed add");

        let applied = apply_batch_mutations(
            &archive_path,
            &[
                BatchMutation::Patch {
                    path: "docs/a.txt".to_string(),
                    source: b.clone(),
                    expected_version: Some(1),
                },
                BatchMutation::Add {
                    path: "docs/new.txt".to_string(),
                    source: b.clone(),
                },
                BatchMutation::Remove {
                    path: "docs/a.txt".to_string(),
                    expected_version: Some(2),
                },
            ],
        )
        .expect("batch should succeed");
        assert_eq!(applied, 3);

        assert_eq!(
            read_file(&archive_path, "docs/new.txt").expect("read"),
            b"b".to_vec()
        );
        assert!(matches!(
            read_file(&archive_path, "docs/a.txt").expect_err("removed"),
            AxonError::NotFound(_)
        ));

        let status = wal_status(&archive_path).expect("wal");
        assert!(status.entry_count >= 4);

        std::fs::remove_file(archive_path).expect("cleanup");
        std::fs::remove_file(a).expect("cleanup");
        std::fs::remove_file(b).expect("cleanup");
    }
}
