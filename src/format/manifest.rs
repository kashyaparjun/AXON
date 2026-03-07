use crate::manifest::{FileEntry, RootManifest, ShardDescriptor, ShardManifest};
use crate::{AxonError, Result};

const ROOT_MAGIC: [u8; 4] = *b"AXRM";
const SHARD_MAGIC: [u8; 4] = *b"AXSM";
const MANIFEST_VERSION: u16 = 1;
const KIND_ROOT: u8 = 1;
const KIND_SHARD: u8 = 2;

pub const FLAG_BINARY_MANIFESTS: u64 = 1 << 0;
pub const FLAG_SHARDED_LAYOUT: u64 = 1 << 1;

pub fn encode_root_manifest(manifest: &RootManifest) -> Result<Vec<u8>> {
    encode_manifest_common(
        ROOT_MAGIC,
        KIND_ROOT,
        manifest.format_flags,
        manifest.total_file_count,
        &manifest.shard_descriptors,
        &manifest.files,
    )
}

pub fn decode_root_manifest(bytes: &[u8]) -> Result<RootManifest> {
    let decoded = decode_manifest_common(bytes, ROOT_MAGIC, KIND_ROOT)?;
    Ok(RootManifest {
        shard_descriptors: decoded.shards,
        total_file_count: decoded.total_file_count,
        format_flags: decoded.format_flags,
        files: decoded.files,
    })
}

pub fn encode_shard_manifest(manifest: &ShardManifest) -> Result<Vec<u8>> {
    encode_manifest_common(
        SHARD_MAGIC,
        KIND_SHARD,
        FLAG_BINARY_MANIFESTS,
        manifest.files.len() as u64,
        &[],
        &manifest.files,
    )
}

pub fn decode_shard_manifest(bytes: &[u8]) -> Result<ShardManifest> {
    let decoded = decode_manifest_common(bytes, SHARD_MAGIC, KIND_SHARD)?;
    Ok(ShardManifest {
        files: decoded.files,
    })
}

pub fn is_binary_root_manifest(bytes: &[u8]) -> bool {
    bytes.len() >= 4 && bytes[0..4] == ROOT_MAGIC
}

pub fn is_binary_shard_manifest(bytes: &[u8]) -> bool {
    bytes.len() >= 4 && bytes[0..4] == SHARD_MAGIC
}

struct DecodedManifest {
    format_flags: u64,
    total_file_count: u64,
    shards: Vec<ShardDescriptor>,
    files: Vec<FileEntry>,
}

fn encode_manifest_common(
    magic: [u8; 4],
    kind: u8,
    format_flags: u64,
    total_file_count: u64,
    shards: &[ShardDescriptor],
    files: &[FileEntry],
) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    out.extend_from_slice(&magic);
    push_u16(&mut out, MANIFEST_VERSION);
    out.push(kind);
    out.push(0);
    push_u64(&mut out, format_flags);
    push_u64(&mut out, total_file_count);
    push_u32(
        &mut out,
        u32::try_from(shards.len()).map_err(|_| AxonError::InvalidArchive("too many shards"))?,
    );
    push_u32(
        &mut out,
        u32::try_from(files.len()).map_err(|_| AxonError::InvalidArchive("too many files"))?,
    );

    for shard in shards {
        push_u32(&mut out, shard.shard_id);
        push_u64(&mut out, shard.shard_offset);
        push_u32(&mut out, shard.shard_size);
        out.push(if shard.compressed { 1 } else { 0 });
        out.extend_from_slice(&[0u8; 3]);
    }

    for file in files {
        encode_file_entry(&mut out, file)?;
    }

    Ok(out)
}

fn decode_manifest_common(bytes: &[u8], magic: [u8; 4], kind: u8) -> Result<DecodedManifest> {
    let mut c = Cursor::new(bytes);
    if c.take(4)? != magic {
        return Err(AxonError::InvalidArchive("invalid manifest magic"));
    }
    let version = c.u16()?;
    if version != MANIFEST_VERSION {
        return Err(AxonError::Unsupported("unsupported manifest version"));
    }
    let found_kind = c.u8()?;
    if found_kind != kind {
        return Err(AxonError::InvalidArchive("manifest kind mismatch"));
    }
    c.u8()?; // reserved

    let format_flags = c.u64()?;
    let total_file_count = c.u64()?;
    let shard_count = c.u32()? as usize;
    let file_count = c.u32()? as usize;

    let mut shards = Vec::with_capacity(shard_count);
    for _ in 0..shard_count {
        let shard_id = c.u32()?;
        let shard_offset = c.u64()?;
        let shard_size = c.u32()?;
        let compressed = c.u8()? != 0;
        c.take(3)?; // reserved
        shards.push(ShardDescriptor {
            shard_id,
            shard_offset,
            shard_size,
            compressed,
        });
    }

    let mut files = Vec::with_capacity(file_count);
    for _ in 0..file_count {
        files.push(decode_file_entry(&mut c)?);
    }

    if c.remaining() != 0 {
        return Err(AxonError::InvalidArchive("manifest trailing bytes"));
    }

    Ok(DecodedManifest {
        format_flags,
        total_file_count,
        shards,
        files,
    })
}

fn encode_file_entry(out: &mut Vec<u8>, file: &FileEntry) -> Result<()> {
    let path = file.path.as_bytes();
    push_u16(
        out,
        u16::try_from(path.len()).map_err(|_| AxonError::InvalidArchive("file path too long"))?,
    );
    out.extend_from_slice(path);

    let block_id = decode_hex_32(&file.block_id_hex)?;
    out.extend_from_slice(&block_id);
    push_u32(out, file.raw_size);
    push_u32(out, file.stored_size);
    out.push(file.algo);
    out.push(if file.tombstoned { 1 } else { 0 });
    out.extend_from_slice(&[0u8; 2]);
    push_u32(out, file.version);
    push_u16(
        out,
        u16::try_from(file.history_block_ids.len())
            .map_err(|_| AxonError::InvalidArchive("history too long"))?,
    );
    for item in &file.history_block_ids {
        out.extend_from_slice(&decode_hex_32(item)?);
    }
    Ok(())
}

fn decode_file_entry(c: &mut Cursor<'_>) -> Result<FileEntry> {
    let path_len = c.u16()? as usize;
    let path_bytes = c.take(path_len)?;
    let path = String::from_utf8(path_bytes.to_vec())
        .map_err(|_| AxonError::InvalidArchive("invalid utf-8 path"))?;
    let block_id = c.take(32)?;
    let raw_size = c.u32()?;
    let stored_size = c.u32()?;
    let algo = c.u8()?;
    let tombstoned = c.u8()? != 0;
    c.take(2)?;
    let version = c.u32()?;
    let history_count = c.u16()? as usize;
    let mut history = Vec::with_capacity(history_count);
    for _ in 0..history_count {
        history.push(encode_hex(c.take(32)?));
    }

    Ok(FileEntry {
        path,
        block_id_hex: encode_hex(block_id),
        raw_size,
        stored_size,
        algo,
        version,
        history_block_ids: history,
        tombstoned,
    })
}

fn decode_hex_32(value: &str) -> Result<[u8; 32]> {
    if value.len() != 64 {
        return Err(AxonError::InvalidArchive("invalid block id hex length"));
    }
    let mut out = [0u8; 32];
    let b = value.as_bytes();
    for i in 0..32 {
        let hi = hex_val(b[2 * i])?;
        let lo = hex_val(b[2 * i + 1])?;
        out[i] = (hi << 4) | lo;
    }
    Ok(out)
}

fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn hex_val(b: u8) -> Result<u8> {
    match b {
        b'0'..=b'9' => Ok(b - b'0'),
        b'a'..=b'f' => Ok(10 + b - b'a'),
        b'A'..=b'F' => Ok(10 + b - b'A'),
        _ => Err(AxonError::InvalidArchive("invalid hex character")),
    }
}

fn push_u16(out: &mut Vec<u8>, value: u16) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn push_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn push_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

struct Cursor<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.pos)
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.remaining() < n {
            return Err(AxonError::InvalidArchive("manifest truncated"));
        }
        let start = self.pos;
        self.pos += n;
        Ok(&self.bytes[start..self.pos])
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16> {
        let bytes: [u8; 2] = self
            .take(2)?
            .try_into()
            .map_err(|_| AxonError::InvalidArchive("invalid u16"))?;
        Ok(u16::from_le_bytes(bytes))
    }

    fn u32(&mut self) -> Result<u32> {
        let bytes: [u8; 4] = self
            .take(4)?
            .try_into()
            .map_err(|_| AxonError::InvalidArchive("invalid u32"))?;
        Ok(u32::from_le_bytes(bytes))
    }

    fn u64(&mut self) -> Result<u64> {
        let bytes: [u8; 8] = self
            .take(8)?
            .try_into()
            .map_err(|_| AxonError::InvalidArchive("invalid u64"))?;
        Ok(u64::from_le_bytes(bytes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn root_manifest_round_trip() {
        let root = RootManifest {
            shard_descriptors: vec![ShardDescriptor {
                shard_id: 3,
                shard_offset: 123,
                shard_size: 40,
                compressed: false,
            }],
            total_file_count: 1,
            format_flags: FLAG_BINARY_MANIFESTS | FLAG_SHARDED_LAYOUT,
            files: vec![FileEntry {
                path: "a.txt".to_string(),
                block_id_hex: "11".repeat(32),
                raw_size: 10,
                stored_size: 10,
                algo: 3,
                version: 2,
                history_block_ids: vec!["22".repeat(32)],
                tombstoned: true,
            }],
        };
        let decoded = decode_root_manifest(&encode_root_manifest(&root).expect("encode root"))
            .expect("decode root");
        assert_eq!(decoded, root);
    }

    #[test]
    fn shard_manifest_round_trip() {
        let shard = ShardManifest {
            files: vec![FileEntry {
                path: "b.txt".to_string(),
                block_id_hex: "33".repeat(32),
                raw_size: 5,
                stored_size: 5,
                algo: 3,
                version: 1,
                history_block_ids: Vec::new(),
                tombstoned: false,
            }],
        };
        let decoded = decode_shard_manifest(&encode_shard_manifest(&shard).expect("encode shard"))
            .expect("decode shard");
        assert_eq!(decoded, shard);
    }
}
