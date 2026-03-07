use crate::{AxonError, Result};
use crc32fast::Hasher;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub const MAGIC: [u8; 4] = *b"AXON";
pub const HEADER_SIZE: usize = 128;
pub const VERSION_MAJOR: u16 = 0;
pub const VERSION_MINOR: u16 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Header {
    pub magic: [u8; 4],
    pub version_major: u16,
    pub version_minor: u16,
    pub root_manifest_offset: u64,
    pub root_manifest_size: u32,
    pub shard_region_offset: u64,
    pub lock_table_offset: u64,
    pub wal_offset: u64,
    pub wal_size: u64,
    pub block_index_offset: u64,
    pub block_index_count: u64,
    pub data_region_offset: u64,
    pub total_files: u64,
    pub target_shard_size: u32,
    pub min_shard_size: u32,
    pub created_at: u64,
    pub header_checksum: u32,
    pub reserved: [u8; 24],
}

impl Header {
    pub fn new_empty(root_manifest_size: u32) -> Self {
        let root_manifest_offset = HEADER_SIZE as u64;
        let next_region_offset = root_manifest_offset + u64::from(root_manifest_size);

        Self {
            magic: MAGIC,
            version_major: VERSION_MAJOR,
            version_minor: VERSION_MINOR,
            root_manifest_offset,
            root_manifest_size,
            shard_region_offset: next_region_offset,
            lock_table_offset: next_region_offset,
            wal_offset: next_region_offset,
            wal_size: 0,
            block_index_offset: next_region_offset,
            block_index_count: 0,
            data_region_offset: next_region_offset,
            total_files: 0,
            target_shard_size: 64 * 1024,
            min_shard_size: 64 * 1024,
            created_at: now_unix_secs(),
            header_checksum: 0,
            reserved: [0; 24],
        }
    }

    pub fn encode(&self) -> [u8; HEADER_SIZE] {
        let mut buf = [0u8; HEADER_SIZE];
        buf[0..4].copy_from_slice(&self.magic);
        write_u16(&mut buf, 4, self.version_major);
        write_u16(&mut buf, 6, self.version_minor);
        write_u64(&mut buf, 8, self.root_manifest_offset);
        write_u32(&mut buf, 16, self.root_manifest_size);
        write_u64(&mut buf, 20, self.shard_region_offset);
        write_u64(&mut buf, 28, self.lock_table_offset);
        write_u64(&mut buf, 36, self.wal_offset);
        write_u64(&mut buf, 44, self.wal_size);
        write_u64(&mut buf, 52, self.block_index_offset);
        write_u64(&mut buf, 60, self.block_index_count);
        write_u64(&mut buf, 68, self.data_region_offset);
        write_u64(&mut buf, 76, self.total_files);
        write_u32(&mut buf, 84, self.target_shard_size);
        write_u32(&mut buf, 88, self.min_shard_size);
        write_u64(&mut buf, 92, self.created_at);
        write_u32(&mut buf, 100, 0);
        buf[104..128].copy_from_slice(&self.reserved);

        let checksum = crc32(&buf[..100]);
        write_u32(&mut buf, 100, checksum);
        buf
    }

    pub fn decode(bytes: [u8; HEADER_SIZE]) -> Result<Self> {
        if bytes[0..4] != MAGIC {
            return Err(AxonError::InvalidArchive("invalid magic"));
        }

        let expected = read_u32(&bytes, 100);
        let actual = crc32(&bytes[..100]);
        if expected != actual {
            return Err(AxonError::InvalidArchive("header checksum mismatch"));
        }

        Ok(Self {
            magic: [bytes[0], bytes[1], bytes[2], bytes[3]],
            version_major: read_u16(&bytes, 4),
            version_minor: read_u16(&bytes, 6),
            root_manifest_offset: read_u64(&bytes, 8),
            root_manifest_size: read_u32(&bytes, 16),
            shard_region_offset: read_u64(&bytes, 20),
            lock_table_offset: read_u64(&bytes, 28),
            wal_offset: read_u64(&bytes, 36),
            wal_size: read_u64(&bytes, 44),
            block_index_offset: read_u64(&bytes, 52),
            block_index_count: read_u64(&bytes, 60),
            data_region_offset: read_u64(&bytes, 68),
            total_files: read_u64(&bytes, 76),
            target_shard_size: read_u32(&bytes, 84),
            min_shard_size: read_u32(&bytes, 88),
            created_at: read_u64(&bytes, 92),
            header_checksum: expected,
            reserved: bytes[104..128].try_into().expect("slice is fixed size"),
        })
    }
}

fn now_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_secs()
}

fn crc32(bytes: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(bytes);
    hasher.finalize()
}

fn write_u16(buf: &mut [u8], offset: usize, value: u16) {
    buf[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
}

fn write_u32(buf: &mut [u8], offset: usize, value: u32) {
    buf[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

fn write_u64(buf: &mut [u8], offset: usize, value: u64) {
    buf[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}

fn read_u16(buf: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes(buf[offset..offset + 2].try_into().expect("slice size"))
}

fn read_u32(buf: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(buf[offset..offset + 4].try_into().expect("slice size"))
}

fn read_u64(buf: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(buf[offset..offset + 8].try_into().expect("slice size"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_round_trip() {
        let mut header = Header::new_empty(48);
        header.total_files = 13;
        header.wal_size = 42;
        let encoded = header.encode();
        let decoded = Header::decode(encoded).expect("decode should work");

        assert_eq!(decoded.magic, MAGIC);
        assert_eq!(decoded.version_major, VERSION_MAJOR);
        assert_eq!(decoded.version_minor, VERSION_MINOR);
        assert_eq!(decoded.root_manifest_offset, HEADER_SIZE as u64);
        assert_eq!(decoded.root_manifest_size, 48);
        assert_eq!(decoded.total_files, 13);
        assert_eq!(decoded.wal_size, 42);
    }

    #[test]
    fn checksum_is_validated() {
        let header = Header::new_empty(8);
        let mut bytes = header.encode();
        bytes[12] ^= 0xFF;
        let err = Header::decode(bytes).expect_err("decode should fail");
        assert!(matches!(
            err,
            AxonError::InvalidArchive("header checksum mismatch")
        ));
    }

    #[test]
    fn magic_is_validated() {
        let header = Header::new_empty(8);
        let mut bytes = header.encode();
        bytes[0] = b'Z';
        let err = Header::decode(bytes).expect_err("decode should fail");
        assert!(matches!(err, AxonError::InvalidArchive("invalid magic")));
    }
}
