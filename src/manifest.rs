use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RootManifest {
    pub shard_descriptors: Vec<ShardDescriptor>,
    pub total_file_count: u64,
    pub format_flags: u64,
    pub files: Vec<FileEntry>,
}

impl RootManifest {
    pub fn empty() -> Self {
        Self {
            shard_descriptors: Vec::new(),
            total_file_count: 0,
            format_flags: 0,
            files: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShardDescriptor {
    #[serde(default)]
    pub shard_id: u32,
    pub shard_offset: u64,
    pub shard_size: u32,
    pub compressed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShardManifest {
    pub files: Vec<FileEntry>,
}

impl ShardManifest {
    pub fn empty() -> Self {
        Self { files: Vec::new() }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FileEntry {
    pub path: String,
    pub block_id_hex: String,
    pub raw_size: u32,
    pub stored_size: u32,
    pub algo: u8,
    pub version: u32,
    pub history_block_ids: Vec<String>,
    #[serde(default)]
    pub tombstoned: bool,
}

pub fn route_path_to_shard(path: &str, shard_count: u32) -> Option<u32> {
    if shard_count == 0 {
        return None;
    }

    let hash = blake3::hash(path.as_bytes());
    let mut first = [0u8; 4];
    first.copy_from_slice(&hash.as_bytes()[0..4]);
    Some(u32::from_le_bytes(first) % shard_count)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn route_path_to_shard_is_deterministic() {
        let a = route_path_to_shard("src/main.rs", 16).expect("shard");
        let b = route_path_to_shard("src/main.rs", 16).expect("shard");
        assert_eq!(a, b);
        assert!(a < 16);
    }

    #[test]
    fn route_path_to_shard_handles_zero_shards() {
        assert_eq!(route_path_to_shard("src/main.rs", 0), None);
    }
}
