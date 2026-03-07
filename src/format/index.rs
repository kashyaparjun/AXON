use crate::{AxonError, Result};

pub const BLOCK_INDEX_ENTRY_SIZE: usize = 44;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct BlockIndexEntry {
    pub block_id: [u8; 32],
    pub offset: u64,
    pub stored_size: u32,
}

impl BlockIndexEntry {
    pub fn encode(&self) -> [u8; BLOCK_INDEX_ENTRY_SIZE] {
        let mut buf = [0u8; BLOCK_INDEX_ENTRY_SIZE];
        buf[0..32].copy_from_slice(&self.block_id);
        buf[32..40].copy_from_slice(&self.offset.to_le_bytes());
        buf[40..44].copy_from_slice(&self.stored_size.to_le_bytes());
        buf
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != BLOCK_INDEX_ENTRY_SIZE {
            return Err(AxonError::InvalidArchive("invalid index entry length"));
        }
        Ok(Self {
            block_id: bytes[0..32]
                .try_into()
                .map_err(|_| AxonError::InvalidArchive("invalid index block id"))?,
            offset: u64::from_le_bytes(
                bytes[32..40]
                    .try_into()
                    .map_err(|_| AxonError::InvalidArchive("invalid index offset"))?,
            ),
            stored_size: u32::from_le_bytes(
                bytes[40..44]
                    .try_into()
                    .map_err(|_| AxonError::InvalidArchive("invalid index stored size"))?,
            ),
        })
    }
}

pub fn find_block<'a>(
    entries: &'a [BlockIndexEntry],
    block_id: &[u8; 32],
) -> Option<&'a BlockIndexEntry> {
    entries
        .binary_search_by(|entry| entry.block_id.cmp(block_id))
        .ok()
        .map(|idx| &entries[idx])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn index_round_trip() {
        let entry = BlockIndexEntry {
            block_id: [7; 32],
            offset: 1024,
            stored_size: 55,
        };
        let decoded = BlockIndexEntry::decode(&entry.encode()).expect("decode");
        assert_eq!(decoded, entry);
    }

    #[test]
    fn binary_search_works() {
        let entries = vec![
            BlockIndexEntry {
                block_id: [1; 32],
                offset: 1,
                stored_size: 1,
            },
            BlockIndexEntry {
                block_id: [2; 32],
                offset: 2,
                stored_size: 2,
            },
        ];
        assert_eq!(find_block(&entries, &[2; 32]).expect("exists").offset, 2);
        assert!(find_block(&entries, &[3; 32]).is_none());
    }
}
