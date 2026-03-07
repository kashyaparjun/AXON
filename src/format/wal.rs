use crate::{AxonError, Result};

const WAL_MAGIC: [u8; 4] = *b"AXWL";
const WAL_VERSION: u16 = 1;

pub const OP_ADD: u8 = 1;
pub const OP_PATCH: u8 = 2;
pub const OP_REMOVE: u8 = 3;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalEntry {
    pub op: u8,
    pub path: String,
    pub expected_version: Option<u32>,
    pub resulting_version: u32,
    pub block_id: [u8; 32],
    pub raw_size: u32,
    pub stored_size: u32,
    pub algo: u8,
    pub tombstoned: bool,
}

pub fn encode_wal(entries: &[WalEntry]) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    out.extend_from_slice(&WAL_MAGIC);
    out.extend_from_slice(&WAL_VERSION.to_le_bytes());
    out.extend_from_slice(
        &u32::try_from(entries.len())
            .map_err(|_| AxonError::InvalidArchive("too many wal entries"))?
            .to_le_bytes(),
    );

    for entry in entries {
        out.push(entry.op);
        out.push(if entry.tombstoned { 1 } else { 0 });
        out.extend_from_slice(&[0u8; 2]);
        out.extend_from_slice(&entry.resulting_version.to_le_bytes());
        out.extend_from_slice(&entry.expected_version.unwrap_or(u32::MAX).to_le_bytes());
        out.extend_from_slice(&entry.block_id);
        out.extend_from_slice(&entry.raw_size.to_le_bytes());
        out.extend_from_slice(&entry.stored_size.to_le_bytes());
        out.push(entry.algo);
        out.extend_from_slice(&[0u8; 3]);
        let path = entry.path.as_bytes();
        out.extend_from_slice(
            &u16::try_from(path.len())
                .map_err(|_| AxonError::InvalidArchive("wal path too long"))?
                .to_le_bytes(),
        );
        out.extend_from_slice(path);
    }
    Ok(out)
}

pub fn decode_wal(bytes: &[u8]) -> Result<Vec<WalEntry>> {
    let mut c = Cursor::new(bytes);
    if c.take(4)? != WAL_MAGIC {
        return Err(AxonError::InvalidArchive("invalid WAL magic"));
    }
    let version = c.u16()?;
    if version != WAL_VERSION {
        return Err(AxonError::Unsupported("unsupported WAL version"));
    }
    let count = c.u32()? as usize;
    let mut entries = Vec::with_capacity(count);
    for _ in 0..count {
        let op = c.u8()?;
        let tombstoned = c.u8()? != 0;
        c.take(2)?;
        let resulting_version = c.u32()?;
        let expected = c.u32()?;
        let expected_version = if expected == u32::MAX {
            None
        } else {
            Some(expected)
        };
        let block_id: [u8; 32] = c
            .take(32)?
            .try_into()
            .map_err(|_| AxonError::InvalidArchive("invalid WAL block id"))?;
        let raw_size = c.u32()?;
        let stored_size = c.u32()?;
        let algo = c.u8()?;
        c.take(3)?;
        let path_len = c.u16()? as usize;
        let path = String::from_utf8(c.take(path_len)?.to_vec())
            .map_err(|_| AxonError::InvalidArchive("invalid WAL path utf-8"))?;
        entries.push(WalEntry {
            op,
            path,
            expected_version,
            resulting_version,
            block_id,
            raw_size,
            stored_size,
            algo,
            tombstoned,
        });
    }

    if c.remaining() != 0 {
        return Err(AxonError::InvalidArchive("WAL trailing bytes"));
    }
    Ok(entries)
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
            return Err(AxonError::InvalidArchive("WAL truncated"));
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
            .map_err(|_| AxonError::InvalidArchive("invalid WAL u16"))?;
        Ok(u16::from_le_bytes(bytes))
    }

    fn u32(&mut self) -> Result<u32> {
        let bytes: [u8; 4] = self
            .take(4)?
            .try_into()
            .map_err(|_| AxonError::InvalidArchive("invalid WAL u32"))?;
        Ok(u32::from_le_bytes(bytes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wal_round_trip() {
        let entry = WalEntry {
            op: OP_PATCH,
            path: "cfg/app.toml".to_string(),
            expected_version: Some(2),
            resulting_version: 3,
            block_id: [7; 32],
            raw_size: 5,
            stored_size: 5,
            algo: 3,
            tombstoned: false,
        };
        let bytes = encode_wal(std::slice::from_ref(&entry)).expect("encode");
        let decoded = decode_wal(&bytes).expect("decode");
        assert_eq!(decoded, vec![entry]);
    }
}
