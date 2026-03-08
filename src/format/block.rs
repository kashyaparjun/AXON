use crate::{AxonError, Result};

pub const BLOCK_TYPE_BASE: u8 = 0;
pub const BLOCK_TYPE_DELTA: u8 = 1;
pub const ALGO_NONE: u8 = 3;
pub const BLOCK_HEADER_SIZE: usize = 84;
const DELTA_MAGIC: [u8; 4] = *b"AXDL";
const DELTA_VERSION: u8 = 1;
const DELTA_OP_COPY: u8 = 0;
const DELTA_OP_INSERT: u8 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockHeader {
    pub block_id: [u8; 32],
    pub block_type: u8,
    pub depth: u8,
    pub algo: u8,
    pub enc_flag: u8,
    pub raw_size: u32,
    pub stored_size: u32,
    pub base_id: [u8; 32],
    pub padding: [u8; 8],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredBlock {
    pub header: BlockHeader,
    pub body: Vec<u8>,
}

impl StoredBlock {
    pub fn from_raw_base(raw: &[u8]) -> Result<Self> {
        let raw_size =
            u32::try_from(raw.len()).map_err(|_| AxonError::InvalidArchive("block too large"))?;
        let block_id = *blake3::hash(raw).as_bytes();
        Ok(Self {
            header: BlockHeader {
                block_id,
                block_type: BLOCK_TYPE_BASE,
                depth: 0,
                algo: ALGO_NONE,
                enc_flag: 0,
                raw_size,
                stored_size: raw_size,
                base_id: [0; 32],
                padding: [0; 8],
            },
            body: raw.to_vec(),
        })
    }

    pub fn from_raw_delta(
        base_raw: &[u8],
        base_block_id: [u8; 32],
        base_depth: u8,
        raw: &[u8],
    ) -> Result<Self> {
        let raw_size =
            u32::try_from(raw.len()).map_err(|_| AxonError::InvalidArchive("block too large"))?;
        let body = encode_delta_body(base_raw, raw)?;
        let stored_size = u32::try_from(body.len())
            .map_err(|_| AxonError::InvalidArchive("delta block too large"))?;

        Ok(Self {
            header: BlockHeader {
                block_id: *blake3::hash(raw).as_bytes(),
                block_type: BLOCK_TYPE_DELTA,
                depth: base_depth.saturating_add(1),
                algo: ALGO_NONE,
                enc_flag: 0,
                raw_size,
                stored_size,
                base_id: base_block_id,
                padding: [0; 8],
            },
            body,
        })
    }

    pub fn encoded_len(&self) -> u64 {
        BLOCK_HEADER_SIZE as u64 + self.body.len() as u64
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut buf = vec![0u8; BLOCK_HEADER_SIZE + self.body.len()];
        buf[0..32].copy_from_slice(&self.header.block_id);
        buf[32] = self.header.block_type;
        buf[33] = self.header.depth;
        buf[34] = self.header.algo;
        buf[35] = self.header.enc_flag;
        buf[36..40].copy_from_slice(&self.header.raw_size.to_le_bytes());
        buf[40..44].copy_from_slice(&self.header.stored_size.to_le_bytes());
        buf[44..76].copy_from_slice(&self.header.base_id);
        buf[76..84].copy_from_slice(&self.header.padding);
        buf[84..].copy_from_slice(&self.body);
        buf
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < BLOCK_HEADER_SIZE {
            return Err(AxonError::InvalidArchive("block shorter than header"));
        }
        let header = BlockHeader {
            block_id: bytes[0..32]
                .try_into()
                .map_err(|_| AxonError::InvalidArchive("invalid block id length"))?,
            block_type: bytes[32],
            depth: bytes[33],
            algo: bytes[34],
            enc_flag: bytes[35],
            raw_size: u32::from_le_bytes(
                bytes[36..40]
                    .try_into()
                    .map_err(|_| AxonError::InvalidArchive("invalid raw size field"))?,
            ),
            stored_size: u32::from_le_bytes(
                bytes[40..44]
                    .try_into()
                    .map_err(|_| AxonError::InvalidArchive("invalid stored size field"))?,
            ),
            base_id: bytes[44..76]
                .try_into()
                .map_err(|_| AxonError::InvalidArchive("invalid base id length"))?,
            padding: bytes[76..84]
                .try_into()
                .map_err(|_| AxonError::InvalidArchive("invalid padding length"))?,
        };

        let stored_size = header.stored_size as usize;
        let expected_len = BLOCK_HEADER_SIZE + stored_size;
        if bytes.len() != expected_len {
            return Err(AxonError::InvalidArchive("block body length mismatch"));
        }

        Ok(Self {
            header,
            body: bytes[BLOCK_HEADER_SIZE..].to_vec(),
        })
    }
}

pub fn apply_delta(
    base_raw: &[u8],
    delta_body: &[u8],
    expected_raw_size: usize,
) -> Result<Vec<u8>> {
    let mut cursor = DeltaCursor::new(delta_body);
    if cursor.take(4)? != DELTA_MAGIC {
        return Err(AxonError::InvalidArchive("invalid delta magic"));
    }
    let version = cursor.u8()?;
    if version != DELTA_VERSION {
        return Err(AxonError::Unsupported("unsupported delta version"));
    }
    let op_count = cursor.u16()? as usize;
    cursor.take(1)?; // reserved

    let mut out = Vec::with_capacity(expected_raw_size);
    for _ in 0..op_count {
        let op = cursor.u8()?;
        match op {
            DELTA_OP_COPY => {
                let offset = cursor.u32()? as usize;
                let len = cursor.u32()? as usize;
                let end = offset
                    .checked_add(len)
                    .ok_or(AxonError::InvalidArchive("delta copy range overflow"))?;
                if end > base_raw.len() {
                    return Err(AxonError::InvalidArchive("delta copy range out of bounds"));
                }
                out.extend_from_slice(&base_raw[offset..end]);
            }
            DELTA_OP_INSERT => {
                let len = cursor.u32()? as usize;
                out.extend_from_slice(cursor.take(len)?);
            }
            _ => return Err(AxonError::InvalidArchive("unknown delta operation")),
        }
    }

    if cursor.remaining() != 0 {
        return Err(AxonError::InvalidArchive("delta trailing bytes"));
    }
    if out.len() != expected_raw_size {
        return Err(AxonError::InvalidArchive("delta output size mismatch"));
    }
    Ok(out)
}

fn encode_delta_body(base_raw: &[u8], raw: &[u8]) -> Result<Vec<u8>> {
    let mut prefix = 0usize;
    while prefix < base_raw.len() && prefix < raw.len() && base_raw[prefix] == raw[prefix] {
        prefix += 1;
    }

    let mut suffix = 0usize;
    let base_tail = base_raw.len().saturating_sub(prefix);
    let raw_tail = raw.len().saturating_sub(prefix);
    while suffix < base_tail
        && suffix < raw_tail
        && base_raw[base_raw.len() - 1 - suffix] == raw[raw.len() - 1 - suffix]
    {
        suffix += 1;
    }

    enum Op<'a> {
        Copy { offset: u32, len: u32 },
        Insert(&'a [u8]),
    }

    let mut ops = Vec::new();
    if prefix > 0 {
        ops.push(Op::Copy {
            offset: 0,
            len: u32::try_from(prefix)
                .map_err(|_| AxonError::InvalidArchive("delta copy prefix too large"))?,
        });
    }

    let insert_start = prefix;
    let insert_end = raw.len().saturating_sub(suffix);
    if insert_end > insert_start {
        ops.push(Op::Insert(&raw[insert_start..insert_end]));
    }

    if suffix > 0 {
        ops.push(Op::Copy {
            offset: u32::try_from(base_raw.len() - suffix)
                .map_err(|_| AxonError::InvalidArchive("delta copy suffix offset too large"))?,
            len: u32::try_from(suffix)
                .map_err(|_| AxonError::InvalidArchive("delta copy suffix too large"))?,
        });
    }

    let mut out = Vec::new();
    out.extend_from_slice(&DELTA_MAGIC);
    out.push(DELTA_VERSION);
    out.extend_from_slice(
        &u16::try_from(ops.len())
            .map_err(|_| AxonError::InvalidArchive("too many delta operations"))?
            .to_le_bytes(),
    );
    out.push(0); // reserved

    for op in ops {
        match op {
            Op::Copy { offset, len } => {
                out.push(DELTA_OP_COPY);
                out.extend_from_slice(&offset.to_le_bytes());
                out.extend_from_slice(&len.to_le_bytes());
            }
            Op::Insert(bytes) => {
                out.push(DELTA_OP_INSERT);
                out.extend_from_slice(
                    &u32::try_from(bytes.len())
                        .map_err(|_| AxonError::InvalidArchive("delta insert too large"))?
                        .to_le_bytes(),
                );
                out.extend_from_slice(bytes);
            }
        }
    }

    Ok(out)
}

struct DeltaCursor<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> DeltaCursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.pos)
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.remaining() < n {
            return Err(AxonError::InvalidArchive("delta truncated"));
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
            .map_err(|_| AxonError::InvalidArchive("invalid delta u16"))?;
        Ok(u16::from_le_bytes(bytes))
    }

    fn u32(&mut self) -> Result<u32> {
        let bytes: [u8; 4] = self
            .take(4)?
            .try_into()
            .map_err(|_| AxonError::InvalidArchive("invalid delta u32"))?;
        Ok(u32::from_le_bytes(bytes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base_block_round_trip() {
        let block = StoredBlock::from_raw_base(b"hello world").expect("build");
        let decoded = StoredBlock::decode(&block.encode()).expect("decode");
        assert_eq!(decoded.header.block_type, BLOCK_TYPE_BASE);
        assert_eq!(decoded.header.algo, ALGO_NONE);
        assert_eq!(decoded.body, b"hello world");
    }

    #[test]
    fn delta_block_round_trip() {
        let base = b"hello stable world";
        let target = b"hello mutable world";
        let delta = StoredBlock::from_raw_delta(base, [9; 32], 2, target).expect("delta");
        let decoded = StoredBlock::decode(&delta.encode()).expect("decode");

        assert_eq!(decoded.header.block_type, BLOCK_TYPE_DELTA);
        assert_eq!(decoded.header.depth, 3);
        let reconstructed =
            apply_delta(base, &decoded.body, decoded.header.raw_size as usize).expect("apply");
        assert_eq!(reconstructed, target);
    }

    #[test]
    fn delta_detects_out_of_bounds_copy() {
        let mut invalid = Vec::new();
        invalid.extend_from_slice(&DELTA_MAGIC);
        invalid.push(DELTA_VERSION);
        invalid.extend_from_slice(&1u16.to_le_bytes());
        invalid.push(0);
        invalid.push(DELTA_OP_COPY);
        invalid.extend_from_slice(&100u32.to_le_bytes());
        invalid.extend_from_slice(&4u32.to_le_bytes());
        let err = apply_delta(b"abc", &invalid, 4).expect_err("must fail");
        assert!(matches!(err, AxonError::InvalidArchive(_)));
    }
}
