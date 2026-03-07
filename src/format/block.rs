use crate::{AxonError, Result};

pub const BLOCK_TYPE_BASE: u8 = 0;
pub const ALGO_NONE: u8 = 3;
pub const BLOCK_HEADER_SIZE: usize = 84;

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
}
