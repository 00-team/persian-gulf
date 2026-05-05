use crate::{shipment::BinDencode, sys_now};

#[derive(Debug, Hash, PartialEq, Eq, Clone, Copy)]
pub struct UniqueId {
    index: u64,
    created_at: u64,
    pepper: u64,
}

impl UniqueId {
    pub fn new(index: u64) -> Self {
        Self { index, created_at: sys_now(), pepper: rand::random() }
    }
}

impl BinDencode for UniqueId {
    async fn write<W: tokio::io::AsyncWriteExt + Unpin>(
        &self, w: &mut W,
    ) -> tokio::io::Result<()> {
        w.write_u64_le(self.index).await?;
        w.write_u64_le(self.created_at).await?;
        w.write_u64_le(self.pepper).await
    }

    async fn read<R: tokio::io::AsyncReadExt + Unpin>(
        r: &mut R,
    ) -> tokio::io::Result<Self> {
        let index = r.read_u64_le().await?;
        let created_at = r.read_u64_le().await?;
        let pepper = r.read_u64_le().await?;

        Ok(Self { index, created_at, pepper })
    }
}
