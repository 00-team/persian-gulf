use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::{uid::UniqueId, utils::SocksHost};

pub trait BinDencode: Sized {
    #![allow(async_fn_in_trait)]

    async fn read<R: AsyncReadExt + Unpin>(
        r: &mut R,
    ) -> tokio::io::Result<Self>;
    async fn write<W: AsyncWriteExt + Unpin>(
        &self, w: &mut W,
    ) -> tokio::io::Result<()>;
}

#[derive(Debug, Clone)]
pub struct Shipment {
    pub ship_id: UniqueId,
    pub order: u64,
    pub order_request: u64,
    pub reset: bool,
    pub tanks: Vec<SpringTank>,
}

impl Shipment {
    pub async fn to_bytes(&self) -> Vec<u8> {
        let buf = Vec::with_capacity(2 * 1024 * 1024);
        let mut cursor = std::io::Cursor::new(buf);
        self.write(&mut cursor).await.unwrap();
        cursor.into_inner()
    }
}

impl BinDencode for Shipment {
    async fn read<R: AsyncReadExt + Unpin>(r: &mut R) -> std::io::Result<Self> {
        let ship_id = UniqueId::read(r).await?;
        let order = r.read_u64_le().await?;
        let order_request = r.read_u64_le().await?;
        let reset = r.read_u8().await? == 1;
        let ch_len = r.read_u16_le().await? as usize;
        let mut channels = Vec::with_capacity(ch_len);
        for _ in 0..ch_len {
            channels.push(SpringTank::read(r).await?);
        }

        Ok(Self { ship_id, order, order_request, reset, tanks: channels })
    }

    async fn write<W: AsyncWriteExt + Unpin>(
        &self, w: &mut W,
    ) -> tokio::io::Result<()> {
        self.ship_id.write(w).await?;
        w.write_u64_le(self.order).await?;
        w.write_u64_le(self.order_request).await?;
        w.write_u8(if self.reset { 1 } else { 0 }).await?;
        w.write_u16_le(self.tanks.len() as u16).await?;

        for ch in self.tanks.iter() {
            ch.write(w).await?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct SpringTank {
    pub id: UniqueId,
    pub ended: bool,
    pub host: SocksHost,
    pub port: u16,
    pub data: Vec<u8>,
}

impl BinDencode for SpringTank {
    async fn read<R: AsyncReadExt + Unpin>(
        r: &mut R,
    ) -> tokio::io::Result<Self> {
        let id = UniqueId::read(r).await?;
        let ended = r.read_u8().await? == 1;
        let host = SocksHost::read(r).await?;
        let port = r.read_u16_le().await?;

        let data_len = r.read_u32_le().await? as usize;
        let mut data = Vec::with_capacity(data_len);
        #[allow(clippy::uninit_vec)]
        unsafe { data.set_len(data_len) };
        r.read_exact(&mut data).await?;

        Ok(Self { id, ended, host, port, data })
    }

    async fn write<W: AsyncWriteExt + Unpin>(
        &self, w: &mut W,
    ) -> tokio::io::Result<()> {
        self.id.write(w).await?;
        w.write_u8(if self.ended { 1 } else { 0 }).await?;
        self.host.write(w).await?;
        w.write_u16_le(self.port).await?;
        w.write_u32_le(self.data.len() as u32).await?;
        w.write_all(&self.data).await
    }
}
