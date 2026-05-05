use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::{uid::UniqueId, utils::SocksHost};

pub trait BinDencode: Sized {
    async fn read<R: AsyncReadExt + Unpin>(
        r: &mut R,
    ) -> tokio::io::Result<Self>;
    async fn write<W: AsyncWriteExt + Unpin>(
        &self, w: &mut W,
    ) -> tokio::io::Result<()>;
}

// pub mod bin_tools {
//     use tokio::io::{AsyncReadExt, AsyncWriteExt, Result};
//
//     macro_rules! wrp {
//         ($([$rfn:ident, $wfn:ident, $pty:ident],)*) => {
//             $(
//             pub fn $rfn<R: AsyncReadExt + Un>(r: &mut R) -> Result<$pty> {
//                 let mut buf = [0u8; core::mem::size_of::<$pty>()];
//                 r.read_exact(&mut buf)?;
//                 Ok($pty::from_le_bytes(buf))
//             }
//
//             pub fn $wfn<W: AsyncWriteExt>(w: &mut W, value: $pty) -> Result<()> {
//                 w.write_all(&value.to_le_bytes())
//             }
//             )*
//         };
//     }
//
//     wrp!(
//         [read_u64, write_u64, u64],
//         [read_u32, write_u32, u32],
//         [read_u16, write_u16, u16],
//         [read_u8, write_u8, u8],
//     );
// }

#[derive(Debug)]
pub struct Shipment {
    pub ship_uid: UniqueId,
    pub order: u64,
    pub channels: Vec<ShipmentChannel>,
    /*
    <evergreen unique id> - <request id: u64> - <number of channels: u16>
    <channel id> - <host> - <port> - <data len: u32> - <data>
    */
}

impl BinDencode for Shipment {
    async fn read<R: AsyncReadExt + Unpin>(r: &mut R) -> std::io::Result<Self> {
        let ship_uid = UniqueId::read(r).await?;
        let order = r.read_u64_le().await?;
        let ch_len = r.read_u16_le().await? as usize;
        let mut channels = Vec::with_capacity(ch_len);
        for _ in 0..ch_len {
            channels.push(ShipmentChannel::read(r).await?);
        }

        Ok(Self { ship_uid, order, channels })
    }

    async fn write<W: AsyncWriteExt + Unpin>(
        &self, w: &mut W,
    ) -> tokio::io::Result<()> {
        self.ship_uid.write(w).await?;
        w.write_u64_le(self.order).await?;
        w.write_u16_le(self.channels.len() as u16).await?;

        for ch in self.channels.iter() {
            ch.write(w).await?;
        }

        Ok(())
    }
}

#[derive(Debug)]
pub struct ShipmentChannel {
    pub id: UniqueId,
    pub host: SocksHost,
    pub port: u16,
    pub data: Vec<u8>,
}

impl BinDencode for ShipmentChannel {
    async fn read<R: AsyncReadExt + Unpin>(
        r: &mut R,
    ) -> tokio::io::Result<Self> {
        let id = UniqueId::read(r).await?;
        let host = SocksHost::read(r).await?;
        let port = r.read_u16_le().await?;

        let data_len = r.read_u32_le().await? as usize;
        let mut data = Vec::with_capacity(data_len);
        unsafe { data.set_len(data_len) };
        r.read_exact(&mut data).await?;

        Ok(Self { id, host, port, data })
    }

    async fn write<W: AsyncWriteExt + Unpin>(
        &self, w: &mut W,
    ) -> tokio::io::Result<()> {
        self.id.write(w).await?;
        self.host.write(w).await?;
        w.write_u16_le(self.port).await?;
        w.write_u32_le(self.data.len() as u32).await?;
        w.write_all(&self.data).await
    }
}
