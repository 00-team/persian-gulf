use crate::shipment::BinDencode;

#[derive(Debug, Clone)]
pub enum SocksHost {
    Ipv4([u8; 4]),
    Ipv6([u8; 16]),
    Domain(String),
}

impl SocksHost {
    pub const ATYP_IPV4: u8 = 0x01;
    pub const ATYP_DOMAIN: u8 = 0x03;
    pub const ATYP_IPV6: u8 = 0x04;

    pub fn to_addr(&self, port: u16) -> String {
        match self {
            Self::Ipv4(ip) => {
                format!("{}.{}.{}.{}:{port}", ip[0], ip[1], ip[2], ip[3])
            }
            Self::Domain(dom) => format!("{dom}:{port}"),
            Self::Ipv6(ip) => format!(
                "{:02x}{:02x}:{:02x}{:02x}:{:02x}{:02x}:{:02x}{:02x}:{:02x}{:02x}:{:02x}{:02x}:{:02x}{:02x}:{:02x}{:02x}:{port}",
                ip[0],
                ip[1],
                ip[2],
                ip[3],
                ip[4],
                ip[5],
                ip[6],
                ip[7],
                ip[8],
                ip[9],
                ip[10],
                ip[11],
                ip[12],
                ip[13],
                ip[14],
                ip[15]
            ),
        }
    }
}

impl BinDencode for SocksHost {
    async fn read<R: tokio::io::AsyncReadExt + Unpin>(
        r: &mut R,
    ) -> tokio::io::Result<Self> {
        let kind = r.read_u8().await?;
        Ok(match kind {
            Self::ATYP_IPV4 => {
                let mut ipv4 = [0u8; 4];
                r.read_exact(&mut ipv4).await?;
                Self::Ipv4(ipv4)
            }
            Self::ATYP_IPV6 => {
                let mut ipv6 = [0u8; 16];
                r.read_exact(&mut ipv6).await?;
                Self::Ipv6(ipv6)
            }
            Self::ATYP_DOMAIN => {
                let len = r.read_u8().await? as usize;
                let mut buf = [0u8; 255];
                r.read_exact(&mut buf[..len]).await?;
                let Ok(domain) = String::from_utf8(buf[..len].to_vec()) else {
                    return Err(std::io::Error::other(
                        "invalid host domain string",
                    ));
                };
                Self::Domain(domain)
            }
            _ => return Err(std::io::Error::other("invalid host kind")),
        })
    }

    async fn write<W: tokio::io::AsyncWriteExt + Unpin>(
        &self, w: &mut W,
    ) -> tokio::io::Result<()> {
        match self {
            Self::Ipv4(v4) => {
                w.write_u8(Self::ATYP_IPV4).await?;
                w.write_all(v4).await?;
            }
            Self::Ipv6(v6) => {
                w.write_u8(Self::ATYP_IPV6).await?;
                w.write_all(v6).await?;
            }
            Self::Domain(domain) => {
                w.write_u8(Self::ATYP_DOMAIN).await?;
                w.write_u8(domain.len() as u8).await?;
                w.write_all(domain.as_bytes()).await?;
            }
        }

        Ok(())
    }
}

impl std::fmt::Display for SocksHost {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ipv4(v) => write!(f, "{}.{}.{}.{}", v[0], v[1], v[2], v[3]),
            Self::Domain(v) => f.write_str(v),
            Self::Ipv6(v) => write!(f, "::{:02x}:{:02x}", v[14], v[15]),
        }
    }
}
