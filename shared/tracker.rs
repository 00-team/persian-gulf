use std::pin::Pin;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::task::{Context, Poll};

use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::TcpStream,
};

#[derive(Debug, Clone, Default)]
pub struct ConnectionStats {
    pub bytes_read: Arc<AtomicU64>,
    pub bytes_written: Arc<AtomicU64>,
}

impl std::fmt::Display for ConnectionStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let r = self.bytes_read.load(Ordering::Relaxed);
        let w = self.bytes_written.load(Ordering::Relaxed);
        write!(f, "↓{} / ↑{}", format_bytes(r), format_bytes(w))
    }
}

fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;

    match bytes {
        0 => "0".to_string(),
        _ if bytes >= GB => format!("{:.1}GB", bytes as f64 / GB as f64),
        _ if bytes >= MB => format!("{:.1}MB", bytes as f64 / MB as f64),
        _ if bytes >= KB => format!("{:.1}KB", bytes as f64 / KB as f64),
        _ => format!("{}B", bytes),
    }
}

#[derive(Debug)]
pub struct TrackedTcpStream {
    inner: TcpStream,
    stats: ConnectionStats,
}

impl TrackedTcpStream {
    pub fn new(stream: TcpStream, stats: ConnectionStats) -> Self {
        Self { inner: stream, stats }
    }
}

impl AsyncRead for TrackedTcpStream {
    fn poll_read(
        mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let before = buf.filled().len();
        let result = Pin::new(&mut self.inner).poll_read(cx, buf);

        if let Poll::Ready(Ok(())) = result {
            let after = buf.filled().len();
            let read_bytes = after - before;
            if read_bytes > 0 {
                self.stats
                    .bytes_read
                    .fetch_add(read_bytes as u64, Ordering::Relaxed);
            }
        }
        result
    }
}

impl AsyncWrite for TrackedTcpStream {
    fn poll_write(
        mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        let result = Pin::new(&mut self.inner).poll_write(cx, buf);

        if let Poll::Ready(Ok(written)) = result {
            if written > 0 {
                self.stats
                    .bytes_written
                    .fetch_add(written as u64, Ordering::Relaxed);
            }
        }
        result
    }

    fn poll_flush(
        mut self: Pin<&mut Self>, cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>, cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}
