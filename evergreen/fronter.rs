use std::{
    io::Cursor,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use reqwest::Url;
use rustls::{ClientConfig, RootCertStore, pki_types::ServerName};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf},
    net::TcpStream,
    sync::Mutex,
    time::Instant,
};
use tokio_rustls::{TlsConnector, client::TlsStream};

use crate::socks::EverError;

type ConnReader = ReadHalf<TlsStream<TcpStream>>;
type ConnWriter = WriteHalf<TlsStream<TcpStream>>;

#[derive(Debug)]
struct ActiveConnection {
    reader: ConnReader,
    writer: ConnWriter,
    created_at: Instant,
}

#[derive(Debug, Clone)]
struct ConnectionInfo {
    connect_host: &'static str,
    sni_host: &'static str,
    tls_config: Arc<ClientConfig>,
}

#[derive(Debug, Clone)]
struct ConnectionPool {
    pool: Arc<Mutex<Vec<ActiveConnection>>>,
    info: ConnectionInfo,
}

impl ConnectionPool {
    const POOL_MAX: usize = 50;
    const POOL_MIN_IDLE: usize = 15;
    const CONN_TTL: u64 = 45;

    async fn open(
        info: ConnectionInfo,
    ) -> Result<(ConnReader, ConnWriter), EverError> {
        let tcp = TcpStream::connect((info.connect_host, 443)).await?;

        let connector = TlsConnector::from(info.tls_config);

        let server_name = ServerName::try_from(info.sni_host)?.to_owned();
        let tls_stream = connector.connect(server_name, tcp).await?;

        let (reader, writer) = tokio::io::split(tls_stream);
        Ok((reader, writer))
    }

    fn fill(&self, count: usize) {
        for _ in 0..count {
            let pool = self.clone();
            tokio::task::spawn(async move {
                pool.new_conn().await;
            });
        }
    }

    async fn maintenance(self) {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(3)).await;

            {
                let mut pm = self.pool.lock().await;
                pm.retain(|ac| {
                    if ac.created_at.elapsed().as_secs() > Self::CONN_TTL {
                        return false;
                    }

                    true
                });
            }

            self.fill(Self::POOL_MIN_IDLE);
        }
    }

    async fn new_conn(self) {
        let Ok(Ok((r, w))) = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            Self::open(self.info.clone()),
        )
        .await
        else {
            return;
        };
        let t = Instant::now();

        let mut pm = self.pool.lock().await;
        if pm.len() < Self::POOL_MAX {
            pm.push(ActiveConnection { reader: r, writer: w, created_at: t });
        }
    }

    async fn acquire(&self) -> Result<ActiveConnection, EverError> {
        {
            let mut pm = self.pool.lock().await;
            while let Some(ac) = pm.pop() {
                if ac.created_at.elapsed().as_secs() > Self::CONN_TTL {
                    continue;
                }
                let pool = self.clone();
                tokio::task::spawn(async move {
                    pool.new_conn().await;
                });
                return Ok(ac);
            }
        }

        let Ok(Ok((r, w))) = tokio::time::timeout(
            Duration::from_secs(3),
            Self::open(self.info.clone()),
        )
        .await
        else {
            return Err(EverError::ConnectionFailed);
        };

        self.fill(8);

        Ok(ActiveConnection {
            reader: r,
            writer: w,
            created_at: Instant::now(),
        })
    }

    pub async fn release(&self, ac: ActiveConnection) {
        if ac.created_at.elapsed().as_secs() > Self::CONN_TTL {
            return;
        }

        self.pool.lock().await.push(ac);
    }
}

pub struct Fronter {
    http_host: &'static str,
    script_ids: Vec<(String, String)>,
    script_idx: Arc<Mutex<usize>>,
    dev_available: bool,
    proxy_url: String,
    // verify_ssl: bool,
    pool: ConnectionPool,
    // semaphore: Semaphore,
    warmed: Arc<AtomicBool>, // refilling: bool,
}

impl Fronter {
    pub fn new(alzahra: &str, script_ids: Vec<(String, String)>) -> Self {
        Self {
            http_host: "script.google.com",
            script_ids,
            script_idx: Default::default(),
            dev_available: false,
            proxy_url: format!("{alzahra}/api/proxy/bin-batch/"),
            // verify_ssl: true,
            // semaphore: Semaphore::new(50),
            // refilling: false,
            pool: ConnectionPool {
                pool: Default::default(),
                info: ConnectionInfo {
                    // google ip
                    connect_host: "216.239.38.120",
                    sni_host: "www.google.com",
                    tls_config: Arc::new(Self::build_tls_config()),
                },
            },
            warmed: Arc::new(AtomicBool::new(false)),
        }
    }

    pub async fn relay(&self, body: String) -> Result<String, EverError> {
        if !self.warmed.load(Ordering::Relaxed) {
            self.warm_pool().await;
        }

        // # Try HTTP/2 first — much faster (multiplexed, no pool checkout)
        // if self._h2 and self._h2.is_connected:
        //     for attempt in range(2):
        //         try:
        //             return await asyncio.wait_for(
        //                 self._relay_single_h2(payload), timeout=25
        //             )
        //         except Exception as e:
        //             if attempt == 0:
        //                 log.debug("H2 relay failed (%s), reconnecting", e)
        //                 try:
        //                     await self._h2.reconnect()
        //                 except Exception:
        //                     log.warning("H2 reconnect failed, falling back to H1")
        //                     break
        //             else:
        //                 raise

        // self.semaphore.acquire().await

        for _ in 0..3 {
            let res = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                self.relay_single(body.clone()),
            )
            .await;

            if let Ok(Ok(res)) = res {
                return Ok(res);
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        Err(EverError::RequestFailed)
    }

    async fn exec_path(&self) -> String {
        let sdx = {
            let mut msi = self.script_idx.lock().await;
            if *msi >= self.script_ids.len() {
                *msi = 0;
            }
            let value = *msi;
            *msi += 1;
            value
        };
        let (sid, auth) = &self.script_ids[sdx];

        format!(
            "/macros/s/{sid}/{}?t={}&a={auth}",
            if self.dev_available { "dev" } else { "exec" },
            self.proxy_url,
        )
    }

    async fn relay_single(&self, payload: String) -> Result<String, EverError> {
        let path = self.exec_path().await;
        let mut ac = self.pool.acquire().await?;

        let head = format!(
            "POST {path} HTTP/1.1\r\nHOST: {}\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nAccept-Encoding: gzip\r\nConnection: keep-alive\r\n\r\n",
            self.http_host,
            payload.len()
        );

        // log::info!("head: {head}");
        ac.writer.write_all(head.as_bytes()).await?;
        ac.writer.write_all(payload.as_bytes()).await?;
        ac.writer.flush().await?;

        let mut res = self.read_http_response(&mut ac.reader).await?;
        for _ in 0..5 {
            if !res.status().is_redirection() {
                break;
            }

            let Some(loc) = res.headers().get("location") else { break };
            let loc = loc.to_str().unwrap();
            let loc = Url::parse(loc).unwrap();
            let mut path = loc.path().to_string();
            if let Some(q) = loc.query() {
                path.push('?');
                path.push_str(q);
            }

            let head = format!(
                "GET {path} HTTP/1.1\r\nHOST: {}\r\nAccept-Encoding: gzip\r\nConnection: keep-alive\r\n\r\n",
                loc.host_str().unwrap(),
            );
            // log::warn!("redirect to \n{head}");
            ac.writer.write_all(head.as_bytes()).await?;
            ac.writer.flush().await?;
            res = self.read_http_response(&mut ac.reader).await?;
        }

        self.pool.release(ac).await;

        let Ok(body) = String::from_utf8(res.body().to_vec()) else {
            return Err(EverError::InvalidBody);
        };

        Ok(body)
    }

    pub async fn warm_pool(&self) {
        if self.warmed.load(Ordering::Relaxed) {
            return;
        }

        self.warmed.store(true, Ordering::SeqCst);
        self.pool.fill(30);
        let pool = self.pool.clone();
        tokio::task::spawn(async move {
            pool.maintenance().await;
        });

        // # Start H2 connection (runs alongside H1 pool)
        // if self._h2:
        //     asyncio.create_task(self._h2_connect_and_warm())
    }

    fn build_tls_config() -> ClientConfig {
        let mut root_store = RootCertStore::empty();

        rustls::crypto::aws_lc_rs::default_provider()
            .install_default()
            .expect("failed to install tls");

        // assert!(self.verify_ssl);
        // if self.verify_ssl {
        // Add platform-native root certificates (best for most users)
        // let native_certs =
        //     rustls_native_certs::load_native_certs().map_err(|e| {
        //         anyhow::anyhow!("failed to load native certs: {}", e)
        //     })?;
        // for cert in native_certs {
        //     root_store.add(cert)?;
        // }
        if root_store.is_empty() {
            root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        }

        ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth()
    }

    async fn read_http_response(
        &self, reader: &mut ConnReader,
    ) -> Result<http::Response<Vec<u8>>, EverError> {
        let mut response = self.read_http_headers(reader).await?;
        Self::read_http_body(&mut response, reader).await?;

        Ok(response)
    }

    async fn read_http_headers(
        &self, reader: &mut ConnReader,
    ) -> Result<http::Response<Vec<u8>>, EverError> {
        let mut buffer = Vec::new();

        loop {
            let mut chunk = vec![0u8; 8192];
            let Ok(Ok(n)) = tokio::time::timeout(
                Duration::from_secs(3),
                reader.read(&mut chunk),
            )
            .await
            else {
                return Err(EverError::ReadTimeout);
            };
            if n == 0 {
                return Err(EverError::Eof);
            }
            buffer.extend_from_slice(&chunk[..n]);
            if buffer.windows(4).any(|w| w == b"\r\n\r\n") {
                break;
            }
        }
        let mut headers = [httparse::EMPTY_HEADER; 64];
        let mut resp = httparse::Response::new(&mut headers);
        let head_end = match resp.parse(&buffer) {
            Ok(httparse::Status::Complete(n)) => n,
            _ => return Err(EverError::InvalidHttpResponse),
        };

        let body = buffer[head_end..].to_vec();

        let Ok(s) = http::StatusCode::from_u16(resp.code.unwrap_or(0)) else {
            return Err(EverError::InvalidHttpResponse);
        };
        let mut builder = http::Response::builder().status(s);

        for header in resp.headers.iter() {
            builder = builder.header(header.name, header.value);
        }

        let Ok(response) = builder.body(body) else {
            return Err(EverError::InvalidHttpResponse);
        };

        Ok(response)
    }

    async fn read_http_body(
        response: &mut http::Response<Vec<u8>>, reader: &mut ConnReader,
    ) -> Result<(), EverError> {
        // // Combine the first_chunk (leftover from header parsing) with the reader
        // let mut combined = std::io::Cursor::new(first_chunk);
        // // We need to read from both: first the leftover, then the real reader.
        // // Easiest: use a chain of readers.
        // let mut chain = tokio::io::BufReader::new(
        //     tokio::io::AsyncReadExt::chain(&mut combined, reader),
        // );

        let transfer_encoding = response
            .headers()
            .get("transfer-encoding")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");

        let mut data = vec![0u8; 65536];

        if transfer_encoding.contains("chunked") {
            let body = response.body_mut();
            let nb = Self::read_chunked(reader, body.clone()).await;
            body.clear();
            body.extend_from_slice(&nb);
        } else if let Some(cl) = response.headers().get("content-length") {
            let content_length: usize =
                cl.to_str().expect("invalid cl").parse().expect("bad cl");

            let body = response.body_mut();
            let mut rem = content_length.saturating_sub(body.len());

            while rem > 0 {
                let Ok(Ok(n)) = tokio::time::timeout(
                    Duration::from_secs(3),
                    reader.read(&mut data[..(rem.min(65536))]),
                )
                .await
                else {
                    return Err(EverError::ReadTimeout);
                };
                if n == 0 {
                    break;
                }
                body.extend_from_slice(&data[..n]);
                rem -= n;
            }
        } else {
            let body = response.body_mut();
            loop {
                let Ok(Ok(n)) = tokio::time::timeout(
                    Duration::from_secs(2),
                    reader.read(&mut data),
                )
                .await
                else {
                    break;
                };
                if n == 0 {
                    break;
                }
                body.extend_from_slice(&data[..n]);
            }
        };

        if let Some(ce) = response.headers().get("content-encoding")
            && ce.to_str().unwrap().to_lowercase() == "gzip"
        {
            let body = response.body_mut();
            // log::info!("gzip body: {body:?}");
            let mut decoder =
                async_compression::tokio::bufread::GzipDecoder::new(
                    tokio::io::BufReader::new(Cursor::new(body.clone())),
                );

            body.clear();
            if decoder.read_to_end(body).await.is_err() {
                return Err(EverError::InvalidGzip);
            }
        }

        Ok(())
    }

    async fn read_chunked(
        reader: &mut ConnReader, mut buf: Vec<u8>,
    ) -> Vec<u8> {
        let mut result = Vec::with_capacity(buf.len());
        let mut data = vec![0u8; 8192];
        loop {
            let end = loop {
                if let Some(p) = buf.windows(2).position(|w| w == b"\r\n") {
                    break p;
                }

                let Ok(Ok(n)) = tokio::time::timeout(
                    Duration::from_secs(3),
                    reader.read(&mut data),
                )
                .await
                else {
                    return result;
                };
                if n == 0 {
                    return result;
                }
                buf.extend_from_slice(&data[..n]);
            };

            let Ok(size_str) = String::from_utf8(buf[..end].to_vec()) else {
                break;
            };
            let size_str: String =
                size_str.chars().filter(|c| !c.is_whitespace()).collect();
            if size_str.is_empty() {
                continue;
            }
            let Ok(size) = usize::from_str_radix(&size_str, 16) else { break };
            if size == 0 {
                break;
            }

            buf = buf[end + 2..].to_vec();
            let mut data = vec![0u8; 65536];
            while buf.len() < size + 2 {
                let Ok(Ok(n)) = tokio::time::timeout(
                    Duration::from_secs(3),
                    reader.read(&mut data),
                )
                .await
                else {
                    return result;
                };
                if n == 0 {
                    result.extend_from_slice(&buf[..size]);
                    return result;
                }
                buf.extend_from_slice(&data[..n]);
            }
            result.extend_from_slice(&buf[..size]);
            buf = buf[size + 2..].to_vec();
        }

        result
    }

    // ── Decompression helper ──────────────────────────────────────────

    // fn maybe_decompress(headers: &HeaderMap, body: Vec<u8>) -> Vec<u8> {
    //     if headers
    //         .get("content-encoding")
    //         .and_then(|v| v.to_str().ok())
    //         .map(|s| s.eq_ignore_ascii_case("gzip"))
    //         .unwrap_or(false)
    //     {
    //         let mut decoder = GzDecoder::new(&body[..]);
    //         let mut decompressed = Vec::new();
    //         if std::io::Read::read_to_end(&mut decoder, &mut decompressed).is_ok() {
    //             return decompressed;
    //         }
    //     }
    //     body
    // }

    //     """Read one HTTP response. Keep-alive safe (no read-until-EOF)."""
    //     raw = b""
    //     while b"\r\n\r\n" not in raw:
    //         chunk = await asyncio.wait_for(reader.read(8192), timeout=8)
    //         if not chunk:
    //             break
    //         raw += chunk
    //
    //     if b"\r\n\r\n" not in raw:
    //         return 0, {}, b""
    //
    //     header_section, body = raw.split(b"\r\n\r\n", 1)
    //     lines = header_section.split(b"\r\n")
    //
    //     status_line = lines[0].decode(errors="replace")
    //     m = re.search(r"\d{3}", status_line)
    //     status = int(m.group()) if m else 0
    //
    //     headers = {}
    //     for line in lines[1:]:
    //         if b":" in line:
    //             k, v = line.decode(errors="replace").split(":", 1)
    //             headers[k.strip().lower()] = v.strip()
    //
    //     content_length = headers.get("content-length")
    //     transfer_encoding = headers.get("transfer-encoding", "")
    //
    //     if "chunked" in transfer_encoding:
    //         body = await self._read_chunked(reader, body)
    //     elif content_length:
    //         remaining = int(content_length) - len(body)
    //         while remaining > 0:
    //             chunk = await asyncio.wait_for(
    //                 reader.read(min(remaining, 65536)), timeout=20
    //             )
    //             if not chunk:
    //                 break
    //             body += chunk
    //             remaining -= len(chunk)
    //     else:
    //         # No framing — short timeout read (keep-alive safe)
    //         while True:
    //             try:
    //                 chunk = await asyncio.wait_for(reader.read(65536), timeout=2)
    //                 if not chunk:
    //                     break
    //                 body += chunk
    //             except asyncio.TimeoutError:
    //                 break
    //
    //     # Auto-decompress gzip from Google frontend
    //     if headers.get("content-encoding", "").lower() == "gzip":
    //         try:
    //             body = gzip.decompress(body)
    //         except Exception:
    //             pass  # not actually gzip, use as-is
    //
    //     return status, headers, body
}

/*
        # HTTP/2 multiplexing — one connection handles all requests
        self._h2 = None
        if mode == "apps_script":
            try:
                from h2_transport import H2Transport, H2_AVAILABLE
                if H2_AVAILABLE:
                    self._h2 = H2Transport(
                        self.connect_host, self.sni_host, self.verify_ssl
                    )
                    log.info("\x1b[32mHTTP/2 multiplexing available — "
                             "all requests will share one connection\x1b[m")
            except ImportError:
                pass

    # ── helpers ───────────────────────────────────────────────────

    def _ssl_ctx(self) -> ssl.SSLContext:
        ctx = ssl.create_default_context()
        if not self.verify_ssl:
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
        return ctx





    async def _flush_pool(self):
        """Close all pooled connections (they may be stale after errors)."""
        async with self._pool_lock:
            for _, writer, _ in self._pool:
                try:
                    writer.close()
                except Exception:
                    pass
            self._pool.clear()





    async def _h2_connect(self):
        """Connect the HTTP/2 transport in background."""
        try:
            await self._h2.ensure_connected()
            log.info("H2 multiplexing active — one conn handles all requests")
        except Exception as e:
            log.warning("H2 connect failed (%s), using H1 pool fallback", e)

    async def _h2_connect_and_warm(self):
        """Connect H2, pre-warm the Apps Script container, start keepalive."""
        await self._h2_connect()
        if self._h2 and self._h2.is_connected:
            asyncio.create_task(self._prewarm_script())
            asyncio.create_task(self._keepalive_loop())

    async def _prewarm_script(self):
        """Pre-warm Apps Script and detect /dev fast path (no redirect)."""
        payload = json.dumps(
            {"m": "HEAD", "u": "http://example.com/", "k": self.auth_key}
        ).encode()
        hdrs = {"content-type": "application/json"}
        sid = self._script_ids[0]

        # Test /dev endpoint — returns data inline (no 302 redirect).
        # If it works, saves ~400ms per request by eliminating one round trip.
        try:
            dev_path = f"/macros/s/{sid}/dev"
            t0 = time.perf_counter()
            status, _, body = await asyncio.wait_for(
                self._h2.request(
                    method="POST", path=dev_path, host=self.http_host,
                    headers=hdrs, body=payload,
                ),
                timeout=15,
            )
            dt = (time.perf_counter() - t0) * 1000
            data = json.loads(body.decode(errors="replace"))
            if "s" in data:
                self._dev_available = True
                log.info("/dev fast path active (%.0fms, no redirect)", dt)
                return
        except Exception as e:
            log.debug("/dev test failed: %s", e)

        # Fallback: warm up with /exec
        try:
            exec_path = f"/macros/s/{sid}/exec"
            t0 = time.perf_counter()
            await asyncio.wait_for(
                self._h2.request(
                    method="POST", path=exec_path, host=self.http_host,
                    headers=hdrs, body=payload,
                ),
                timeout=15,
            )
            dt = (time.perf_counter() - t0) * 1000
            log.info("Apps Script pre-warmed in %.0fms", dt)
        except Exception as e:
            log.debug("Pre-warm failed: %s", e)

    async def _keepalive_loop(self):
        """Send periodic pings to keep Apps Script warm + H2 connection alive."""
        while True:
            try:
                await asyncio.sleep(240)  # 4 minutes — saves ~90 quota hits/day vs 180s
                                          # Google's container timeout is ~5 min idle
                if not self._h2 or not self._h2.is_connected:
                    try:
                        await self._h2.reconnect()
                    except Exception:
                        continue

                # H2 PING to keep connection alive
                await self._h2.ping()

                # Apps Script keepalive — warm the container
                payload = {"m": "HEAD", "u": "http://example.com/", "k": self.auth_key}
                path = self._exec_path()
                t0 = time.perf_counter()
                await asyncio.wait_for(
                    self._h2.request(
                        method="POST", path=path, host=self.http_host,
                        headers={"content-type": "application/json"},
                        body=json.dumps(payload).encode(),
                    ),
                    timeout=20,
                )
                dt = (time.perf_counter() - t0) * 1000
                log.debug("Keepalive ping: %.0fms", dt)
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.debug("Keepalive failed: %s", e)


    def _auth_header(self) -> str:
        return f"X-Auth-Key: {self.auth_key}\r\n" if self.auth_key else ""

    # ── WebSocket tunnel (CONNECT / HTTPS) ────────────────────────

    async def tunnel(self, target_host: str, target_port: int,
                     client_r: asyncio.StreamReader,
                     client_w: asyncio.StreamWriter):
        """Tunnel raw TCP bytes through a domain-fronted WebSocket."""
        try:
            remote_r, remote_w = await self._open()
        except Exception as e:
            log.error("TLS connect to %s failed: %s", self.connect_host, e)
            return

        try:
            # ---- WebSocket upgrade ----
            ws_key = base64.b64encode(os.urandom(16)).decode()
            path = f"{self.worker_path}/tunnel?host={target_host}&port={target_port}"
            handshake = (
                f"GET {path} HTTP/1.1\r\n"
                f"Host: {self.http_host}\r\n"
                f"Upgrade: websocket\r\n"
                f"Connection: Upgrade\r\n"
                f"Sec-WebSocket-Key: {ws_key}\r\n"
                f"Sec-WebSocket-Version: 13\r\n"
                f"{self._auth_header()}"
                f"\r\n"
            )
            remote_w.write(handshake.encode())
            await remote_w.drain()

            # Read the 101 Switching Protocols response
            resp = b""
            while b"\r\n\r\n" not in resp:
                chunk = await asyncio.wait_for(remote_r.read(4096), timeout=15)
                if not chunk:
                    raise ConnectionError("No WebSocket handshake response")
                resp += chunk

            status_line = resp.split(b"\r\n")[0]
            if b"101" not in status_line:
                raise ConnectionError(
                    f"WebSocket upgrade rejected: {status_line.decode(errors='replace')}"
                )

            log.info("Tunnel ready → %s:%d", target_host, target_port)

            # ---- bidirectional relay ----
            await asyncio.gather(
                self._client_to_ws(client_r, remote_w),
                self._ws_to_client(remote_r, client_w),
            )

        except Exception as e:
            log.error("Tunnel error (%s:%d): %s", target_host, target_port, e)
        finally:
            try:
                remote_w.close()
            except Exception:
                pass

    async def _client_to_ws(self, src: asyncio.StreamReader,
                            dst: asyncio.StreamWriter):
        """Read plaintext from the browser, wrap in WS frames, send to CDN."""
        try:
            while True:
                data = await src.read(16384)
                if not data:
                    # Send a WS close frame
                    dst.write(ws_encode(b"", opcode=0x08))
                    await dst.drain()
                    break
                dst.write(ws_encode(data))
                await dst.drain()
        except (ConnectionError, asyncio.CancelledError):
            pass

    async def _ws_to_client(self, src: asyncio.StreamReader,
                            dst: asyncio.StreamWriter):
        """Read WS frames from CDN, unwrap, write plaintext to browser."""
        buf = b""
        try:
            while True:
                chunk = await src.read(16384)
                if not chunk:
                    break
                buf += chunk
                while buf:
                    result = ws_decode(buf)
                    if result is None:
                        break  # need more data
                    opcode, payload, consumed = result
                    buf = buf[consumed:]
                    if opcode == 0x08:  # close
                        return
                    if payload:
                        dst.write(payload)
                        await dst.drain()
        except (ConnectionError, asyncio.CancelledError):
            pass

    # ── HTTP forwarding ───────────────────────────────────────────

    async def forward(self, raw_request: bytes) -> bytes:
        """Forward a plain HTTP request through the domain-fronted channel.

        Uses keep-alive connections from the pool for efficiency.
        """
        try:
            reader, writer, created = await self._acquire()

            # Wrap the original HTTP request inside a POST to the worker.
            request = (
                f"POST {self.worker_path}/forward HTTP/1.1\r\n"
                f"Host: {self.http_host}\r\n"
                f"Content-Type: application/octet-stream\r\n"
                f"Content-Length: {len(raw_request)}\r\n"
                f"Connection: keep-alive\r\n"
                f"{self._auth_header()}"
                f"\r\n"
            )
            writer.write(request.encode() + raw_request)
            await writer.drain()

            status, resp_headers, resp_body = await self._read_http_response(reader)

            await self._release(reader, writer, created)

            # The worker wraps the target's response in its own HTTP
            # envelope.  The body IS the raw HTTP response from the target.
            return resp_body

        except Exception as e:
            log.error("Forward failed: %s", e)
            return b"HTTP/1.1 502 Bad Gateway\r\n\r\nDomain fronting request failed\r\n"

    # ── Apps Script relay (apps_script mode) ──────────────────────

    async def relay_parallel(self, method: str, url: str,
                             headers: dict, body: bytes = b"",
                             chunk_size: int = 256 * 1024,
                             max_parallel: int = 16) -> bytes:
        """Relay with parallel range acceleration for large downloads.

        Strategy:
          1. Send initial GET with Range: bytes=0-<chunk_size-1>
          2. If target returns 206 (supports ranges), fetch remaining
             chunks concurrently via HTTP/2 multiplexing.
          3. If target returns 200 (no range support) or small file,
             return the single response.

        Since each Apps Script call takes ~2s regardless of payload size,
        we use:
          - 256 KB chunks (safe under Apps Script response limit)
          - Up to 16 chunks in flight at once via H2 multiplexing
          - Aggregate throughput of ~2 MB per round-trip (~2-3s)
        """
        if method != "GET" or body:
            return await self.relay(method, url, headers, body)

        # Probe: first chunk with Range header
        range_headers = dict(headers) if headers else {}
        range_headers["Range"] = f"bytes=0-{chunk_size - 1}"
        first_resp = await self.relay("GET", url, range_headers, b"")

        status, resp_hdrs, resp_body = self._split_raw_response(first_resp)

        # No range support → return the single response as-is (status 200
        # from the origin). The client sent a plain GET, so 200 is what it
        # expects.
        if status != 206:
            return first_resp

        # Parse total size from Content-Range: "bytes 0-262143/1048576"
        content_range = resp_hdrs.get("content-range", "")
        m = re.search(r"/(\d+)", content_range)
        if not m:
            # Can't parse — downgrade to 200 so the client (which sent a
            # plain GET) doesn't get confused by 206 + Content-Range.
            return self._rewrite_206_to_200(first_resp)
        total_size = int(m.group(1))

        # Small file: probe already fetched it all. MUST rewrite to 200
        # because the client never sent a Range header — a stray 206 here
        # breaks fetch()/XHR on sites like x.com and Cloudflare challenges.
        if total_size <= chunk_size or len(resp_body) >= total_size:
            return self._rewrite_206_to_200(first_resp)

        # Calculate remaining ranges
        ranges = []
        start = len(resp_body)
        while start < total_size:
            end = min(start + chunk_size - 1, total_size - 1)
            ranges.append((start, end))
            start = end + 1

        log.info("Parallel download: %d bytes, %d chunks of %d KB",
                 total_size, len(ranges) + 1, chunk_size // 1024)

        # Concurrency-limited parallel fetch
        sem = asyncio.Semaphore(max_parallel)

        async def fetch_range(s, e, max_tries: int = 3):
            async with sem:
                rh_base = dict(headers) if headers else {}
                rh_base["Range"] = f"bytes={s}-{e}"
                expected = e - s + 1
                last_err = None
                for attempt in range(max_tries):
                    try:
                        raw = await self.relay("GET", url, rh_base, b"")
                        _, _, chunk_body = self._split_raw_response(raw)
                        if len(chunk_body) == expected:
                            return chunk_body
                        last_err = (
                            f"short chunk {len(chunk_body)}/{expected} B"
                        )
                    except Exception as e_:
                        last_err = repr(e_)
                    log.warning("Range %d-%d retry %d/%d: %s",
                                s, e, attempt + 1, max_tries, last_err)
                    await asyncio.sleep(0.3 * (attempt + 1))
                raise RuntimeError(
                    f"chunk {s}-{e} failed after {max_tries} tries: {last_err}"
                )

        t0 = asyncio.get_event_loop().time()
        results = await asyncio.gather(
            *[fetch_range(s, e) for s, e in ranges],
            return_exceptions=True,
        )
        elapsed = asyncio.get_event_loop().time() - t0

        # Assemble full body
        parts = [resp_body]
        for i, r in enumerate(results):
            if isinstance(r, Exception):
                log.error("Range chunk %d failed: %s", i, r)
                return self._error_response(502, f"Parallel download failed: {r}")
            parts.append(r)

        full_body = b"".join(parts)
        kbs = (len(full_body) / 1024) / elapsed if elapsed > 0 else 0
        log.info("Parallel download complete: %d B in %.2fs = %.1f KB/s",
                 len(full_body), elapsed, kbs)

        # Return as 200 OK (client sent a normal GET)
        result = f"HTTP/1.1 200 OK\r\n"
        skip = {"transfer-encoding", "connection", "keep-alive",
                "content-length", "content-encoding", "content-range"}
        for k, v in resp_hdrs.items():
            if k.lower() not in skip:
                result += f"{k}: {v}\r\n"
        result += f"Content-Length: {len(full_body)}\r\n"
        result += "\r\n"
        return result.encode() + full_body

    @staticmethod
    def _rewrite_206_to_200(raw: bytes) -> bytes:
        """Rewrite a 206 Partial Content response to 200 OK.

        Used when we probed with a synthetic Range header but the client
        never asked for one. Handing a 206 back to the browser for a plain
        GET breaks XHR/fetch on sites like x.com and Cloudflare challenges
        (they see it as an aborted/partial response). We drop the
        Content-Range header and set Content-Length to the body size.
        """
        sep = b"\r\n\r\n"
        if sep not in raw:
            return raw
        header_section, body = raw.split(sep, 1)
        lines = header_section.decode(errors="replace").split("\r\n")
        if not lines:
            return raw
        # Replace status line
        first = lines[0]
        if " 206" in first:
            lines[0] = first.replace(" 206 Partial Content", " 200 OK")\
                             .replace(" 206", " 200 OK")
        # Drop Content-Range and recalculate Content-Length
        filtered = [lines[0]]
        for ln in lines[1:]:
            low = ln.lower()
            if low.startswith("content-range:"):
                continue
            if low.startswith("content-length:"):
                continue
            filtered.append(ln)
        filtered.append(f"Content-Length: {len(body)}")
        return ("\r\n".join(filtered) + "\r\n\r\n").encode() + body


    # ── Batch collector ───────────────────────────────────────────

    async def _batch_submit(self, payload: dict) -> bytes:
        """Submit a request to the batch collector. Returns raw HTTP response."""
        # If batching is disabled (old Code.gs), go direct
        if not self._batch_enabled:
            return await self._relay_with_retry(payload)

        future = asyncio.get_event_loop().create_future()

        async with self._batch_lock:
            self._batch_pending.append((payload, future))

            if len(self._batch_pending) >= self._batch_max:
                # Batch is full — flush now
                batch = self._batch_pending[:]
                self._batch_pending.clear()
                if self._batch_task and not self._batch_task.done():
                    self._batch_task.cancel()
                self._batch_task = None
                asyncio.create_task(self._batch_send(batch))
            elif self._batch_task is None or self._batch_task.done():
                # First request in a new batch window — start timer
                self._batch_task = asyncio.create_task(self._batch_timer())

        return await future

    async def _batch_timer(self):
        """Two-tier batch window: 5ms micro + 45ms macro.

        Single requests (link clicks) get only 5ms delay.
        Burst traffic (page sub-resources, range chunks) gets a 50ms
        window to accumulate, enabling much larger batches.
        """
        # Tier 1: micro-window — detect if burst or single
        await asyncio.sleep(self._batch_window_micro)
        async with self._batch_lock:
            if len(self._batch_pending) <= 1:
                # Single request — send immediately (only 5ms delay)
                if self._batch_pending:
                    batch = self._batch_pending[:]
                    self._batch_pending.clear()
                    self._batch_task = None
                    asyncio.create_task(self._batch_send(batch))
                return

        # Tier 2: burst detected — wait more to accumulate
        await asyncio.sleep(self._batch_window_macro - self._batch_window_micro)
        async with self._batch_lock:
            if self._batch_pending:
                batch = self._batch_pending[:]
                self._batch_pending.clear()
                self._batch_task = None
                asyncio.create_task(self._batch_send(batch))

    async def _batch_send(self, batch: list):
        """Send a batch of requests. Uses fetchAll for multi, single for one."""
        if len(batch) == 1:
            payload, future = batch[0]
            try:
                result = await self._relay_with_retry(payload)
                if not future.done():
                    future.set_result(result)
            except Exception as e:
                if not future.done():
                    future.set_result(self._error_response(502, str(e)))
        else:
            log.info("Batch relay: %d requests", len(batch))
            try:
                results = await self._relay_batch([p for p, _ in batch])
                for (_, future), result in zip(batch, results):
                    if not future.done():
                        future.set_result(result)
            except Exception as e:
                log.warning("Batch relay failed, disabling batch mode. "
                            "Redeploy Code.gs for batch support. Error: %s", e)
                self._batch_enabled = False
                # Fallback: send individually
                tasks = []
                for payload, future in batch:
                    tasks.append(self._relay_fallback(payload, future))
                await asyncio.gather(*tasks)

    async def _relay_fallback(self, payload, future):
        """Fallback: relay a single request from a failed batch."""
        try:
            result = await self._relay_with_retry(payload)
            if not future.done():
                future.set_result(result)
        except Exception as e:
            if not future.done():
                future.set_result(self._error_response(502, str(e)))

    # ── Core relay with retry ─────────────────────────────────────


    async def _relay_single_h2(self, payload: dict) -> bytes:
        """Execute a relay through HTTP/2 multiplexing.

        Uses the shared H2 connection — no pool checkout needed.
        Many concurrent calls all share one TLS connection.
        """
        full_payload = dict(payload)
        full_payload["k"] = self.auth_key
        json_body = json.dumps(full_payload).encode()

        path = self._exec_path()

        log.info(f"\x1b[92mh2 path\x1b[m: {path}")
        status, headers, body = await self._h2.request(
            method="POST", path=path, host=self.http_host,
            headers={"content-type": "application/json"},
            body=json_body,
        )

        return self._parse_relay_response(body)


    async def _relay_batch(self, payloads: list[dict]) -> list[bytes]:
        log.info("\x1b[33mBATCH\x1b[m")
        """Send multiple requests in one POST using Apps Script fetchAll."""
        batch_payload = {
            "k": self.auth_key,
            "q": payloads,
        }
        json_body = json.dumps(batch_payload).encode()
        path = self._exec_path()

        # Try HTTP/2 first
        if self._h2 and self._h2.is_connected:
            try:
                status, headers, body = await asyncio.wait_for(
                    self._h2.request(
                        method="POST", path=path, host=self.http_host,
                        headers={"content-type": "application/json"},
                        body=json_body,
                    ),
                    timeout=30,
                )
                return self._parse_batch_body(body, payloads)
            except Exception as e:
                log.debug("H2 batch failed (%s), falling back to H1", e)

        # HTTP/1.1 fallback
        async with self._semaphore:
            reader, writer, created = await self._acquire()
            try:
                request = (
                    f"POST {path} HTTP/1.1\r\n"
                    f"Host: {self.http_host}\r\n"
                    f"Content-Type: application/json\r\n"
                    f"Content-Length: {len(json_body)}\r\n"
                    f"Accept-Encoding: gzip\r\n"
                    f"Connection: keep-alive\r\n"
                    f"\r\n"
                )
                writer.write(request.encode() + json_body)
                await writer.drain()

                status, resp_headers, resp_body = await self._read_http_response(reader)

                # Follow redirects
                for _ in range(5):
                    if status not in (301, 302, 303, 307, 308):
                        break
                    location = resp_headers.get("location")
                    if not location:
                        break
                    parsed = urlparse(location)
                    rpath = parsed.path + ("?" + parsed.query if parsed.query else "")
                    request = (
                        f"GET {rpath} HTTP/1.1\r\n"
                        f"Host: {parsed.netloc}\r\n"
                        f"Accept-Encoding: gzip\r\n"
                        f"Connection: keep-alive\r\n"
                        f"\r\n"
                    )
                    writer.write(request.encode())
                    await writer.drain()
                    status, resp_headers, resp_body = await self._read_http_response(reader)

                await self._release(reader, writer, created)

            except Exception:
                try:
                    writer.close()
                except Exception:
                    pass
                raise

        return self._parse_batch_body(resp_body, payloads)

    def _parse_batch_body(self, resp_body: bytes,
                          payloads: list[dict]) -> list[bytes]:
        """Parse a batch response body into individual results."""
        text = resp_body.decode(errors="replace").strip()
        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            m = re.search(r'\{.*\}', text, re.DOTALL)
            data = json.loads(m.group()) if m else None
        if not data:
            raise RuntimeError(f"Bad batch response: {text[:200]}")

        if "e" in data:
            raise RuntimeError(f"Batch error: {data['e']}")

        items = data.get("q", [])
        if len(items) != len(payloads):
            raise RuntimeError(
                f"Batch size mismatch: {len(items)} vs {len(payloads)}"
            )

        results = []
        for item in items:
            results.append(self._parse_relay_json(item))
        return results

    # ── HTTP response reading (keep-alive safe) ──────────────────



    # ── Response parsing ──────────────────────────────────────────

    def _parse_relay_response(self, body: bytes) -> bytes:
        """Parse JSON from Apps Script and reconstruct an HTTP response."""
        text = body.decode(errors="replace").strip()
        if not text:
            return self._error_response(502, "Empty response from relay")

        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            m = re.search(r'\{.*\}', text, re.DOTALL)
            if m:
                try:
                    data = json.loads(m.group())
                except json.JSONDecodeError:
                    return self._error_response(502, f"Bad JSON: {text[:200]}")
            else:
                return self._error_response(502, f"No JSON: {text[:200]}")

        return self._parse_relay_json(data)

    def _parse_relay_json(self, data: dict) -> bytes:
        """Convert a parsed relay JSON dict to raw HTTP response bytes."""
        if "e" in data:
            return self._error_response(502, f"Relay error: {data['e']}")

        status = data.get("s", 200)
        resp_headers = data.get("h", {})
        resp_body = base64.b64decode(data.get("b", ""))

        status_text = {200: "OK", 206: "Partial Content",
                       301: "Moved", 302: "Found", 304: "Not Modified",
                       400: "Bad Request", 403: "Forbidden", 404: "Not Found",
                       500: "Internal Server Error"}.get(status, "OK")
        result = f"HTTP/1.1 {status} {status_text}\r\n"

        skip = {"transfer-encoding", "connection", "keep-alive",
                "content-length", "content-encoding"}
        for k, v in resp_headers.items():
            if k.lower() in skip:
                continue
            # Apps Script returns multi-valued headers (e.g. Set-Cookie) as a
            # JavaScript array. Emit each value as its own header line.
            # A single string that holds multiple Set-Cookie values joined
            # with ", " also needs to be split, otherwise the browser sees
            # one malformed cookie and sites like x.com fail.
            values = v if isinstance(v, list) else [v]
            if k.lower() == "set-cookie":
                expanded = []
                for item in values:
                    expanded.extend(self._split_set_cookie(str(item)))
                values = expanded
            for val in values:
                result += f"{k}: {val}\r\n"
        result += f"Content-Length: {len(resp_body)}\r\n"
        result += "\r\n"
        return result.encode() + resp_body

    @staticmethod
    def _split_set_cookie(blob: str) -> list[str]:
        """Split a Set-Cookie string that may contain multiple cookies.

        Apps Script sometimes joins multiple Set-Cookie values with ", ",
        which collides with the comma that legitimately appears inside the
        `Expires` attribute (e.g. "Expires=Wed, 21 Oct 2026 ..."). We split
        only on commas that are immediately followed by a cookie name=value
        pair (token '=' ...), leaving date commas intact.
        """
        if not blob:
            return []
        # Split on ", " but only when the following text looks like the start
        # of a new cookie (a token followed by '=').
        parts = re.split(r",\s*(?=[A-Za-z0-9!#$%&'*+\-.^_`|~]+=)", blob)
        return [p.strip() for p in parts if p.strip()]

    def _split_raw_response(self, raw: bytes):
        """Split a raw HTTP response into (status, headers_dict, body)."""
        if b"\r\n\r\n" not in raw:
            return 0, {}, raw
        header_section, body = raw.split(b"\r\n\r\n", 1)
        lines = header_section.split(b"\r\n")
        m = re.search(r"\d{3}", lines[0].decode(errors="replace"))
        status = int(m.group()) if m else 0
        headers = {}
        for line in lines[1:]:
            if b":" in line:
                k, v = line.decode(errors="replace").split(":", 1)
                headers[k.strip().lower()] = v.strip()
        return status, headers, body

    def _error_response(self, status: int, message: str) -> bytes:
        body = f"<html><body><h1>{status}</h1><p>{message}</p></body></html>"
        return (
            f"HTTP/1.1 {status} Error\r\n"
            f"Content-Type: text/html\r\n"
            f"Content-Length: {len(body)}\r\n"
            f"\r\n"
            f"{body}"
        ).encode()

*/
