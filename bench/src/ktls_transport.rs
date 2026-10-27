//! [`KtlsConnector`]: an HTTP transport for object_store whose TLS the
//! kernel decrypts (kTLS), so that a response's body lands, decrypted, in
//! the buffer it is handed over in.
//!
//! A byte of a body costs one pass: the kernel decrypts it from its
//! receive buffers into ours. object_store's own transport, reqwest over
//! hyper and rustls, costs five before the body reaches its caller: the
//! kernel's copy of the ciphertext into rustls's buffer, rustls decrypting
//! it in place, its copies of the plaintext into its queue and from there
//! into hyper's buffer, and `bytes()` joining hyper's pieces into one.
//!
//! The handshake is rustls's, with ring and the system's roots as reqwest
//! sets it up; then the session's keys go to the kernel, and the
//! connection speaks HTTP/1.1 ([`Http1Connection`]). Connections are kept
//! per host and reused, one exchange at a time each, new ones spread over
//! the addresses the host resolves to. It ignores [`ClientOptions`]'
//! timeouts and proxies. Given a runtime, it runs every exchange there,
//! object_store's [`SpawnService`](object_store::client::SpawnService)
//! moving the bodies over. kTLS is Linux's:
//! elsewhere the connector fails.
//!
//! The `ktls` crate ignores a TLS 1.3 `KeyUpdate`, which then fails the
//! connection's later reads; object_store retries such a request.

use object_store::ClientOptions;
#[cfg(target_os = "linux")]
use object_store::client::SpawnService;
use object_store::client::{HttpClient, HttpConnector};
use tokio::runtime::Handle;

/// Gives a store's builder kTLS clients:
/// `with_http_connector(KtlsConnector::new(io))`.
#[derive(Debug)]
pub struct KtlsConnector {
    // Only Linux's clients run on it.
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    io: Option<Handle>,
}

impl KtlsConnector {
    /// Clients that run on `io`'s runtime if given, else on whichever
    /// calls them.
    pub fn new(io: Option<Handle>) -> Self {
        Self { io }
    }
}

impl HttpConnector for KtlsConnector {
    #[cfg(target_os = "linux")]
    fn connect(&self, _options: &ClientOptions) -> object_store::Result<HttpClient> {
        let transport =
            linux::KtlsTransport::new().map_err(|source| object_store::Error::Generic {
                store: "kTLS",
                source: source.into(),
            })?;
        Ok(match &self.io {
            Some(io) => HttpClient::new(SpawnService::new(transport, io.clone())),
            None => HttpClient::new(transport),
        })
    }

    #[cfg(not(target_os = "linux"))]
    fn connect(&self, _options: &ClientOptions) -> object_store::Result<HttpClient> {
        Err(object_store::Error::NotSupported {
            source: "kTLS is Linux's".into(),
        })
    }
}

#[cfg(target_os = "linux")]
mod linux {
    use std::collections::HashMap;
    use std::fmt;
    use std::io;
    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex, PoisonError};

    use async_trait::async_trait;
    use ktls::{CorkStream, KtlsStream};
    use object_store::client::{HttpError, HttpErrorKind, HttpRequest, HttpResponse, HttpService};
    use rustls::pki_types::ServerName;
    use rustls::{ClientConfig, RootCertStore};
    use tokio::net::TcpStream;
    use tokio_rustls::TlsConnector;

    use crate::http1_connection::{ExchangeError, Http1Connection, OutgoingRequest};

    type KtlsConnection = Http1Connection<KtlsStream<TcpStream>>;

    /// object_store's HTTP client over kTLS connections; its clones share
    /// them.
    #[derive(Clone)]
    pub(super) struct KtlsTransport {
        tls: Arc<ClientConfig>,
        idle: Arc<IdleConnections>,
        /// Counts new connections, to spread them over a host's addresses.
        opened: Arc<AtomicUsize>,
    }

    impl KtlsTransport {
        pub(super) fn new() -> Result<Self, rustls::Error> {
            let mut roots = RootCertStore::empty();
            roots.add_parsable_certificates(rustls_native_certs::load_native_certs().certs);
            let mut tls = ClientConfig::builder_with_provider(Arc::new(
                rustls::crypto::ring::default_provider(),
            ))
            .with_safe_default_protocol_versions()?
            .with_root_certificates(roots)
            .with_no_client_auth();
            // The kernel takes the session's keys once the handshake is done.
            tls.enable_secret_extraction = true;
            tls.alpn_protocols = vec![b"http/1.1".to_vec()];
            Ok(Self {
                tls: Arc::new(tls),
                idle: Arc::default(),
                opened: Arc::default(),
            })
        }

        /// A new connection to `request`'s host, its TLS handed to the
        /// kernel.
        async fn open(&self, request: &OutgoingRequest) -> Result<KtlsConnection, HttpError> {
            let connect = |error: io::Error| HttpError::new(HttpErrorKind::Connect, error);
            let addresses: Vec<SocketAddr> =
                tokio::net::lookup_host((request.host(), request.port()))
                    .await
                    .map_err(connect)?
                    .collect();
            let address = addresses
                .get(self.opened.fetch_add(1, Ordering::Relaxed) % addresses.len().max(1))
                .ok_or_else(|| connect(io::ErrorKind::AddrNotAvailable.into()))?;
            let tcp = TcpStream::connect(address).await.map_err(connect)?;
            tcp.set_nodelay(true).map_err(connect)?;
            let name = ServerName::try_from(request.host().to_owned())
                .map_err(|error| connect(io::Error::new(io::ErrorKind::InvalidInput, error)))?;
            // CorkStream stops rustls at a record's end, so that whatever it
            // read past the handshake is drained, not lost, when the kernel
            // takes over.
            let tls = TlsConnector::from(Arc::clone(&self.tls))
                .connect(name, CorkStream::new(tcp))
                .await
                .map_err(connect)?;
            let stream = ktls::config_ktls_client(tls)
                .await
                .map_err(|error| connect(io::Error::other(error)))?;
            Ok(Http1Connection::new(stream))
        }
    }

    #[async_trait]
    impl HttpService for KtlsTransport {
        async fn call(&self, request: HttpRequest) -> Result<HttpResponse, HttpError> {
            let request = OutgoingRequest::encode(request).await?;
            let authority = request.authority();
            let (mut connection, kept) = match self.idle.take(&authority) {
                Some(connection) => (connection, true),
                None => (self.open(&request).await?, false),
            };
            let mut answer = connection.exchange(&request).await;
            // A kept connection the server closed meanwhile fails before
            // answering: the request goes once more, on a new one.
            if kept && request.idempotent() && matches!(answer, Err(ExchangeError::Unanswered(_))) {
                connection = self.open(&request).await?;
                answer = connection.exchange(&request).await;
            }
            let answer = answer?;
            if answer.reusable() {
                self.idle.put(authority, connection);
            }
            Ok(answer.into_response())
        }
    }

    impl fmt::Debug for KtlsTransport {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("KtlsTransport")
                .field("opened", &self.opened)
                .finish_non_exhaustive()
        }
    }

    /// Connections between exchanges, by the host and port they reach.
    #[derive(Default)]
    struct IdleConnections {
        by_authority: Mutex<HashMap<String, Vec<KtlsConnection>>>,
    }

    impl IdleConnections {
        fn take(&self, authority: &str) -> Option<KtlsConnection> {
            self.by_authority
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .get_mut(authority)?
                .pop()
        }

        fn put(&self, authority: String, connection: KtlsConnection) {
            self.by_authority
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .entry(authority)
                .or_default()
                .push(connection);
        }
    }
}
