//! [`Http1Connection`]: one HTTP/1.1 connection's exchanges, a request and
//! its response at a time, over any byte stream; as much of HTTP/1.1 as an
//! object store's answers need.
//!
//! A response's body is read whole into one buffer, sized from its
//! `Content-Length`, which the stream fills in place: over kTLS
//! (`ktls_transport`), the kernel decrypts straight into it, and the body
//! reaches object_store as the one piece it is, which its `bytes()` takes
//! without a copy. A response read whole is what object_store's `bytes()`
//! would hold anyway; a body framed by `Transfer-Encoding: chunked`, as
//! listings can be, is gathered from its chunks.

use std::fmt::Write as _;
use std::io;

use bytes::{BufMut, Bytes, BytesMut};
use http::header::{CONNECTION, CONTENT_LENGTH, HOST, TRANSFER_ENCODING};
use http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode, Version};
use http_body_util::BodyExt;
use object_store::client::{HttpError, HttpErrorKind, HttpRequest, HttpResponse, HttpResponseBody};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// The response headers one read takes, before more are read.
const HEAD_READ: usize = 16 << 10;
/// The longest line of a chunked body's framing.
const MAX_LINE: usize = 4 << 10;
/// The most headers a response may have.
const MAX_HEADERS: usize = 64;

/// A request as its bytes go out, and what its answer needs known of it.
pub(crate) struct OutgoingRequest {
    host: String,
    port: u16,
    head_only: bool,
    idempotent: bool,
    wire: Vec<u8>,
}

impl OutgoingRequest {
    /// `request` as HTTP/1.1 puts it on the wire, with `Host` and, where
    /// the request has a body or a method that may carry one,
    /// `Content-Length`. Only `https` URLs: the transports here speak TLS.
    pub(crate) async fn encode(request: HttpRequest) -> Result<Self, HttpError> {
        let (parts, body) = request.into_parts();
        let body = match body.as_bytes() {
            Some(bytes) => bytes.clone(),
            None => body.collect().await?.to_bytes(),
        };
        let uri = &parts.uri;
        if uri.scheme_str() != Some("https") {
            return Err(Self::invalid(format!("{uri}: only https URLs")));
        }
        let host = uri
            .host()
            .ok_or_else(|| Self::invalid(format!("{uri}: no host")))?;
        let port = uri.port_u16().unwrap_or(443);
        let target = uri.path_and_query().map_or("/", |target| target.as_str());

        let mut head = format!("{} {target} HTTP/1.1\r\n", parts.method);
        if !parts.headers.contains_key(HOST) {
            match uri.port_u16() {
                Some(port) => write!(head, "host: {host}:{port}\r\n"),
                None => write!(head, "host: {host}\r\n"),
            }
            .expect("writing to a String");
        }
        let mut wire = head.into_bytes();
        for (name, value) in &parts.headers {
            wire.extend_from_slice(name.as_str().as_bytes());
            wire.extend_from_slice(b": ");
            wire.extend_from_slice(value.as_bytes());
            wire.extend_from_slice(b"\r\n");
        }
        let bodiless = matches!(parts.method, Method::GET | Method::HEAD);
        if !parts.headers.contains_key(CONTENT_LENGTH) && !(bodiless && body.is_empty()) {
            wire.extend_from_slice(format!("content-length: {}\r\n", body.len()).as_bytes());
        }
        wire.extend_from_slice(b"\r\n");
        wire.extend_from_slice(&body);
        Ok(Self {
            host: host.to_owned(),
            port,
            head_only: parts.method == Method::HEAD,
            idempotent: parts.method.is_idempotent(),
            wire,
        })
    }

    pub(crate) fn host(&self) -> &str {
        &self.host
    }

    pub(crate) fn port(&self) -> u16 {
        self.port
    }

    /// The host and port connections to it are kept by.
    pub(crate) fn authority(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }

    /// Whether sending it twice does what sending it once does, so that it
    /// may go again on another connection.
    pub(crate) fn idempotent(&self) -> bool {
        self.idempotent
    }

    fn invalid(message: String) -> HttpError {
        HttpError::new(
            HttpErrorKind::Request,
            io::Error::new(io::ErrorKind::InvalidInput, message),
        )
    }
}

/// An exchange that failed: before any byte of the answer came back, so
/// that the request may go again on another connection, as when a kept
/// connection was closed by the server meanwhile; or after.
pub(crate) enum ExchangeError {
    Unanswered(io::Error),
    Failed(HttpError),
}

impl From<ExchangeError> for HttpError {
    fn from(error: ExchangeError) -> Self {
        match error {
            ExchangeError::Unanswered(error) => HttpError::new(HttpErrorKind::Request, error),
            ExchangeError::Failed(error) => error,
        }
    }
}

/// A response read whole, and whether its connection may carry another.
pub(crate) struct Answer {
    response: HttpResponse,
    reusable: bool,
}

impl Answer {
    pub(crate) fn reusable(&self) -> bool {
        self.reusable
    }

    pub(crate) fn into_response(self) -> HttpResponse {
        self.response
    }
}

/// An HTTP/1.1 connection over `S`, carrying one exchange at a time.
pub(crate) struct Http1Connection<S> {
    stream: S,
    /// Bytes read past the end of what a read wanted: the start of what
    /// comes next.
    unread: BytesMut,
}

impl<S: AsyncRead + AsyncWrite + Unpin> Http1Connection<S> {
    pub(crate) fn new(stream: S) -> Self {
        Self {
            stream,
            unread: BytesMut::new(),
        }
    }

    /// Sends `request` and reads its response, body and all.
    pub(crate) async fn exchange(
        &mut self,
        request: &OutgoingRequest,
    ) -> Result<Answer, ExchangeError> {
        self.stream
            .write_all(&request.wire)
            .await
            .map_err(ExchangeError::Unanswered)?;
        let head = self.read_head().await?;
        let framing = head.framing(request.head_only)?;
        let reusable = head.keeps_connection(&framing);
        let body = self.read_body(framing).await.map_err(|error| {
            ExchangeError::Failed(HttpError::new(HttpErrorKind::Request, error))
        })?;
        Ok(Answer {
            response: head.into_response(body),
            reusable,
        })
    }

    /// The response's status line and headers, read until they end.
    async fn read_head(&mut self) -> Result<ResponseHead, ExchangeError> {
        loop {
            let mut headers = [httparse::EMPTY_HEADER; MAX_HEADERS];
            let mut parsed = httparse::Response::new(&mut headers);
            match parsed.parse(&self.unread) {
                Ok(httparse::Status::Complete(length)) => {
                    let head = ResponseHead::of(&parsed).map_err(ExchangeError::Failed)?;
                    let _ = self.unread.split_to(length);
                    return Ok(head);
                }
                Ok(httparse::Status::Partial) => {}
                Err(error) => {
                    return Err(ExchangeError::Failed(ResponseHead::malformed(
                        error.to_string(),
                    )));
                }
            }
            let answered = !self.unread.is_empty();
            let read = self.read_more(HEAD_READ).await;
            match read {
                Ok(0) if !answered => {
                    return Err(ExchangeError::Unanswered(
                        io::ErrorKind::UnexpectedEof.into(),
                    ));
                }
                Ok(0) => {
                    return Err(ExchangeError::Failed(ResponseHead::malformed(
                        "the connection ended in the headers".to_owned(),
                    )));
                }
                Ok(_) => {}
                Err(error) if !answered => return Err(ExchangeError::Unanswered(error)),
                Err(error) => {
                    return Err(ExchangeError::Failed(HttpError::new(
                        HttpErrorKind::Request,
                        error,
                    )));
                }
            }
        }
    }

    async fn read_body(&mut self, framing: BodyFraming) -> io::Result<Bytes> {
        match framing {
            BodyFraming::Empty => Ok(Bytes::new()),
            BodyFraming::Length(length) => {
                let mut body = BytesMut::with_capacity(length);
                self.read_into(&mut body, length).await?;
                Ok(body.freeze())
            }
            BodyFraming::Chunked => self.read_chunked().await,
            BodyFraming::UntilClose => {
                let mut body = BytesMut::new();
                body.extend_from_slice(&self.unread.split());
                loop {
                    body.reserve(HEAD_READ);
                    if self.stream.read_buf(&mut body).await? == 0 {
                        return Ok(body.freeze());
                    }
                }
            }
        }
    }

    /// A body framed in chunks, each a line of its size in hex, its bytes,
    /// and a line's end; a chunk of size 0, then trailers, end it.
    async fn read_chunked(&mut self) -> io::Result<Bytes> {
        let mut body = BytesMut::new();
        loop {
            let line = self.read_line().await?;
            let size = line.split(';').next().unwrap_or_default().trim();
            let size = usize::from_str_radix(size, 16)
                .map_err(|_| Self::malformed(format!("chunk size {line:?}")))?;
            if size == 0 {
                while !self.read_line().await?.is_empty() {}
                return Ok(body.freeze());
            }
            body.reserve(size);
            self.read_into(&mut body, size).await?;
            if !self.read_line().await?.is_empty() {
                return Err(Self::malformed("a chunk longer than its size".to_owned()));
            }
        }
    }

    /// Appends the next `count` bytes to `body`: what was read already
    /// first, then the rest straight from the stream into `body`'s spare
    /// capacity.
    async fn read_into(&mut self, body: &mut BytesMut, count: usize) -> io::Result<()> {
        let early = self.unread.len().min(count);
        body.extend_from_slice(&self.unread.split_to(early));
        let mut remaining = count - early;
        while remaining > 0 {
            let read = self
                .stream
                .read_buf(&mut (&mut *body).limit(remaining))
                .await?;
            if read == 0 {
                return Err(io::ErrorKind::UnexpectedEof.into());
            }
            remaining -= read;
        }
        Ok(())
    }

    /// The next line, without its `\r\n`.
    async fn read_line(&mut self) -> io::Result<String> {
        loop {
            if let Some(end) = self.unread.windows(2).position(|pair| pair == b"\r\n") {
                let line = self.unread.split_to(end + 2);
                return String::from_utf8(line[..end].to_vec())
                    .map_err(|_| Self::malformed("a line not in UTF-8".to_owned()));
            }
            if self.unread.len() > MAX_LINE {
                return Err(Self::malformed("a line too long".to_owned()));
            }
            if self.read_more(MAX_LINE).await? == 0 {
                return Err(io::ErrorKind::UnexpectedEof.into());
            }
        }
    }

    /// Reads what the stream has, up to `at_most` bytes, after what was
    /// read already; 0 at the stream's end.
    async fn read_more(&mut self, at_most: usize) -> io::Result<usize> {
        self.unread.reserve(at_most);
        self.stream
            .read_buf(&mut (&mut self.unread).limit(at_most))
            .await
    }

    fn malformed(message: String) -> io::Error {
        io::Error::new(io::ErrorKind::InvalidData, message)
    }
}

/// How a response's body ends.
#[derive(Debug, PartialEq)]
enum BodyFraming {
    Empty,
    Length(usize),
    Chunked,
    UntilClose,
}

/// A response's status line and headers.
struct ResponseHead {
    status: StatusCode,
    headers: HeaderMap,
}

impl ResponseHead {
    fn of(parsed: &httparse::Response<'_, '_>) -> Result<Self, HttpError> {
        let status = parsed
            .code
            .and_then(|code| StatusCode::from_u16(code).ok())
            .ok_or_else(|| Self::malformed("no status".to_owned()))?;
        let mut headers = HeaderMap::with_capacity(parsed.headers.len());
        for header in parsed.headers.iter() {
            let name = HeaderName::from_bytes(header.name.as_bytes())
                .map_err(|error| Self::malformed(error.to_string()))?;
            let value = HeaderValue::from_bytes(header.value)
                .map_err(|error| Self::malformed(error.to_string()))?;
            headers.append(name, value);
        }
        Ok(Self { status, headers })
    }

    /// How the body ends: none after `HEAD`, 1xx, 204 and 304; else by
    /// chunks, by `Content-Length`, or by the connection's end.
    fn framing(&self, head_only: bool) -> Result<BodyFraming, ExchangeError> {
        if head_only
            || self.status.is_informational()
            || self.status == StatusCode::NO_CONTENT
            || self.status == StatusCode::NOT_MODIFIED
        {
            return Ok(BodyFraming::Empty);
        }
        if self.has_token(&TRANSFER_ENCODING, "chunked") {
            return Ok(BodyFraming::Chunked);
        }
        match self.headers.get(CONTENT_LENGTH) {
            Some(length) => length
                .to_str()
                .ok()
                .and_then(|length| length.parse().ok())
                .map(BodyFraming::Length)
                .ok_or_else(|| {
                    ExchangeError::Failed(Self::malformed(format!("Content-Length {length:?}")))
                }),
            None => Ok(BodyFraming::UntilClose),
        }
    }

    /// Whether the connection may carry another exchange once the body is
    /// read: HTTP/1.1 keeps it, unless the response says `close` or ends
    /// its body by closing.
    fn keeps_connection(&self, framing: &BodyFraming) -> bool {
        *framing != BodyFraming::UntilClose && !self.has_token(&CONNECTION, "close")
    }

    fn has_token(&self, name: &HeaderName, token: &str) -> bool {
        self.headers.get_all(name).iter().any(|value| {
            value.to_str().is_ok_and(|value| {
                value
                    .split(',')
                    .any(|item| item.trim().eq_ignore_ascii_case(token))
            })
        })
    }

    fn into_response(self, body: Bytes) -> HttpResponse {
        let mut response = HttpResponse::new(HttpResponseBody::from(body));
        *response.status_mut() = self.status;
        *response.version_mut() = Version::HTTP_11;
        *response.headers_mut() = self.headers;
        response
    }

    fn malformed(message: String) -> HttpError {
        HttpError::new(
            HttpErrorKind::Decode,
            io::Error::new(io::ErrorKind::InvalidData, message),
        )
    }
}

#[cfg(test)]
mod tests {
    use object_store::client::HttpRequestBody;
    use tokio::io::{DuplexStream, duplex};

    use super::*;

    fn request(method: &str, uri: &str) -> HttpRequest {
        http::Request::builder()
            .method(method)
            .uri(uri)
            .header("range", "bytes=0-9")
            .body(HttpRequestBody::empty())
            .unwrap()
    }

    /// A connection whose server has already answered with `answers`.
    async fn answered(answers: &[u8]) -> (Http1Connection<DuplexStream>, DuplexStream) {
        let (client, mut server) = duplex(1 << 20);
        server.write_all(answers).await.unwrap();
        (Http1Connection::new(client), server)
    }

    async fn body(answer: Answer) -> Bytes {
        answer.into_response().into_body().bytes().await.unwrap()
    }

    #[tokio::test]
    async fn a_get_goes_out_with_its_host_and_no_length() {
        let request = OutgoingRequest::encode(request("GET", "https://bucket.example/key?x=1"))
            .await
            .unwrap();
        assert_eq!(
            String::from_utf8(request.wire).unwrap(),
            "GET /key?x=1 HTTP/1.1\r\nhost: bucket.example\r\nrange: bytes=0-9\r\n\r\n"
        );
        assert_eq!(
            (request.host.as_str(), request.port),
            ("bucket.example", 443)
        );
    }

    #[tokio::test]
    async fn plain_http_is_refused() {
        assert!(
            OutgoingRequest::encode(request("GET", "http://bucket.example/key"))
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn bodies_by_length_come_whole_and_keep_the_connection() {
        let (mut connection, _server) = answered(
            b"HTTP/1.1 206 Partial Content\r\ncontent-length: 5\r\n\r\nhello\
              HTTP/1.1 200 OK\r\ncontent-length: 3\r\n\r\nbye",
        )
        .await;
        let get = OutgoingRequest::encode(request("GET", "https://h/k"))
            .await
            .unwrap();
        let first = connection.exchange(&get).await.ok().unwrap();
        assert!(first.reusable());
        assert_eq!(first.response.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(body(first).await, "hello");
        let second = connection.exchange(&get).await.ok().unwrap();
        assert_eq!(body(second).await, "bye");
    }

    #[tokio::test]
    async fn a_head_has_no_body_whatever_its_length() {
        let (mut connection, _server) =
            answered(b"HTTP/1.1 200 OK\r\ncontent-length: 100\r\netag: \"e\"\r\n\r\n").await;
        let head = OutgoingRequest::encode(request("HEAD", "https://h/k"))
            .await
            .unwrap();
        let answer = connection.exchange(&head).await.ok().unwrap();
        assert!(answer.reusable());
        assert_eq!(answer.response.headers()["etag"], "\"e\"");
        assert!(body(answer).await.is_empty());
    }

    #[tokio::test]
    async fn chunked_bodies_are_gathered() {
        let (mut connection, _server) = answered(
            b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n\
              5\r\nhello\r\n7;ext=1\r\n, world\r\n0\r\n\r\n",
        )
        .await;
        let get = OutgoingRequest::encode(request("GET", "https://h/k"))
            .await
            .unwrap();
        let answer = connection.exchange(&get).await.ok().unwrap();
        assert!(answer.reusable());
        assert_eq!(body(answer).await, "hello, world");
    }

    #[tokio::test]
    async fn a_body_until_close_ends_the_connection() {
        let (mut connection, mut server) =
            answered(b"HTTP/1.1 200 OK\r\nconnection: close\r\n\r\nall of it").await;
        server.shutdown().await.unwrap();
        let get = OutgoingRequest::encode(request("GET", "https://h/k"))
            .await
            .unwrap();
        let answer = connection.exchange(&get).await.ok().unwrap();
        assert!(!answer.reusable());
        assert_eq!(body(answer).await, "all of it");
    }

    #[tokio::test]
    async fn a_connection_closed_before_answering_leaves_it_unanswered() {
        let (mut connection, server) = answered(b"").await;
        drop(server);
        let get = OutgoingRequest::encode(request("GET", "https://h/k"))
            .await
            .unwrap();
        assert!(matches!(
            connection.exchange(&get).await,
            Err(ExchangeError::Unanswered(_))
        ));
    }

    #[tokio::test]
    async fn a_body_cut_short_fails() {
        let (mut connection, mut server) =
            answered(b"HTTP/1.1 200 OK\r\ncontent-length: 10\r\n\r\nshort").await;
        server.shutdown().await.unwrap();
        let get = OutgoingRequest::encode(request("GET", "https://h/k"))
            .await
            .unwrap();
        assert!(matches!(
            connection.exchange(&get).await,
            Err(ExchangeError::Failed(_))
        ));
    }
}
