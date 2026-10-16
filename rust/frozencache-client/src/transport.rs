//! TCP connection, optionally wrapped in TLS.

use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{CryptoProvider, verify_tls12_signature, verify_tls13_signature};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{ClientConfig, DigitallySignedStruct, SignatureScheme};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

use crate::error::{Error, Result};

/// A plain or TLS stream.
pub(crate) trait Transport: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Transport for T {}

pub(crate) async fn open(
    host: &str,
    port: u16,
    use_tls: bool,
    validate_server_certificate: bool,
    connect_timeout: Duration,
) -> Result<Box<dyn Transport>> {
    let address = format!("{host}:{port}");
    let deadline = tokio::time::Instant::now() + connect_timeout;

    let tcp = tokio::time::timeout_at(deadline, connect_tcp(host, port, &address))
        .await
        .map_err(|_| Error::Connect {
            address: address.clone(),
            source: std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!("timed out after {connect_timeout:?}"),
            ),
        })??;

    // low latency for small request/response messages, as the server does
    tcp.set_nodelay(true).map_err(|source| Error::Connect {
        address: address.clone(),
        source,
    })?;

    if !use_tls {
        return Ok(Box::new(tcp));
    }

    let server_name = ServerName::try_from(host.to_owned())
        .map_err(|e| Error::InvalidArgument(format!("invalid host name {host}: {e}")))?;

    // Under the same deadline: a server without TLS doesn't reject the handshake, it reads the ClientHello as a
    // message header whose "length" it then waits for, while the client waits for the ServerHello.
    let handshake =
        TlsConnector::from(tls_config(validate_server_certificate)?).connect(server_name, tcp);
    let tls = match tokio::time::timeout_at(deadline, handshake).await {
        Ok(Ok(tls)) => tls,
        Ok(Err(e)) => {
            return Err(Error::Tls {
                address,
                message: e.to_string(),
            });
        }
        Err(_) => {
            return Err(Error::Tls {
                address,
                message: format!(
                    "no TLS answer within {connect_timeout:?}, the server probably doesn't use TLS"
                ),
            });
        }
    };

    Ok(Box::new(tls))
}

/// Accepts a host name, an IPv4 or an IPv6 address. Like the .NET client, prefers IPv4 addresses of a host name,
/// then tries the others.
async fn connect_tcp(host: &str, port: u16, address: &str) -> Result<TcpStream> {
    let candidates: Vec<SocketAddr> = match host.parse::<IpAddr>() {
        Ok(ip) => vec![SocketAddr::new(ip, port)],
        Err(_) => {
            let mut resolved: Vec<SocketAddr> = tokio::net::lookup_host((host, port))
                .await
                .map_err(|source| Error::Connect {
                    address: address.to_owned(),
                    source,
                })?
                .collect();
            resolved.sort_by_key(|a| !a.is_ipv4());
            resolved
        }
    };

    let mut last_error = std::io::Error::new(
        std::io::ErrorKind::NotFound,
        format!("unable to resolve host {host}"),
    );
    for candidate in candidates {
        match TcpStream::connect(candidate).await {
            Ok(stream) => return Ok(stream),
            Err(e) => last_error = e,
        }
    }
    Err(Error::Connect {
        address: address.to_owned(),
        source: last_error,
    })
}

fn crypto_provider() -> Arc<CryptoProvider> {
    static PROVIDER: OnceLock<Arc<CryptoProvider>> = OnceLock::new();
    PROVIDER
        .get_or_init(|| Arc::new(rustls::crypto::ring::default_provider()))
        .clone()
}

/// TLS configurations are expensive to build (the platform verifier loads the OS trust store), so each kind is
/// built once.
fn tls_config(validate_server_certificate: bool) -> Result<Arc<ClientConfig>> {
    static VALIDATING: OnceLock<std::result::Result<Arc<ClientConfig>, String>> = OnceLock::new();
    static NOT_VALIDATING: OnceLock<std::result::Result<Arc<ClientConfig>, String>> =
        OnceLock::new();

    let cell = if validate_server_certificate {
        &VALIDATING
    } else {
        &NOT_VALIDATING
    };
    cell.get_or_init(|| build_tls_config(validate_server_certificate).map_err(|e| e.to_string()))
        .clone()
        .map_err(|message| Error::Tls {
            address: String::new(),
            message,
        })
}

fn build_tls_config(
    validate_server_certificate: bool,
) -> std::result::Result<Arc<ClientConfig>, rustls::Error> {
    let provider = crypto_provider();

    let verifier: Arc<dyn ServerCertVerifier> = if validate_server_certificate {
        // the operating system's trust store, as the .NET client's SslStream uses
        Arc::new(rustls_platform_verifier::Verifier::new(provider.clone())?)
    } else {
        Arc::new(AcceptAnyCertificate(provider.clone()))
    };

    let config = ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()?
        .dangerous()
        .with_custom_certificate_verifier(verifier)
        .with_no_client_auth();
    Ok(Arc::new(config))
}

/// Accepts any server certificate, like the .NET client with `validateServerCertificate: false`: only for testing
/// against a self-signed certificate. The handshake signatures are still checked, so the connection is encrypted
/// with the holder of the certificate's key; only the identity of that holder is not verified.
#[derive(Debug)]
struct AcceptAnyCertificate(Arc<CryptoProvider>);

impl ServerCertVerifier for AcceptAnyCertificate {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> std::result::Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls12_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls13_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}
