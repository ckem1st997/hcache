//! TLS for the listener: PEM files in, a rustls configuration out.

use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::WebPkiClientVerifier;
use rustls::{RootCertStore, ServerConfig};
use std::io;
use std::path::Path;
use std::sync::Arc;

/// Whether a client has to show a certificate of its own.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientCerts {
    /// Checked when shown, not asked for otherwise.
    Optional,
    Required,
}

fn invalid(what: &str, path: &Path, why: impl std::fmt::Display) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        format!("{what} {}: {why}", path.display()),
    )
}

fn certificates(path: &Path) -> io::Result<Vec<CertificateDer<'static>>> {
    let certs = CertificateDer::pem_file_iter(path)
        .and_then(Iterator::collect::<Result<Vec<_>, _>>)
        .map_err(|e| invalid("cannot read the certificates in", path, e))?;
    if certs.is_empty() {
        return Err(invalid(
            "no certificate in",
            path,
            "the file has no PEM certificate",
        ));
    }
    Ok(certs)
}

/// The configuration of a TLS listener: TLS 1.2 and 1.3, the certificate
/// chain in `cert` with the private key in `key`. With `client_ca`, client
/// certificates are checked against the authorities in that file.
pub fn server_config(
    cert: &Path,
    key: &Path,
    client_ca: Option<(&Path, ClientCerts)>,
) -> io::Result<Arc<ServerConfig>> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let chain = certificates(cert)?;
    // The error never quotes the file: it holds the key.
    let key = PrivateKeyDer::from_pem_file(key)
        .map_err(|e| invalid("cannot read the private key in", key, e))?;
    let builder = ServerConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .map_err(io::Error::other)?;
    let builder = match client_ca {
        None => builder.with_no_client_auth(),
        Some((path, mode)) => {
            let mut roots = RootCertStore::empty();
            for authority in certificates(path)? {
                roots
                    .add(authority)
                    .map_err(|e| invalid("not a usable authority in", path, e))?;
            }
            let verifier = WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider);
            let verifier = match mode {
                ClientCerts::Optional => verifier.allow_unauthenticated(),
                ClientCerts::Required => verifier,
            };
            builder.with_client_cert_verifier(verifier.build().map_err(io::Error::other)?)
        }
    };
    builder
        .with_single_cert(chain, key)
        .map(Arc::new)
        .map_err(|e| invalid("the certificate and the key do not fit:", cert, e))
}
