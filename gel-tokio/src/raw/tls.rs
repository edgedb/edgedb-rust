pub fn read_root_cert_pem(data: &str) -> anyhow::Result<rustls::RootCertStore> {
    let mut cursor = io::Cursor::new(data);
    let open_data = rustls_pemfile::read_all(&mut cursor);
    let mut cert_store = rustls::RootCertStore::empty();
    for item in open_data {
        match item {
            Ok(rustls_pemfile::Item::X509Certificate(data)) => {
                cert_store
                    .add(data)
                    .context("certificate data found, but is not a valid root certificate")?;
            }
            Ok(rustls_pemfile::Item::Pkcs1Key(_))
            | Ok(rustls_pemfile::Item::Pkcs8Key(_))
            | Ok(rustls_pemfile::Item::Sec1Key(_)) => {
                log::debug!("Skipping private key in cert data");
            }
            Ok(rustls_pemfile::Item::Crl(_)) => {
                log::debug!("Skipping CRL in cert data");
            }
            Ok(_) => {
                log::debug!("Skipping unknown item cert data");
            }
            Err(e) => {
                log::error!("could not parse item in PEM file: {:?}", e);
            }
        }
    }
    Ok(cert_store)
}