//! Sets `spate_openssl_vendored` when openssl-sys reports that it built the
//! linked OpenSSL from source.

fn main() {
    println!("cargo::rustc-check-cfg=cfg(spate_openssl_vendored)");
    println!("cargo::rerun-if-env-changed=DEP_OPENSSL_VENDORED");
    if std::env::var_os("DEP_OPENSSL_VENDORED").is_some_and(|v| v == "1") {
        println!("cargo::rustc-cfg=spate_openssl_vendored");
    }
}
