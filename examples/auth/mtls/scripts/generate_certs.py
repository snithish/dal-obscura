from __future__ import annotations

import datetime as dt
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

RUNTIME_DIR = Path("/workspace/runtime")
CERT_DIR = RUNTIME_DIR / "certs"
UTC = dt.timezone.utc


def main() -> None:
    CERT_DIR.mkdir(parents=True, exist_ok=True)
    ca_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "dal-obscura-example-ca")])
    ca_cert = (
        x509.CertificateBuilder()
        .subject_name(ca_name)
        .issuer_name(ca_name)
        .public_key(ca_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(dt.datetime.now(UTC) - dt.timedelta(minutes=1))
        .not_valid_after(dt.datetime.now(UTC) + dt.timedelta(days=2))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(ca_key, hashes.SHA256())
    )
    _write_key(CERT_DIR / "ca.key", ca_key)
    _write_cert(CERT_DIR / "ca.crt", ca_cert)
    _issue_leaf(
        "dal-obscura",
        CERT_DIR / "server.key",
        CERT_DIR / "server.crt",
        ca_key,
        ca_cert,
        dns_names=["dal-obscura"],
    )
    _issue_leaf(
        "example-client",
        CERT_DIR / "client.key",
        CERT_DIR / "client.crt",
        ca_key,
        ca_cert,
        uri_names=["urn:dal-obscura:example-client"],
    )


def _issue_leaf(
    common_name: str,
    key_path: Path,
    cert_path: Path,
    ca_key: rsa.RSAPrivateKey,
    ca_cert: x509.Certificate,
    dns_names: list[str] | None = None,
    uri_names: list[str] | None = None,
) -> None:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common_name)])
    alt_names: list[x509.GeneralName] = [
        *(x509.DNSName(name) for name in dns_names or []),
        *(x509.UniformResourceIdentifier(name) for name in uri_names or []),
    ]
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(ca_cert.subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(dt.datetime.now(UTC) - dt.timedelta(minutes=1))
        .not_valid_after(dt.datetime.now(UTC) + dt.timedelta(days=2))
        .add_extension(x509.SubjectAlternativeName(alt_names), critical=False)
        .sign(ca_key, hashes.SHA256())
    )
    _write_key(key_path, key)
    _write_cert(cert_path, cert)


def _write_key(path: Path, key: rsa.RSAPrivateKey) -> None:
    path.write_bytes(
        key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
    )


def _write_cert(path: Path, cert: x509.Certificate) -> None:
    path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))


if __name__ == "__main__":
    main()
