"""Validated public CA certificates stored with persistent Web configuration."""

import hashlib
import os
import re
import ssl
import tempfile
from pathlib import Path

from src.trading.broker_client import BrokerError

MAX_CERTIFICATE_BYTES = 65536


def save_certificate(data_dir: Path, contents: bytes) -> str:
    if not contents or len(contents) > MAX_CERTIFICATE_BYTES:
        raise BrokerError("INVALID_CERTIFICATE", "请选择不超过 64 KB 的 CA 证书文件")
    try:
        if b"-----BEGIN" in contents:
            blocks = re.findall(
                rb"-----BEGIN CERTIFICATE-----.*?-----END CERTIFICATE-----",
                contents,
                re.DOTALL,
            )
            remainder = contents
            for block in blocks:
                remainder = remainder.replace(block, b"", 1)
            if not blocks or remainder.strip():
                raise ValueError
            pem = b"\n".join(blocks).decode("ascii") + "\n"
        else:
            pem = ssl.DER_cert_to_PEM_cert(contents)
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        context.load_verify_locations(cadata=pem)
        if not context.cert_store_stats()["x509_ca"]:
            raise ValueError
    except (ValueError, UnicodeError, ssl.SSLError):
        raise BrokerError(
            "INVALID_CERTIFICATE",
            "文件不是有效的 CA 证书，请上传 PEM 或 DER 公钥证书，不要上传私钥",
        ) from None

    encoded = pem.encode("ascii")
    certificate_id = hashlib.sha256(encoded).hexdigest()
    directory = data_dir / "qmt-certificates"
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    target = directory / (certificate_id + ".pem")
    # Each certificate has an immutable content-derived name; older channels keep their CA.
    if not target.is_file():
        with tempfile.NamedTemporaryFile(dir=directory, delete=False) as stream:
            temporary = Path(stream.name)
            try:
                stream.write(encoded)
                stream.flush()
                os.fsync(stream.fileno())
            except BaseException:
                stream.close()
                temporary.unlink(missing_ok=True)
                raise
        try:
            os.replace(temporary, target)
        finally:
            temporary.unlink(missing_ok=True)
    return certificate_id


def certificate_path(data_dir: Path, certificate_id: str) -> str:
    if not re.fullmatch(r"[0-9a-f]{64}", certificate_id):
        raise BrokerError("INVALID_CERTIFICATE", "证书标识无效，请重新上传证书")
    path = data_dir / "qmt-certificates" / (certificate_id + ".pem")
    if not path.is_file():
        raise BrokerError("MISSING_CERTIFICATE", "未找到已上传的证书，请重新上传")
    return str(path)
