"""Disposable HTTP S3 emulator; exercises native clients, never mocks their IO."""

from contextlib import contextmanager
from multiprocessing import get_context
from uuid import uuid4

import boto3
from moto.server import ThreadedMotoServer


def _serve(queue, stop):
    server = ThreadedMotoServer(ip_address="127.0.0.1", port=0, verbose=False)
    server.start()
    queue.put(server.get_host_and_port())
    try:
        stop.wait()
    finally:
        server.stop()


@contextmanager
def s3_bucket(monkeypatch):
    context = get_context("spawn")
    queue, stop = context.Queue(), context.Event()
    process = context.Process(target=_serve, args=(queue, stop))
    process.start()
    try:
        host, port = queue.get(timeout=20)
        endpoint = f"http://{host}:{port}"
        with monkeypatch.context() as environment:
            for key, value in {
                "AWS_ACCESS_KEY_ID": "dal-test",
                "AWS_SECRET_ACCESS_KEY": "dal-test-secret",
                "AWS_DEFAULT_REGION": "us-east-1",
                "AWS_REGION": "us-east-1",
                "AWS_ENDPOINT_URL": endpoint,
                "AWS_ENDPOINT_URL_S3": endpoint,
                "AWS_ALLOW_HTTP": "true",
                "AWS_EC2_METADATA_DISABLED": "true",
            }.items():
                environment.setenv(key, value)
            for key in (
                "AWS_SESSION_TOKEN",
                "AWS_PROFILE",
                "AWS_ROLE_ARN",
                "AWS_WEB_IDENTITY_TOKEN_FILE",
            ):
                environment.delenv(key, raising=False)
            client = boto3.client("s3", endpoint_url=endpoint, region_name="us-east-1")
            try:
                bucket = "dal-test-" + uuid4().hex
                client.create_bucket(Bucket=bucket)
                yield client, bucket
            finally:
                client.close()
    finally:
        stop.set()
        process.join(timeout=10)
        if process.is_alive():
            process.terminate()
            process.join(timeout=5)
        queue.close()


def upload_directory(client, bucket, directory, prefix):
    for path in directory.rglob("*"):
        if path.is_file():
            client.put_object(
                Bucket=bucket,
                Key=prefix + "/" + path.relative_to(directory).as_posix(),
                Body=path.read_bytes(),
            )
