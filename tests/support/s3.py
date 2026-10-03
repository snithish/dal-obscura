"""Disposable HTTP S3 emulator; exercises native clients, never mocks their IO."""

from contextlib import contextmanager
from dataclasses import dataclass
from multiprocessing import get_context
from multiprocessing.managers import DictProxy, ListProxy
from urllib.parse import unquote, urlsplit
from uuid import uuid4

import boto3
from moto.server import ThreadedMotoServer


@dataclass
class DataReadGuard:
    allowed: DictProxy
    reads: ListProxy

    def assign(self, tasks):
        for index, task in enumerate(tasks):
            self.allowed["dal-worker-" + str(index)] = [
                "/" + urlsplit(path).netloc + unquote(urlsplit(path).path)
                for path in task.to_json()["files"]
            ]

    def assert_disjoint_reads(self):
        seen = {}
        for worker, path in self.reads:
            seen.setdefault(worker, set()).add(path)
        assert seen == {worker: set(paths) for worker, paths in self.allowed.items()}


@contextmanager
def data_read_guard():
    with get_context("spawn").Manager() as manager:
        yield DataReadGuard(manager.dict(), manager.list())


def _guard_data_reads(app, guard):
    def guarded(environ, start_response):
        path = unquote(environ["PATH_INFO"])
        credentials = environ.get("HTTP_AUTHORIZATION", "").partition("Credential=")[2]
        worker = credentials.partition("/")[0]
        allowed = dict(guard.allowed)
        data_paths = {path for paths in allowed.values() for path in paths}
        if environ["REQUEST_METHOD"] == "GET" and path in data_paths:
            if path not in allowed.get(worker, []):
                start_response("403 Forbidden", [("Content-Type", "text/plain")])
                return [b"Worker attempted to read another task's data file"]
            guard.reads.append((worker, path))
        return app(environ, start_response)

    return guarded


def _serve(queue, stop, guard):
    server = ThreadedMotoServer(ip_address="127.0.0.1", port=0, verbose=False)
    server.start()
    if guard is not None:
        assert server._server is not None
        server._server.app = _guard_data_reads(server._server.app, guard)
    queue.put(server.get_host_and_port())
    try:
        stop.wait()
    finally:
        server.stop()


@contextmanager
def s3_bucket(monkeypatch, *, guard=None):
    context = get_context("spawn")
    queue, stop = context.Queue(), context.Event()
    process = context.Process(target=_serve, args=(queue, stop, guard))
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
