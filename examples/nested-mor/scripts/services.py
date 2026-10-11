"""Disposable S3 emulator with IO evidence, and an explicitly synthetic JWT issuer."""

import json
import os
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import jwt
from cryptography.hazmat.primitives.asymmetric import rsa
from jwt.algorithms import RSAAlgorithm
from werkzeug.serving import run_simple


def s3():
    os.environ["S3_IGNORE_SUBDOMAIN_BUCKETNAME"] = "true"
    from moto.server import create_backend_app

    backend = create_backend_app("s3")
    events, allowed = [], {}
    lock = threading.Lock()
    phase = "setup"

    def app(environ, respond):
        nonlocal phase, allowed
        path, method = environ["PATH_INFO"], environ["REQUEST_METHOD"]
        if path == "/__evidence":
            with lock:
                if method == "POST":
                    value = json.loads(environ["wsgi.input"].read(int(environ["CONTENT_LENGTH"])))
                    phase, allowed = value["phase"], value.get("allowed", {})
                    events.clear()
                result = {"phase": phase, "events": list(events)}
            body = json.dumps(result).encode()
            respond("200 OK", [("Content-Type", "application/json")])
            return [body]
        credential = environ.get("HTTP_AUTHORIZATION", "").partition("Credential=")[2]
        worker = credential.partition("/")[0]
        if not worker:
            return backend(environ, respond)
        denied = (
            phase != "setup" and "/other_" in path and method == "GET" and path.endswith(".parquet")
        )
        data_file = "/data/" in path or ("/delta/customers/" in path and "/_delta_log/" not in path)
        if method == "GET" and path.endswith(".parquet") and data_file:
            denied = denied or (worker in allowed and path not in allowed[worker])
        if denied:
            with lock:
                events.append({"worker": worker, "method": method, "path": path, "status": 403})
            respond("403 Forbidden", [("Content-Type", "text/plain")])
            return [b"Scenario IO guard rejected unrelated data"]

        def recorded(status, headers, exc_info=None):
            response_headers = {key.lower(): value for key, value in headers}
            with lock:
                events.append(
                    {
                        "worker": worker,
                        "method": method,
                        "path": path,
                        "query": environ.get("QUERY_STRING", ""),
                        "range": environ.get("HTTP_RANGE"),
                        "status": int(status[:3]),
                        "response_bytes": int(response_headers.get("content-length", "0")),
                    }
                )
            return respond(status, headers, exc_info)

        return backend(environ, recorded)

    run_simple("0.0.0.0", 5000, app, threaded=True)


def issuer():
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    public = json.loads(RSAAlgorithm.to_jwk(key.public_key()))
    public.update(kid="scenario", alg="RS256", use="sig")

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == "/jwks":
                result = {"keys": [public]}
            elif self.path == "/token":
                now = int(time.time())
                result = {
                    "access_token": jwt.encode(
                        {
                            "iss": "http://issuer:8080",
                            "aud": "scenario",
                            "sub": "analyst",
                            "iat": now,
                            "exp": now + 3600,
                            "groups": ["analysts"],
                        },
                        key,
                        algorithm="RS256",
                        headers={"kid": "scenario"},
                    )
                }
            else:
                self.send_error(404)
                return
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(result).encode())

    ThreadingHTTPServer(("0.0.0.0", 8080), Handler).serve_forever()


if __name__ == "__main__":
    {"s3": s3, "issuer": issuer}[sys.argv[1]]()
