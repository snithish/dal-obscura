from __future__ import annotations


def test_delta_and_file_format_runtime_dependencies_are_importable():
    import deltalake
    import fastavro
    import httpx

    assert deltalake is not None
    assert fastavro is not None
    assert httpx is not None
