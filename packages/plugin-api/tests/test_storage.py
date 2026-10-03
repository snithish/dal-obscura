import pytest
from dal_obscura_plugin_api.storage import StorageRoot


def test_storage_reads_bounded_members_and_rejects_escapes(tmp_path):
    (tmp_path / "nested").mkdir()
    (tmp_path / "nested" / "a # %.json").write_bytes(b"12345")
    storage = StorageRoot(str(tmp_path))
    member = storage.member("nested/a%20%23%20%25.json", encoded=True)
    assert storage.read(member, limit=5) == b"12345"
    with pytest.raises(ValueError, match="byte budget"):
        storage.read(member, limit=4)
    for value in ("../outside", "s3://other/table/a", str(tmp_path.parent / "outside")):
        with pytest.raises(ValueError):
            storage.member(value, exists=False)
    assert storage.relative(member) == "nested/a # %.json"


def test_storage_rejects_symlink_roots_and_members(tmp_path):
    outside = tmp_path / "outside"
    outside.mkdir()
    root = tmp_path / "root"
    root.mkdir()
    (root / "link").symlink_to(outside, target_is_directory=True)
    storage = StorageRoot(str(root))
    with pytest.raises(ValueError, match="symlink"):
        storage.member("link/anything", exists=False)
    with pytest.raises(ValueError, match="symlink"):
        StorageRoot(str(root / "link"))


@pytest.mark.parametrize(
    "uri",
    [
        "s3://user:pass@bucket/root",
        "s3://bucket/root?token=x",
        "s3://bucket/root#x",
        "s3://bucket/../root",
    ],
)
def test_storage_rejects_ambiguous_s3_roots(uri):
    with pytest.raises(ValueError):
        StorageRoot(uri)
