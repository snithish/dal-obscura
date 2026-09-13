from __future__ import annotations

from fastapi.testclient import TestClient

from tests.interfaces.control_plane.workspace_helpers import (
    ADMIN_HEADERS,
    _client,
    _provision_draft,
)


def test_activation_freezes_a_reviewed_generation_until_a_new_publication() -> None:
    client: TestClient = _client()
    asset = _provision_draft(client)
    assert (
        client.put(
            f"/v1/assets/{asset['id']}/owners",
            headers=ADMIN_HEADERS,
            json={"owners": ["platform:admin"]},
        ).status_code
        == 200
    )

    publication = client.post("/v1/workspace/publications", headers=ADMIN_HEADERS)
    assert publication.status_code == 200, publication.json()
    publication_id = publication.json()["publication_id"]
    activated = client.post(
        f"/v1/workspace/publications/{publication_id}/activate",
        headers=ADMIN_HEADERS,
    )
    assert activated.status_code == 200, activated.json()

    before = client.get("/v1/workspace/observations", headers=ADMIN_HEADERS).json()
    assert before["available"] is True
    generation = before["generation"]

    draft = client.get(f"/v1/assets/{asset['id']}/draft", headers=ADMIN_HEADERS).json()
    changed = client.put(
        f"/v1/assets/{asset['id']}/draft",
        headers=ADMIN_HEADERS,
        json={
            "expected_revision": draft["revision"],
            "rules": [
                {
                    "ordinal": 10,
                    "effect": "allow",
                    "principals": ["user1"],
                    "when": {"tenant": "default"},
                    "columns": ["id"],
                    "masks": {},
                    "row_filter": None,
                }
            ]
        },
    )
    assert changed.status_code == 200, changed.json()

    after = client.get("/v1/workspace/observations", headers=ADMIN_HEADERS).json()
    assert after["generation"] == generation
