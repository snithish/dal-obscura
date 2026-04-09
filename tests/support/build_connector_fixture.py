from __future__ import annotations

import argparse
import importlib
import json
import sys
from pathlib import Path

import jwt
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

create_iceberg_table = importlib.import_module("tests.support.iceberg").create_iceberg_table

JWT_SECRET = "spark-jwt-secret-32-characters-long"
TICKET_SECRET = "spark-ticket-secret-32-characters"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--port", required=True, type=int)
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    table_id = create_iceberg_table(output_dir, "spark_catalog", "warehouse", [1, 2, 3, 4])
    catalog_path = output_dir / "catalogs.yaml"
    policy_path = output_dir / "policies.yaml"
    app_path = output_dir / "app.yaml"

    catalog_path.write_text(
        yaml.safe_dump(
            {
                "catalogs": {
                    "spark_catalog": {
                        "module": (
                            "dal_obscura.infrastructure.adapters."
                            "catalog_registry.IcebergCatalog"
                        ),
                        "options": {
                            "type": "sql",
                            "uri": f"sqlite:///{output_dir / 'spark_catalog.db'}",
                            "warehouse": str(output_dir / "warehouse"),
                        },
                    }
                }
            },
            sort_keys=False,
        )
    )

    policy_path.write_text(
        yaml.safe_dump(
            {
                "version": 1,
                "catalogs": {
                    "spark_catalog": {
                        "targets": {
                            table_id: {
                                "rules": [
                                    {
                                        "principals": ["spark_user"],
                                        "columns": ["id", "email", "region"],
                                        "row_filter": "id >= 2",
                                    }
                                ]
                            }
                        }
                    }
                },
            },
            sort_keys=False,
        )
    )

    app_path.write_text(
        yaml.safe_dump(
            {
                "location": f"grpc://0.0.0.0:{args.port}",
                "catalog_file": str(catalog_path),
                "policy_file": str(policy_path),
                "secret_provider": {
                    "module": (
                        "dal_obscura.infrastructure.adapters.secret_providers."
                        "EnvSecretProvider"
                    ),
                    "args": {},
                },
                "ticket": {
                    "ttl_seconds": 900,
                    "max_tickets": 16,
                    "secret": {"key": "DAL_OBSCURA_TICKET_SECRET"},
                },
                "auth": {"jwt_secret": {"key": "DAL_OBSCURA_JWT_SECRET"}},
                "logging": {"level": "INFO", "json": True},
            },
            sort_keys=False,
        )
    )

    user_token = jwt.encode({"sub": "spark_user"}, JWT_SECRET, algorithm="HS256")

    print(
        json.dumps(
            {
                "uri": f"grpc+tcp://localhost:{args.port}",
                "catalog": "spark_catalog",
                "target": table_id,
                "app_path": str(app_path),
                "jwt_secret": JWT_SECRET,
                "ticket_secret": TICKET_SECRET,
                "user_token": user_token,
            }
        )
    )


if __name__ == "__main__":
    main()
