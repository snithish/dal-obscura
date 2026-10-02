"""Move the admitted identity owner and invalidate executable scan tickets.

Revision ID: 20261002_0003
Revises: 20260930_0002
"""

import sqlalchemy as sa
from alembic import op

revision = "20261002_0003"
down_revision = "20260930_0002"
branch_labels = None
depends_on = None

_OLD_PROVIDER = (
    "dal_obscura.data_plane.infrastructure.adapters.identity_oidc_jwks.OidcJwksIdentityProvider"
)
_NEW_PROVIDER = "dal_obscura.identity.oidc.OidcJwksIdentityProvider"


def upgrade() -> None:
    providers = sa.table("auth_providers", sa.column("module", sa.Text()))
    op.execute(
        providers.update().where(providers.c.module == _OLD_PROVIDER).values(module=_NEW_PROVIDER)
    )
    # Tickets contain executable objects in the previous codec. Never restore
    # or decode those objects. Clients re-plan against passive scan envelopes.
    op.execute(sa.table("data_plane_tickets").delete())


def downgrade() -> None:
    providers = sa.table("auth_providers", sa.column("module", sa.Text()))
    op.execute(
        providers.update().where(providers.c.module == _NEW_PROVIDER).values(module=_OLD_PROVIDER)
    )
    # Expired/revoked tickets cannot be restored during a downgrade.
