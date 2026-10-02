"""Install narrow query doubles for control command unit tests.

The passed test object supplies query results; no fake SQLAlchemy engine or ORM
internals are needed. Database behavior remains owned by integration tests.
"""

from dal_obscura.storage import assets, audit, catalogs, policies, workspace


def install_query_doubles(monkeypatch, names):
    for module in (assets, audit, catalogs, policies, workspace):
        for name in names:
            if hasattr(module, name):

                def query(session, *args, _name=name, **kwargs):
                    return getattr(session, _name)(*args, **kwargs)

                monkeypatch.setattr(module, name, query)
