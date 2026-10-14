from contextlib import ExitStack

import pytest
from pyiceberg.catalog import load_catalog


@pytest.fixture
def catalog_factory():
    """Keep fixture catalogs open until their tables are no longer used."""
    with ExitStack() as resources:

        def create(name, **options):
            catalog = load_catalog(name, **options)
            resources.callback(catalog.close)
            return catalog

        yield create
