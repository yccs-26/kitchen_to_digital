"""New v2 test databases; legacy fixture files are never migrated."""

from streaming.state.fenced_application import FencedStateApplication
from streaming.state.writer_fencing import (
    ClaimedDispatcher, FencedPublicationStore,
)
from tests.fixtures.phase4.application_fixture import bundle


def fenced_bundle(path, *, initialize=False):
    legacy = bundle(path, initialize=initialize)
    roots = []
    if initialize:
        for equipment in ('fryer-001', 'hood-001'):
            for axis in ('OPERATIONAL', 'HEALTH'):
                roots.append(legacy.store.state(
                    ('store-001', equipment, axis),
                ))
    legacy.store.close()
    store = FencedPublicationStore(path / 'fenced.sqlite')
    for root in roots:
        store.initialize(root.axis_key, root.generation, root.snapshot)
    legacy.store = store
    legacy.app.store = store
    legacy.coordinator = ClaimedDispatcher(
        store, legacy.history, legacy.kafka, legacy.current,
    )
    return legacy, FencedStateApplication(legacy.app)
