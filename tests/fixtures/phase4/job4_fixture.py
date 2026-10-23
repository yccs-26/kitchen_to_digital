"""New Job 4 DB and injected real Avro codec + Fake Registry/Sinks."""

from io import BytesIO
import json
from pathlib import Path
from types import SimpleNamespace

from fastavro import schemaless_writer

from streaming.state.fenced_application import FencedStateApplication
from streaming.state.job4_batch import Job4BatchApplication
from streaming.state.job4_contract import InputPosition
from streaming.state.job4_input import ValidatedKafkaRecord
from streaming.state.job4_store import Job4Store
from streaming.state.writer_fencing import ClaimedDispatcher
from streaming.sinks.validated_publisher import serialize_validated_event
from tests.fixtures.phase4.application_fixture import bundle, numeric


SCHEMA_TEXT = Path('schemas/avro/sensor_metric_event.avsc').read_text()
SCHEMA = json.loads(SCHEMA_TEXT)


class FakeRegistry:
    def get_schema(self, schema_id):
        if schema_id != 17:
            raise ValueError('fixture only knows schema ID 17')
        return SimpleNamespace(schema_str=SCHEMA_TEXT)


def record(second=0, offset=10, equipment_id='hood-001', value=800, *,
           updates=None):
    event = numeric(second, value, equipment_id)
    event.update(updates or {})

    def serialize(payload, context):
        stream = BytesIO()
        schemaless_writer(stream, SCHEMA, payload)
        return b'\x00' + (17).to_bytes(4, 'big') + stream.getvalue()

    key, data = serialize_validated_event(event, serialize)
    return ValidatedKafkaRecord(
        InputPosition('kitchen.sensor.validated', 0, offset, key, 17),
        data, event['event_time'],
    )


def resources(path, *, initialize=False):
    local = bundle(path, initialize=initialize)
    roots = []
    if initialize:
        for equipment in ('hood-001', 'fryer-001'):
            for axis in ('OPERATIONAL', 'HEALTH'):
                roots.append(local.store.state(('store-001', equipment, axis)))
    local.store.close()
    path = path / 'job4.sqlite'
    local.store = (Job4Store.create(path, source_epoch='fixture-source-v1')
                   if initialize else Job4Store(path))
    for root in roots:
        local.store.initialize(root.axis_key, root.generation, root.snapshot)
    local.app.store = local.store
    local.coordinator = ClaimedDispatcher(local.store, local.history,
                                          local.kafka, local.current)
    app = Job4BatchApplication(
        FencedStateApplication(local.app), FakeRegistry(),
    )
    return local, app


def writers(local, owner='writer'):
    return {(binding.store_id, binding.equipment_id):
            local.store.acquire_writer((binding.store_id,
                                        binding.equipment_id), owner)
            for binding in local.app.operational_registry.bindings}
