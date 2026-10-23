"""Cloud 호출 없이 이관 충돌·중단·schema 보존 경계를 검증한다."""

import copy
import json
import shutil
from unittest.mock import Mock

import pytest

from scripts import registry_migration as migration
from scripts.registry_runtime_smoke import framing_failure_summary
from streaming.validation.avro_decoder import SensorDecodeError


class FakeRegistry:
    """요청 순서와 중단 이전 쓰기를 관측하는 오프라인 Registry다."""

    def __init__(self):
        self.calls = []
        self.versions = {}
        self.schemas = {}
        self.modes = {}
        self.config = {}
        self.deleted_subjects = []

    def request(self, method, path, body=None):
        self.calls.append((method, path, body))
        route = path.split('?')[0]
        if route == '/mode':
            return {'mode': 'READWRITE'}
        if route == '/subjects':
            return sorted(self.versions) + self.deleted_subjects
        parts = route.strip('/').split('/')
        if parts[0] == 'mode':
            if method == 'PUT':
                self.modes[parts[1]] = body['mode']
            return {'mode': self.modes.get(parts[1], 'READWRITE')}
        if parts[0] == 'config':
            if method == 'PUT':
                self.config[parts[1]] = body['compatibility']
            return {'compatibilityLevel': self.config.get(parts[1])}
        if parts[:2] == ['schemas', 'ids']:
            schema_id = int(parts[2])
            if schema_id not in self.schemas:
                raise migration.ApiError(404, 40403)
            if len(parts) == 4:
                return [
                    {'subject': subject, 'version': version}
                    for subject, versions in self.versions.items()
                    for version, entry in versions.items()
                    if entry['id'] == schema_id
                ]
            return self.schemas[schema_id]
        if parts[0] == 'subjects':
            subject = parts[1]
            if method == 'POST':
                versions = self.versions.setdefault(subject, {})
                schema_id = body.get('id', 1)
                version = body.get('version', len(versions) + 1)
                entry = {
                    **body, 'subject': subject,
                    'id': schema_id, 'version': version,
                }
                versions[version] = entry
                self.schemas[schema_id] = entry
                return {'id': schema_id}
            if subject not in self.versions:
                raise migration.ApiError(404, 40401)
            if len(parts) == 3:
                return sorted(self.versions[subject])
            return self.versions[subject][int(parts[3])]
        raise AssertionError('Unplanned API route')


@pytest.fixture
def manifest():
    return migration.load_bundle(migration.DEFAULT_BUNDLE)


def test_prepared_sequence_preserves_ids_versions_and_validated(manifest):
    api = FakeRegistry()
    migration.import_raw(api, manifest, migration.DEFAULT_BUNDLE)
    result = migration.register_validated(
        api, manifest, migration.DEFAULT_BUNDLE,
    )
    assert result['validated_actual_id'] == 1
    assert result['validated_actual_version'] == 1
    posts = [call for call in api.calls if call[0] == 'POST']
    assert [call[2].get('id') for call in posts] == [1, 2, None]
    assert [call[2].get('version') for call in posts] == [1, 2, None]
    assert all(call[0] != 'DELETE' for call in api.calls)
    assert all('force=' not in call[1] for call in api.calls)


@pytest.mark.parametrize('same_schema', [False, True])
def test_occupied_id_stops_before_any_write(manifest, same_schema):
    api = FakeRegistry()
    api.schemas[1] = (
        migration.request_file(migration.DEFAULT_BUNDLE, 'raw-v1.import.json')
        if same_schema else {'schema': '"string"'}
    )
    with pytest.raises(migration.MigrationError, match='Existing resource'):
        migration.import_raw(api, manifest, migration.DEFAULT_BUNDLE)
    assert all(call[0] == 'GET' for call in api.calls)


def test_deleted_subject_stops_before_writes(manifest):
    api = FakeRegistry()
    api.deleted_subjects = [migration.RAW]
    with pytest.raises(migration.MigrationError, match='deleted history'):
        migration.import_raw(api, manifest, migration.DEFAULT_BUNDLE)
    assert all(call[0] == 'GET' for call in api.calls)


@pytest.mark.parametrize('status,code', [
    (401, 401), (403, 403), (404, 40401), (404, None), (500, 50001),
])
def test_only_exact_schema_missing_error_is_absence(status, code):
    api = Mock()
    api.request.side_effect = migration.ApiError(status, code)
    with pytest.raises(migration.ApiError):
        migration.expect_absent(api, '/schemas/ids/1', 40403)


def test_content_drift_aborts_before_validated_registration(manifest):
    api = FakeRegistry()
    migration.import_raw(api, manifest, migration.DEFAULT_BUNDLE)
    before = len(api.calls)
    api.schemas[2] = {**api.schemas[2], 'schema': '"string"'}
    with pytest.raises(migration.MigrationError, match='content/hash'):
        migration.register_validated(api, manifest, migration.DEFAULT_BUNDLE)
    assert all(call[0] == 'GET' for call in api.calls[before:])


def test_partial_import_failure_does_not_retry_or_overwrite(manifest):
    api = FakeRegistry()
    original = api.request

    def fail_second_post(method, path, body=None):
        if method == 'POST' and body.get('id') == 2:
            api.calls.append((method, path, body))
            raise migration.ApiError(500, 50002)
        return original(method, path, body)

    api.request = fail_second_post
    with pytest.raises(migration.ApiError):
        migration.import_raw(api, manifest, migration.DEFAULT_BUNDLE)
    assert len([c for c in api.calls if c[0] == 'POST']) == 2
    assert api.modes[migration.RAW] == 'IMPORT'
    assert set(api.schemas) == {1}


def test_bundle_request_tamper_is_rejected(tmp_path):
    bundle = tmp_path / 'bundle'
    shutil.copytree(migration.DEFAULT_BUNDLE, bundle)
    path = bundle / 'raw-v2.import.json'
    body = json.loads(path.read_text())
    body['id'] = 3
    path.write_text(json.dumps(body))
    with pytest.raises(migration.MigrationError, match='Request does not'):
        migration.load_bundle(bundle)


def test_structure_hash_preserves_nullable_default_and_field_order(manifest):
    entry = manifest['raw'][1]
    request = migration.request_file(
        migration.DEFAULT_BUNDLE, entry['request_file'],
    )
    schema = json.loads(request['schema'])
    changed = copy.deepcopy(schema)
    del changed['fields'][-1]['default']
    assert migration.structure_hash(json.dumps(changed)) != (
        entry['json_structure_sha256']
    )
    changed = copy.deepcopy(schema)
    changed['fields'].reverse()
    assert migration.structure_hash(json.dumps(changed)) != (
        entry['json_structure_sha256']
    )
    assert migration.structure_hash(json.dumps(schema, indent=4)) == (
        entry['json_structure_sha256']
    )


@pytest.mark.parametrize('url', [
    'http://registry.example', 'https://user:pass@registry.example',
    'https://registry.example?token=x', 'https://registry.example/contexts/x',
])
def test_endpoint_rejects_insecure_or_custom_context_url(url):
    with pytest.raises(migration.MigrationError):
        migration.validate_endpoint(url)


@pytest.mark.parametrize('value', [None, b'\x00\x01', b'\x01\x00\x00\x00\x01'])
def test_corrupt_frame_is_candidate_without_registry_request(value):
    client = Mock()
    result = framing_failure_summary(value, client)
    assert result['code'] == 'INVALID_CONFLUENT_FRAME'
    assert result['quarantine_candidate'] is True
    assert result['quarantine_published'] is False
    client.get_schema.assert_not_called()


def test_unexpected_decode_failure_is_not_silently_accepted(monkeypatch):
    from scripts import registry_runtime_smoke as smoke

    def fail(value, client):
        raise SensorDecodeError('UNKNOWN_SCHEMA_ID', 'schema_id', 'unknown')

    monkeypatch.setattr(smoke, 'decode_sensor_event', fail)
    with pytest.raises(migration.MigrationError, match='Unexpected error'):
        smoke.framing_failure_summary(b'bad', Mock())


def test_cli_mutation_without_flag_never_requests_credentials(monkeypatch):
    monkeypatch.setattr('sys.argv', ['registry_migration.py', 'import-raw'])
    credential_prompt = Mock(side_effect=AssertionError('Must not prompt'))
    monkeypatch.setattr(migration.getpass, 'getpass', credential_prompt)
    with pytest.raises(SystemExit) as error:
        migration.main()
    assert error.value.code == 1
    credential_prompt.assert_not_called()


def test_transport_does_not_echo_error_response_credentials(monkeypatch):
    import io
    from urllib.error import HTTPError

    api = migration.RegistryApi('https://registry.example', 'key', 'secret')
    error = HTTPError(
        'https://registry.example', 401, 'unauthorized', {},
        io.BytesIO(b'{"error_code":401,"message":"sensitive-response"}'),
    )
    monkeypatch.setattr(api._opener, 'open', Mock(side_effect=error))
    with pytest.raises(migration.ApiError) as raised:
        api.request('GET', '/subjects')
    assert str(raised.value) == 'HTTP 401; Registry error_code=401'


def test_authorization_redirect_is_disabled():
    assert migration.NoRedirect().redirect_request(
        None, None, 302, '', {}, 'https://different.example',
    ) is None
