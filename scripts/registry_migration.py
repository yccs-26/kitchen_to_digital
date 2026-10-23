"""P2-8A의 준비된 요청을 검증하고 승인된 시점에만 이관을 실행한다."""

import argparse
import base64
import getpass
import hashlib
import json
import os
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener


DEFAULT_BUNDLE = (
    Path(__file__).resolve().parents[1]
    / 'docs/evidence/phase-2/p2-8a/registry-migration'
)
RAW = 'kitchen.sensor.raw-value'
VALIDATED = 'kitchen.sensor.validated-value'


class MigrationError(RuntimeError):
    """비밀값이나 서버 응답 본문을 노출하지 않는 중단 사유다."""


class ApiError(MigrationError):
    """HTTP 상태와 Registry 오류 코드만 보관한다."""

    def __init__(self, status, code):
        self.status = status
        self.code = code
        super().__init__(f'HTTP {status}; Registry error_code={code}')


class NoRedirect(HTTPRedirectHandler):
    """인증 헤더가 다른 endpoint로 전달되지 않도록 redirect를 거부한다."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def require(condition, message):
    if not condition:
        raise MigrationError(message)


def validate_endpoint(url):
    try:
        parsed = urlsplit(url)
        valid = (
            parsed.scheme == 'https' and parsed.hostname
            and parsed.path in ('', '/')
            and not parsed.username and not parsed.password
            and not parsed.query and not parsed.fragment
        )
    except ValueError:
        valid = False
    require(valid, 'HTTPS Registry base endpoint without credentials required')
    return url.rstrip('/')


def structure_hash(schema):
    """필드 배열 순서와 default 등을 보존하는 JSON 구조 hash다."""
    normalized = json.dumps(
        json.loads(schema), sort_keys=True, separators=(',', ':'),
        ensure_ascii=False,
    )
    return hashlib.sha256(normalized.encode()).hexdigest()


class RegistryApi:
    def __init__(self, url, api_key, api_secret):
        self.url = validate_endpoint(url)
        require(bool(api_key and api_secret), 'Credentials must not be empty')
        encoded = base64.b64encode(
            f'{api_key}:{api_secret}'.encode()
        ).decode()
        self._authorization = f'Basic {encoded}'
        self._opener = build_opener(NoRedirect())

    def request(self, method, path, body=None):
        data = None if body is None else json.dumps(body).encode()
        request = Request(
            self.url + path, data=data, method=method,
            headers={
                'Authorization': self._authorization,
                'Accept': 'application/vnd.schemaregistry.v1+json',
                'Content-Type': 'application/vnd.schemaregistry.v1+json',
            },
        )
        try:
            with self._opener.open(request, timeout=30) as response:
                result = json.load(response)
        except HTTPError as error:
            try:
                code = json.load(error).get('error_code')
                code = code if isinstance(code, int) else None
            except (ValueError, AttributeError):
                code = None
            raise ApiError(error.code, code) from None
        except (URLError, TimeoutError, ValueError):
            raise MigrationError('Registry transport/JSON failure') from None
        print(f'{method} {path}: PASS')
        return result


def load_bundle(directory):
    """전송 전 schema 원문과 요청 본문의 hash 및 ID/version을 검사한다."""
    directory = Path(directory)
    manifest = json.loads((directory / 'manifest.json').read_text())
    require(manifest['destination_context'] == '.', 'Default context required')
    require(
        [(e['subject'], e['id'], e['version']) for e in manifest['raw']]
        == [(RAW, 1, 1), (RAW, 2, 2)],
        'Unexpected raw mapping',
    )
    for entry in [*manifest['raw'], manifest['validated']]:
        schema = (directory / entry['schema_file']).read_text()
        request = json.loads((directory / entry['request_file']).read_text())
        require(
            hashlib.sha256(schema.encode()).hexdigest()
            == entry['source_text_sha256'], 'Source schema bytes changed',
        )
        require(
            structure_hash(schema) == entry['json_structure_sha256'],
            'Source schema structure changed',
        )
        expected = {'schemaType': 'AVRO', 'references': [], 'schema': schema}
        if entry['subject'] == RAW:
            expected.update(id=entry['id'], version=entry['version'])
        require(request == expected, 'Request does not match exported schema')
    require(manifest['validated']['subject'] == VALIDATED,
            'Unexpected validated subject')
    require(
        manifest['validated']['json_structure_sha256']
        == manifest['raw'][0]['json_structure_sha256'],
        'Validated schema must equal raw v1',
    )
    return manifest


def expect_absent(api, path, error_code):
    """인증 실패나 임의 404를 schema 부재로 오인하지 않는다."""
    try:
        api.request('GET', path)
    except ApiError as error:
        if error.status == 404 and error.code == error_code:
            return
        raise
    raise MigrationError(f'Existing resource: {path}; stop for review')


def preflight(api):
    """새 목적지 전용 절차이며 동일 schema라도 기존 ID면 자동 진행하지 않는다."""
    require(api.request('GET', '/mode')['mode'] == 'READWRITE',
            'Global mode must be READWRITE')
    require(api.request('GET', '/subjects?deleted=true') == [],
            'Destination has subjects/deleted history; stop for review')
    for subject in (RAW, VALIDATED):
        expect_absent(
            api, f'/subjects/{subject}/versions?deleted=true', 40401,
        )
    for schema_id in (1, 2):
        expect_absent(api, f'/schemas/ids/{schema_id}', 40403)
    return {'status': 'EMPTY_DESTINATION_PREFLIGHT_PASS'}


def check_definition(response, entry):
    require(response.get('schemaType', 'AVRO') == 'AVRO',
            'Unexpected schema type')
    require(response.get('references', []) == [], 'Unexpected references')
    require(not response.get('deleted', False), 'Schema is deleted')
    require(not response.get('ruleSet'), 'Unexpected schema rules')
    require(
        structure_hash(response['schema']) == entry['json_structure_sha256'],
        'Schema content/hash mismatch; STOP',
    )


def verify_version(api, subject, version, schema_id, entry):
    response = api.request('GET', f'/subjects/{subject}/versions/{version}')
    require(
        (response['subject'], response['version'], response['id'])
        == (subject, version, schema_id), 'Subject/version/ID mismatch; STOP',
    )
    check_definition(response, entry)
    check_definition(api.request('GET', f'/schemas/ids/{schema_id}'), entry)
    associations = api.request(
        'GET', f'/schemas/ids/{schema_id}/versions?deleted=true',
    )
    require({'subject': subject, 'version': version} in associations,
            'Missing ID-to-subject/version association')


def verify_raw(api, manifest):
    subjects = api.request('GET', '/subjects')
    require(RAW in subjects, 'Raw subject is missing')
    require(api.request('GET', f'/subjects/{RAW}/versions') == [1, 2],
            'Unexpected raw versions')
    require(
        api.request('GET', f'/subjects/{RAW}/versions?deleted=true') == [1, 2],
        'Unexpected raw deleted/version history',
    )
    for entry in manifest['raw']:
        verify_version(api, RAW, entry['version'], entry['id'], entry)
    require(api.request('GET', f'/mode/{RAW}')['mode'] == 'READWRITE',
            'Raw mode not restored')
    require(
        api.request('GET', f'/config/{RAW}')['compatibilityLevel']
        == 'BACKWARD', 'Raw compatibility mismatch',
    )
    return {'status': 'RAW_VERIFIED', 'ids': [1, 2], 'versions': [1, 2]}


def request_file(directory, name):
    return json.loads((Path(directory) / name).read_text())


def import_raw(api, manifest, directory):
    """중단 후 자동 재시도·삭제·강제 복구를 하지 않는 raw 전용 이관이다."""
    preflight(api)
    result = api.request('PUT', f'/mode/{RAW}', {'mode': 'IMPORT'})
    require(result['mode'] == 'IMPORT', 'IMPORT mode not acknowledged')
    require(api.request('GET', f'/mode/{RAW}')['mode'] == 'IMPORT',
            'IMPORT mode not active')
    for entry in manifest['raw']:
        # 앞 단계 이후 다른 writer가 ID를 차지했는지도 다시 검사한다.
        expect_absent(api, f"/schemas/ids/{entry['id']}", 40403)
        result = api.request(
            'POST', f'/subjects/{RAW}/versions',
            request_file(directory, entry['request_file']),
        )
        require(result['id'] == entry['id'], 'Imported ID mismatch; STOP')
        verify_version(api, RAW, entry['version'], entry['id'], entry)
    api.request('PUT', f'/mode/{RAW}', {'mode': 'READWRITE'})
    api.request('PUT', f'/config/{RAW}', {'compatibility': 'BACKWARD'})
    return verify_raw(api, manifest)


def verify_all(api, manifest):
    verify_raw(api, manifest)
    require(VALIDATED in api.request('GET', '/subjects'),
            'Validated subject is missing')
    require(
        api.request('GET', f'/subjects/{VALIDATED}/versions?deleted=true')
        == [1], 'Unexpected validated versions/deleted history',
    )
    entry = manifest['validated']
    response = api.request('GET', f'/subjects/{VALIDATED}/versions/1')
    actual_id = response['id']
    require(actual_id == 1, 'Canonical v1 did not reuse ID 1; STOP')
    verify_version(api, VALIDATED, 1, actual_id, entry)
    require(
        api.request('GET', f'/mode/{VALIDATED}?defaultToGlobal=true')['mode']
        == 'READWRITE', 'Validated mode is not READWRITE',
    )
    require(
        api.request('GET', f'/config/{VALIDATED}')['compatibilityLevel']
        == 'BACKWARD', 'Validated compatibility mismatch',
    )
    return {
        'status': 'ALL_VERIFIED', 'raw_ids': [1, 2],
        'validated_actual_id': actual_id, 'validated_actual_version': 1,
    }


def register_validated(api, manifest, directory):
    verify_raw(api, manifest)
    expect_absent(
        api, f'/subjects/{VALIDATED}/versions?deleted=true', 40401,
    )
    require(api.request('GET', '/mode')['mode'] == 'READWRITE',
            'Global mode must be READWRITE')
    # 존재하지 않는 subject의 개별 mode 설정도 상속값으로 확인한다.
    require(
        api.request('GET', f'/mode/{VALIDATED}?defaultToGlobal=true')['mode']
        == 'READWRITE', 'Validated effective mode must be READWRITE',
    )
    api.request('PUT', f'/config/{VALIDATED}', {'compatibility': 'BACKWARD'})
    result = api.request(
        'POST', f'/subjects/{VALIDATED}/versions',
        request_file(directory, manifest['validated']['request_file']),
    )
    require(result['id'] == 1, 'Validated returned unexpected ID; STOP')
    return verify_all(api, manifest)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=[
        'offline', 'preflight', 'import-raw', 'verify-raw',
        'register-validated', 'verify-all',
    ])
    parser.add_argument('--bundle', type=Path, default=DEFAULT_BUNDLE)
    parser.add_argument('--execute', action='store_true')
    args = parser.parse_args()
    try:
        manifest = load_bundle(args.bundle)
        if args.action == 'offline':
            print('OFFLINE_BUNDLE_PASS; no network requests')
            return
        mutation = args.action in ('import-raw', 'register-validated')
        require(not mutation or args.execute,
                'Mutation requires explicit --execute; no requests made')
        url = validate_endpoint(os.environ.get('KTD_REGISTRY_URL', ''))
        api = RegistryApi(
            url, getpass.getpass('Registry API key (hidden): '),
            getpass.getpass('Registry API secret (hidden): '),
        )
        actions = {
            'preflight': lambda: preflight(api),
            'import-raw': lambda: import_raw(api, manifest, args.bundle),
            'verify-raw': lambda: verify_raw(api, manifest),
            'register-validated': lambda: register_validated(
                api, manifest, args.bundle,
            ),
            'verify-all': lambda: verify_all(api, manifest),
        }
        print(json.dumps(actions[args.action](), indent=2))
    except (MigrationError, KeyError, ValueError, OSError) as error:
        message = str(error) if isinstance(error, MigrationError) else (
            'Invalid bundle/response or local I/O failure'
        )
        parser.exit(1, f'STOP: {message}\n')


if __name__ == '__main__':
    main()
