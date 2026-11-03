"""Record real test server and PGDATA mount facts without connection secrets."""
import json
import os
from pathlib import Path
import subprocess
import psycopg
from psycopg.rows import dict_row


def main():
    with psycopg.connect(os.environ['KG_TEST_DATABASE_URL'], row_factory=dict_row) as conn:
        info = conn.execute('SELECT version() AS version, current_database() AS database').fetchone()
        settings = {r['name']: r['setting'] for r in conn.execute("SELECT name,setting FROM pg_settings WHERE name IN ('data_directory','fsync','full_page_writes','synchronous_commit','data_checksums')")}
    report = {'server': info, 'settings': settings, 'lifetime': 'ephemeral CI test service', 'durable_deployment_verified': False}
    container = os.environ.get('PG_SERVICE_CONTAINER')
    if container:
        mounts = json.loads(subprocess.check_output(['docker','inspect','--format','{{json .Mounts}}',container], text=True))
        directory = settings['data_directory'].rstrip('/')
        matched = [m for m in mounts if directory == m['Destination'].rstrip('/') or directory.startswith(m['Destination'].rstrip('/')+'/')]
        report['data_mounts'] = matched
        for mount in matched:
            result = subprocess.run(['findmnt','--json','-T',mount['Source'],'-o','TARGET,SOURCE,FSTYPE'], capture_output=True, text=True)
            mount['host_filesystem'] = json.loads(result.stdout) if result.returncode == 0 else {'verified': False}
    else:
        report['data_mounts'] = 'unverified: no service container supplied'
    for name in ('fsync','full_page_writes','synchronous_commit'):
        assert settings[name] == 'on', (name, settings[name])
    target = Path(os.environ['KG_TEST_EVIDENCE_DIR']) / 'storage-environment.json'
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))

if __name__ == '__main__':
    main()
