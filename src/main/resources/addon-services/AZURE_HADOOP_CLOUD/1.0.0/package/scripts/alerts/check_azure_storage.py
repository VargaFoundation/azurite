#!/usr/bin/env python3
"""
Alert script: checks Azure storage connectivity by running hdfs dfs -ls against
the configured storage backend.
"""
import subprocess
import traceback

RESULT_STATE_OK = 'OK'
RESULT_STATE_WARNING = 'WARNING'
RESULT_STATE_CRITICAL = 'CRITICAL'
RESULT_STATE_UNKNOWN = 'UNKNOWN'


def get_tokens():
    return ()


def execute(configurations={}, parameters={}, host_name=None):
    try:
        backend = configurations.get('{{azure-cloud-env/azure_storage_backend}}', 'hdfs')
        account = configurations.get('{{azure-storage-site/azure.storage.account.name}}', '')
        container = configurations.get('{{azure-storage-site/azure.storage.container.name}}', '')

        if backend == 'adls_gen2' and account and container:
            fqdn = '{0}.dfs.core.windows.net'.format(account)
            uri = 'abfs://{0}@{1}/'.format(container, fqdn)
        elif backend == 'wasb' and account and container:
            fqdn = '{0}.blob.core.windows.net'.format(account)
            uri = 'wasbs://{0}@{1}/'.format(container, fqdn)
        elif backend == 'hdfs':
            uri = '/'
        else:
            return (RESULT_STATE_WARNING, ['Storage backend not fully configured.'])

        cmd = ['hdfs', 'dfs', '-ls', uri]
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=30)

        if proc.returncode == 0:
            return (RESULT_STATE_OK, ['Azure storage ({0}) is reachable at {1}'.format(backend, uri)])
        else:
            return (RESULT_STATE_CRITICAL,
                    ['Azure storage ({0}) is NOT reachable at {1}: {2}'.format(backend, uri, proc.stderr.strip())])

    except subprocess.TimeoutExpired:
        return (RESULT_STATE_CRITICAL, ['Azure storage connectivity check timed out after 30s.'])
    except Exception:
        return (RESULT_STATE_UNKNOWN, [traceback.format_exc()])
