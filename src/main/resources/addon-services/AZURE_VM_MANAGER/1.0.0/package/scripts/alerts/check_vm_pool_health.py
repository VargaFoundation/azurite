#!/usr/bin/env python3
"""
Alert script: checks if all managed VMs are in running state.
"""
import json
import traceback
try:
    from urllib.request import urlopen
except ImportError:
    from urllib2 import urlopen

RESULT_STATE_OK = 'OK'
RESULT_STATE_WARNING = 'WARNING'
RESULT_STATE_CRITICAL = 'CRITICAL'
RESULT_STATE_UNKNOWN = 'UNKNOWN'


def get_tokens():
    return ()


def execute(configurations={}, parameters={}, host_name=None):
    try:
        port = configurations.get('{{azure-vm-manager-env/vm_manager_port}}', '8470')
        url = 'http://localhost:{0}/api/v1/vms'.format(port)
        response = urlopen(url, timeout=10)
        data = json.loads(response.read().decode())
        vms = data.get('vms', [])

        if not vms:
            return (RESULT_STATE_OK, ['No managed VMs found.'])

        not_running = [v['name'] for v in vms if v.get('status') != 'running']
        if not_running:
            return (RESULT_STATE_WARNING,
                    ['VMs not in running state: {0}'.format(', '.join(not_running))])

        return (RESULT_STATE_OK,
                ['All {0} managed VMs are running.'.format(len(vms))])

    except Exception:
        return (RESULT_STATE_UNKNOWN, [traceback.format_exc()])
