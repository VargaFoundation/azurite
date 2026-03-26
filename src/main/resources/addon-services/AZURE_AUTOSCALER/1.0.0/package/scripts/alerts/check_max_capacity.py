#!/usr/bin/env python3
"""Alert: checks if worker count is at maximum capacity."""
import json, traceback
try:
    from urllib.request import urlopen
except ImportError:
    from urllib2 import urlopen

RESULT_STATE_OK = 'OK'
RESULT_STATE_WARNING = 'WARNING'
RESULT_STATE_UNKNOWN = 'UNKNOWN'

def get_tokens(): return ()

def execute(configurations={}, parameters={}, host_name=None):
    try:
        port = configurations.get('{{azure-autoscaler-env/autoscaler_port}}', '8471')
        response = urlopen('http://localhost:{0}/api/v1/status'.format(port), timeout=10)
        data = json.loads(response.read().decode())
        current = data.get('current_worker_count', 0)
        # Get max from autoscaler config
        last_metrics = data.get('last_metrics', {})
        pending = last_metrics.get('pending_containers', 0)

        if pending > 0 and data.get('last_decision') == 'SCALE_OUT':
            return (RESULT_STATE_WARNING,
                    ['Workers may be at maximum capacity. Current: {0}, Pending containers: {1}'.format(
                        current, pending)])
        return (RESULT_STATE_OK, ['Worker count: {0}'.format(current)])
    except Exception:
        return (RESULT_STATE_UNKNOWN, [traceback.format_exc()])
