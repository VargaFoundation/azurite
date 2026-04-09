#!/usr/bin/env python3
"""Alert: checks if estimated daily cost exceeds the configured budget."""
import json
import traceback
try:
    from urllib.request import urlopen, Request
except ImportError:
    from urllib2 import urlopen, Request

RESULT_STATE_OK = 'OK'
RESULT_STATE_WARNING = 'WARNING'
RESULT_STATE_CRITICAL = 'CRITICAL'
RESULT_STATE_UNKNOWN = 'UNKNOWN'


def get_tokens():
    return ()


def execute(configurations={}, parameters={}, host_name=None):
    try:
        port = configurations.get('{{azure-vm-manager-env/vm_manager_port}}', '8470')
        data_dir = configurations.get('{{azure-vm-manager-env/vm_manager_data_dir}}',
                                      '/var/lib/azure-vm-manager')

        # Read the API token from the config file
        import os
        config_path = os.path.join(data_dir, 'vm_manager_config.json')
        token = ''
        if os.path.exists(config_path):
            with open(config_path, 'r') as f:
                cfg = json.load(f)
            token = cfg.get('api_token', '')
            budget = float(cfg.get('daily_budget', 0))
        else:
            return (RESULT_STATE_UNKNOWN, ['VM Manager config not found.'])

        if budget <= 0:
            return (RESULT_STATE_OK, ['No budget limit configured.'])

        url = 'http://localhost:{0}/api/v1/cost'.format(port)
        req = Request(url)
        if token:
            req.add_header('Authorization', 'Bearer {0}'.format(token))
        response = urlopen(req, timeout=10)
        data = json.loads(response.read().decode())

        daily_cost = data.get('estimated_daily_cost', 0)
        if daily_cost > budget:
            return (RESULT_STATE_CRITICAL,
                    ['Estimated daily cost ${0:.2f} exceeds budget ${1:.2f}'.format(
                        daily_cost, budget)])
        elif daily_cost > budget * 0.8:
            return (RESULT_STATE_WARNING,
                    ['Estimated daily cost ${0:.2f} is at {1:.0f}% of budget ${2:.2f}'.format(
                        daily_cost, (daily_cost / budget) * 100, budget)])

        return (RESULT_STATE_OK,
                ['Estimated daily cost ${0:.2f} within budget ${1:.2f}'.format(
                    daily_cost, budget)])

    except Exception:
        return (RESULT_STATE_UNKNOWN, [traceback.format_exc()])
