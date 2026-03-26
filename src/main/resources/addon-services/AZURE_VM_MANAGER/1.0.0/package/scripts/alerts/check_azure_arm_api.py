#!/usr/bin/env python3
"""
Alert script: checks if the Azure Resource Manager API is reachable.
"""
import traceback
try:
    from urllib.request import urlopen, Request
except ImportError:
    from urllib2 import urlopen, Request

RESULT_STATE_OK = 'OK'
RESULT_STATE_CRITICAL = 'CRITICAL'
RESULT_STATE_UNKNOWN = 'UNKNOWN'

ARM_HEALTH_URL = 'https://management.azure.com/tenants?api-version=2020-01-01'


def get_tokens():
    return ()


def execute(configurations={}, parameters={}, host_name=None):
    try:
        req = Request(ARM_HEALTH_URL)
        response = urlopen(req, timeout=10)
        # Any response (even 401) means ARM is reachable
        return (RESULT_STATE_OK, ['Azure ARM API is reachable.'])
    except Exception as e:
        error_str = str(e)
        # HTTP errors (401, 403) mean the API is reachable but auth is needed
        if '401' in error_str or '403' in error_str:
            return (RESULT_STATE_OK, ['Azure ARM API is reachable (auth required).'])
        return (RESULT_STATE_CRITICAL,
                ['Azure ARM API is NOT reachable: {0}'.format(error_str)])
