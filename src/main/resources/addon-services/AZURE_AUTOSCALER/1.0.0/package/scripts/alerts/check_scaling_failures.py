#!/usr/bin/env python3
"""Alert: checks for recent scaling failures in autoscaler logs."""
import os, traceback

RESULT_STATE_OK = 'OK'
RESULT_STATE_WARNING = 'WARNING'
RESULT_STATE_UNKNOWN = 'UNKNOWN'

def get_tokens(): return ()

def execute(configurations={}, parameters={}, host_name=None):
    try:
        log_dir = '/var/log/azure-autoscaler'
        log_file = os.path.join(log_dir, 'autoscaler.log')
        if not os.path.exists(log_file):
            return (RESULT_STATE_OK, ['No autoscaler log file found.'])

        # Check last 100 lines for errors
        with open(log_file, 'r') as f:
            lines = f.readlines()
        recent = lines[-100:] if len(lines) > 100 else lines
        errors = [l.strip() for l in recent if 'Scale-out failed' in l or 'Scale-in failed' in l]

        if errors:
            return (RESULT_STATE_WARNING,
                    ['Recent scaling failures ({0}): {1}'.format(len(errors), errors[-1])])
        return (RESULT_STATE_OK, ['No recent scaling failures.'])
    except Exception:
        return (RESULT_STATE_UNKNOWN, [traceback.format_exc()])
