#!/usr/bin/env python3
"""
Alert: detects scaling oscillation (rapid scale-out/scale-in cycles).
Oscillation indicates misconfigured thresholds or cooldown periods.
"""
import os
import traceback

RESULT_STATE_OK = 'OK'
RESULT_STATE_WARNING = 'WARNING'
RESULT_STATE_UNKNOWN = 'UNKNOWN'


def get_tokens():
    return ()


def execute(configurations={}, parameters={}, host_name=None):
    try:
        log_dir = configurations.get('{{azure-autoscaler-env/autoscaler_log_dir}}',
                                     '/var/log/azure-autoscaler')
        log_file = os.path.join(log_dir, 'autoscaler.log')
        if not os.path.exists(log_file):
            return (RESULT_STATE_OK, ['No autoscaler log file found.'])

        with open(log_file, 'r') as f:
            lines = f.readlines()

        # Check last 200 lines for alternating SCALE OUT / SCALE IN events
        recent = lines[-200:] if len(lines) > 200 else lines
        scale_events = []
        for line in recent:
            if 'SCALE OUT:' in line:
                scale_events.append('OUT')
            elif 'SCALE IN:' in line:
                scale_events.append('IN')

        if len(scale_events) < 4:
            return (RESULT_STATE_OK,
                    ['No oscillation detected ({0} recent scaling events).'.format(
                        len(scale_events))])

        # Count direction changes in the last events
        changes = 0
        for i in range(1, len(scale_events)):
            if scale_events[i] != scale_events[i - 1]:
                changes += 1

        # If more than 60% of transitions are direction changes, that's oscillation
        ratio = changes / (len(scale_events) - 1) if len(scale_events) > 1 else 0
        if ratio > 0.6 and len(scale_events) >= 6:
            return (RESULT_STATE_WARNING,
                    ['Scaling oscillation detected: {0} direction changes in {1} events '
                     '({2:.0f}%). Consider increasing cooldown periods or widening '
                     'threshold gaps.'.format(changes, len(scale_events), ratio * 100)])

        return (RESULT_STATE_OK,
                ['No oscillation detected ({0} events, {1} direction changes).'.format(
                    len(scale_events), changes)])

    except Exception:
        return (RESULT_STATE_UNKNOWN, [traceback.format_exc()])
