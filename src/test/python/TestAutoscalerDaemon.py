#!/usr/bin/env python3
"""
Unit tests for the AutoscalerDaemon class and its REST API handler.
All external dependencies (MetricsCollector, ScalingPolicyEngine,
YarnDecommissioner, HTTP calls) are mocked.
"""
import sys
import os
import json
import threading
import time
import unittest

# Python 2/3 compatible mock import
try:
    from unittest.mock import patch, MagicMock, PropertyMock
except ImportError:
    from mock import patch, MagicMock, PropertyMock

try:
    from http.client import HTTPConnection
except ImportError:
    from httplib import HTTPConnection

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                '..', '..', '..', 'src', 'main', 'resources',
                                'addon-services', 'AZURE_AUTOSCALER', '1.0.0', 'package', 'files'))


def _make_config(**overrides):
    """Return a minimal valid config for AutoscalerDaemon."""
    config = {
        'enabled': True,
        'mode': 'load_based',
        'port': 0,
        'evaluation_interval': 60,
        'yarn_rm_url': 'http://localhost:8088',
        'vm_manager_url': 'http://localhost:8470',
        'worker_min_count': 1,
        'worker_max_count': 20,
        'graceful_decommission_timeout': 60,
        'metrics_source': 'yarn_and_system',
        'api_token': '',
        'vm_manager_api_token': '',
        'cpu_scale_out_threshold': 80,
        'cpu_scale_in_threshold': 30,
        'memory_scale_out_threshold': 80,
        'memory_scale_in_threshold': 30,
        'yarn_pending_containers_threshold': 10,
        'yarn_available_memory_scale_in_pct': 60,
        'scale_out_trigger_duration': 0,
        'scale_in_trigger_duration': 0,
        'cooldown_scale_out': 300,
        'cooldown_scale_in': 600,
        'scale_out_increment': 1,
        'scale_in_decrement': 1,
    }
    config.update(overrides)
    return config


# Patch the heavy imports so that AutoscalerDaemon.__init__ succeeds without
# connecting to any real service.
@patch('autoscaler_daemon.YarnDecommissioner')
@patch('autoscaler_daemon.ScalingPolicyEngine')
@patch('autoscaler_daemon.MetricsCollector')
def _create_daemon(config, mock_mc, mock_spe, mock_yd):
    """Helper: create an AutoscalerDaemon with all collaborators mocked."""
    from autoscaler_daemon import AutoscalerDaemon
    daemon = AutoscalerDaemon(config)
    return daemon


# ====================================================================== #
# REST API tests (real HTTP against an ephemeral server)
# ====================================================================== #
class TestAutoscalerRestApi(unittest.TestCase):
    """Test the REST API served by AutoscalerRequestHandler."""

    @classmethod
    def setUpClass(cls):
        from http.server import HTTPServer
        from autoscaler_daemon import AutoscalerRequestHandler

        cls.config = _make_config(api_token='test-secret-token')
        cls.daemon = _create_daemon(cls.config)

        AutoscalerRequestHandler.daemon_instance = cls.daemon
        AutoscalerRequestHandler.api_token = cls.config['api_token']

        cls.server = HTTPServer(('127.0.0.1', 0), AutoscalerRequestHandler)
        cls.port = cls.server.server_address[1]
        cls.api_thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.api_thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()

    def _get(self, path, token=None):
        conn = HTTPConnection('127.0.0.1', self.port)
        headers = {}
        if token:
            headers['Authorization'] = 'Bearer {0}'.format(token)
        conn.request('GET', path, headers=headers)
        resp = conn.getresponse()
        body = json.loads(resp.read().decode())
        status = resp.status
        conn.close()
        return status, body

    def _post(self, path, token=None, data=None):
        conn = HTTPConnection('127.0.0.1', self.port)
        headers = {}
        if token:
            headers['Authorization'] = 'Bearer {0}'.format(token)
        body_bytes = None
        if data is not None:
            body_bytes = json.dumps(data).encode()
            headers['Content-Type'] = 'application/json'
            headers['Content-Length'] = str(len(body_bytes))
        conn.request('POST', path, body=body_bytes, headers=headers)
        resp = conn.getresponse()
        body = json.loads(resp.read().decode())
        status = resp.status
        conn.close()
        return status, body

    def _put(self, path, token=None, data=None):
        conn = HTTPConnection('127.0.0.1', self.port)
        headers = {}
        if token:
            headers['Authorization'] = 'Bearer {0}'.format(token)
        body_bytes = None
        if data is not None:
            body_bytes = json.dumps(data).encode()
            headers['Content-Type'] = 'application/json'
            headers['Content-Length'] = str(len(body_bytes))
        conn.request('PUT', path, body=body_bytes, headers=headers)
        resp = conn.getresponse()
        body = json.loads(resp.read().decode())
        status = resp.status
        conn.close()
        return status, body

    # ------------------------------------------------------------------ #
    # Health endpoint (no auth required)
    # ------------------------------------------------------------------ #
    def test_health_endpoint_no_auth(self):
        """GET /api/v1/health without a token should return 401 (auth is mandatory)."""
        status, body = self._get('/api/v1/health')
        self.assertEqual(status, 401)

    def test_health_endpoint_with_auth(self):
        """GET /api/v1/health with a valid token should return 200."""
        status, body = self._get('/api/v1/health', token='test-secret-token')
        self.assertEqual(status, 200)
        self.assertEqual(body['status'], 'healthy')

    # ------------------------------------------------------------------ #
    # Status endpoint (auth required)
    # ------------------------------------------------------------------ #
    def test_status_endpoint_requires_auth(self):
        """GET /api/v1/status without a token should return 401."""
        status, body = self._get('/api/v1/status')
        self.assertEqual(status, 401)
        self.assertIn('error', body)

    def test_status_endpoint_with_auth(self):
        """GET /api/v1/status with a valid token should return 200."""
        status, body = self._get('/api/v1/status', token='test-secret-token')
        self.assertEqual(status, 200)
        self.assertIn('running', body)
        self.assertIn('paused', body)

    # ------------------------------------------------------------------ #
    # Pause / Resume
    # ------------------------------------------------------------------ #
    def test_pause_resume(self):
        """POST /api/v1/pause should set paused=True; POST /api/v1/resume should set paused=False."""
        # Pause
        status, body = self._post('/api/v1/pause', token='test-secret-token')
        self.assertEqual(status, 200)
        self.assertTrue(body['paused'])
        self.assertTrue(self.daemon.paused)

        # Resume
        status, body = self._post('/api/v1/resume', token='test-secret-token')
        self.assertEqual(status, 200)
        self.assertFalse(body['paused'])
        self.assertFalse(self.daemon.paused)

    # ------------------------------------------------------------------ #
    # GET /api/v1/schedule/rules
    # ------------------------------------------------------------------ #
    def test_get_schedule_rules(self):
        """GET /api/v1/schedule/rules should return current rules."""
        status, body = self._get('/api/v1/schedule/rules', token='test-secret-token')
        self.assertEqual(status, 200)
        self.assertIn('rules', body)
        self.assertIn('timezone', body)

    # ------------------------------------------------------------------ #
    # PUT /api/v1/schedule/rules
    # ------------------------------------------------------------------ #
    def test_update_schedule_rules(self):
        """PUT /api/v1/schedule/rules should update rules at runtime."""
        new_rules = [
            {'cron': '0 8 * * 1-5', 'target_count': 10, 'label': 'Weekday morning'},
            {'cron': '0 20 * * 1-5', 'target_count': 3, 'label': 'Weekday evening'},
        ]
        status, body = self._put('/api/v1/schedule/rules', token='test-secret-token',
                                 data={'rules': new_rules, 'timezone': 'Europe/Paris'})
        self.assertEqual(status, 200)
        self.assertTrue(body['updated'])
        self.assertEqual(body['rule_count'], 2)
        self.assertEqual(body['timezone'], 'Europe/Paris')

        # Verify rules are persisted in memory
        status, body = self._get('/api/v1/schedule/rules', token='test-secret-token')
        self.assertEqual(len(body['rules']), 2)
        self.assertEqual(body['timezone'], 'Europe/Paris')

    def test_update_schedule_rules_invalid_cron(self):
        """PUT with invalid cron should return 400."""
        bad_rules = [{'cron': '0 8 *', 'target_count': 5}]
        status, body = self._put('/api/v1/schedule/rules', token='test-secret-token',
                                 data={'rules': bad_rules})
        self.assertEqual(status, 400)
        self.assertIn('error', body)

    def test_update_schedule_rules_missing_target(self):
        """PUT with missing target_count should return 400."""
        bad_rules = [{'cron': '0 8 * * *'}]
        status, body = self._put('/api/v1/schedule/rules', token='test-secret-token',
                                 data={'rules': bad_rules})
        self.assertEqual(status, 400)
        self.assertIn('error', body)

    # ------------------------------------------------------------------ #
    # POST /api/v1/scale/to
    # ------------------------------------------------------------------ #
    def test_scale_to_target(self):
        """POST /api/v1/scale/to should accept a target_count."""
        self.daemon.current_worker_count = 2
        self.daemon._execute_scale_out = MagicMock()
        status, body = self._post('/api/v1/scale/to', token='test-secret-token',
                                  data={'target_count': 5})
        self.assertEqual(status, 200)
        self.assertEqual(body['action'], 'scale_out')
        self.assertEqual(body['target'], 5)

    def test_scale_to_invalid(self):
        """POST /api/v1/scale/to with invalid target should return 400."""
        status, body = self._post('/api/v1/scale/to', token='test-secret-token',
                                  data={'target_count': 'abc'})
        self.assertEqual(status, 400)

    def test_scale_to_no_change(self):
        """POST /api/v1/scale/to with current count should return no_change."""
        self.daemon.current_worker_count = 5
        status, body = self._post('/api/v1/scale/to', token='test-secret-token',
                                  data={'target_count': 5})
        self.assertEqual(status, 200)
        self.assertEqual(body['action'], 'no_change')


# ====================================================================== #
# Daemon method tests (no HTTP, mocked collaborators)
# ====================================================================== #
class TestAutoscalerDaemon(unittest.TestCase):
    """Tests for AutoscalerDaemon methods with mocked dependencies."""

    def setUp(self):
        self.config = _make_config()
        self.daemon = _create_daemon(self.config)

    # ------------------------------------------------------------------ #
    # Evaluation skipped when paused
    # ------------------------------------------------------------------ #
    def test_evaluation_skipped_when_paused(self):
        """When daemon.paused is True the main loop should NOT call _evaluation_cycle."""
        self.daemon.paused = True
        self.daemon.running = True

        # Replace _evaluation_cycle with a mock to detect if it's called
        self.daemon._evaluation_cycle = MagicMock()

        # Run a single iteration of the main loop in a background thread
        # by setting running=False after a brief delay
        def stop_soon():
            time.sleep(0.15)
            self.daemon.running = False

        # Override evaluation_interval so the loop doesn't block long
        self.daemon.evaluation_interval = 0.05
        stopper = threading.Thread(target=stop_soon, daemon=True)
        stopper.start()
        self.daemon.run()
        stopper.join(timeout=2)

        self.daemon._evaluation_cycle.assert_not_called()

    # ------------------------------------------------------------------ #
    # get_status returns expected keys
    # ------------------------------------------------------------------ #
    def test_get_status_returns_metrics(self):
        """get_status() should return a dict with all expected keys."""
        status = self.daemon.get_status()
        expected_keys = {
            'running', 'paused', 'current_worker_count',
            'last_decision', 'last_decision_reason', 'last_decision_time',
            'scale_out_events', 'scale_in_events', 'last_metrics',
        }
        self.assertEqual(set(status.keys()), expected_keys)
        self.assertTrue(status['running'])
        self.assertEqual(status['last_decision'], 'NO_ACTION')
        self.assertEqual(status['scale_out_events'], 0)
        self.assertEqual(status['scale_in_events'], 0)

    # ------------------------------------------------------------------ #
    # Evaluation cycle delegates to collaborators
    # ------------------------------------------------------------------ #
    def test_evaluation_cycle_calls_policy_engine(self):
        """_evaluation_cycle should collect metrics and evaluate the policy."""
        fake_metrics = {'avg_cpu_pct': 50, 'avg_memory_pct': 50}
        self.daemon.metrics_collector.get_aggregated_metrics.return_value = fake_metrics

        from scaling_policy import ScalingDecision
        self.daemon.policy_engine.evaluate.return_value = (
            ScalingDecision.NO_ACTION, 5, 'Within thresholds')

        with patch.object(self.daemon, '_get_worker_count', return_value=5):
            self.daemon._evaluation_cycle()

        self.daemon.metrics_collector.get_aggregated_metrics.assert_called_once()
        self.daemon.policy_engine.evaluate.assert_called_once()
        self.assertEqual(self.daemon.last_decision, 'NO_ACTION')
        self.assertEqual(self.daemon.last_metrics, fake_metrics)

    # ------------------------------------------------------------------ #
    # Evaluation cycle handles None metrics gracefully
    # ------------------------------------------------------------------ #
    def test_evaluation_cycle_skips_on_none_metrics(self):
        """When metrics are None, the policy engine should NOT be consulted."""
        self.daemon.metrics_collector.get_aggregated_metrics.return_value = None

        with patch.object(self.daemon, '_get_worker_count', return_value=5):
            self.daemon._evaluation_cycle()

        self.daemon.policy_engine.evaluate.assert_not_called()


if __name__ == '__main__':
    unittest.main()
