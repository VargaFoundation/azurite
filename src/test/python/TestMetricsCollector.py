#!/usr/bin/env python3
"""
Unit tests for the MetricsCollector class.
All HTTP calls are mocked so no real YARN RM is needed.
"""
import sys
import os
import json
import unittest

# Python 2/3 compatible mock import
try:
    from unittest.mock import patch, MagicMock
except ImportError:
    from mock import patch, MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                '..', '..', '..', 'src', 'main', 'resources',
                                'addon-services', 'AZURE_AUTOSCALER', '1.0.0', 'package', 'files'))
from metrics_collector import MetricsCollector

# Determine the correct module path for patching urlopen
_MC_MODULE = 'metrics_collector'


def _yarn_metrics_response(**overrides):
    """Build a fake YARN /ws/v1/cluster/metrics JSON response."""
    defaults = {
        'containersPending': 5,
        'pendingMemoryMB': 1024,
        'pendingVirtualCores': 4,
        'availableMB': 8192,
        'availableVirtualCores': 16,
        'allocatedMB': 16384,
        'allocatedVirtualCores': 32,
        'totalMB': 24576,
        'totalVirtualCores': 48,
        'activeNodes': 3,
        'decommissioningNodes': 0,
        'appsRunning': 2,
        'appsPending': 1,
    }
    defaults.update(overrides)
    return json.dumps({'clusterMetrics': defaults}).encode()


class TestMetricsCollector(unittest.TestCase):
    """Tests for MetricsCollector."""

    # ------------------------------------------------------------------ #
    # YARN metrics
    # ------------------------------------------------------------------ #
    @patch(_MC_MODULE + '.urlopen')
    def test_collect_yarn_metrics_success(self, mock_urlopen):
        """Valid YARN JSON response should be parsed into the expected dict keys."""
        mock_response = MagicMock()
        mock_response.read.return_value = _yarn_metrics_response()
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_response

        collector = MetricsCollector('http://rm-host:8088')
        result = collector.collect_yarn_metrics()

        self.assertIsNotNone(result)
        self.assertEqual(result['pending_containers'], 5)
        self.assertEqual(result['available_memory_mb'], 8192)
        self.assertEqual(result['allocated_memory_mb'], 16384)
        self.assertEqual(result['total_memory_mb'], 24576)
        self.assertEqual(result['active_nodes'], 3)
        self.assertEqual(result['apps_running'], 2)
        self.assertEqual(result['apps_pending'], 1)

    @patch(_MC_MODULE + '.urlopen')
    def test_collect_yarn_metrics_failure(self, mock_urlopen):
        """URLError from urlopen should cause collect_yarn_metrics to return None."""
        try:
            from urllib.error import URLError
        except ImportError:
            from urllib2 import URLError

        mock_urlopen.side_effect = URLError('connection refused')

        collector = MetricsCollector('http://rm-host:8088')
        result = collector.collect_yarn_metrics()

        self.assertIsNone(result, 'Should return None when YARN RM is unreachable')

    # ------------------------------------------------------------------ #
    # Aggregated metrics
    # ------------------------------------------------------------------ #
    @patch.object(MetricsCollector, 'collect_system_metrics')
    @patch.object(MetricsCollector, 'collect_yarn_metrics')
    def test_get_aggregated_metrics_both(self, mock_yarn, mock_system):
        """yarn_and_system mode should merge YARN and system metrics."""
        mock_yarn.return_value = {
            'pending_containers': 3,
            'available_memory_mb': 4096,
            'total_memory_mb': 16384,
            'allocated_memory_mb': 12288,
        }
        mock_system.return_value = {
            'avg_cpu_pct': 55.0,
            'avg_memory_pct': 70.0,
        }

        collector = MetricsCollector('http://rm-host:8088', metrics_source='yarn_and_system')
        result = collector.get_aggregated_metrics()

        self.assertIsNotNone(result)
        # YARN keys present
        self.assertEqual(result['pending_containers'], 3)
        # System keys present
        self.assertAlmostEqual(result['avg_cpu_pct'], 55.0)
        self.assertAlmostEqual(result['avg_memory_pct'], 70.0)
        # Computed YARN memory utilisation
        self.assertIn('yarn_memory_used_pct', result)
        self.assertIn('yarn_memory_available_pct', result)

    @patch.object(MetricsCollector, 'collect_system_metrics')
    @patch.object(MetricsCollector, 'collect_yarn_metrics')
    def test_get_aggregated_metrics_yarn_only(self, mock_yarn, mock_system):
        """yarn_only mode should not call collect_system_metrics."""
        mock_yarn.return_value = {
            'pending_containers': 1,
            'available_memory_mb': 2048,
            'total_memory_mb': 8192,
            'allocated_memory_mb': 6144,
        }

        collector = MetricsCollector('http://rm-host:8088', metrics_source='yarn_only')
        result = collector.get_aggregated_metrics()

        mock_system.assert_not_called()
        self.assertIsNotNone(result)
        self.assertEqual(result['pending_containers'], 1)

    # ------------------------------------------------------------------ #
    # YARN memory utilisation computation
    # ------------------------------------------------------------------ #
    @patch.object(MetricsCollector, 'collect_system_metrics')
    @patch.object(MetricsCollector, 'collect_yarn_metrics')
    def test_yarn_memory_utilization_computed(self, mock_yarn, mock_system):
        """yarn_memory_used_pct and yarn_memory_available_pct should be computed from totals."""
        mock_yarn.return_value = {
            'available_memory_mb': 2000,
            'total_memory_mb': 10000,
            'allocated_memory_mb': 8000,
        }
        mock_system.return_value = None  # system metrics unavailable

        collector = MetricsCollector('http://rm-host:8088', metrics_source='yarn_and_system')
        result = collector.get_aggregated_metrics()

        self.assertIsNotNone(result)
        expected_used_pct = ((10000 - 2000) / 10000) * 100  # 80%
        expected_available_pct = (2000 / 10000) * 100         # 20%
        self.assertAlmostEqual(result['yarn_memory_used_pct'], expected_used_pct, places=2)
        self.assertAlmostEqual(result['yarn_memory_available_pct'], expected_available_pct, places=2)

    @patch.object(MetricsCollector, 'collect_system_metrics')
    @patch.object(MetricsCollector, 'collect_yarn_metrics')
    def test_yarn_memory_utilization_zero_total(self, mock_yarn, mock_system):
        """When total_memory_mb is 0, used_pct should default to 0 and available to 100."""
        mock_yarn.return_value = {
            'available_memory_mb': 0,
            'total_memory_mb': 0,
            'allocated_memory_mb': 0,
        }
        mock_system.return_value = None

        collector = MetricsCollector('http://rm-host:8088', metrics_source='yarn_and_system')
        result = collector.get_aggregated_metrics()

        self.assertIsNotNone(result)
        self.assertEqual(result['yarn_memory_used_pct'], 0)
        self.assertEqual(result['yarn_memory_available_pct'], 100)


if __name__ == '__main__':
    unittest.main()
