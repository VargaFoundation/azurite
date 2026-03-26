#!/usr/bin/env python3
"""
Unit tests for the NodeHealthMonitor class.
All Azure compute calls are mocked so no real Azure subscription is needed.
"""
import sys
import os
import unittest

# Python 2/3 compatible mock import
try:
    from unittest.mock import MagicMock, patch, call
except ImportError:
    from mock import MagicMock, patch, call

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                '..', '..', '..', 'src', 'main', 'resources',
                                'addon-services', 'AZURE_VM_MANAGER', '1.0.0', 'package', 'files'))
from node_health_monitor import NodeHealthMonitor


def _make_instance_view(*power_states):
    """Build a mock instance_view with the given PowerState codes."""
    view = MagicMock()
    statuses = []
    for code in power_states:
        s = MagicMock()
        s.code = code
        statuses.append(s)
    view.statuses = statuses
    return view


def _make_inventory(*vm_specs):
    """Build an inventory dict from (name, role, status) tuples."""
    return {'vms': [{'name': n, 'role': r, 'status': s} for n, r, s in vm_specs]}


class TestNodeHealthMonitor(unittest.TestCase):
    """Tests for NodeHealthMonitor."""

    # ------------------------------------------------------------------ #
    # Power state detection
    # ------------------------------------------------------------------ #
    def test_all_healthy(self):
        """All VMs with PowerState/running should be reported as healthy."""
        mock_compute = MagicMock()
        mock_compute.virtual_machines.instance_view.return_value = \
            _make_instance_view('ProvisioningState/succeeded', 'PowerState/running')

        monitor = NodeHealthMonitor(mock_compute, 'rg-test')
        inv = _make_inventory(('h1', 'head', 'running'), ('w1', 'worker', 'running'))
        results = monitor.check_vm_power_states(inv)

        self.assertEqual(len(results), 2)
        for name, info in results.items():
            self.assertTrue(info['healthy'], '{0} should be healthy'.format(name))
            self.assertEqual(info['actual'], 'running')

    def test_detect_stopped_vm(self):
        """VM with PowerState/stopped should be detected as unhealthy."""
        mock_compute = MagicMock()
        mock_compute.virtual_machines.instance_view.return_value = \
            _make_instance_view('PowerState/stopped')

        monitor = NodeHealthMonitor(mock_compute, 'rg-test')
        inv = _make_inventory(('w1', 'worker', 'running'))
        results = monitor.check_vm_power_states(inv)

        self.assertFalse(results['w1']['healthy'])
        self.assertEqual(results['w1']['actual'], 'stopped')

    def test_detect_deallocated_vm(self):
        """VM with PowerState/deallocated should be detected as unhealthy."""
        mock_compute = MagicMock()
        mock_compute.virtual_machines.instance_view.return_value = \
            _make_instance_view('PowerState/deallocated')

        monitor = NodeHealthMonitor(mock_compute, 'rg-test')
        inv = _make_inventory(('w1', 'worker', 'running'))
        results = monitor.check_vm_power_states(inv)

        self.assertFalse(results['w1']['healthy'])
        self.assertEqual(results['w1']['actual'], 'deallocated')

    def test_detect_not_found_vm(self):
        """ResourceNotFound exception should set actual='not_found'."""
        mock_compute = MagicMock()
        mock_compute.virtual_machines.instance_view.side_effect = \
            Exception('ResourceNotFound: VM was not found')

        monitor = NodeHealthMonitor(mock_compute, 'rg-test')
        inv = _make_inventory(('w1', 'worker', 'running'))
        results = monitor.check_vm_power_states(inv)

        self.assertFalse(results['w1']['healthy'])
        self.assertEqual(results['w1']['actual'], 'not_found')

    # ------------------------------------------------------------------ #
    # Dead worker filtering
    # ------------------------------------------------------------------ #
    def test_get_dead_workers_only_workers(self):
        """get_dead_workers should only return VMs with role 'worker'."""
        mock_compute = MagicMock()
        mock_compute.virtual_machines.instance_view.return_value = \
            _make_instance_view('PowerState/deallocated')

        monitor = NodeHealthMonitor(mock_compute, 'rg-test')
        inv = _make_inventory(
            ('w1', 'worker', 'running'),
            ('w2', 'worker', 'running'),
            ('z1', 'zookeeper', 'running'),
        )
        dead = monitor.get_dead_workers(inv)

        self.assertIn('w1', dead)
        self.assertIn('w2', dead)
        self.assertNotIn('z1', dead)

    def test_head_nodes_not_in_dead_workers(self):
        """Dead head nodes should not be returned by get_dead_workers."""
        mock_compute = MagicMock()
        mock_compute.virtual_machines.instance_view.return_value = \
            _make_instance_view('PowerState/stopped')

        monitor = NodeHealthMonitor(mock_compute, 'rg-test')
        inv = _make_inventory(('h1', 'head', 'running'), ('w1', 'worker', 'running'))
        dead = monitor.get_dead_workers(inv)

        self.assertNotIn('h1', dead)
        self.assertIn('w1', dead)

    # ------------------------------------------------------------------ #
    # No compute client
    # ------------------------------------------------------------------ #
    def test_no_compute_client_returns_empty(self):
        """If compute_client is None, check_vm_power_states should return empty dict."""
        monitor = NodeHealthMonitor(None, 'rg-test')
        inv = _make_inventory(('w1', 'worker', 'running'))
        results = monitor.check_vm_power_states(inv)
        self.assertEqual(results, {})

    # ------------------------------------------------------------------ #
    # Auto-recovery
    # ------------------------------------------------------------------ #
    def test_auto_recover_provisions_replacement(self):
        """auto_recover should call create_vm on vm_operations for each dead worker."""
        mock_compute = MagicMock()
        mock_compute.virtual_machines.instance_view.return_value = \
            _make_instance_view('PowerState/deallocated')

        mock_vm_ops = MagicMock()

        monitor = NodeHealthMonitor(mock_compute, 'rg-test', vm_operations=mock_vm_ops)
        inv = _make_inventory(('w1', 'worker', 'running'))
        pool_config = {'size': 'Standard_D4_v3', 'data_disks': 4,
                       'data_disk_size_gb': 512, 'disk_type': 'Standard_LRS',
                       'disk_size_gb': 128, 'spot_enabled': False, 'spot_max_price': -1}

        actions = monitor.auto_recover(inv, pool_config=pool_config)

        self.assertEqual(len(actions), 1)
        self.assertEqual(actions[0]['type'], 'replaced')
        self.assertEqual(actions[0]['dead_vm'], 'w1')
        self.assertEqual(actions[0]['reason'], 'deallocated')

        # Verify delete_vm was called for the deallocated (non-not_found) VM
        mock_vm_ops.delete_vm.assert_called_once_with('w1')
        # Verify create_vm was called exactly once with expected arguments
        mock_vm_ops.create_vm.assert_called_once()
        create_kwargs = mock_vm_ops.create_vm.call_args
        self.assertEqual(create_kwargs[1]['vm_size'], 'Standard_D4_v3')
        self.assertEqual(create_kwargs[1]['role'], 'worker')


if __name__ == '__main__':
    unittest.main()
