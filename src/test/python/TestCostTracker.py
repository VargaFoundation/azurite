#!/usr/bin/env python3
"""
Unit tests for the CostTracker class.
All filesystem access is avoided by passing inventory dicts directly.
"""
import sys
import os
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                '..', '..', '..', 'src', 'main', 'resources',
                                'addon-services', 'AZURE_VM_MANAGER', '1.0.0', 'package', 'files'))
from cost_tracker import CostTracker, AZURE_VM_PRICES


def _make_vm(name, role, size, status='running', spot=False):
    """Helper to build a VM inventory entry."""
    vm = {'name': name, 'role': role, 'size': size, 'status': status}
    if spot:
        vm['spot'] = True
    return vm


class TestCostTracker(unittest.TestCase):
    """Tests for CostTracker."""

    # ------------------------------------------------------------------ #
    # Hourly cost
    # ------------------------------------------------------------------ #
    def test_hourly_cost_empty_inventory(self):
        """No VMs in the inventory should produce all-zero costs."""
        tracker = CostTracker()
        costs = tracker.get_hourly_cost(inventory={'vms': []})
        self.assertEqual(costs['head'], 0.0)
        self.assertEqual(costs['worker'], 0.0)
        self.assertEqual(costs['zookeeper'], 0.0)
        self.assertEqual(costs['other'], 0.0)
        self.assertEqual(costs['total'], 0.0)

    def test_hourly_cost_single_worker(self):
        """One running worker Standard_D4_v3 should cost 0.192/hr."""
        tracker = CostTracker()
        inv = {'vms': [_make_vm('w1', 'worker', 'Standard_D4_v3')]}
        costs = tracker.get_hourly_cost(inventory=inv)
        self.assertAlmostEqual(costs['worker'], 0.192)
        self.assertAlmostEqual(costs['total'], 0.192)

    def test_hourly_cost_mixed_roles(self):
        """2 head D4_v3 + 3 workers D4_v3 + 3 ZK A2_v2 should sum correctly."""
        vms = (
            [_make_vm('h{0}'.format(i), 'head', 'Standard_D4_v3') for i in range(2)] +
            [_make_vm('w{0}'.format(i), 'worker', 'Standard_D4_v3') for i in range(3)] +
            [_make_vm('z{0}'.format(i), 'zookeeper', 'Standard_A2_v2') for i in range(3)]
        )
        tracker = CostTracker()
        costs = tracker.get_hourly_cost(inventory={'vms': vms})

        expected_head = 2 * AZURE_VM_PRICES['Standard_D4_v3']
        expected_worker = 3 * AZURE_VM_PRICES['Standard_D4_v3']
        expected_zk = 3 * AZURE_VM_PRICES['Standard_A2_v2']
        expected_total = expected_head + expected_worker + expected_zk

        self.assertAlmostEqual(costs['head'], expected_head)
        self.assertAlmostEqual(costs['worker'], expected_worker)
        self.assertAlmostEqual(costs['zookeeper'], expected_zk)
        self.assertAlmostEqual(costs['total'], expected_total)

    def test_stopped_vms_not_counted(self):
        """A VM with status 'stopped' should not contribute to cost."""
        tracker = CostTracker()
        inv = {'vms': [
            _make_vm('w1', 'worker', 'Standard_D4_v3', status='stopped'),
            _make_vm('w2', 'worker', 'Standard_D4_v3', status='running'),
        ]}
        costs = tracker.get_hourly_cost(inventory=inv)
        self.assertAlmostEqual(costs['worker'], 0.192)
        self.assertAlmostEqual(costs['total'], 0.192)

    def test_spot_discount_applied(self):
        """A spot VM should have a 70% discount applied."""
        tracker = CostTracker()
        inv = {'vms': [_make_vm('w1', 'worker', 'Standard_D4_v3', spot=True)]}
        costs = tracker.get_hourly_cost(inventory=inv)
        expected = AZURE_VM_PRICES['Standard_D4_v3'] * (1 - 0.7)
        self.assertAlmostEqual(costs['worker'], expected)
        self.assertAlmostEqual(costs['total'], expected)

    # ------------------------------------------------------------------ #
    # Daily and monthly projections
    # ------------------------------------------------------------------ #
    def test_daily_cost_is_24x_hourly(self):
        """Daily cost should be exactly 24 times the hourly cost."""
        tracker = CostTracker()
        inv = {'vms': [_make_vm('w1', 'worker', 'Standard_D4_v3')]}
        hourly = tracker.get_hourly_cost(inventory=inv)
        daily = tracker.get_daily_cost(inventory=inv)
        self.assertAlmostEqual(daily['total'], hourly['total'] * 24)
        self.assertAlmostEqual(daily['worker'], hourly['worker'] * 24)

    def test_monthly_cost_is_730x_hourly(self):
        """Monthly cost should be hourly * 730 (rounded to 2 decimals)."""
        tracker = CostTracker()
        inv = {'vms': [_make_vm('w1', 'worker', 'Standard_D4_v3')]}
        hourly = tracker.get_hourly_cost(inventory=inv)
        monthly = tracker.get_monthly_cost(inventory=inv)
        self.assertAlmostEqual(monthly['total'], round(hourly['total'] * 730, 2))

    # ------------------------------------------------------------------ #
    # Budget checks
    # ------------------------------------------------------------------ #
    def test_budget_not_exceeded(self):
        """Daily cost < budget -> over_budget should be False."""
        inv = {'vms': [_make_vm('w1', 'worker', 'Standard_D4_v3')]}
        daily_total = AZURE_VM_PRICES['Standard_D4_v3'] * 24  # ~4.608
        tracker = CostTracker(budget_limit=100.0)
        over_budget, daily_cost, budget, pct = tracker.check_budget(inventory=inv)
        self.assertFalse(over_budget)
        self.assertAlmostEqual(daily_cost, daily_total)
        self.assertEqual(budget, 100.0)

    def test_budget_exceeded(self):
        """Daily cost > budget -> over_budget should be True."""
        inv = {'vms': [_make_vm('w1', 'worker', 'Standard_D4_v3')]}
        tracker = CostTracker(budget_limit=1.0)  # budget much lower than daily ~4.608
        over_budget, daily_cost, budget, pct = tracker.check_budget(inventory=inv)
        self.assertTrue(over_budget)
        self.assertGreater(daily_cost, budget)

    def test_budget_zero_means_no_limit(self):
        """Budget=0 should always return over_budget=False regardless of costs."""
        inv = {'vms': [
            _make_vm('w{0}'.format(i), 'worker', 'Standard_D32_v3') for i in range(100)
        ]}
        tracker = CostTracker(budget_limit=0)
        over_budget, daily_cost, budget, pct = tracker.check_budget(inventory=inv)
        self.assertFalse(over_budget)

    # ------------------------------------------------------------------ #
    # Edge cases
    # ------------------------------------------------------------------ #
    def test_unknown_vm_size_zero_cost(self):
        """Unknown VM size should be treated as 0 cost without raising an error."""
        tracker = CostTracker()
        inv = {'vms': [_make_vm('x1', 'worker', 'Standard_NONEXISTENT_v99')]}
        costs = tracker.get_hourly_cost(inventory=inv)
        self.assertEqual(costs['worker'], 0.0)
        self.assertEqual(costs['total'], 0.0)


if __name__ == '__main__':
    unittest.main()
