#!/usr/bin/env python3
"""
Cost Tracker - Estimates cluster running costs based on VM inventory.
Provides hourly/daily/monthly cost estimates and budget alerting.
"""
import json
import logging
import os
import time

logger = logging.getLogger('cost_tracker')

# Azure VM pay-as-you-go prices (USD/hour) for common sizes
# Source: Azure pricing calculator, East US region, Linux
# These are approximate - actual prices vary by region and discounts
AZURE_VM_PRICES = {
    # General Purpose - Dv3
    'Standard_D2_v3': 0.096,
    'Standard_D4_v3': 0.192,
    'Standard_D8_v3': 0.384,
    'Standard_D16_v3': 0.768,
    'Standard_D32_v3': 1.536,
    # General Purpose - Dv4
    'Standard_D2_v4': 0.096,
    'Standard_D4_v4': 0.192,
    'Standard_D8_v4': 0.384,
    'Standard_D16_v4': 0.768,
    # General Purpose - Dv5
    'Standard_D2_v5': 0.096,
    'Standard_D4_v5': 0.192,
    'Standard_D8_v5': 0.384,
    # Memory Optimized - Ev3
    'Standard_E2_v3': 0.126,
    'Standard_E4_v3': 0.252,
    'Standard_E8_v3': 0.504,
    'Standard_E16_v3': 1.008,
    # Compute Optimized - Fv2
    'Standard_F2s_v2': 0.085,
    'Standard_F4s_v2': 0.170,
    'Standard_F8s_v2': 0.340,
    # Budget - Av2
    'Standard_A2_v2': 0.043,
    'Standard_A4_v2': 0.087,
    'Standard_A8_v2': 0.174,
    # Budget - Bv2
    'Standard_B2s': 0.042,
    'Standard_B4ms': 0.166,
}

# Spot discount approximation (varies, typically 60-80% off)
SPOT_DISCOUNT = 0.7  # 70% discount


class CostTracker:
    """Tracks and estimates cluster running costs."""

    def __init__(self, inventory_file=None, budget_limit=0, history_file=None):
        """
        Args:
            inventory_file: Path to vm_inventory.json
            budget_limit: Daily budget limit in USD (0 = no limit)
            history_file: Path to cost history JSON file
        """
        self.inventory_file = inventory_file
        self.budget_limit = budget_limit
        self.history_file = history_file or (
            os.path.join(os.path.dirname(inventory_file), 'cost_history.json')
            if inventory_file else '/tmp/cost_history.json'
        )
        self._history = self._load_history()

    def get_hourly_cost(self, inventory=None):
        """
        Calculate current hourly cost based on running VMs.
        Returns dict with per-role and total costs.
        """
        if inventory is None:
            inventory = self._load_inventory()

        vms = inventory.get('vms', [])
        costs = {'head': 0.0, 'worker': 0.0, 'zookeeper': 0.0, 'other': 0.0, 'total': 0.0}

        for vm in vms:
            if vm.get('status') != 'running':
                continue
            size = vm.get('size', '')
            price = AZURE_VM_PRICES.get(size, 0.0)

            # Apply spot discount if applicable
            if vm.get('spot', False):
                price *= (1 - SPOT_DISCOUNT)

            role = vm.get('role', 'other')
            if role in costs:
                costs[role] += price
            else:
                costs['other'] += price
            costs['total'] += price

        return costs

    def get_daily_cost(self, inventory=None):
        """Estimate daily cost (hourly * 24)."""
        hourly = self.get_hourly_cost(inventory)
        return {k: v * 24 for k, v in hourly.items()}

    def get_monthly_cost(self, inventory=None):
        """Estimate monthly cost (hourly * 730)."""
        hourly = self.get_hourly_cost(inventory)
        return {k: round(v * 730, 2) for k, v in hourly.items()}

    def check_budget(self, inventory=None):
        """
        Check if daily cost exceeds budget limit.
        Returns (over_budget: bool, daily_cost: float, budget: float, pct: float)
        """
        if self.budget_limit <= 0:
            return (False, 0.0, 0.0, 0.0)

        daily = self.get_daily_cost(inventory)
        daily_total = daily['total']
        pct = (daily_total / self.budget_limit) * 100 if self.budget_limit > 0 else 0

        return (daily_total > self.budget_limit, daily_total, self.budget_limit, pct)

    def record_snapshot(self, inventory=None):
        """Record a cost snapshot for historical tracking."""
        costs = self.get_hourly_cost(inventory)
        snapshot = {
            'timestamp': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
            'hourly_cost': costs['total'],
            'vm_count': sum(1 for vm in (inventory or self._load_inventory()).get('vms', [])
                           if vm.get('status') == 'running'),
        }
        self._history.append(snapshot)
        # Keep last 30 days (720 hourly snapshots)
        if len(self._history) > 720:
            self._history = self._history[-720:]
        self._save_history()

    def get_cost_summary(self, inventory=None):
        """Get complete cost summary for API response."""
        hourly = self.get_hourly_cost(inventory)
        daily = self.get_daily_cost(inventory)
        monthly = self.get_monthly_cost(inventory)
        over_budget, daily_total, budget, pct = self.check_budget(inventory)

        return {
            'hourly': hourly,
            'daily': daily,
            'monthly': monthly,
            'budget': {
                'limit': self.budget_limit,
                'daily_cost': daily_total,
                'percentage': round(pct, 1),
                'over_budget': over_budget,
            },
            'history_points': len(self._history),
        }

    def _load_inventory(self):
        if self.inventory_file and os.path.exists(self.inventory_file):
            with open(self.inventory_file, 'r') as f:
                return json.load(f)
        return {'vms': []}

    def _load_history(self):
        if self.history_file and os.path.exists(self.history_file):
            try:
                with open(self.history_file, 'r') as f:
                    return json.load(f)
            except (json.JSONDecodeError, IOError):
                return []
        return []

    def _save_history(self):
        if self.history_file:
            try:
                with open(self.history_file, 'w') as f:
                    json.dump(self._history, f)
            except IOError as e:
                logger.error('Failed to save cost history: %s', e)
