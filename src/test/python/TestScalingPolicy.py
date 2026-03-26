#!/usr/bin/env python3
"""
Unit tests for the ScalingPolicyEngine and ScalingDecision classes.
"""
import sys
import os
import time
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                '..', '..', '..', 'src', 'main', 'resources',
                                'addon-services', 'AZURE_AUTOSCALER', '1.0.0', 'package', 'files'))
from scaling_policy import ScalingDecision, ScalingPolicyEngine


def _make_config(**overrides):
    """Return a default test config with optional overrides."""
    config = {
        'cpu_scale_out_threshold': 80,
        'cpu_scale_in_threshold': 30,
        'memory_scale_out_threshold': 80,
        'memory_scale_in_threshold': 30,
        'yarn_pending_containers_threshold': 10,
        'yarn_available_memory_scale_in_pct': 60,
        'scale_out_trigger_duration': 0,   # instant for most tests
        'scale_in_trigger_duration': 0,    # instant for most tests
        'cooldown_scale_out': 300,
        'cooldown_scale_in': 600,
        'scale_out_increment': 1,
        'scale_in_decrement': 1,
    }
    config.update(overrides)
    return config


class TestScalingPolicy(unittest.TestCase):
    """Tests for ScalingPolicyEngine."""

    # ------------------------------------------------------------------ #
    # Helper
    # ------------------------------------------------------------------ #
    def _engine(self, **config_overrides):
        return ScalingPolicyEngine(_make_config(**config_overrides))

    # ------------------------------------------------------------------ #
    # Basic threshold tests
    # ------------------------------------------------------------------ #
    def test_no_action_within_thresholds(self):
        """Metrics well within thresholds should produce NO_ACTION."""
        engine = self._engine()
        metrics = {'avg_cpu_pct': 50, 'avg_memory_pct': 50}
        decision, target, reason = engine.evaluate(metrics, current_worker_count=5,
                                                   min_workers=2, max_workers=10)
        self.assertEqual(decision, ScalingDecision.NO_ACTION)
        self.assertEqual(target, 5)

    def test_scale_out_on_high_cpu(self):
        """CPU above scale-out threshold (sustained) should trigger SCALE_OUT."""
        engine = self._engine()
        metrics = {'avg_cpu_pct': 90}
        # First call records the breach; with trigger_duration=0 the second
        # call (same breach still present) will satisfy the sustained check.
        engine.evaluate(metrics, 5, 2, 10)
        decision, target, _ = engine.evaluate(metrics, 5, 2, 10)
        self.assertEqual(decision, ScalingDecision.SCALE_OUT)
        self.assertEqual(target, 6)

    def test_scale_out_on_high_memory(self):
        """Memory above scale-out threshold (sustained) should trigger SCALE_OUT."""
        engine = self._engine()
        metrics = {'avg_memory_pct': 90}
        engine.evaluate(metrics, 5, 2, 10)
        decision, target, _ = engine.evaluate(metrics, 5, 2, 10)
        self.assertEqual(decision, ScalingDecision.SCALE_OUT)
        self.assertEqual(target, 6)

    def test_scale_out_on_yarn_pending(self):
        """YARN pending containers above threshold (sustained) should trigger SCALE_OUT."""
        engine = self._engine()
        metrics = {'pending_containers': 20}
        engine.evaluate(metrics, 5, 2, 10)
        decision, target, reason = engine.evaluate(metrics, 5, 2, 10)
        self.assertEqual(decision, ScalingDecision.SCALE_OUT)
        self.assertIn('pending containers', reason)

    def test_scale_in_on_low_cpu(self):
        """CPU below scale-in threshold (sustained) should trigger SCALE_IN."""
        engine = self._engine()
        metrics = {'avg_cpu_pct': 10}
        engine.evaluate(metrics, 5, 2, 10)
        decision, target, _ = engine.evaluate(metrics, 5, 2, 10)
        self.assertEqual(decision, ScalingDecision.SCALE_IN)
        self.assertEqual(target, 4)

    def test_scale_in_on_low_memory(self):
        """Memory below scale-in threshold (sustained) should trigger SCALE_IN."""
        engine = self._engine()
        metrics = {'avg_memory_pct': 10}
        engine.evaluate(metrics, 5, 2, 10)
        decision, target, _ = engine.evaluate(metrics, 5, 2, 10)
        self.assertEqual(decision, ScalingDecision.SCALE_IN)
        self.assertEqual(target, 4)

    # ------------------------------------------------------------------ #
    # Sustained breach
    # ------------------------------------------------------------------ #
    def test_sustained_breach_required(self):
        """With non-zero trigger duration, the first evaluation must NOT trigger scaling."""
        engine = self._engine(scale_out_trigger_duration=60)
        metrics = {'avg_cpu_pct': 90}
        decision, target, _ = engine.evaluate(metrics, 5, 2, 10)
        self.assertEqual(decision, ScalingDecision.NO_ACTION,
                         'First evaluation should not trigger scale-out when duration > 0')

    def test_sustained_breach_triggers_after_duration(self):
        """After the breach tracker has aged past the trigger duration, scaling should trigger."""
        engine = self._engine(scale_out_trigger_duration=60)
        metrics = {'avg_cpu_pct': 90}
        # First call plants the breach timestamp
        engine.evaluate(metrics, 5, 2, 10)
        # Simulate that the breach was recorded 61 seconds ago
        engine.breach_tracker['cpu_high'] = time.time() - 61
        decision, target, _ = engine.evaluate(metrics, 5, 2, 10)
        self.assertEqual(decision, ScalingDecision.SCALE_OUT)

    # ------------------------------------------------------------------ #
    # Cooldown
    # ------------------------------------------------------------------ #
    def test_cooldown_prevents_scale_out(self):
        """After recording a scale-out, the next evaluation within cooldown should be NO_ACTION."""
        engine = self._engine()
        metrics = {'avg_cpu_pct': 90}

        # Build sustained breach
        engine.evaluate(metrics, 5, 2, 10)
        decision, _, _ = engine.evaluate(metrics, 5, 2, 10)
        self.assertEqual(decision, ScalingDecision.SCALE_OUT)

        # Record the action (sets cooldown)
        engine.record_scale_out()

        # Re-establish breach (record_scale_out clears the tracker)
        engine.evaluate(metrics, 6, 2, 10)
        decision2, target2, _ = engine.evaluate(metrics, 6, 2, 10)
        self.assertEqual(decision2, ScalingDecision.NO_ACTION,
                         'Scale-out should be blocked during cooldown')
        self.assertEqual(target2, 6)

    def test_cooldown_prevents_scale_in(self):
        """After recording a scale-in, the next evaluation within cooldown should be NO_ACTION."""
        engine = self._engine()
        metrics = {'avg_cpu_pct': 10}

        # Build sustained breach
        engine.evaluate(metrics, 5, 2, 10)
        decision, _, _ = engine.evaluate(metrics, 5, 2, 10)
        self.assertEqual(decision, ScalingDecision.SCALE_IN)

        # Record the action
        engine.record_scale_in()

        # Re-establish breach
        engine.evaluate(metrics, 4, 2, 10)
        decision2, target2, _ = engine.evaluate(metrics, 4, 2, 10)
        self.assertEqual(decision2, ScalingDecision.NO_ACTION,
                         'Scale-in should be blocked during cooldown')
        self.assertEqual(target2, 4)

    # ------------------------------------------------------------------ #
    # Min / Max bounds
    # ------------------------------------------------------------------ #
    def test_max_workers_cap(self):
        """Scale-out should not exceed max_workers."""
        engine = self._engine()
        metrics = {'avg_cpu_pct': 90}
        engine.evaluate(metrics, 10, 2, 10)
        decision, target, _ = engine.evaluate(metrics, 10, 2, 10)
        # At max already; scale_out condition is current < max, so NO_ACTION
        self.assertEqual(decision, ScalingDecision.NO_ACTION)
        self.assertEqual(target, 10)

    def test_max_workers_cap_near_max(self):
        """When one below max, scale-out target must equal max, not exceed it."""
        engine = self._engine(scale_out_increment=5)
        metrics = {'avg_cpu_pct': 90}
        engine.evaluate(metrics, 9, 2, 10)
        decision, target, _ = engine.evaluate(metrics, 9, 2, 10)
        self.assertEqual(decision, ScalingDecision.SCALE_OUT)
        self.assertEqual(target, 10, 'Target must be capped at max_workers')

    def test_min_workers_floor(self):
        """Scale-in should not go below min_workers."""
        engine = self._engine()
        metrics = {'avg_cpu_pct': 10}
        engine.evaluate(metrics, 2, 2, 10)
        decision, target, _ = engine.evaluate(metrics, 2, 2, 10)
        # At min already; scale_in condition is current > min, so NO_ACTION
        self.assertEqual(decision, ScalingDecision.NO_ACTION)
        self.assertEqual(target, 2)

    def test_min_workers_floor_near_min(self):
        """When one above min, scale-in target must equal min, not go below."""
        engine = self._engine(scale_in_decrement=5)
        metrics = {'avg_cpu_pct': 10}
        engine.evaluate(metrics, 3, 2, 10)
        decision, target, _ = engine.evaluate(metrics, 3, 2, 10)
        self.assertEqual(decision, ScalingDecision.SCALE_IN)
        self.assertEqual(target, 2, 'Target must be floored at min_workers')

    # ------------------------------------------------------------------ #
    # Priority / edge cases
    # ------------------------------------------------------------------ #
    def test_scale_out_wins_over_scale_in(self):
        """When both scale-out and scale-in signals are present, scale-out takes priority."""
        engine = self._engine()
        # CPU is extremely high (scale-out) and memory is extremely low (scale-in)
        metrics = {'avg_cpu_pct': 95, 'avg_memory_pct': 5}
        engine.evaluate(metrics, 5, 2, 10)
        decision, target, _ = engine.evaluate(metrics, 5, 2, 10)
        self.assertEqual(decision, ScalingDecision.SCALE_OUT,
                         'SCALE_OUT should win when both out and in are triggered')

    def test_no_metrics_returns_no_action(self):
        """None metrics snapshot should return NO_ACTION."""
        engine = self._engine()
        decision, target, reason = engine.evaluate(None, 5, 2, 10)
        self.assertEqual(decision, ScalingDecision.NO_ACTION)
        self.assertEqual(target, 5)
        self.assertIn('No metrics', reason)

    def test_empty_metrics_returns_no_action(self):
        """Empty dict metrics should return NO_ACTION."""
        engine = self._engine()
        decision, target, _ = engine.evaluate({}, 5, 2, 10)
        self.assertEqual(decision, ScalingDecision.NO_ACTION)
        self.assertEqual(target, 5)


if __name__ == '__main__':
    unittest.main()
