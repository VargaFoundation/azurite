#!/usr/bin/env python3
"""
Scaling policy engine for the autoscaler.
Evaluates metrics against thresholds, tracks sustained breaches,
enforces cooldown periods, and respects min/max bounds.
"""
import logging
import time
from enum import Enum

logger = logging.getLogger('scaling_policy')


class ScalingDecision(Enum):
    NO_ACTION = 0
    SCALE_OUT = 1
    SCALE_IN = 2


class ScalingPolicyEngine:
    """
    Evaluates cluster metrics and produces scaling decisions.

    Key design principles:
    - Sustained breach: metrics must exceed thresholds for a configurable duration
    - Cooldown: minimum time between scaling actions to prevent thrashing
    - Safety bias: scale-out wins when both scale-out and scale-in are triggered
    - Bounds: respects min_workers and max_workers constraints
    """

    def __init__(self, config):
        self.config = config
        self.breach_tracker = {}  # metric_key -> first_breach_timestamp
        self.last_scale_out_time = 0
        self.last_scale_in_time = 0

    def evaluate(self, metrics_snapshot, current_worker_count, min_workers, max_workers):
        """
        Evaluate metrics and return a scaling decision.

        Args:
            metrics_snapshot: dict of current metrics values
            current_worker_count: current number of worker nodes
            min_workers: minimum allowed workers
            max_workers: maximum allowed workers

        Returns:
            tuple: (ScalingDecision, target_count, reason_string)
        """
        if not metrics_snapshot:
            return (ScalingDecision.NO_ACTION, current_worker_count, 'No metrics available')

        scale_out_reasons = []
        scale_in_reasons = []
        now = time.time()

        scale_out_duration = self.config.get('scale_out_trigger_duration', 300)
        scale_in_duration = self.config.get('scale_in_trigger_duration', 300)

        # ---- CPU evaluation ----
        avg_cpu = metrics_snapshot.get('avg_cpu_pct', None)
        if avg_cpu is not None:
            cpu_out = self.config.get('cpu_scale_out_threshold', 80)
            cpu_in = self.config.get('cpu_scale_in_threshold', 30)

            if avg_cpu > cpu_out:
                if self._sustained_breach('cpu_high', scale_out_duration, now):
                    scale_out_reasons.append('CPU {0:.1f}% > {1}%'.format(avg_cpu, cpu_out))
            else:
                self._clear_breach('cpu_high')

            if avg_cpu < cpu_in:
                if self._sustained_breach('cpu_low', scale_in_duration, now):
                    scale_in_reasons.append('CPU {0:.1f}% < {1}%'.format(avg_cpu, cpu_in))
            else:
                self._clear_breach('cpu_low')

        # ---- Memory evaluation ----
        avg_mem = metrics_snapshot.get('avg_memory_pct', None)
        if avg_mem is not None:
            mem_out = self.config.get('memory_scale_out_threshold', 80)
            mem_in = self.config.get('memory_scale_in_threshold', 30)

            if avg_mem > mem_out:
                if self._sustained_breach('mem_high', scale_out_duration, now):
                    scale_out_reasons.append('Memory {0:.1f}% > {1}%'.format(avg_mem, mem_out))
            else:
                self._clear_breach('mem_high')

            if avg_mem < mem_in:
                if self._sustained_breach('mem_low', scale_in_duration, now):
                    scale_in_reasons.append('Memory {0:.1f}% < {1}%'.format(avg_mem, mem_in))
            else:
                self._clear_breach('mem_low')

        # ---- YARN pending containers ----
        pending = metrics_snapshot.get('pending_containers', None)
        if pending is not None:
            pending_threshold = self.config.get('yarn_pending_containers_threshold', 10)
            if pending > pending_threshold:
                if self._sustained_breach('yarn_pending', scale_out_duration, now):
                    scale_out_reasons.append('YARN pending containers {0} > {1}'.format(pending, pending_threshold))
            else:
                self._clear_breach('yarn_pending')

        # ---- YARN available memory (scale-in signal) ----
        yarn_avail_pct = metrics_snapshot.get('yarn_memory_available_pct', None)
        if yarn_avail_pct is not None:
            avail_threshold = self.config.get('yarn_available_memory_scale_in_pct', 60)
            if yarn_avail_pct > avail_threshold:
                if self._sustained_breach('yarn_avail_high', scale_in_duration, now):
                    scale_in_reasons.append(
                        'YARN available memory {0:.1f}% > {1}%'.format(yarn_avail_pct, avail_threshold))
            else:
                self._clear_breach('yarn_avail_high')

        # ---- Decision logic ----
        # Scale-out takes priority (safety bias)
        if scale_out_reasons and current_worker_count < max_workers:
            if not self._in_cooldown('scale_out', now):
                target = min(current_worker_count + self.config.get('scale_out_increment', 1), max_workers)
                reason = '; '.join(scale_out_reasons)
                logger.info('SCALE_OUT decision: %s -> %s workers. Reason: %s',
                            current_worker_count, target, reason)
                return (ScalingDecision.SCALE_OUT, target, reason)
            else:
                logger.debug('Scale-out triggered but in cooldown')

        if scale_in_reasons and not scale_out_reasons and current_worker_count > min_workers:
            if not self._in_cooldown('scale_in', now):
                target = max(current_worker_count - self.config.get('scale_in_decrement', 1), min_workers)
                reason = '; '.join(scale_in_reasons)
                logger.info('SCALE_IN decision: %s -> %s workers. Reason: %s',
                            current_worker_count, target, reason)
                return (ScalingDecision.SCALE_IN, target, reason)
            else:
                logger.debug('Scale-in triggered but in cooldown')

        return (ScalingDecision.NO_ACTION, current_worker_count, 'All metrics within thresholds')

    def record_scale_out(self):
        """Record that a scale-out action was executed."""
        self.last_scale_out_time = time.time()
        # Clear scale-out breaches after action
        for key in list(self.breach_tracker.keys()):
            if 'high' in key or 'pending' in key:
                del self.breach_tracker[key]

    def record_scale_in(self):
        """Record that a scale-in action was executed."""
        self.last_scale_in_time = time.time()
        # Clear scale-in breaches after action
        for key in list(self.breach_tracker.keys()):
            if 'low' in key or 'avail' in key:
                del self.breach_tracker[key]

    def _sustained_breach(self, metric_key, duration_seconds, now):
        """
        Track a metric breach. Returns True if breach has been sustained
        for at least duration_seconds.
        """
        if metric_key not in self.breach_tracker:
            self.breach_tracker[metric_key] = now
            return False

        elapsed = now - self.breach_tracker[metric_key]
        return elapsed >= duration_seconds

    def _clear_breach(self, metric_key):
        """Clear a breach tracker when metric returns to normal."""
        self.breach_tracker.pop(metric_key, None)

    def _in_cooldown(self, action_type, now):
        """Check if we're within a cooldown period."""
        if action_type == 'scale_out':
            cooldown = self.config.get('cooldown_scale_out', 300)
            return (now - self.last_scale_out_time) < cooldown
        else:
            cooldown = self.config.get('cooldown_scale_in', 600)
            return (now - self.last_scale_in_time) < cooldown
