#!/usr/bin/env python3
"""
Metrics collector for the autoscaler.
Collects YARN cluster metrics and system-level CPU/memory metrics.
"""
import json
import logging
import os

try:
    from urllib.request import urlopen, Request
    from urllib.error import URLError
except ImportError:
    from urllib2 import urlopen, Request, URLError

logger = logging.getLogger('metrics_collector')


class MetricsCollector:
    """Collects cluster metrics from YARN ResourceManager and system sources."""

    def __init__(self, yarn_rm_url, metrics_source='yarn_and_system'):
        self.yarn_rm_url = yarn_rm_url.rstrip('/')
        self.metrics_source = metrics_source

    def collect_yarn_metrics(self):
        """
        Query YARN ResourceManager REST API for cluster metrics.
        Endpoint: GET /ws/v1/cluster/metrics
        """
        url = '{0}/ws/v1/cluster/metrics'.format(self.yarn_rm_url)
        try:
            req = Request(url)
            req.add_header('Accept', 'application/json')
            response = urlopen(req, timeout=10)
            data = json.loads(response.read().decode())
            metrics = data.get('clusterMetrics', {})

            return {
                'pending_containers': metrics.get('containersPending', 0),
                'pending_memory_mb': metrics.get('pendingMemoryMB', 0) if 'pendingMemoryMB' in metrics
                else metrics.get('availableMB', 0) - metrics.get('allocatedMB', 0),
                'pending_vcores': metrics.get('pendingVirtualCores', 0) if 'pendingVirtualCores' in metrics
                else 0,
                'available_memory_mb': metrics.get('availableMB', 0),
                'available_vcores': metrics.get('availableVirtualCores', 0),
                'allocated_memory_mb': metrics.get('allocatedMB', 0),
                'allocated_vcores': metrics.get('allocatedVirtualCores', 0),
                'total_memory_mb': metrics.get('totalMB', 0),
                'total_vcores': metrics.get('totalVirtualCores', 0),
                'active_nodes': metrics.get('activeNodes', 0),
                'decommissioning_nodes': metrics.get('decommissioningNodes', 0),
                'apps_running': metrics.get('appsRunning', 0),
                'apps_pending': metrics.get('appsPending', 0),
            }
        except (URLError, ValueError, KeyError) as e:
            logger.error('Failed to collect YARN metrics from %s: %s', url, e)
            return None

    def collect_system_metrics(self):
        """
        Collect system-level CPU and memory metrics.
        Uses /proc/stat and /proc/meminfo for local node metrics.
        """
        try:
            cpu_pct = self._get_cpu_usage()
            mem_pct = self._get_memory_usage()
            return {
                'avg_cpu_pct': cpu_pct,
                'avg_memory_pct': mem_pct,
            }
        except Exception as e:
            logger.error('Failed to collect system metrics: %s', e)
            return None

    def get_aggregated_metrics(self):
        """
        Combine YARN and system metrics into a unified snapshot.
        Returns None if metrics collection fails completely.
        """
        result = {}

        if self.metrics_source in ('yarn_only', 'yarn_and_system'):
            yarn = self.collect_yarn_metrics()
            if yarn:
                result.update(yarn)

        if self.metrics_source in ('system_only', 'yarn_and_system'):
            system = self.collect_system_metrics()
            if system:
                result.update(system)

        # Compute YARN memory utilization percentage
        total_mem = result.get('total_memory_mb', 0)
        available_mem = result.get('available_memory_mb', 0)
        if total_mem > 0:
            result['yarn_memory_used_pct'] = ((total_mem - available_mem) / total_mem) * 100
            result['yarn_memory_available_pct'] = (available_mem / total_mem) * 100
        else:
            result['yarn_memory_used_pct'] = 0
            result['yarn_memory_available_pct'] = 100

        # Compute CPU from YARN vcores if available (more accurate than local /proc/stat)
        total_vcores = result.get('total_vcores', 0)
        allocated_vcores = result.get('allocated_vcores', 0)
        if total_vcores > 0:
            result['avg_cpu_pct'] = (allocated_vcores / total_vcores) * 100

        return result if result else None

    def _get_cpu_usage(self):
        """Get CPU usage percentage from /proc/stat (two-sample delta)."""
        try:
            import time
            sample1 = self._read_proc_stat()
            time.sleep(0.5)
            sample2 = self._read_proc_stat()

            delta_idle = sample2['idle'] - sample1['idle']
            delta_total = sample2['total'] - sample1['total']

            if delta_total == 0:
                return 0.0
            return ((delta_total - delta_idle) / delta_total) * 100
        except Exception:
            return 0.0

    def _read_proc_stat(self):
        """Read CPU times from /proc/stat."""
        with open('/proc/stat', 'r') as f:
            line = f.readline()  # First line is aggregate CPU
        parts = line.split()
        # user, nice, system, idle, iowait, irq, softirq, steal
        times = [int(p) for p in parts[1:9]]
        return {
            'idle': times[3] + times[4],  # idle + iowait
            'total': sum(times),
        }

    def _get_memory_usage(self):
        """Get memory usage percentage from /proc/meminfo."""
        try:
            meminfo = {}
            with open('/proc/meminfo', 'r') as f:
                for line in f:
                    parts = line.split(':')
                    if len(parts) == 2:
                        key = parts[0].strip()
                        val = int(parts[1].strip().split()[0])  # value in kB
                        meminfo[key] = val

            total = meminfo.get('MemTotal', 1)
            available = meminfo.get('MemAvailable', meminfo.get('MemFree', 0))
            return ((total - available) / total) * 100
        except Exception:
            return 0.0
