#!/usr/bin/env python3
"""
YARN Decommissioner - Gracefully decommissions YARN NodeManagers
before VM deletion to prevent container failures and data loss.
"""
import json
import logging
import time

try:
    from urllib.request import urlopen, Request
    from urllib.error import URLError
except ImportError:
    from urllib2 import urlopen, Request, URLError

logger = logging.getLogger('yarn_decommissioner')


class YarnDecommissioner:
    """Handles graceful YARN decommission of worker nodes."""

    def __init__(self, yarn_rm_url):
        self.yarn_rm_url = yarn_rm_url.rstrip('/')

    def graceful_decommission(self, hostnames, timeout_seconds=3600):
        """
        Gracefully decommission YARN nodes.

        1. Set nodes to DECOMMISSIONING state via YARN RM API
        2. Wait for all containers to drain (or timeout)
        3. Return list of successfully decommissioned hosts

        Args:
            hostnames: list of hostnames to decommission
            timeout_seconds: max time to wait for graceful drain

        Returns:
            list of successfully decommissioned hostnames
        """
        if not hostnames:
            return []

        logger.info('Starting graceful decommission for %d nodes: %s', len(hostnames), hostnames)

        # Step 1: Request decommission for each node
        for hostname in hostnames:
            self._request_decommission(hostname)

        # Step 2: Wait for containers to drain
        decommissioned = self.wait_for_drain(hostnames, timeout_seconds)

        # Step 3: Force decommission any that didn't drain in time
        remaining = set(hostnames) - set(decommissioned)
        if remaining:
            logger.warning('Forcing decommission for nodes that did not drain: %s', list(remaining))
            for hostname in remaining:
                self._force_decommission(hostname)
            decommissioned.extend(remaining)

        return decommissioned

    def wait_for_drain(self, hostnames, timeout_seconds):
        """
        Poll YARN RM for container count on each host until all are zero.

        Returns list of hostnames that successfully drained.
        """
        start_time = time.time()
        drained = set()
        remaining = set(hostnames)

        while remaining and (time.time() - start_time) < timeout_seconds:
            for hostname in list(remaining):
                containers = self._get_node_containers(hostname)
                if containers == 0:
                    logger.info('Node %s fully drained', hostname)
                    drained.add(hostname)
                    remaining.discard(hostname)
                else:
                    logger.debug('Node %s still has %d containers', hostname, containers)

            if remaining:
                time.sleep(10)  # Poll every 10 seconds

        if remaining:
            elapsed = time.time() - start_time
            logger.warning('Drain timeout after %.0fs. Remaining nodes: %s', elapsed, list(remaining))

        return list(drained)

    def _request_decommission(self, hostname):
        """Request YARN to decommission a specific node."""
        # Find the node ID for the hostname
        node_id = self._get_node_id(hostname)
        if not node_id:
            logger.warning('Could not find YARN node ID for %s', hostname)
            return

        url = '{0}/ws/v1/cluster/nodes/{1}/state'.format(self.yarn_rm_url, node_id)
        data = json.dumps({'state': 'DECOMMISSIONING'}).encode()

        try:
            req = Request(url, data=data, method='PUT')
            req.add_header('Content-Type', 'application/json')
            response = urlopen(req, timeout=10)
            logger.info('Decommission requested for node %s (ID: %s)', hostname, node_id)
        except Exception as e:
            logger.error('Failed to request decommission for %s: %s', hostname, e)

    def _force_decommission(self, hostname):
        """Force decommission without waiting for drain."""
        node_id = self._get_node_id(hostname)
        if not node_id:
            return

        url = '{0}/ws/v1/cluster/nodes/{1}/state'.format(self.yarn_rm_url, node_id)
        data = json.dumps({'state': 'DECOMMISSIONED'}).encode()

        try:
            req = Request(url, data=data, method='PUT')
            req.add_header('Content-Type', 'application/json')
            urlopen(req, timeout=10)
            logger.info('Forced decommission for node %s', hostname)
        except Exception as e:
            logger.error('Failed to force decommission %s: %s', hostname, e)

    def _get_node_id(self, hostname):
        """Get YARN node ID for a hostname."""
        url = '{0}/ws/v1/cluster/nodes'.format(self.yarn_rm_url)
        try:
            req = Request(url)
            req.add_header('Accept', 'application/json')
            response = urlopen(req, timeout=10)
            data = json.loads(response.read().decode())
            nodes = data.get('nodes', {}).get('node', [])

            for node in nodes:
                node_host = node.get('nodeHostName', '')
                if node_host == hostname or node_host.split('.')[0] == hostname.split('.')[0]:
                    return node.get('id', '')

            return None
        except Exception as e:
            logger.error('Failed to get node ID for %s: %s', hostname, e)
            return None

    def _get_node_containers(self, hostname):
        """Get number of running containers on a node."""
        node_id = self._get_node_id(hostname)
        if not node_id:
            return 0

        url = '{0}/ws/v1/cluster/nodes/{1}'.format(self.yarn_rm_url, node_id)
        try:
            req = Request(url)
            req.add_header('Accept', 'application/json')
            response = urlopen(req, timeout=10)
            data = json.loads(response.read().decode())
            node = data.get('node', {})
            return node.get('numContainers', 0)
        except Exception as e:
            logger.error('Failed to get container count for %s: %s', hostname, e)
            return 0
