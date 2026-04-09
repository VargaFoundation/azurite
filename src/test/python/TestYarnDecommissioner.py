#!/usr/bin/env python3
"""
Unit tests for the YarnDecommissioner class.
All HTTP calls are mocked so no real YARN RM is needed.
"""
import sys
import os
import json
import unittest

try:
    from unittest.mock import patch, MagicMock, call
except ImportError:
    from mock import patch, MagicMock, call

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                '..', '..', '..', 'src', 'main', 'resources',
                                'addon-services', 'AZURE_AUTOSCALER', '1.0.0', 'package', 'files'))
from yarn_decommissioner import YarnDecommissioner


def _mock_response(read_value=None):
    """Create a MagicMock that works as a context manager (for `with urlopen() as resp:`)."""
    resp = MagicMock()
    if read_value is not None:
        resp.read.return_value = read_value
    resp.__enter__ = MagicMock(return_value=resp)
    resp.__exit__ = MagicMock(return_value=False)
    return resp

_YD_MODULE = 'yarn_decommissioner'


def _nodes_list_response(nodes):
    """Build a YARN /ws/v1/cluster/nodes JSON response.

    Args:
        nodes: list of dicts, each with 'nodeHostName', 'id', and optionally 'numContainers'.
    """
    return json.dumps({'nodes': {'node': nodes}}).encode()


def _node_detail_response(num_containers=0):
    """Build a YARN /ws/v1/cluster/nodes/<id> JSON response."""
    return json.dumps({'node': {'numContainers': num_containers}}).encode()


class TestYarnDecommissioner(unittest.TestCase):
    """Tests for YarnDecommissioner."""

    # ------------------------------------------------------------------ #
    # Helpers
    # ------------------------------------------------------------------ #
    def _make_urlopen_side_effect(self, host_containers_sequence):
        """Return a side_effect function for urlopen.

        Args:
            host_containers_sequence: list of dicts mapping hostname -> numContainers.
                Each dict represents one round of polling.  When the list is
                exhausted the last entry is repeated.
        """
        nodes_payload = _nodes_list_response([
            {'nodeHostName': h, 'id': '{0}:8041'.format(h)}
            for h in host_containers_sequence[0].keys()
        ])
        poll_index = [0]  # mutable counter in closure

        def side_effect(req, **kwargs):
            url = req.get_full_url() if hasattr(req, 'get_full_url') else req

            # Node list requests
            if url.endswith('/ws/v1/cluster/nodes'):
                return _mock_response(nodes_payload)

            # State change (PUT) requests -- just succeed
            if '/state' in url:
                return _mock_response()

            # Single-node detail requests
            for host in host_containers_sequence[0].keys():
                node_id = '{0}:8041'.format(host)
                if node_id in url:
                    idx = min(poll_index[0], len(host_containers_sequence) - 1)
                    containers = host_containers_sequence[idx].get(host, 0)
                    return _mock_response(_node_detail_response(containers))

            return _mock_response(_node_detail_response(0))

        def advance_poll():
            poll_index[0] += 1

        return side_effect, advance_poll

    # ------------------------------------------------------------------ #
    # Tests
    # ------------------------------------------------------------------ #
    @patch(_YD_MODULE + '.time.sleep', return_value=None)
    @patch(_YD_MODULE + '.urlopen')
    def test_graceful_decommission_success(self, mock_urlopen, mock_sleep):
        """Nodes that drain to 0 containers should be reported as decommissioned."""
        # Round 0: 3 containers, Round 1: 0 containers
        sequence = [
            {'worker1': 3, 'worker2': 2},
            {'worker1': 0, 'worker2': 0},
        ]
        side_effect, advance = self._make_urlopen_side_effect(sequence)

        call_count = [0]
        original_side_effect = side_effect

        def counting_side_effect(req, **kwargs):
            result = original_side_effect(req, **kwargs)
            call_count[0] += 1
            return result

        mock_urlopen.side_effect = counting_side_effect

        # Make time.sleep advance the poll index so containers go to 0
        def sleep_advance(secs):
            advance()
        mock_sleep.side_effect = sleep_advance

        decomm = YarnDecommissioner('http://rm-host:8088')
        result = decomm.graceful_decommission(['worker1', 'worker2'], timeout_seconds=120)

        self.assertIn('worker1', result)
        self.assertIn('worker2', result)
        self.assertEqual(len(result), 2)

    @patch(_YD_MODULE + '.time.sleep', return_value=None)
    @patch(_YD_MODULE + '.time.time')
    @patch(_YD_MODULE + '.urlopen')
    def test_decommission_timeout_forces(self, mock_urlopen, mock_time, mock_sleep):
        """Nodes that never drain should be force-decommissioned after timeout."""
        nodes_payload = _nodes_list_response([
            {'nodeHostName': 'worker1', 'id': 'worker1:8041'},
        ])

        def urlopen_side_effect(req, **kwargs):
            url = req.get_full_url() if hasattr(req, 'get_full_url') else req

            if url.endswith('/ws/v1/cluster/nodes'):
                return _mock_response(nodes_payload)

            if '/state' in url:
                return _mock_response()

            if 'worker1:8041' in url and '/state' not in url:
                return _mock_response(_node_detail_response(5))

            return _mock_response(_node_detail_response(0))

        mock_urlopen.side_effect = urlopen_side_effect

        # Simulate time advancing past the timeout
        # time.time() is called:  start, loop check, loop check (past timeout)
        time_values = [1000.0, 1000.0, 1000.0, 2000.0]
        mock_time.side_effect = time_values + [2000.0] * 20  # extra values for safety

        decomm = YarnDecommissioner('http://rm-host:8088')
        result = decomm.graceful_decommission(['worker1'], timeout_seconds=60)

        # worker1 should still appear in results (force-decommissioned)
        self.assertIn('worker1', result)

        # Verify that a PUT with DECOMMISSIONED state was attempted (force)
        put_calls = [c for c in mock_urlopen.call_args_list
                     if len(c[0]) > 0 and hasattr(c[0][0], 'data')
                     and c[0][0].data is not None
                     and b'DECOMMISSIONED' in c[0][0].data]
        self.assertTrue(len(put_calls) > 0,
                        'Force decommission (DECOMMISSIONED state) should have been called')

    def test_empty_hostnames_returns_empty(self):
        """Passing an empty list should immediately return an empty list."""
        decomm = YarnDecommissioner('http://rm-host:8088')
        result = decomm.graceful_decommission([])
        self.assertEqual(result, [])

    @patch(_YD_MODULE + '.urlopen')
    def test_get_node_containers_zero(self, mock_urlopen):
        """A node reporting 0 containers should return 0."""
        nodes_payload = _nodes_list_response([
            {'nodeHostName': 'worker1', 'id': 'worker1:8041'},
        ])

        def urlopen_side_effect(req, **kwargs):
            url = req.get_full_url() if hasattr(req, 'get_full_url') else req
            if url.endswith('/ws/v1/cluster/nodes'):
                return _mock_response(nodes_payload)
            if 'worker1:8041' in url:
                return _mock_response(_node_detail_response(0))
            return _mock_response(_node_detail_response(0))

        mock_urlopen.side_effect = urlopen_side_effect

        decomm = YarnDecommissioner('http://rm-host:8088')
        count = decomm._get_node_containers('worker1')
        self.assertEqual(count, 0)

    @patch(_YD_MODULE + '.urlopen')
    def test_get_node_containers_nonzero(self, mock_urlopen):
        """A node reporting N containers should return N."""
        nodes_payload = _nodes_list_response([
            {'nodeHostName': 'worker1', 'id': 'worker1:8041'},
        ])

        def urlopen_side_effect(req, **kwargs):
            url = req.get_full_url() if hasattr(req, 'get_full_url') else req
            if url.endswith('/ws/v1/cluster/nodes'):
                return _mock_response(nodes_payload)
            if 'worker1:8041' in url:
                return _mock_response(_node_detail_response(7))
            return _mock_response(_node_detail_response(0))

        mock_urlopen.side_effect = urlopen_side_effect

        decomm = YarnDecommissioner('http://rm-host:8088')
        count = decomm._get_node_containers('worker1')
        self.assertEqual(count, 7)


if __name__ == '__main__':
    unittest.main()
