#!/usr/bin/env python3
"""
Node Health Monitor - Detects failed/evicted VMs and triggers auto-recovery.
Runs as part of the VM Manager daemon's periodic check cycle.
"""
import json
import logging
import time

logger = logging.getLogger('node_health_monitor')


class NodeHealthMonitor:
    """Monitors Azure VM health and triggers auto-recovery for failed nodes."""

    def __init__(self, compute_client, resource_group, vm_operations=None):
        """
        Args:
            compute_client: Azure ComputeManagementClient
            resource_group: Azure resource group name
            vm_operations: AzureVmOperations instance for recovery actions
        """
        self._compute_client = compute_client
        self._resource_group = resource_group
        self._vm_operations = vm_operations
        self._last_check = {}  # vm_name -> last_known_state

    def check_vm_power_states(self, inventory):
        """
        Check actual Azure VM power states against inventory.
        Returns dict of {vm_name: {'expected': status, 'actual': power_state, 'healthy': bool}}
        """
        results = {}
        if not self._compute_client:
            return results

        for vm_entry in inventory.get('vms', []):
            vm_name = vm_entry.get('name', '')
            expected = vm_entry.get('status', 'unknown')
            if not vm_name or expected in ('deleted', 'delete_failed'):
                continue

            try:
                instance_view = self._compute_client.virtual_machines.instance_view(
                    self._resource_group, vm_name)
                power_state = 'unknown'
                for status in (instance_view.statuses or []):
                    if status.code and status.code.startswith('PowerState/'):
                        power_state = status.code.replace('PowerState/', '')
                        break

                healthy = power_state == 'running'
                results[vm_name] = {
                    'expected': expected,
                    'actual': power_state,
                    'healthy': healthy,
                    'role': vm_entry.get('role', 'unknown'),
                }

                self._last_check[vm_name] = power_state

            except Exception as e:
                # VM not found in Azure = likely deleted/evicted
                error_str = str(e)
                if 'ResourceNotFound' in error_str or '404' in error_str:
                    results[vm_name] = {
                        'expected': expected,
                        'actual': 'not_found',
                        'healthy': False,
                        'role': vm_entry.get('role', 'unknown'),
                    }
                else:
                    logger.warning('Failed to check VM %s: %s', vm_name, e)
                    results[vm_name] = {
                        'expected': expected,
                        'actual': 'check_failed',
                        'healthy': True,  # Assume healthy if we can't check
                        'role': vm_entry.get('role', 'unknown'),
                    }

        return results

    def get_unhealthy_vms(self, inventory):
        """Get list of VMs that are not in a healthy running state."""
        states = self.check_vm_power_states(inventory)
        return {name: info for name, info in states.items() if not info['healthy']}

    def get_dead_workers(self, inventory):
        """Get worker VMs that are dead/evicted and need replacement."""
        unhealthy = self.get_unhealthy_vms(inventory)
        return {name: info for name, info in unhealthy.items()
                if info['role'] == 'worker' and info['actual'] in ('deallocated', 'stopped', 'not_found')}

    def auto_recover(self, inventory, pool_config=None):
        """
        Auto-recover dead worker VMs:
        1. Identify dead workers
        2. Remove them from inventory
        3. Provision replacements

        Returns list of recovery actions taken.
        """
        dead = self.get_dead_workers(inventory)
        if not dead:
            return []

        actions = []
        for vm_name, info in dead.items():
            logger.warning('Auto-recovery: VM %s is %s (was %s), replacing...',
                           vm_name, info['actual'], info['expected'])

            # Clean up the dead VM
            if self._vm_operations and info['actual'] != 'not_found':
                try:
                    self._vm_operations.delete_vm(vm_name)
                except Exception as e:
                    logger.error('Failed to clean up dead VM %s: %s', vm_name, e)

            # Remove from inventory if not_found
            if info['actual'] == 'not_found' and self._vm_operations:
                self._vm_operations._remove_from_inventory(vm_name)

            # Provision replacement
            if self._vm_operations and pool_config:
                try:
                    new_name = 'azr-worker-{0}'.format(int(time.time() * 1000))
                    self._vm_operations.create_vm(
                        vm_name=new_name,
                        vm_size=pool_config.get('size', 'Standard_D4_v3'),
                        role='worker',
                        data_disk_count=pool_config.get('data_disks', 4),
                        data_disk_size_gb=pool_config.get('data_disk_size_gb', 512),
                        disk_type=pool_config.get('disk_type', 'Standard_LRS'),
                        os_disk_size_gb=pool_config.get('disk_size_gb', 128),
                        spot=pool_config.get('spot_enabled', False),
                        spot_max_price=pool_config.get('spot_max_price', -1),
                    )
                    actions.append({
                        'type': 'replaced',
                        'dead_vm': vm_name,
                        'new_vm': new_name,
                        'reason': info['actual'],
                    })
                except Exception as e:
                    logger.error('Failed to provision replacement for %s: %s', vm_name, e)
                    actions.append({
                        'type': 'replace_failed',
                        'dead_vm': vm_name,
                        'error': str(e),
                    })
            else:
                actions.append({
                    'type': 'detected',
                    'dead_vm': vm_name,
                    'reason': info['actual'],
                    'note': 'No vm_operations available for auto-recovery',
                })

        return actions

    def get_health_summary(self, inventory):
        """Get a health summary suitable for API response."""
        states = self.check_vm_power_states(inventory)
        total = len(states)
        healthy = sum(1 for s in states.values() if s['healthy'])
        unhealthy = total - healthy

        return {
            'total_vms': total,
            'healthy': healthy,
            'unhealthy': unhealthy,
            'details': states,
        }
