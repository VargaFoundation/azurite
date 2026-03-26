#!/usr/bin/env python3
"""Unit tests for AzureVmManagerServiceAdvisor."""
import importlib.util
import os
import sys
import unittest

# Load the service advisor module under a unique name to avoid collisions
# with other service_advisor.py files in sibling service directories.
_SA_PATH = os.path.normpath(os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    '..', '..', 'main', 'resources', 'addon-services',
    'AZURE_VM_MANAGER', '1.0.0', 'service_advisor.py'
))
_spec = importlib.util.spec_from_file_location('azure_vm_manager_service_advisor', _SA_PATH)
_mod = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = _mod
_spec.loader.exec_module(_mod)
AzureVmManagerServiceAdvisor = _mod.AzureVmManagerServiceAdvisor


def _build_services(vm_env=None, vm_site=None, pool_site=None):
    """Build a services dict in the Ambari format expected by _get_service_configs."""
    configurations = []
    if vm_env is not None:
        configurations.append({'azure-vm-manager-env': {'properties': vm_env}})
    if vm_site is not None:
        configurations.append({'azure-vm-manager-site': {'properties': vm_site}})
    if pool_site is not None:
        configurations.append({'azure-vm-pool-site': {'properties': pool_site}})
    return {'configurations': configurations}


def _valid_managed_services():
    """Return a services dict representing a fully valid managed-mode configuration."""
    return _build_services(
        vm_env={'vm_manager_mode': 'managed'},
        vm_site={
            'azure.vm.vnet.name': 'my-vnet',
            'azure.vm.ssh.public.key': 'ssh-rsa AAAAB3...',
        },
        pool_site={
            'azure.vm.pool.worker.min.count': '1',
            'azure.vm.pool.worker.max.count': '20',
            'azure.vm.pool.worker.initial.count': '3',
            'azure.vm.pool.zk.count': '3',
            'azure.vm.pool.worker.spot.enabled': 'false',
        },
    )


class TestVmManagerServiceAdvisorValidation(unittest.TestCase):
    """Tests for getServiceConfigurationsValidationItems."""

    def setUp(self):
        self.advisor = AzureVmManagerServiceAdvisor()

    # --- Managed mode networking ---

    def test_managed_mode_requires_vnet(self):
        services = _build_services(
            vm_env={'vm_manager_mode': 'managed'},
            vm_site={
                'azure.vm.vnet.name': '',
                'azure.vm.ssh.public.key': 'ssh-rsa AAAAB3...',
            },
            pool_site={
                'azure.vm.pool.worker.min.count': '1',
                'azure.vm.pool.worker.max.count': '20',
                'azure.vm.pool.worker.initial.count': '3',
                'azure.vm.pool.zk.count': '3',
                'azure.vm.pool.worker.spot.enabled': 'false',
            },
        )
        items = self.advisor.getServiceConfigurationsValidationItems({}, {}, services, {})
        error_names = [i['config-name'] for i in items if i['level'] == 'ERROR']
        self.assertIn('azure.vm.vnet.name', error_names)

    def test_managed_mode_requires_ssh_key(self):
        services = _build_services(
            vm_env={'vm_manager_mode': 'managed'},
            vm_site={
                'azure.vm.vnet.name': 'my-vnet',
                'azure.vm.ssh.public.key': '',
            },
            pool_site={
                'azure.vm.pool.worker.min.count': '1',
                'azure.vm.pool.worker.max.count': '20',
                'azure.vm.pool.worker.initial.count': '3',
                'azure.vm.pool.zk.count': '3',
                'azure.vm.pool.worker.spot.enabled': 'false',
            },
        )
        items = self.advisor.getServiceConfigurationsValidationItems({}, {}, services, {})
        error_names = [i['config-name'] for i in items if i['level'] == 'ERROR']
        self.assertIn('azure.vm.ssh.public.key', error_names)

    def test_existing_mode_skips_vnet(self):
        services = _build_services(
            vm_env={'vm_manager_mode': 'existing'},
            vm_site={
                'azure.vm.vnet.name': '',
                'azure.vm.ssh.public.key': '',
            },
            pool_site={
                'azure.vm.pool.worker.min.count': '1',
                'azure.vm.pool.worker.max.count': '20',
                'azure.vm.pool.worker.initial.count': '3',
                'azure.vm.pool.zk.count': '3',
                'azure.vm.pool.worker.spot.enabled': 'false',
            },
        )
        items = self.advisor.getServiceConfigurationsValidationItems({}, {}, services, {})
        error_names = [i['config-name'] for i in items if i['level'] == 'ERROR']
        self.assertNotIn('azure.vm.vnet.name', error_names)
        self.assertNotIn('azure.vm.ssh.public.key', error_names)

    # --- Worker pool bounds ---

    def test_worker_min_exceeds_max(self):
        services = _build_services(
            vm_env={'vm_manager_mode': 'existing'},
            vm_site={},
            pool_site={
                'azure.vm.pool.worker.min.count': '25',
                'azure.vm.pool.worker.max.count': '10',
                'azure.vm.pool.worker.initial.count': '8',
                'azure.vm.pool.zk.count': '3',
                'azure.vm.pool.worker.spot.enabled': 'false',
            },
        )
        items = self.advisor.getServiceConfigurationsValidationItems({}, {}, services, {})
        error_names = [i['config-name'] for i in items if i['level'] == 'ERROR']
        self.assertIn('azure.vm.pool.worker.min.count', error_names)

    def test_worker_initial_out_of_bounds(self):
        # initial (25) > max (20)
        services = _build_services(
            vm_env={'vm_manager_mode': 'existing'},
            vm_site={},
            pool_site={
                'azure.vm.pool.worker.min.count': '1',
                'azure.vm.pool.worker.max.count': '20',
                'azure.vm.pool.worker.initial.count': '25',
                'azure.vm.pool.zk.count': '3',
                'azure.vm.pool.worker.spot.enabled': 'false',
            },
        )
        items = self.advisor.getServiceConfigurationsValidationItems({}, {}, services, {})
        error_names = [i['config-name'] for i in items if i['level'] == 'ERROR']
        self.assertIn('azure.vm.pool.worker.initial.count', error_names)

    # --- ZooKeeper count ---

    def test_zk_count_even(self):
        services = _build_services(
            vm_env={'vm_manager_mode': 'existing'},
            vm_site={},
            pool_site={
                'azure.vm.pool.worker.min.count': '1',
                'azure.vm.pool.worker.max.count': '20',
                'azure.vm.pool.worker.initial.count': '3',
                'azure.vm.pool.zk.count': '4',
                'azure.vm.pool.worker.spot.enabled': 'false',
            },
        )
        items = self.advisor.getServiceConfigurationsValidationItems({}, {}, services, {})
        error_names = [i['config-name'] for i in items if i['level'] == 'ERROR']
        self.assertIn('azure.vm.pool.zk.count', error_names)

    # --- Spot VM warning ---

    def test_spot_warning(self):
        services = _build_services(
            vm_env={'vm_manager_mode': 'existing'},
            vm_site={},
            pool_site={
                'azure.vm.pool.worker.min.count': '1',
                'azure.vm.pool.worker.max.count': '20',
                'azure.vm.pool.worker.initial.count': '3',
                'azure.vm.pool.zk.count': '3',
                'azure.vm.pool.worker.spot.enabled': 'true',
            },
        )
        items = self.advisor.getServiceConfigurationsValidationItems({}, {}, services, {})
        warnings = [i for i in items if i['level'] == 'WARN']
        warn_names = [i['config-name'] for i in warnings]
        self.assertIn('azure.vm.pool.worker.spot.enabled', warn_names)

    # --- Fully valid config ---

    def test_valid_config_no_errors(self):
        services = _valid_managed_services()
        items = self.advisor.getServiceConfigurationsValidationItems({}, {}, services, {})
        errors = [i for i in items if i['level'] == 'ERROR']
        self.assertEqual(len(errors), 0)


if __name__ == '__main__':
    unittest.main()
