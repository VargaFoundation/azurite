#!/usr/bin/env python3
"""
Service advisor for AZURE_VM_MANAGER.
Validates networking configuration, VM pool bounds, and ZooKeeper count.
"""
import os
import sys
import traceback

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
STACKS_DIR = os.path.join(SCRIPT_DIR, '../../../stacks/')
PARENT_FILE = os.path.join(STACKS_DIR, 'service_advisor.py')

try:
    if os.path.exists(PARENT_FILE):
        import importlib.util
        spec = importlib.util.spec_from_file_location('service_advisor', PARENT_FILE)
        service_advisor = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(service_advisor)
    else:
        class ServiceAdvisor(object):
            def getServiceComponentLayoutValidations(self, services, hosts):
                return []
            def getServiceConfigurationRecommendations(self, configurations, clusterData, services, hosts):
                pass
            def getServiceConfigurationsValidationItems(self, configurations, recommendedDefaults, services, hosts):
                return []
        service_advisor = type(sys)('service_advisor')
        service_advisor.ServiceAdvisor = ServiceAdvisor
except Exception:
    traceback.print_exc()
    class ServiceAdvisor(object):
        def getServiceComponentLayoutValidations(self, services, hosts):
            return []
        def getServiceConfigurationRecommendations(self, configurations, clusterData, services, hosts):
            pass
        def getServiceConfigurationsValidationItems(self, configurations, recommendedDefaults, services, hosts):
            return []
    service_advisor = type(sys)('service_advisor')
    service_advisor.ServiceAdvisor = ServiceAdvisor


class AzureVmManagerServiceAdvisor(service_advisor.ServiceAdvisor):

    def getServiceConfigurationsValidationItems(self, configurations, recommendedDefaults, services, hosts):
        items = []
        props = self._get_service_configs(services)
        if not props:
            return items

        mode = props.get('azure-vm-manager-env', {}).get('vm_manager_mode', 'existing')

        if mode == 'managed':
            # VNet required in managed mode
            vnet = props.get('azure-vm-manager-site', {}).get('azure.vm.vnet.name', '')
            if not vnet:
                items.append({
                    'type': 'configuration', 'level': 'ERROR',
                    'message': 'Virtual Network name is required in managed mode.',
                    'config-type': 'azure-vm-manager-site',
                    'config-name': 'azure.vm.vnet.name'
                })

            # SSH key required
            ssh_key = props.get('azure-vm-manager-site', {}).get('azure.vm.ssh.public.key', '')
            if not ssh_key:
                items.append({
                    'type': 'configuration', 'level': 'ERROR',
                    'message': 'SSH public key is required for VM provisioning in managed mode.',
                    'config-type': 'azure-vm-manager-site',
                    'config-name': 'azure.vm.ssh.public.key'
                })

        # Worker count bounds
        pool = props.get('azure-vm-pool-site', {})
        min_count = int(pool.get('azure.vm.pool.worker.min.count', '1'))
        max_count = int(pool.get('azure.vm.pool.worker.max.count', '20'))
        initial_count = int(pool.get('azure.vm.pool.worker.initial.count', '3'))

        if min_count > max_count:
            items.append({
                'type': 'configuration', 'level': 'ERROR',
                'message': 'Worker minimum count ({0}) must be <= maximum count ({1}).'.format(min_count, max_count),
                'config-type': 'azure-vm-pool-site',
                'config-name': 'azure.vm.pool.worker.min.count'
            })

        if initial_count < min_count or initial_count > max_count:
            items.append({
                'type': 'configuration', 'level': 'ERROR',
                'message': 'Worker initial count ({0}) must be between min ({1}) and max ({2}).'.format(
                    initial_count, min_count, max_count),
                'config-type': 'azure-vm-pool-site',
                'config-name': 'azure.vm.pool.worker.initial.count'
            })

        # ZooKeeper count must be odd
        zk_count = int(pool.get('azure.vm.pool.zk.count', '3'))
        if zk_count % 2 == 0:
            items.append({
                'type': 'configuration', 'level': 'ERROR',
                'message': 'ZooKeeper count must be odd (got {0}).'.format(zk_count),
                'config-type': 'azure-vm-pool-site',
                'config-name': 'azure.vm.pool.zk.count'
            })

        # TLS warning
        tls_enabled = props.get('azure-vm-manager-env', {}).get('vm_manager_tls_enabled', 'false')
        if tls_enabled != 'true':
            items.append({
                'type': 'configuration', 'level': 'WARN',
                'message': 'TLS is disabled for the VM Manager REST API. '
                           'Strongly recommended for production deployments.',
                'config-type': 'azure-vm-manager-env',
                'config-name': 'vm_manager_tls_enabled'
            })

        # Spot VM warning
        spot_enabled = pool.get('azure.vm.pool.worker.spot.enabled', 'false')
        if spot_enabled == 'true':
            items.append({
                'type': 'configuration', 'level': 'WARN',
                'message': 'Spot VMs may be evicted by Azure at any time. Not recommended for core/HDFS nodes.',
                'config-type': 'azure-vm-pool-site',
                'config-name': 'azure.vm.pool.worker.spot.enabled'
            })

        return items

    def getServiceComponentLayoutValidations(self, services, hosts):
        return []

    def getServiceConfigurationRecommendations(self, configurations, clusterData, services, hosts):
        pass

    def _get_service_configs(self, services):
        if not services or 'configurations' not in services:
            return {}
        configs = {}
        for config_type in services['configurations']:
            for key, value in config_type.items():
                if isinstance(value, dict) and 'properties' in value:
                    configs[key] = value['properties']
        return configs
