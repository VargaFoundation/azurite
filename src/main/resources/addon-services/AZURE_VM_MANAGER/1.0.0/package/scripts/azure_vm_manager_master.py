#!/usr/bin/env python3
"""
AZURE_VM_MANAGER_MASTER component handler.
Manages Azure VM lifecycle via a REST daemon and custom commands.
"""
import json
import os
import secrets
import signal
import sys

from resource_management.core.exceptions import Fail
from resource_management.core.resources.system import Directory, Execute, File
from resource_management.libraries.functions.check_process_status import check_process_status
from resource_management.libraries.functions.format import format
from resource_management.libraries.script.script import Script


class AzureVmManagerMaster(Script):

    def install(self, env):
        import params
        env.set_params(params)
        self.install_packages(env)

        # Install Azure SDK
        Execute('pip3 install azure-mgmt-compute azure-mgmt-network azure-mgmt-resource azure-identity',
                user='root',
                logoutput=True)

        # Create service directories
        for d in [params.vm_manager_log_dir, params.vm_manager_pid_dir, params.vm_manager_data_dir]:
            Directory(d,
                      owner=params.vm_manager_user,
                      group=params.vm_manager_group,
                      create_parents=True,
                      mode=0o755)

        self.configure(env)

    def configure(self, env):
        import params
        env.set_params(params)

        # Write VM manager configuration
        vm_config = {
            'mode': params.vm_manager_mode,
            'port': params.vm_manager_port,
            'subscription_id': params.azure_subscription_id,
            'resource_group': params.azure_resource_group,
            'region': params.azure_region,
            'identity': {
                'provider': params.identity_provider,
                'tenant_id': params.identity_tenant_id,
                'client_id': params.identity_client_id,
            },
            'networking': {
                'vnet_name': params.vm_vnet_name,
                'vnet_resource_group': params.vnet_rg,
                'subnet_name': params.vm_subnet_name,
                'nsg_name': params.vm_nsg_name,
            },
            'vm_image': {
                'publisher': params.vm_image_publisher,
                'offer': params.vm_image_offer,
                'sku': params.vm_image_sku,
                'version': params.vm_image_version,
            },
            'admin': {
                'username': params.vm_admin_username,
                'ssh_public_key': params.vm_ssh_public_key,
            },
            'ambari_server_url': params.vm_ambari_server_url,
            'tags': params.vm_tags,
            'availability_zone': params.vm_availability_zone,
            'pools': {
                'head': {
                    'size': params.head_size,
                    'count': params.head_count,
                    'disk_type': params.head_disk_type,
                    'disk_size_gb': params.head_disk_size_gb,
                    'data_disks': params.head_data_disks,
                    'data_disk_size_gb': params.head_data_disk_size_gb,
                },
                'worker': {
                    'size': params.worker_size,
                    'min_count': params.worker_min_count,
                    'max_count': params.worker_max_count,
                    'initial_count': params.worker_initial_count,
                    'disk_type': params.worker_disk_type,
                    'disk_size_gb': params.worker_disk_size_gb,
                    'data_disks': params.worker_data_disks,
                    'data_disk_size_gb': params.worker_data_disk_size_gb,
                    'type': params.worker_type,
                    'secondary_sizes': params.worker_secondary_sizes,
                    'spot_enabled': params.worker_spot_enabled,
                    'spot_max_price': params.worker_spot_max_price,
                },
                'zookeeper': {
                    'size': params.zk_size,
                    'count': params.zk_count,
                },
            },
            'data_dir': params.vm_manager_data_dir,
            'log_dir': params.vm_manager_log_dir,
        }

        vm_config['api_token'] = secrets.token_hex(32)

        File(os.path.join(params.vm_manager_data_dir, 'vm_manager_config.json'),
             content=json.dumps(vm_config, indent=2),
             owner=params.vm_manager_user,
             group=params.vm_manager_group,
             mode=0o600)

    def start(self, env):
        import params
        env.set_params(params)
        self.configure(env)

        # Resolve paths
        daemon_script = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                                     'files', 'azure_vm_operations.py')
        config_path = os.path.join(params.vm_manager_data_dir, 'vm_manager_config.json')
        log_file = os.path.join(params.vm_manager_log_dir, 'vm_manager.log')

        # Start the VM Manager REST daemon
        cmd = ('nohup python3 -u {daemon} --config {config} --port {port} '
               '>> {log} 2>&1 & echo $! > {pid}').format(
            daemon=daemon_script,
            config=config_path,
            port=params.vm_manager_port,
            log=log_file,
            pid=params.vm_manager_pid_file)

        Execute(cmd,
                user=params.vm_manager_user,
                logoutput=True)

    def stop(self, env):
        import params
        env.set_params(params)

        if os.path.isfile(params.vm_manager_pid_file):
            with open(params.vm_manager_pid_file, 'r') as f:
                pid = f.read().strip()
            if pid:
                Execute('kill {0} || true'.format(pid), user=params.vm_manager_user)
            os.remove(params.vm_manager_pid_file)

    def status(self, env):
        import params
        env.set_params(params)
        check_process_status(params.vm_manager_pid_file)

    def _read_api_token(self, params):
        """Read the API token from the VM manager config file."""
        config_path = os.path.join(params.vm_manager_data_dir, 'vm_manager_config.json')
        try:
            with open(config_path, 'r') as f:
                cfg = json.load(f)
            return cfg.get('api_token', '')
        except Exception:
            return ''

    def provision_workers(self, env):
        """Custom command: provision additional worker VMs."""
        import params
        env.set_params(params)

        if params.vm_manager_mode != 'managed':
            raise Fail('Cannot provision workers: VM Manager is in "existing" mode.')

        token = self._read_api_token(params)
        Execute('curl -s -X POST -H "Authorization: Bearer {token}" '
                'http://localhost:{port}/api/v1/workers/provision'.format(
                    token=token, port=params.vm_manager_port),
                user=params.vm_manager_user,
                logoutput=True)

    def decommission_workers(self, env):
        """Custom command: decommission and delete worker VMs."""
        import params
        env.set_params(params)

        if params.vm_manager_mode != 'managed':
            raise Fail('Cannot decommission workers: VM Manager is in "existing" mode.')

        token = self._read_api_token(params)
        Execute('curl -s -X POST -H "Authorization: Bearer {token}" '
                'http://localhost:{port}/api/v1/workers/decommission'.format(
                    token=token, port=params.vm_manager_port),
                user=params.vm_manager_user,
                logoutput=True)

    def list_vms(self, env):
        """Custom command: list all managed VMs."""
        import params
        env.set_params(params)

        token = self._read_api_token(params)
        Execute('curl -s -H "Authorization: Bearer {token}" '
                'http://localhost:{port}/api/v1/vms'.format(
                    token=token, port=params.vm_manager_port),
                user=params.vm_manager_user,
                logoutput=True)


if __name__ == '__main__':
    AzureVmManagerMaster().execute()
