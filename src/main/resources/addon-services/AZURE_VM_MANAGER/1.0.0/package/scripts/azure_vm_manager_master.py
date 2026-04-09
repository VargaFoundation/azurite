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

        # Start the VM Manager REST daemon (PID written by the daemon itself)
        cmd = ('nohup python3 -u {daemon} --config {config} --port {port} '
               '--pid-file {pid} >> {log} 2>&1 &').format(
            daemon=daemon_script,
            config=config_path,
            port=params.vm_manager_port,
            pid=params.vm_manager_pid_file,
            log=log_file)

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
                # Verify the PID belongs to our daemon before killing
                cmdline_file = '/proc/{0}/cmdline'.format(pid)
                if os.path.exists(cmdline_file):
                    with open(cmdline_file, 'r') as cf:
                        cmdline = cf.read()
                    if 'azure_vm_operations' in cmdline:
                        Execute('kill {0}'.format(pid), user=params.vm_manager_user)
                    else:
                        from resource_management.core.logger import Logger
                        Logger.warning('PID %s does not belong to VM Manager daemon, skipping kill', pid)
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

    def _api_call(self, params, method, path):
        """Make an authenticated API call to the local VM Manager daemon."""
        from resource_management.core.logger import Logger
        try:
            from urllib.request import urlopen, Request
        except ImportError:
            from urllib2 import urlopen, Request
        token = self._read_api_token(params)
        url = 'http://localhost:{port}{path}'.format(port=params.vm_manager_port, path=path)
        req = Request(url, method=method, data=b'' if method == 'POST' else None)
        req.add_header('Authorization', 'Bearer {0}'.format(token))
        response = urlopen(req, timeout=30)
        result = response.read().decode()
        Logger.info(result)

    def provision_workers(self, env):
        """Custom command: provision additional worker VMs."""
        import params
        env.set_params(params)

        if params.vm_manager_mode != 'managed':
            raise Fail('Cannot provision workers: VM Manager is in "existing" mode.')

        self._api_call(params, 'POST', '/api/v1/workers/provision')

    def decommission_workers(self, env):
        """Custom command: decommission and delete worker VMs."""
        import params
        env.set_params(params)

        if params.vm_manager_mode != 'managed':
            raise Fail('Cannot decommission workers: VM Manager is in "existing" mode.')

        self._api_call(params, 'POST', '/api/v1/workers/decommission')

    def list_vms(self, env):
        """Custom command: list all managed VMs."""
        import params
        env.set_params(params)

        self._api_call(params, 'GET', '/api/v1/vms')


if __name__ == '__main__':
    AzureVmManagerMaster().execute()
