#!/usr/bin/env python3
"""
Linux-specific parameter extraction for AZURE_VM_MANAGER service.
"""
import json
from resource_management.libraries.script.script import Script
from resource_management.libraries.functions.default import default

config = Script.get_config()

# ---- Environment ----
vm_manager_user = config['configurations'].get('azure-vm-manager-env', {}).get('vm_manager_user', 'azurehdp')
vm_manager_group = config['configurations'].get('azure-vm-manager-env', {}).get('vm_manager_group', 'azurehdp')
vm_manager_log_dir = config['configurations'].get('azure-vm-manager-env', {}).get('vm_manager_log_dir', '/var/log/azure-vm-manager')
vm_manager_pid_dir = config['configurations'].get('azure-vm-manager-env', {}).get('vm_manager_pid_dir', '/var/run/azure-vm-manager')
vm_manager_data_dir = config['configurations'].get('azure-vm-manager-env', {}).get('vm_manager_data_dir', '/var/lib/azure-vm-manager')
vm_manager_port = int(config['configurations'].get('azure-vm-manager-env', {}).get('vm_manager_port', '8470'))
vm_manager_mode = config['configurations'].get('azure-vm-manager-env', {}).get('vm_manager_mode', 'existing')
vm_manager_pid_file = '{0}/azure-vm-manager.pid'.format(vm_manager_pid_dir)

# ---- Azure Cloud (from AZURE_HADOOP_CLOUD) ----
azure_subscription_id = config['configurations'].get('azure-cloud-env', {}).get('azure_subscription_id', '')
azure_resource_group = config['configurations'].get('azure-cloud-env', {}).get('azure_resource_group', '')
azure_region = config['configurations'].get('azure-cloud-env', {}).get('azure_region', 'eastus')

# ---- Identity (from AZURE_HADOOP_CLOUD) ----
identity_provider = config['configurations'].get('azure-identity-site', {}).get('azure.identity.provider', 'managed_identity')
identity_tenant_id = config['configurations'].get('azure-identity-site', {}).get('azure.identity.tenant.id', '')
identity_client_id = config['configurations'].get('azure-identity-site', {}).get('azure.identity.client.id', '')
identity_client_secret = config['configurations'].get('azure-identity-site', {}).get('azure.identity.client.secret', '')

# ---- Networking ----
vm_vnet_name = config['configurations'].get('azure-vm-manager-site', {}).get('azure.vm.vnet.name', '')
vm_vnet_resource_group = config['configurations'].get('azure-vm-manager-site', {}).get('azure.vm.vnet.resource.group', '')
vm_subnet_name = config['configurations'].get('azure-vm-manager-site', {}).get('azure.vm.subnet.name', 'default')
vm_nsg_name = config['configurations'].get('azure-vm-manager-site', {}).get('azure.vm.nsg.name', '')
vm_ssh_public_key = config['configurations'].get('azure-vm-manager-site', {}).get('azure.vm.ssh.public.key', '')
vm_admin_username = config['configurations'].get('azure-vm-manager-site', {}).get('azure.vm.admin.username', 'azureadmin')

# ---- VM Image ----
vm_image_publisher = config['configurations'].get('azure-vm-manager-site', {}).get('azure.vm.image.publisher', 'Canonical')
vm_image_offer = config['configurations'].get('azure-vm-manager-site', {}).get('azure.vm.image.offer',
                                                                               '0001-com-ubuntu-server-focal')
vm_image_sku = config['configurations'].get('azure-vm-manager-site', {}).get('azure.vm.image.sku', '20_04-lts-gen2')
vm_image_version = config['configurations'].get('azure-vm-manager-site', {}).get('azure.vm.image.version', 'latest')
vm_ambari_server_url = config['configurations'].get('azure-vm-manager-site', {}).get('azure.vm.ambari.server.url', '')
vm_tags_json = config['configurations'].get('azure-vm-manager-site', {}).get('azure.vm.tags',
                                                                             '{"managed-by": "ambari-azure-mpack"}')
vm_availability_zone = config['configurations'].get('azure-vm-manager-site', {}).get('azure.vm.availability.zone', '')

try:
    vm_tags = json.loads(vm_tags_json)
except (json.JSONDecodeError, TypeError):
    vm_tags = {'managed-by': 'ambari-azure-mpack'}

# ---- Node Pools ----
pool = config['configurations'].get('azure-vm-pool-site', {})
head_size = pool.get('azure.vm.pool.head.size', 'Standard_D4_v3')
head_count = int(pool.get('azure.vm.pool.head.count', '2'))
head_disk_type = pool.get('azure.vm.pool.head.disk.type', 'Premium_LRS')
head_disk_size_gb = int(pool.get('azure.vm.pool.head.disk.size.gb', '128'))
head_data_disks = int(pool.get('azure.vm.pool.head.data.disks', '2'))
head_data_disk_size_gb = int(pool.get('azure.vm.pool.head.data.disk.size.gb', '256'))

worker_size = pool.get('azure.vm.pool.worker.size', 'Standard_D4_v3')
worker_min_count = int(pool.get('azure.vm.pool.worker.min.count', '1'))
worker_max_count = int(pool.get('azure.vm.pool.worker.max.count', '20'))
worker_initial_count = int(pool.get('azure.vm.pool.worker.initial.count', '3'))
worker_disk_type = pool.get('azure.vm.pool.worker.disk.type', 'Standard_LRS')
worker_disk_size_gb = int(pool.get('azure.vm.pool.worker.disk.size.gb', '128'))
worker_data_disks = int(pool.get('azure.vm.pool.worker.data.disks', '4'))
worker_data_disk_size_gb = int(pool.get('azure.vm.pool.worker.data.disk.size.gb', '512'))
worker_type = pool.get('azure.vm.pool.worker.type', 'core')
worker_secondary_sizes = [s.strip() for s in pool.get('azure.vm.pool.worker.secondary.sizes', '').split(',')
                          if s.strip()]
worker_spot_enabled = pool.get('azure.vm.pool.worker.spot.enabled', 'false') == 'true'
worker_spot_max_price = float(pool.get('azure.vm.pool.worker.spot.max.price', '-1'))

zk_size = pool.get('azure.vm.pool.zk.size', 'Standard_A2_v2')
zk_count = int(pool.get('azure.vm.pool.zk.count', '3'))

# ---- Computed ----
hostname = config.get('agentLevelParams', {}).get('hostname', 'localhost')
java_home = config.get('ambariLevelParams', {}).get('java_home', '/usr/lib/jvm/java-8-openjdk-amd64')
vnet_rg = vm_vnet_resource_group if vm_vnet_resource_group else azure_resource_group
