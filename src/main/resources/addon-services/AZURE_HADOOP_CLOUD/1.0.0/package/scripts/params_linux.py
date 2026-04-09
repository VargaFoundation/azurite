#!/usr/bin/env python3
"""
Linux-specific parameter extraction for AZURE_HADOOP_CLOUD service.
Reads configuration from Ambari and computes derived values.
"""
from resource_management.libraries.script.script import Script
from resource_management.libraries.functions.default import default
from resource_management.libraries.functions.format import format

config = Script.get_config()

# ---- Environment ----
azure_cloud_user = config['configurations'].get('azure-cloud-env', {}).get('azure_cloud_user', 'azurehdp')
azure_cloud_group = config['configurations'].get('azure-cloud-env', {}).get('azure_cloud_group', 'azurehdp')
azure_cloud_log_dir = config['configurations'].get('azure-cloud-env', {}).get('azure_cloud_log_dir', '/var/log/azure-hadoop-cloud')
azure_cloud_pid_dir = config['configurations'].get('azure-cloud-env', {}).get('azure_cloud_pid_dir', '/var/run/azure-hadoop-cloud')
azure_storage_backend = config['configurations'].get('azure-cloud-env', {}).get('azure_storage_backend', 'adls_gen2')
azure_subscription_id = config['configurations'].get('azure-cloud-env', {}).get('azure_subscription_id', '')
azure_resource_group = config['configurations'].get('azure-cloud-env', {}).get('azure_resource_group', '')
azure_region = config['configurations'].get('azure-cloud-env', {}).get('azure_region', 'eastus')

# ---- Storage ----
storage_account_name = config['configurations'].get('azure-storage-site', {}).get('azure.storage.account.name', '')
storage_container_name = config['configurations'].get('azure-storage-site', {}).get('azure.storage.container.name', '')
storage_auth_type = config['configurations'].get('azure-storage-site', {}).get('azure.storage.auth.type', 'managed_identity')
storage_account_key = config['configurations'].get('azure-storage-site', {}).get('azure.storage.account.key', '')
storage_sas_token = config['configurations'].get('azure-storage-site', {}).get('azure.storage.sas.token', '')
managed_identity_client_id = config['configurations'].get('azure-storage-site', {}).get('azure.managed.identity.client.id', '')
managed_identity_tenant_id = config['configurations'].get('azure-storage-site', {}).get('azure.managed.identity.tenant.id', '')
oauth2_client_id = config['configurations'].get('azure-storage-site', {}).get('azure.oauth2.client.id', '')
oauth2_client_secret = config['configurations'].get('azure-storage-site', {}).get('azure.oauth2.client.secret', '')
oauth2_client_endpoint = config['configurations'].get('azure-storage-site', {}).get('azure.oauth2.client.endpoint', '')
wasb_secure_mode = config['configurations'].get('azure-storage-site', {}).get('azure.wasb.secure.mode', 'true')

# ---- Identity (for ARM API) ----
identity_provider = config['configurations'].get('azure-identity-site', {}).get('azure.identity.provider', 'managed_identity')
identity_tenant_id = config['configurations'].get('azure-identity-site', {}).get('azure.identity.tenant.id', '')
identity_client_id = config['configurations'].get('azure-identity-site', {}).get('azure.identity.client.id', '')
identity_client_secret = config['configurations'].get('azure-identity-site', {}).get('azure.identity.client.secret', '')

# ---- Computed: fs.defaultFS ----
if azure_storage_backend == 'adls_gen2':
    adls_fqdn = '{0}.dfs.core.windows.net'.format(storage_account_name)
    fs_default_fs = 'abfs://{0}@{1}/'.format(storage_container_name, adls_fqdn)
elif azure_storage_backend == 'wasb':
    wasb_fqdn = '{0}.blob.core.windows.net'.format(storage_account_name)
    wasb_scheme = 'wasbs' if wasb_secure_mode == 'true' else 'wasb'
    fs_default_fs = '{0}://{1}@{2}/'.format(wasb_scheme, storage_container_name, wasb_fqdn)
else:
    fs_default_fs = default('/configurations/core-site/fs.defaultFS', 'hdfs://localhost:8020')

# ---- Hadoop ----
java_home = config.get('ambariLevelParams', {}).get('java_home', '/usr/lib/jvm/java-8-openjdk-amd64')
hadoop_conf_dir = '/etc/hadoop/conf'
hostname = config.get('agentLevelParams', {}).get('hostname', 'localhost')
