#!/usr/bin/env python3
"""
Service advisor for AZURE_HADOOP_CLOUD.
Validates storage configuration, auto-computes fs.defaultFS,
and injects Azure filesystem properties into core-site.xml.
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
        # Fallback: define a minimal base class
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


class AzureHadoopCloudServiceAdvisor(service_advisor.ServiceAdvisor):
    """
    Validates and recommends Azure Hadoop Cloud configuration.
    """

    def getServiceConfigurationRecommendations(self, configurations, clusterData, services, hosts):
        """Auto-compute fs.defaultFS and inject Azure filesystem properties."""
        props = self._get_service_configs(services)
        if not props:
            return

        backend = props.get('azure-cloud-env', {}).get('azure_storage_backend', 'hdfs')
        account = props.get('azure-storage-site', {}).get('azure.storage.account.name', '')
        container = props.get('azure-storage-site', {}).get('azure.storage.container.name', '')
        auth_type = props.get('azure-storage-site', {}).get('azure.storage.auth.type', 'managed_identity')

        # Ensure core-site config type exists in recommendations
        if 'core-site' not in configurations:
            configurations['core-site'] = {'properties': {}}
        core_site = configurations['core-site']['properties']

        if backend == 'adls_gen2' and account and container:
            fqdn = '{0}.dfs.core.windows.net'.format(account)
            core_site['fs.defaultFS'] = 'abfs://{0}@{1}/'.format(container, fqdn)
            core_site['fs.abfs.impl'] = 'org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem'
            core_site['fs.abfss.impl'] = 'org.apache.hadoop.fs.azurebfs.SecureAzureBlobFileSystem'

            if auth_type == 'managed_identity':
                tenant = props.get('azure-storage-site', {}).get('azure.managed.identity.tenant.id', '')
                client_id = props.get('azure-storage-site', {}).get('azure.managed.identity.client.id', '')
                core_site['fs.azure.account.auth.type.{0}'.format(fqdn)] = 'OAuth'
                core_site['fs.azure.account.oauth.provider.type.{0}'.format(fqdn)] = \
                    'org.apache.hadoop.fs.azurebfs.oauth2.MsiTokenProvider'
                if tenant:
                    core_site['fs.azure.account.oauth2.msi.tenant.{0}'.format(fqdn)] = tenant
                if client_id:
                    core_site['fs.azure.account.oauth2.client.id.{0}'.format(fqdn)] = client_id

            elif auth_type == 'storage_key':
                key = props.get('azure-storage-site', {}).get('azure.storage.account.key', '')
                if key:
                    core_site['fs.azure.account.key.{0}'.format(fqdn)] = key

            elif auth_type == 'oauth2_client_credential':
                core_site['fs.azure.account.auth.type.{0}'.format(fqdn)] = 'OAuth'
                core_site['fs.azure.account.oauth.provider.type.{0}'.format(fqdn)] = \
                    'org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider'
                endpoint = props.get('azure-storage-site', {}).get('azure.oauth2.client.endpoint', '')
                client_id = props.get('azure-storage-site', {}).get('azure.oauth2.client.id', '')
                if endpoint:
                    core_site['fs.azure.account.oauth2.client.endpoint.{0}'.format(fqdn)] = endpoint
                if client_id:
                    core_site['fs.azure.account.oauth2.client.id.{0}'.format(fqdn)] = client_id

        elif backend == 'wasb' and account and container:
            fqdn = '{0}.blob.core.windows.net'.format(account)
            secure = props.get('azure-storage-site', {}).get('azure.wasb.secure.mode', 'true')
            scheme = 'wasbs' if secure == 'true' else 'wasb'
            core_site['fs.defaultFS'] = '{0}://{1}@{2}/'.format(scheme, container, fqdn)
            core_site['fs.wasb.impl'] = 'org.apache.hadoop.fs.azure.NativeAzureFileSystem'
            core_site['fs.wasbs.impl'] = 'org.apache.hadoop.fs.azure.NativeAzureFileSystem$Secure'

            if auth_type == 'storage_key':
                key = props.get('azure-storage-site', {}).get('azure.storage.account.key', '')
                if key:
                    core_site['fs.azure.account.key.{0}'.format(fqdn)] = key

    def getServiceConfigurationsValidationItems(self, configurations, recommendedDefaults, services, hosts):
        """Validate Azure storage configuration completeness."""
        items = []
        props = self._get_service_configs(services)
        if not props:
            return items

        backend = props.get('azure-cloud-env', {}).get('azure_storage_backend', 'hdfs')

        if backend in ('adls_gen2', 'wasb'):
            account = props.get('azure-storage-site', {}).get('azure.storage.account.name', '')
            container = props.get('azure-storage-site', {}).get('azure.storage.container.name', '')
            auth_type = props.get('azure-storage-site', {}).get('azure.storage.auth.type', '')

            if not account:
                items.append({
                    'type': 'configuration',
                    'level': 'ERROR',
                    'message': 'Storage account name is required when using Azure storage backend ({0}).'.format(
                        backend),
                    'config-type': 'azure-storage-site',
                    'config-name': 'azure.storage.account.name'
                })

            if not container:
                items.append({
                    'type': 'configuration',
                    'level': 'ERROR',
                    'message': 'Storage container name is required when using Azure storage backend ({0}).'.format(
                        backend),
                    'config-type': 'azure-storage-site',
                    'config-name': 'azure.storage.container.name'
                })

            # Validate credentials based on auth type
            if auth_type == 'managed_identity':
                if not props.get('azure-storage-site', {}).get('azure.managed.identity.client.id', ''):
                    items.append({
                        'type': 'configuration',
                        'level': 'ERROR',
                        'message': 'Managed Identity Client ID is required for managed_identity authentication.',
                        'config-type': 'azure-storage-site',
                        'config-name': 'azure.managed.identity.client.id'
                    })

            elif auth_type == 'storage_key':
                if not props.get('azure-storage-site', {}).get('azure.storage.account.key', ''):
                    items.append({
                        'type': 'configuration',
                        'level': 'ERROR',
                        'message': 'Storage account key is required for storage_key authentication.',
                        'config-type': 'azure-storage-site',
                        'config-name': 'azure.storage.account.key'
                    })

            elif auth_type == 'sas_token':
                if not props.get('azure-storage-site', {}).get('azure.storage.sas.token', ''):
                    items.append({
                        'type': 'configuration',
                        'level': 'ERROR',
                        'message': 'SAS token is required for sas_token authentication.',
                        'config-type': 'azure-storage-site',
                        'config-name': 'azure.storage.sas.token'
                    })

            elif auth_type == 'oauth2_client_credential':
                for field, label in [
                    ('azure.oauth2.client.id', 'OAuth2 Client ID'),
                    ('azure.oauth2.client.secret', 'OAuth2 Client Secret'),
                    ('azure.oauth2.client.endpoint', 'OAuth2 Token Endpoint'),
                ]:
                    if not props.get('azure-storage-site', {}).get(field, ''):
                        items.append({
                            'type': 'configuration',
                            'level': 'ERROR',
                            'message': '{0} is required for oauth2_client_credential authentication.'.format(label),
                            'config-type': 'azure-storage-site',
                            'config-name': field
                        })

        return items

    def getServiceComponentLayoutValidations(self, services, hosts):
        """Validate component placement."""
        return []

    def _get_service_configs(self, services):
        """Extract configuration properties from services descriptor."""
        if not services or 'configurations' not in services:
            return {}
        configs = {}
        for config_type in services['configurations']:
            for key, value in config_type.items():
                if isinstance(value, dict) and 'properties' in value:
                    configs[key] = value['properties']
        return configs
