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
        """Auto-compute fs.defaultFS, inject Azure filesystem properties, and configure dependent services."""
        props = self._get_service_configs(services)
        if not props:
            return

        backend = props.get('azure-cloud-env', {}).get('azure_storage_backend', 'adls_gen2')
        account = props.get('azure-storage-site', {}).get('azure.storage.account.name', '')
        container = props.get('azure-storage-site', {}).get('azure.storage.container.name', '')
        auth_type = props.get('azure-storage-site', {}).get('azure.storage.auth.type', 'managed_identity')
        endpoint_suffix = props.get('azure-storage-site', {}).get('azure.storage.endpoint.suffix', 'core.windows.net')

        # Ensure core-site config type exists in recommendations
        if 'core-site' not in configurations:
            configurations['core-site'] = {'properties': {}}
        core_site = configurations['core-site']['properties']

        if backend == 'adls_gen2' and account and container:
            fqdn = '{0}.dfs.{1}'.format(account, endpoint_suffix)
            adls_secure = props.get('azure-storage-site', {}).get('azure.adls.secure.mode', 'false')
            adls_scheme = 'abfss' if adls_secure == 'true' else 'abfs'
            core_site['fs.defaultFS'] = '{0}://{1}@{2}/'.format(adls_scheme, container, fqdn)
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
            fqdn = '{0}.blob.{1}'.format(account, endpoint_suffix)
            secure = props.get('azure-storage-site', {}).get('azure.wasb.secure.mode', 'true')
            scheme = 'wasbs' if secure == 'true' else 'wasb'
            core_site['fs.defaultFS'] = '{0}://{1}@{2}/'.format(scheme, container, fqdn)
            core_site['fs.wasb.impl'] = 'org.apache.hadoop.fs.azure.NativeAzureFileSystem'
            core_site['fs.wasbs.impl'] = 'org.apache.hadoop.fs.azure.NativeAzureFileSystem$Secure'

            if auth_type == 'storage_key':
                key = props.get('azure-storage-site', {}).get('azure.storage.account.key', '')
                if key:
                    core_site['fs.azure.account.key.{0}'.format(fqdn)] = key

        # ABFS/WASB performance tuning defaults
        if backend in ('adls_gen2', 'wasb'):
            tuning_defaults = {
                'fs.azure.read.request.size': '4194304',
                'fs.azure.write.request.size': '8388608',
                'fs.azure.block.size': '268435456',
                'fs.azure.io.retry.max.retries': '3',
                'fs.azure.io.retry.backoff.interval': '3000',
                'fs.azure.threads.max': '16',
                'fs.azure.enable.autothrottling': 'true',
                'fs.azure.readaheadqueue.depth': '2',
            }
            for key, default_val in tuning_defaults.items():
                if key not in core_site:
                    core_site[key] = default_val
            # Trash defaults for cloud storage
            if 'fs.trash.interval' not in core_site:
                core_site['fs.trash.interval'] = '1440'
            if 'fs.trash.checkpoint.interval' not in core_site:
                core_site['fs.trash.checkpoint.interval'] = '720'

        # Configure dependent Hadoop services for cloud storage
        default_fs = core_site.get('fs.defaultFS', '')
        self._recommend_dependent_service_configs(configurations, services, default_fs)

    def getServiceConfigurationsValidationItems(self, configurations, recommendedDefaults, services, hosts):
        """Validate Azure storage configuration completeness."""
        items = []
        props = self._get_service_configs(services)
        if not props:
            return items

        backend = props.get('azure-cloud-env', {}).get('azure_storage_backend', 'adls_gen2')

        if backend == 'hdfs':
            items.append({
                'type': 'configuration',
                'level': 'WARN',
                'message': 'HDFS storage backend is selected. For Azure deployments, '
                           'ADLS Gen2 or WASB is strongly recommended for durability and scalability.',
                'config-type': 'azure-cloud-env',
                'config-name': 'azure_storage_backend'
            })
            installed = self._get_installed_service_names(services)
            if 'HDFS' not in installed:
                items.append({
                    'type': 'configuration',
                    'level': 'ERROR',
                    'message': 'HDFS storage backend is selected but HDFS service is not installed. '
                               'Either install HDFS or switch to ADLS Gen2/WASB.',
                    'config-type': 'azure-cloud-env',
                    'config-name': 'azure_storage_backend'
                })

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

        # Validate ADLS secure mode requires OAuth-based auth
        if backend == 'adls_gen2':
            adls_secure = props.get('azure-storage-site', {}).get('azure.adls.secure.mode', 'false')
            auth_type_val = props.get('azure-storage-site', {}).get('azure.storage.auth.type', '')
            if adls_secure == 'true' and auth_type_val == 'storage_key':
                items.append({
                    'type': 'configuration',
                    'level': 'ERROR',
                    'message': 'ADLS Secure Mode (abfss://) requires OAuth-based authentication. '
                               'Storage Account Key cannot be used with abfss://.',
                    'config-type': 'azure-storage-site',
                    'config-name': 'azure.adls.secure.mode'
                })

        # Validate trash checkpoint <= trash interval
        try:
            trash_interval = int(props.get('core-site', {}).get('fs.trash.interval', '1440'))
            trash_checkpoint = int(props.get('core-site', {}).get('fs.trash.checkpoint.interval', '720'))
            if trash_interval > 0 and trash_checkpoint > trash_interval:
                items.append({
                    'type': 'configuration',
                    'level': 'ERROR',
                    'message': 'Trash checkpoint interval ({0}) must be <= trash interval ({1}).'.format(
                        trash_checkpoint, trash_interval),
                    'config-type': 'core-site',
                    'config-name': 'fs.trash.checkpoint.interval'
                })
        except (ValueError, TypeError):
            pass

        # Cross-service validation: warn if dependent services use hdfs:// paths with cloud backend
        if backend in ('adls_gen2', 'wasb'):
            hdfs_path_checks = [
                ('hive-site', 'hive.metastore.warehouse.dir', 'Hive warehouse directory'),
                ('yarn-site', 'yarn.nodemanager.remote-app-log-dir', 'YARN remote app log directory'),
                ('mapred-site', 'mapreduce.jobhistory.done-dir', 'MapReduce history done directory'),
                ('tez-site', 'tez.am.staging-dir', 'Tez AM staging directory'),
            ]
            for config_type, config_name, label in hdfs_path_checks:
                value = props.get(config_type, {}).get(config_name, '')
                if value.startswith('hdfs://'):
                    items.append({
                        'type': 'configuration',
                        'level': 'WARN',
                        'message': '{0} uses hdfs:// but storage backend is {1}. '
                                   'Use a relative path (e.g. /apps/hive/warehouse) to resolve against '
                                   'the cloud storage defaultFS.'.format(label, backend),
                        'config-type': config_type,
                        'config-name': config_name
                    })

        return items

    def getServiceComponentLayoutValidations(self, services, hosts):
        """Validate component placement."""
        return []

    def _recommend_dependent_service_configs(self, configurations, services, default_fs):
        """Inject cloud-aware paths into dependent Hadoop service configs."""
        if not default_fs or default_fs.startswith('hdfs://'):
            return

        installed = self._get_installed_service_names(services)

        def put_config(config_type, key, value):
            if config_type not in configurations:
                configurations[config_type] = {'properties': {}}
            elif 'properties' not in configurations[config_type]:
                configurations[config_type]['properties'] = {}
            configurations[config_type]['properties'][key] = value

        if 'HIVE' in installed:
            put_config('hive-site', 'hive.metastore.warehouse.dir', '/apps/hive/warehouse')
            put_config('hive-site', 'hive.exec.scratchdir', '/tmp/hive')

        if 'YARN' in installed:
            put_config('yarn-site', 'yarn.nodemanager.remote-app-log-dir', '/app-logs')
            put_config('yarn-site', 'yarn.app.mapreduce.am.staging-dir', '/user')

        if 'MAPREDUCE2' in installed:
            put_config('mapred-site', 'mapreduce.jobhistory.intermediate-done-dir', '/mr-history/tmp')
            put_config('mapred-site', 'mapreduce.jobhistory.done-dir', '/mr-history/done')

        if 'TEZ' in installed:
            put_config('tez-site', 'tez.am.staging-dir', '/tmp/tez-staging')

        if 'SPARK2' in installed:
            put_config('spark2-defaults', 'spark.sql.warehouse.dir', '/apps/spark/warehouse')
            put_config('spark2-defaults', 'spark.eventLog.dir', '/spark2-history')
            put_config('spark2-defaults', 'spark.history.fs.logDirectory', '/spark2-history')
        elif 'SPARK' in installed:
            put_config('spark-defaults', 'spark.sql.warehouse.dir', '/apps/spark/warehouse')
            put_config('spark-defaults', 'spark.eventLog.dir', '/spark-history')
            put_config('spark-defaults', 'spark.history.fs.logDirectory', '/spark-history')

    def _get_installed_service_names(self, services):
        """Return set of installed service names from the services descriptor."""
        if not services or 'services' not in services:
            return set()
        return {s.get('StackServices', {}).get('service_name', '')
                for s in services.get('services', [])}

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
