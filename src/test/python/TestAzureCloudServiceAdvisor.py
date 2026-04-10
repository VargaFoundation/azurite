#!/usr/bin/env python3
"""Unit tests for AzureHadoopCloudServiceAdvisor."""
import importlib.util
import os
import sys
import unittest

# Load the service advisor module under a unique name to avoid collisions
# with other service_advisor.py files in sibling service directories.
_SA_PATH = os.path.normpath(os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    '..', '..', 'main', 'resources', 'addon-services',
    'AZURE_HADOOP_CLOUD', '1.0.0', 'service_advisor.py'
))
_spec = importlib.util.spec_from_file_location('azure_hadoop_cloud_service_advisor', _SA_PATH)
_mod = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = _mod
_spec.loader.exec_module(_mod)
AzureHadoopCloudServiceAdvisor = _mod.AzureHadoopCloudServiceAdvisor


def _build_services(cloud_env=None, storage_site=None, identity_site=None):
    """Build a services dict in the Ambari format expected by _get_service_configs."""
    configurations = []
    if cloud_env is not None:
        configurations.append({'azure-cloud-env': {'properties': cloud_env}})
    if storage_site is not None:
        configurations.append({'azure-storage-site': {'properties': storage_site}})
    if identity_site is not None:
        configurations.append({'azure-identity-site': {'properties': identity_site}})
    return {'configurations': configurations}


def _build_services_with_installed(installed_service_names, cloud_env=None, storage_site=None, identity_site=None,
                                   extra_configs=None):
    """Build a services dict that includes installed service names and optional extra config types."""
    base = _build_services(cloud_env, storage_site, identity_site)
    base['services'] = [
        {'StackServices': {'service_name': name}} for name in installed_service_names
    ]
    if extra_configs:
        for config_type, props in extra_configs.items():
            base['configurations'].append({config_type: {'properties': props}})
    return base


def _valid_adls_gen2_services():
    """Return a services dict representing a fully valid ADLS Gen2 configuration."""
    return _build_services(
        cloud_env={'azure_storage_backend': 'adls_gen2'},
        storage_site={
            'azure.storage.account.name': 'myaccount',
            'azure.storage.container.name': 'mycontainer',
            'azure.storage.auth.type': 'managed_identity',
            'azure.managed.identity.client.id': 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee',
        },
    )


class TestAzureCloudServiceAdvisorValidation(unittest.TestCase):
    """Tests for getServiceConfigurationsValidationItems."""

    def setUp(self):
        self.advisor = AzureHadoopCloudServiceAdvisor()

    # --- Storage backend requires account name / container ---

    def test_adls_gen2_requires_account_name(self):
        services = _build_services(
            cloud_env={'azure_storage_backend': 'adls_gen2'},
            storage_site={
                'azure.storage.account.name': '',
                'azure.storage.container.name': 'mycontainer',
                'azure.storage.auth.type': 'managed_identity',
                'azure.managed.identity.client.id': 'some-id',
            },
        )
        items = self.advisor.getServiceConfigurationsValidationItems({}, {}, services, {})
        error_names = [i['config-name'] for i in items if i['level'] == 'ERROR']
        self.assertIn('azure.storage.account.name', error_names)

    def test_adls_gen2_requires_container(self):
        services = _build_services(
            cloud_env={'azure_storage_backend': 'adls_gen2'},
            storage_site={
                'azure.storage.account.name': 'myaccount',
                'azure.storage.container.name': '',
                'azure.storage.auth.type': 'managed_identity',
                'azure.managed.identity.client.id': 'some-id',
            },
        )
        items = self.advisor.getServiceConfigurationsValidationItems({}, {}, services, {})
        error_names = [i['config-name'] for i in items if i['level'] == 'ERROR']
        self.assertIn('azure.storage.container.name', error_names)

    def test_wasb_requires_account_name(self):
        services = _build_services(
            cloud_env={'azure_storage_backend': 'wasb'},
            storage_site={
                'azure.storage.account.name': '',
                'azure.storage.container.name': 'mycontainer',
                'azure.storage.auth.type': 'storage_key',
                'azure.storage.account.key': 'somekey',
            },
        )
        items = self.advisor.getServiceConfigurationsValidationItems({}, {}, services, {})
        error_names = [i['config-name'] for i in items if i['level'] == 'ERROR']
        self.assertIn('azure.storage.account.name', error_names)

    def test_hdfs_no_azure_validation(self):
        """HDFS backend should not require Azure storage fields, but emits a WARN."""
        services = _build_services_with_installed(
            ['AZURE_HADOOP_CLOUD', 'HDFS'],
            cloud_env={'azure_storage_backend': 'hdfs'},
            storage_site={
                'azure.storage.account.name': '',
                'azure.storage.container.name': '',
            },
        )
        items = self.advisor.getServiceConfigurationsValidationItems({}, {}, services, {})
        errors = [i for i in items if i['level'] == 'ERROR']
        self.assertEqual(len(errors), 0)
        warns = [i for i in items if i['level'] == 'WARN']
        self.assertTrue(len(warns) > 0)

    # --- Auth-type credential validation ---

    def test_managed_identity_requires_client_id(self):
        services = _build_services(
            cloud_env={'azure_storage_backend': 'adls_gen2'},
            storage_site={
                'azure.storage.account.name': 'myaccount',
                'azure.storage.container.name': 'mycontainer',
                'azure.storage.auth.type': 'managed_identity',
                'azure.managed.identity.client.id': '',
            },
        )
        items = self.advisor.getServiceConfigurationsValidationItems({}, {}, services, {})
        error_names = [i['config-name'] for i in items if i['level'] == 'ERROR']
        self.assertIn('azure.managed.identity.client.id', error_names)

    def test_storage_key_requires_key(self):
        services = _build_services(
            cloud_env={'azure_storage_backend': 'adls_gen2'},
            storage_site={
                'azure.storage.account.name': 'myaccount',
                'azure.storage.container.name': 'mycontainer',
                'azure.storage.auth.type': 'storage_key',
                'azure.storage.account.key': '',
            },
        )
        items = self.advisor.getServiceConfigurationsValidationItems({}, {}, services, {})
        error_names = [i['config-name'] for i in items if i['level'] == 'ERROR']
        self.assertIn('azure.storage.account.key', error_names)

    def test_sas_token_requires_token(self):
        services = _build_services(
            cloud_env={'azure_storage_backend': 'wasb'},
            storage_site={
                'azure.storage.account.name': 'myaccount',
                'azure.storage.container.name': 'mycontainer',
                'azure.storage.auth.type': 'sas_token',
                'azure.storage.sas.token': '',
            },
        )
        items = self.advisor.getServiceConfigurationsValidationItems({}, {}, services, {})
        error_names = [i['config-name'] for i in items if i['level'] == 'ERROR']
        self.assertIn('azure.storage.sas.token', error_names)

    def test_oauth2_requires_all_fields(self):
        services = _build_services(
            cloud_env={'azure_storage_backend': 'adls_gen2'},
            storage_site={
                'azure.storage.account.name': 'myaccount',
                'azure.storage.container.name': 'mycontainer',
                'azure.storage.auth.type': 'oauth2_client_credential',
                'azure.oauth2.client.id': '',
                'azure.oauth2.client.secret': '',
                'azure.oauth2.client.endpoint': '',
            },
        )
        items = self.advisor.getServiceConfigurationsValidationItems({}, {}, services, {})
        error_names = [i['config-name'] for i in items if i['level'] == 'ERROR']
        self.assertIn('azure.oauth2.client.id', error_names)
        self.assertIn('azure.oauth2.client.secret', error_names)
        self.assertIn('azure.oauth2.client.endpoint', error_names)

    def test_valid_config_no_errors(self):
        services = _valid_adls_gen2_services()
        items = self.advisor.getServiceConfigurationsValidationItems({}, {}, services, {})
        errors = [i for i in items if i['level'] == 'ERROR']
        self.assertEqual(len(errors), 0)


class TestAzureCloudServiceAdvisorRecommendations(unittest.TestCase):
    """Tests for getServiceConfigurationRecommendations."""

    def setUp(self):
        self.advisor = AzureHadoopCloudServiceAdvisor()

    def test_fs_default_fs_recommendation_adls(self):
        services = _build_services(
            cloud_env={'azure_storage_backend': 'adls_gen2'},
            storage_site={
                'azure.storage.account.name': 'myaccount',
                'azure.storage.container.name': 'mycontainer',
                'azure.storage.auth.type': 'managed_identity',
            },
        )
        configurations = {}
        self.advisor.getServiceConfigurationRecommendations(configurations, {}, services, {})
        core_site = configurations.get('core-site', {}).get('properties', {})
        self.assertEqual(
            core_site.get('fs.defaultFS'),
            'abfs://mycontainer@myaccount.dfs.core.windows.net/'
        )

    def test_fs_default_fs_recommendation_wasb(self):
        services = _build_services(
            cloud_env={'azure_storage_backend': 'wasb'},
            storage_site={
                'azure.storage.account.name': 'myaccount',
                'azure.storage.container.name': 'mycontainer',
                'azure.storage.auth.type': 'storage_key',
                'azure.wasb.secure.mode': 'true',
            },
        )
        configurations = {}
        self.advisor.getServiceConfigurationRecommendations(configurations, {}, services, {})
        core_site = configurations.get('core-site', {}).get('properties', {})
        self.assertEqual(
            core_site.get('fs.defaultFS'),
            'wasbs://mycontainer@myaccount.blob.core.windows.net/'
        )


class TestCrossServiceRecommendations(unittest.TestCase):
    """Tests for cross-service configuration recommendations."""

    def setUp(self):
        self.advisor = AzureHadoopCloudServiceAdvisor()

    def _adls_services_with(self, installed_names):
        return _build_services_with_installed(
            installed_names,
            cloud_env={'azure_storage_backend': 'adls_gen2'},
            storage_site={
                'azure.storage.account.name': 'myaccount',
                'azure.storage.container.name': 'mycontainer',
                'azure.storage.auth.type': 'managed_identity',
                'azure.managed.identity.client.id': 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee',
            },
        )

    def test_hive_warehouse_dir_recommended_for_adls(self):
        services = self._adls_services_with(['AZURE_HADOOP_CLOUD', 'HIVE'])
        configurations = {}
        self.advisor.getServiceConfigurationRecommendations(configurations, {}, services, {})
        hive_site = configurations.get('hive-site', {}).get('properties', {})
        self.assertEqual(hive_site.get('hive.metastore.warehouse.dir'), '/apps/hive/warehouse')
        self.assertEqual(hive_site.get('hive.exec.scratchdir'), '/tmp/hive')

    def test_yarn_log_dir_recommended_for_cloud(self):
        services = self._adls_services_with(['AZURE_HADOOP_CLOUD', 'YARN'])
        configurations = {}
        self.advisor.getServiceConfigurationRecommendations(configurations, {}, services, {})
        yarn_site = configurations.get('yarn-site', {}).get('properties', {})
        self.assertEqual(yarn_site.get('yarn.nodemanager.remote-app-log-dir'), '/app-logs')
        self.assertEqual(yarn_site.get('yarn.app.mapreduce.am.staging-dir'), '/user')

    def test_mapreduce_dirs_recommended_for_cloud(self):
        services = self._adls_services_with(['AZURE_HADOOP_CLOUD', 'MAPREDUCE2'])
        configurations = {}
        self.advisor.getServiceConfigurationRecommendations(configurations, {}, services, {})
        mapred_site = configurations.get('mapred-site', {}).get('properties', {})
        self.assertEqual(mapred_site.get('mapreduce.jobhistory.intermediate-done-dir'), '/mr-history/tmp')
        self.assertEqual(mapred_site.get('mapreduce.jobhistory.done-dir'), '/mr-history/done')

    def test_tez_staging_recommended_for_cloud(self):
        services = self._adls_services_with(['AZURE_HADOOP_CLOUD', 'TEZ'])
        configurations = {}
        self.advisor.getServiceConfigurationRecommendations(configurations, {}, services, {})
        tez_site = configurations.get('tez-site', {}).get('properties', {})
        self.assertEqual(tez_site.get('tez.am.staging-dir'), '/tmp/tez-staging')

    def test_spark2_configs_recommended_for_cloud(self):
        services = self._adls_services_with(['AZURE_HADOOP_CLOUD', 'SPARK2'])
        configurations = {}
        self.advisor.getServiceConfigurationRecommendations(configurations, {}, services, {})
        spark2 = configurations.get('spark2-defaults', {}).get('properties', {})
        self.assertEqual(spark2.get('spark.sql.warehouse.dir'), '/apps/spark/warehouse')
        self.assertEqual(spark2.get('spark.eventLog.dir'), '/spark2-history')
        self.assertEqual(spark2.get('spark.history.fs.logDirectory'), '/spark2-history')

    def test_spark1_configs_recommended_when_spark2_absent(self):
        services = self._adls_services_with(['AZURE_HADOOP_CLOUD', 'SPARK'])
        configurations = {}
        self.advisor.getServiceConfigurationRecommendations(configurations, {}, services, {})
        spark1 = configurations.get('spark-defaults', {}).get('properties', {})
        self.assertEqual(spark1.get('spark.sql.warehouse.dir'), '/apps/spark/warehouse')
        self.assertNotIn('spark2-defaults', configurations)

    def test_no_cross_service_for_hdfs_backend(self):
        services = _build_services_with_installed(
            ['AZURE_HADOOP_CLOUD', 'HIVE', 'YARN', 'SPARK2'],
            cloud_env={'azure_storage_backend': 'hdfs'},
            storage_site={
                'azure.storage.account.name': '',
                'azure.storage.container.name': '',
            },
        )
        configurations = {}
        self.advisor.getServiceConfigurationRecommendations(configurations, {}, services, {})
        self.assertNotIn('hive-site', configurations)
        self.assertNotIn('yarn-site', configurations)
        self.assertNotIn('spark2-defaults', configurations)

    def test_spark_not_set_when_not_installed(self):
        services = self._adls_services_with(['AZURE_HADOOP_CLOUD', 'HIVE'])
        configurations = {}
        self.advisor.getServiceConfigurationRecommendations(configurations, {}, services, {})
        self.assertNotIn('spark2-defaults', configurations)
        self.assertNotIn('spark-defaults', configurations)

    def test_wasb_cross_service_recommendations(self):
        services = _build_services_with_installed(
            ['AZURE_HADOOP_CLOUD', 'HIVE', 'YARN'],
            cloud_env={'azure_storage_backend': 'wasb'},
            storage_site={
                'azure.storage.account.name': 'myaccount',
                'azure.storage.container.name': 'mycontainer',
                'azure.storage.auth.type': 'storage_key',
                'azure.storage.account.key': 'somekey',
                'azure.wasb.secure.mode': 'true',
            },
        )
        configurations = {}
        self.advisor.getServiceConfigurationRecommendations(configurations, {}, services, {})
        hive_site = configurations.get('hive-site', {}).get('properties', {})
        self.assertEqual(hive_site.get('hive.metastore.warehouse.dir'), '/apps/hive/warehouse')
        yarn_site = configurations.get('yarn-site', {}).get('properties', {})
        self.assertEqual(yarn_site.get('yarn.nodemanager.remote-app-log-dir'), '/app-logs')


class TestSovereignCloudAndSecureMode(unittest.TestCase):
    """Tests for sovereign cloud endpoint suffix, ADLS secure mode, tuning and trash."""

    def setUp(self):
        self.advisor = AzureHadoopCloudServiceAdvisor()

    def test_fs_default_fs_adls_secure_mode(self):
        services = _build_services(
            cloud_env={'azure_storage_backend': 'adls_gen2'},
            storage_site={
                'azure.storage.account.name': 'myaccount',
                'azure.storage.container.name': 'mycontainer',
                'azure.storage.auth.type': 'managed_identity',
                'azure.adls.secure.mode': 'true',
            },
        )
        configurations = {}
        self.advisor.getServiceConfigurationRecommendations(configurations, {}, services, {})
        core_site = configurations.get('core-site', {}).get('properties', {})
        self.assertEqual(
            core_site.get('fs.defaultFS'),
            'abfss://mycontainer@myaccount.dfs.core.windows.net/'
        )

    def test_fs_default_fs_sovereign_cloud_gov(self):
        services = _build_services(
            cloud_env={'azure_storage_backend': 'adls_gen2'},
            storage_site={
                'azure.storage.account.name': 'govaccount',
                'azure.storage.container.name': 'govcontainer',
                'azure.storage.auth.type': 'managed_identity',
                'azure.storage.endpoint.suffix': 'core.usgovcloudapi.net',
            },
        )
        configurations = {}
        self.advisor.getServiceConfigurationRecommendations(configurations, {}, services, {})
        core_site = configurations.get('core-site', {}).get('properties', {})
        self.assertEqual(
            core_site.get('fs.defaultFS'),
            'abfs://govcontainer@govaccount.dfs.core.usgovcloudapi.net/'
        )

    def test_fs_default_fs_sovereign_cloud_china(self):
        services = _build_services(
            cloud_env={'azure_storage_backend': 'wasb'},
            storage_site={
                'azure.storage.account.name': 'cnaccount',
                'azure.storage.container.name': 'cncontainer',
                'azure.storage.auth.type': 'storage_key',
                'azure.storage.account.key': 'somekey',
                'azure.wasb.secure.mode': 'true',
                'azure.storage.endpoint.suffix': 'core.chinacloudapi.cn',
            },
        )
        configurations = {}
        self.advisor.getServiceConfigurationRecommendations(configurations, {}, services, {})
        core_site = configurations.get('core-site', {}).get('properties', {})
        self.assertEqual(
            core_site.get('fs.defaultFS'),
            'wasbs://cncontainer@cnaccount.blob.core.chinacloudapi.cn/'
        )

    def test_abfs_tuning_defaults_injected(self):
        services = _build_services(
            cloud_env={'azure_storage_backend': 'adls_gen2'},
            storage_site={
                'azure.storage.account.name': 'myaccount',
                'azure.storage.container.name': 'mycontainer',
                'azure.storage.auth.type': 'managed_identity',
            },
        )
        configurations = {}
        self.advisor.getServiceConfigurationRecommendations(configurations, {}, services, {})
        core_site = configurations.get('core-site', {}).get('properties', {})
        self.assertEqual(core_site.get('fs.azure.threads.max'), '16')
        self.assertEqual(core_site.get('fs.azure.enable.autothrottling'), 'true')
        self.assertEqual(core_site.get('fs.azure.readaheadqueue.depth'), '2')

    def test_abfs_tuning_not_injected_for_hdfs(self):
        services = _build_services(
            cloud_env={'azure_storage_backend': 'hdfs'},
            storage_site={'azure.storage.account.name': '', 'azure.storage.container.name': ''},
        )
        configurations = {}
        self.advisor.getServiceConfigurationRecommendations(configurations, {}, services, {})
        core_site = configurations.get('core-site', {}).get('properties', {})
        self.assertNotIn('fs.azure.threads.max', core_site)

    def test_trash_defaults_injected(self):
        services = _build_services(
            cloud_env={'azure_storage_backend': 'adls_gen2'},
            storage_site={
                'azure.storage.account.name': 'myaccount',
                'azure.storage.container.name': 'mycontainer',
                'azure.storage.auth.type': 'managed_identity',
            },
        )
        configurations = {}
        self.advisor.getServiceConfigurationRecommendations(configurations, {}, services, {})
        core_site = configurations.get('core-site', {}).get('properties', {})
        self.assertEqual(core_site.get('fs.trash.interval'), '1440')
        self.assertEqual(core_site.get('fs.trash.checkpoint.interval'), '720')

    def test_trash_checkpoint_exceeds_interval_error(self):
        services = _build_services(
            cloud_env={'azure_storage_backend': 'adls_gen2'},
            storage_site={
                'azure.storage.account.name': 'myaccount',
                'azure.storage.container.name': 'mycontainer',
                'azure.storage.auth.type': 'managed_identity',
                'azure.managed.identity.client.id': 'some-id',
            },
        )
        services['configurations'].append(
            {'core-site': {'properties': {'fs.trash.interval': '100', 'fs.trash.checkpoint.interval': '200'}}}
        )
        items = self.advisor.getServiceConfigurationsValidationItems({}, {}, services, {})
        errors = [i for i in items if i['level'] == 'ERROR' and i['config-name'] == 'fs.trash.checkpoint.interval']
        self.assertEqual(len(errors), 1)

    def test_adls_secure_mode_with_storage_key_error(self):
        services = _build_services(
            cloud_env={'azure_storage_backend': 'adls_gen2'},
            storage_site={
                'azure.storage.account.name': 'myaccount',
                'azure.storage.container.name': 'mycontainer',
                'azure.storage.auth.type': 'storage_key',
                'azure.storage.account.key': 'somekey',
                'azure.adls.secure.mode': 'true',
            },
        )
        items = self.advisor.getServiceConfigurationsValidationItems({}, {}, services, {})
        errors = [i for i in items if i['level'] == 'ERROR' and i['config-name'] == 'azure.adls.secure.mode']
        self.assertEqual(len(errors), 1)
        self.assertIn('abfss://', errors[0]['message'])


class TestCrossServiceValidation(unittest.TestCase):
    """Tests for cross-service validation warnings."""

    def setUp(self):
        self.advisor = AzureHadoopCloudServiceAdvisor()

    def test_hdfs_backend_emits_warning(self):
        services = _build_services_with_installed(
            ['AZURE_HADOOP_CLOUD', 'HDFS'],
            cloud_env={'azure_storage_backend': 'hdfs'},
            storage_site={'azure.storage.account.name': '', 'azure.storage.container.name': ''},
        )
        items = self.advisor.getServiceConfigurationsValidationItems({}, {}, services, {})
        warns = [i for i in items if i['level'] == 'WARN' and i['config-name'] == 'azure_storage_backend']
        self.assertEqual(len(warns), 1)
        self.assertIn('ADLS Gen2 or WASB', warns[0]['message'])

    def test_hdfs_backend_without_hdfs_service_emits_error(self):
        services = _build_services_with_installed(
            ['AZURE_HADOOP_CLOUD'],
            cloud_env={'azure_storage_backend': 'hdfs'},
            storage_site={'azure.storage.account.name': '', 'azure.storage.container.name': ''},
        )
        items = self.advisor.getServiceConfigurationsValidationItems({}, {}, services, {})
        errors = [i for i in items if i['level'] == 'ERROR' and i['config-name'] == 'azure_storage_backend']
        self.assertEqual(len(errors), 1)
        self.assertIn('HDFS service is not installed', errors[0]['message'])

    def test_hdfs_path_in_hive_warns_with_cloud_backend(self):
        services = _build_services_with_installed(
            ['AZURE_HADOOP_CLOUD', 'HIVE'],
            cloud_env={'azure_storage_backend': 'adls_gen2'},
            storage_site={
                'azure.storage.account.name': 'myaccount',
                'azure.storage.container.name': 'mycontainer',
                'azure.storage.auth.type': 'managed_identity',
                'azure.managed.identity.client.id': 'some-id',
            },
            extra_configs={
                'hive-site': {'hive.metastore.warehouse.dir': 'hdfs://namenode:8020/apps/hive/warehouse'},
            },
        )
        items = self.advisor.getServiceConfigurationsValidationItems({}, {}, services, {})
        warns = [i for i in items if i['level'] == 'WARN' and i['config-name'] == 'hive.metastore.warehouse.dir']
        self.assertEqual(len(warns), 1)

    def test_relative_path_in_hive_no_warn(self):
        services = _build_services_with_installed(
            ['AZURE_HADOOP_CLOUD', 'HIVE'],
            cloud_env={'azure_storage_backend': 'adls_gen2'},
            storage_site={
                'azure.storage.account.name': 'myaccount',
                'azure.storage.container.name': 'mycontainer',
                'azure.storage.auth.type': 'managed_identity',
                'azure.managed.identity.client.id': 'some-id',
            },
            extra_configs={
                'hive-site': {'hive.metastore.warehouse.dir': '/apps/hive/warehouse'},
            },
        )
        items = self.advisor.getServiceConfigurationsValidationItems({}, {}, services, {})
        warns = [i for i in items if i['level'] == 'WARN' and i['config-name'] == 'hive.metastore.warehouse.dir']
        self.assertEqual(len(warns), 0)


if __name__ == '__main__':
    unittest.main()
