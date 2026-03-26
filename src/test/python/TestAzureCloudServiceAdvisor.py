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
        services = _build_services(
            cloud_env={'azure_storage_backend': 'hdfs'},
            storage_site={
                'azure.storage.account.name': '',
                'azure.storage.container.name': '',
            },
        )
        items = self.advisor.getServiceConfigurationsValidationItems({}, {}, services, {})
        self.assertEqual(len(items), 0)

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


if __name__ == '__main__':
    unittest.main()
