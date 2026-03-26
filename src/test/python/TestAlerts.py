#!/usr/bin/env python3
"""
Unit tests for alert scripts in AZURE_HADOOP_CLOUD.
All filesystem and subprocess calls are mocked so no real cluster is needed.
"""
import sys
import os
import subprocess
import unittest
from datetime import datetime, timezone, timedelta

# Python 2/3 compatible mock import
try:
    from unittest.mock import patch, MagicMock
except ImportError:
    from mock import patch, MagicMock

CLOUD_ALERTS = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            '..', '..', '..', 'src', 'main', 'resources',
                            'addon-services', 'AZURE_HADOOP_CLOUD', '1.0.0',
                            'package', 'scripts', 'alerts')
sys.path.insert(0, CLOUD_ALERTS)

import check_azure_jars
import check_credential_expiry
import check_azure_storage


# ====================================================================== #
# check_azure_jars
# ====================================================================== #
class TestCheckAzureJars(unittest.TestCase):
    """Tests for the check_azure_jars alert."""

    def test_hdfs_backend_skips_check(self):
        """When backend is HDFS the alert should return OK without inspecting JARs."""
        configs = {'{{azure-cloud-env/azure_storage_backend}}': 'hdfs'}
        state, messages = check_azure_jars.execute(configurations=configs)
        self.assertEqual(state, 'OK')
        self.assertIn('HDFS', messages[0])

    @patch('check_azure_jars.glob.glob')
    def test_jars_found(self, mock_glob):
        """When all required JARs are found the alert should return OK."""
        # Every glob call finds a matching JAR file
        mock_glob.return_value = ['/usr/hdp/current/hadoop-client/lib/hadoop-azure-3.3.1.jar']
        configs = {'{{azure-cloud-env/azure_storage_backend}}': 'adls_gen2'}
        state, messages = check_azure_jars.execute(configurations=configs)
        self.assertEqual(state, 'OK')
        self.assertIn('found', messages[0].lower())

    @patch('check_azure_jars.glob.glob')
    def test_jars_missing(self, mock_glob):
        """When no JARs are found the alert should return CRITICAL."""
        mock_glob.return_value = []
        configs = {'{{azure-cloud-env/azure_storage_backend}}': 'adls_gen2'}
        state, messages = check_azure_jars.execute(configurations=configs)
        self.assertEqual(state, 'CRITICAL')
        self.assertIn('Missing', messages[0])


# ====================================================================== #
# check_credential_expiry
# ====================================================================== #
class TestCheckCredentialExpiry(unittest.TestCase):
    """Tests for the check_credential_expiry alert."""

    def test_managed_identity_always_ok(self):
        """Managed Identity auth should always return OK (tokens auto-renewed)."""
        configs = {'{{azure-storage-site/azure.storage.auth.type}}': 'managed_identity'}
        state, messages = check_credential_expiry.execute(configurations=configs)
        self.assertEqual(state, 'OK')
        self.assertIn('auto-renewed', messages[0].lower())

    def test_storage_key_always_ok(self):
        """Storage key auth should return OK (keys do not expire)."""
        configs = {'{{azure-storage-site/azure.storage.auth.type}}': 'storage_key'}
        state, messages = check_credential_expiry.execute(configurations=configs)
        self.assertEqual(state, 'OK')
        self.assertIn('do not expire', messages[0].lower())

    def test_sas_token_valid(self):
        """SAS token with expiry 30 days from now should return OK."""
        future = datetime.now(timezone.utc) + timedelta(days=30)
        expiry_str = future.strftime('%Y-%m-%dT%H:%M:%SZ')
        sas = '?sv=2021-06-08&se={0}&sr=c&sp=rl&sig=abc'.format(expiry_str)
        configs = {
            '{{azure-storage-site/azure.storage.auth.type}}': 'sas_token',
            '{{azure-storage-site/azure.storage.sas.token}}': sas,
        }
        state, messages = check_credential_expiry.execute(configurations=configs)
        self.assertEqual(state, 'OK')
        self.assertIn('valid', messages[0].lower())

    def test_sas_token_expiring_soon(self):
        """SAS token with expiry 3 days from now should return WARNING."""
        future = datetime.now(timezone.utc) + timedelta(days=3)
        expiry_str = future.strftime('%Y-%m-%dT%H:%M:%SZ')
        sas = '?sv=2021-06-08&se={0}&sr=c&sp=rl&sig=abc'.format(expiry_str)
        configs = {
            '{{azure-storage-site/azure.storage.auth.type}}': 'sas_token',
            '{{azure-storage-site/azure.storage.sas.token}}': sas,
        }
        state, messages = check_credential_expiry.execute(configurations=configs)
        self.assertEqual(state, 'WARNING')
        self.assertIn('expires in', messages[0].lower())

    def test_sas_token_expired(self):
        """SAS token with expiry yesterday should return CRITICAL."""
        past = datetime.now(timezone.utc) - timedelta(days=1)
        expiry_str = past.strftime('%Y-%m-%dT%H:%M:%SZ')
        sas = '?sv=2021-06-08&se={0}&sr=c&sp=rl&sig=abc'.format(expiry_str)
        configs = {
            '{{azure-storage-site/azure.storage.auth.type}}': 'sas_token',
            '{{azure-storage-site/azure.storage.sas.token}}': sas,
        }
        state, messages = check_credential_expiry.execute(configurations=configs)
        self.assertEqual(state, 'CRITICAL')

    def test_sas_token_empty(self):
        """Empty SAS token should return WARNING."""
        configs = {
            '{{azure-storage-site/azure.storage.auth.type}}': 'sas_token',
            '{{azure-storage-site/azure.storage.sas.token}}': '',
        }
        state, messages = check_credential_expiry.execute(configurations=configs)
        self.assertEqual(state, 'WARNING')
        self.assertIn('empty', messages[0].lower())


# ====================================================================== #
# check_azure_storage
# ====================================================================== #
class TestCheckAzureStorage(unittest.TestCase):
    """Tests for the check_azure_storage alert."""

    @patch('check_azure_storage.subprocess.run')
    def test_hdfs_backend_success(self, mock_run):
        """HDFS backend with successful subprocess should return OK."""
        mock_run.return_value = MagicMock(returncode=0, stderr='')
        configs = {
            '{{azure-cloud-env/azure_storage_backend}}': 'hdfs',
            '{{azure-storage-site/azure.storage.account.name}}': '',
            '{{azure-storage-site/azure.storage.container.name}}': '',
        }
        state, messages = check_azure_storage.execute(configurations=configs)
        self.assertEqual(state, 'OK')
        # Verify the command used plain '/' for HDFS
        cmd_args = mock_run.call_args[0][0]
        self.assertEqual(cmd_args[-1], '/')

    @patch('check_azure_storage.subprocess.run')
    def test_adls_backend_failure(self, mock_run):
        """ADLS Gen2 backend with non-zero exit code should return CRITICAL."""
        mock_run.return_value = MagicMock(returncode=1, stderr='Connection refused')
        configs = {
            '{{azure-cloud-env/azure_storage_backend}}': 'adls_gen2',
            '{{azure-storage-site/azure.storage.account.name}}': 'mystorageacct',
            '{{azure-storage-site/azure.storage.container.name}}': 'mycontainer',
        }
        state, messages = check_azure_storage.execute(configurations=configs)
        self.assertEqual(state, 'CRITICAL')
        self.assertIn('NOT reachable', messages[0])

    @patch('check_azure_storage.subprocess.run')
    def test_timeout(self, mock_run):
        """subprocess.TimeoutExpired should yield CRITICAL with timeout message."""
        mock_run.side_effect = subprocess.TimeoutExpired(cmd='hdfs dfs -ls', timeout=30)
        configs = {
            '{{azure-cloud-env/azure_storage_backend}}': 'adls_gen2',
            '{{azure-storage-site/azure.storage.account.name}}': 'mystorageacct',
            '{{azure-storage-site/azure.storage.container.name}}': 'mycontainer',
        }
        state, messages = check_azure_storage.execute(configurations=configs)
        self.assertEqual(state, 'CRITICAL')
        self.assertIn('timed out', messages[0].lower())

    def test_unconfigured(self):
        """Empty account name with a cloud backend should return WARNING."""
        configs = {
            '{{azure-cloud-env/azure_storage_backend}}': 'adls_gen2',
            '{{azure-storage-site/azure.storage.account.name}}': '',
            '{{azure-storage-site/azure.storage.container.name}}': '',
        }
        state, messages = check_azure_storage.execute(configurations=configs)
        self.assertEqual(state, 'WARNING')
        self.assertIn('not fully configured', messages[0].lower())


if __name__ == '__main__':
    unittest.main()
