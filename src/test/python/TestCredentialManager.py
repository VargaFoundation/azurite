#!/usr/bin/env python3
"""
Unit tests for the CredentialManager class.
All Azure Key Vault calls are mocked so no real vault is needed.
"""
import sys
import os
import unittest
from datetime import datetime, timezone, timedelta

# Python 2/3 compatible mock import
try:
    from unittest.mock import MagicMock, patch
except ImportError:
    from mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                '..', '..', '..', 'src', 'main', 'resources',
                                'addon-services', 'AZURE_HADOOP_CLOUD', '1.0.0', 'package', 'files'))

# We need to patch the azure imports before importing CredentialManager,
# since _init_client runs during __init__.
_CM_MODULE = 'credential_manager'


def _create_manager_with_mock_client():
    """Create a CredentialManager with a pre-injected mock client (skip _init_client)."""
    with patch(_CM_MODULE + '.CredentialManager._init_client'):
        mgr = __import__(_CM_MODULE).CredentialManager('https://myvault.vault.azure.net')
    mgr._client = MagicMock()
    return mgr


class TestCredentialManager(unittest.TestCase):
    """Tests for CredentialManager."""

    # ------------------------------------------------------------------ #
    # get_secret
    # ------------------------------------------------------------------ #
    def test_get_secret_success(self):
        """Mock client returns a value -> get_secret should return that value."""
        mgr = _create_manager_with_mock_client()
        mock_secret = MagicMock()
        mock_secret.value = 'super-secret-value'
        mgr._client.get_secret.return_value = mock_secret

        result = mgr.get_secret('my-secret')
        self.assertEqual(result, 'super-secret-value')
        mgr._client.get_secret.assert_called_once_with('my-secret')

    def test_get_secret_not_found(self):
        """When client raises an exception, get_secret should return None."""
        mgr = _create_manager_with_mock_client()
        mgr._client.get_secret.side_effect = Exception('SecretNotFound')

        result = mgr.get_secret('missing-secret')
        self.assertIsNone(result)

    # ------------------------------------------------------------------ #
    # set_secret
    # ------------------------------------------------------------------ #
    def test_set_secret_success(self):
        """Successful set_secret should return True."""
        mgr = _create_manager_with_mock_client()

        result = mgr.set_secret('my-secret', 'new-value')
        self.assertTrue(result)
        mgr._client.set_secret.assert_called_once_with(
            'my-secret', 'new-value', content_type='', expires_on=None)

    # ------------------------------------------------------------------ #
    # check_expiry
    # ------------------------------------------------------------------ #
    def test_check_expiry_with_date(self):
        """Secret with expires_on 30 days from now should return (~30, iso_date)."""
        mgr = _create_manager_with_mock_client()
        expires = datetime.now(timezone.utc) + timedelta(days=30)
        mock_secret = MagicMock()
        mock_secret.properties.expires_on = expires
        mgr._client.get_secret.return_value = mock_secret

        days, date_str = mgr.check_expiry('my-secret')
        # Allow for sub-second drift between timedelta construction and now() inside check_expiry
        self.assertIn(days, (29, 30))
        self.assertEqual(date_str, expires.isoformat())

    def test_check_expiry_no_date(self):
        """Secret with no expires_on should return (None, None)."""
        mgr = _create_manager_with_mock_client()
        mock_secret = MagicMock()
        mock_secret.properties.expires_on = None
        mgr._client.get_secret.return_value = mock_secret

        days, date_str = mgr.check_expiry('my-secret')
        self.assertIsNone(days)
        self.assertIsNone(date_str)

    # ------------------------------------------------------------------ #
    # is_available
    # ------------------------------------------------------------------ #
    def test_is_available_true(self):
        """When list_properties_of_secrets works, is_available should return True."""
        mgr = _create_manager_with_mock_client()
        # next() on the iterator should return without error
        mgr._client.list_properties_of_secrets.return_value = iter([MagicMock()])

        self.assertTrue(mgr.is_available())

    def test_is_available_no_client(self):
        """When _client is None, is_available should return False."""
        with patch(_CM_MODULE + '.CredentialManager._init_client'):
            from credential_manager import CredentialManager
            mgr = CredentialManager('https://myvault.vault.azure.net')
        mgr._client = None

        self.assertFalse(mgr.is_available())

    # ------------------------------------------------------------------ #
    # No Key Vault SDK
    # ------------------------------------------------------------------ #
    def test_no_keyvault_sdk(self):
        """When azure.keyvault.secrets is unavailable, methods should degrade gracefully."""
        # Simulate ImportError during _init_client by letting it actually run
        # but with the import failing
        with patch('builtins.__import__', side_effect=_selective_import_error):
            from credential_manager import CredentialManager
            mgr = CredentialManager('https://myvault.vault.azure.net',
                                    credential=MagicMock())

        # _client should be None because the import of azure.keyvault.secrets failed
        self.assertIsNone(mgr._client)

        # All methods should return safe defaults
        self.assertIsNone(mgr.get_secret('anything'))
        self.assertFalse(mgr.set_secret('anything', 'value'))
        days, date_str = mgr.check_expiry('anything')
        self.assertIsNone(days)
        self.assertIsNone(date_str)
        self.assertFalse(mgr.is_available())


def _selective_import_error(name, *args, **kwargs):
    """Raise ImportError only for azure.keyvault.secrets; allow everything else."""
    if name == 'azure.keyvault.secrets':
        raise ImportError('No module named azure.keyvault.secrets')
    return original_import(name, *args, **kwargs)


import builtins
original_import = builtins.__import__


if __name__ == '__main__':
    unittest.main()
