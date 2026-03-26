#!/usr/bin/env python3
"""
Credential Manager - Azure Key Vault integration for secret storage and rotation.
Used by AZURE_HADOOP_CLOUD to manage storage credentials securely.
"""
import logging
import time

logger = logging.getLogger('credential_manager')


class CredentialManager:
    """Manages credentials via Azure Key Vault."""

    def __init__(self, keyvault_url, credential=None):
        """
        Args:
            keyvault_url: Azure Key Vault URL (e.g., https://myvault.vault.azure.net/)
            credential: Azure credential object (ManagedIdentityCredential or ClientSecretCredential)
        """
        self.keyvault_url = keyvault_url.rstrip('/')
        self._client = None
        self._credential = credential
        self._init_client()

    def _init_client(self):
        """Initialize Key Vault SecretClient."""
        try:
            from azure.keyvault.secrets import SecretClient
            if not self._credential:
                from azure.identity import ManagedIdentityCredential
                self._credential = ManagedIdentityCredential()
            self._client = SecretClient(vault_url=self.keyvault_url, credential=self._credential)
            logger.info('Key Vault client initialized for %s', self.keyvault_url)
        except ImportError:
            logger.warning('azure-keyvault-secrets not installed. Key Vault integration disabled.')
        except Exception as e:
            logger.error('Failed to initialize Key Vault client: %s', e)

    def get_secret(self, name):
        """Retrieve a secret value from Key Vault."""
        if not self._client:
            return None
        try:
            secret = self._client.get_secret(name)
            return secret.value
        except Exception as e:
            logger.error('Failed to get secret %s: %s', name, e)
            return None

    def set_secret(self, name, value, content_type='', expires_on=None):
        """Store or update a secret in Key Vault."""
        if not self._client:
            return False
        try:
            self._client.set_secret(name, value, content_type=content_type, expires_on=expires_on)
            logger.info('Secret %s stored/updated in Key Vault', name)
            return True
        except Exception as e:
            logger.error('Failed to set secret %s: %s', name, e)
            return False

    def check_expiry(self, name):
        """
        Check expiration of a secret.
        Returns (days_remaining, expiry_date_str) or (None, None) if no expiry set.
        """
        if not self._client:
            return (None, None)
        try:
            secret = self._client.get_secret(name)
            if secret.properties.expires_on:
                from datetime import datetime, timezone
                now = datetime.now(timezone.utc)
                expires = secret.properties.expires_on
                days = (expires - now).days
                return (days, expires.isoformat())
            return (None, None)
        except Exception as e:
            logger.error('Failed to check expiry for %s: %s', name, e)
            return (None, None)

    def rotate_storage_key(self, subscription_id, resource_group, account_name, credential=None):
        """
        Rotate a storage account key:
        1. Regenerate key2
        2. Store new key2 in Key Vault
        3. Return the new key for config update
        """
        try:
            from azure.mgmt.storage import StorageManagementClient
            cred = credential or self._credential
            storage_client = StorageManagementClient(cred, subscription_id)

            # Regenerate key2
            result = storage_client.storage_accounts.regenerate_key(
                resource_group, account_name, {'key_name': 'key2'})
            new_key = None
            for key in result.keys:
                if key.key_name == 'key2':
                    new_key = key.value
                    break

            if new_key:
                # Store in Key Vault
                secret_name = '{0}-storage-key'.format(account_name)
                self.set_secret(secret_name, new_key, content_type='storage-account-key')
                logger.info('Storage key rotated for account %s, stored as %s', account_name, secret_name)
                return new_key

            logger.error('Key2 not found after regeneration for account %s', account_name)
            return None
        except ImportError:
            logger.error('azure-mgmt-storage not installed. Cannot rotate storage key.')
            return None
        except Exception as e:
            logger.error('Failed to rotate storage key for %s: %s', account_name, e)
            return None

    def is_available(self):
        """Check if Key Vault client is initialized and reachable."""
        if not self._client:
            return False
        try:
            # List one secret to test connectivity
            next(self._client.list_properties_of_secrets(max_page_size=1), None)
            return True
        except Exception:
            return False
