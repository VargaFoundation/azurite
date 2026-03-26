#!/usr/bin/env python3
"""
Alert script: checks if Azure credentials are approaching expiration.
For SAS tokens, parses the 'se' (signed expiry) parameter.
For other auth types, this check is informational only.
"""
import traceback
from datetime import datetime, timezone
try:
    from urllib.parse import parse_qs, urlparse
except ImportError:
    from urlparse import parse_qs, urlparse

RESULT_STATE_OK = 'OK'
RESULT_STATE_WARNING = 'WARNING'
RESULT_STATE_CRITICAL = 'CRITICAL'
RESULT_STATE_UNKNOWN = 'UNKNOWN'

WARNING_DAYS = 7
CRITICAL_DAYS = 1


def get_tokens():
    return ()


def execute(configurations={}, parameters={}, host_name=None):
    try:
        auth_type = configurations.get('{{azure-storage-site/azure.storage.auth.type}}', 'managed_identity')

        if auth_type == 'sas_token':
            sas_token = configurations.get('{{azure-storage-site/azure.storage.sas.token}}', '')
            if not sas_token:
                return (RESULT_STATE_WARNING, ['SAS token is empty.'])

            # Parse SAS token for expiry (se parameter)
            params = parse_qs(sas_token.lstrip('?'))
            expiry_str = params.get('se', [None])[0]
            if not expiry_str:
                return (RESULT_STATE_WARNING, ['Could not parse SAS token expiry (se parameter).'])

            expiry = datetime.fromisoformat(expiry_str.replace('Z', '+00:00'))
            now = datetime.now(timezone.utc)
            days_remaining = (expiry - now).days

            if days_remaining < CRITICAL_DAYS:
                return (RESULT_STATE_CRITICAL,
                        ['SAS token expires in {0} days (at {1}).'.format(days_remaining, expiry_str)])
            elif days_remaining < WARNING_DAYS:
                return (RESULT_STATE_WARNING,
                        ['SAS token expires in {0} days (at {1}).'.format(days_remaining, expiry_str)])
            else:
                return (RESULT_STATE_OK,
                        ['SAS token valid for {0} more days.'.format(days_remaining)])

        elif auth_type == 'managed_identity':
            return (RESULT_STATE_OK, ['Managed Identity tokens are auto-renewed by Azure.'])

        elif auth_type == 'storage_key':
            return (RESULT_STATE_OK, ['Storage account keys do not expire (rotate manually).'])

        elif auth_type == 'oauth2_client_credential':
            return (RESULT_STATE_OK,
                    ['OAuth2 client credential tokens are auto-renewed. Check client secret expiry in Azure AD.'])

        return (RESULT_STATE_OK, ['Auth type: {0}'.format(auth_type)])

    except Exception:
        return (RESULT_STATE_UNKNOWN, [traceback.format_exc()])
