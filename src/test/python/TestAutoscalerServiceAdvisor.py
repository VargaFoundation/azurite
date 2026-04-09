#!/usr/bin/env python3
"""Unit tests for AzureAutoscalerServiceAdvisor."""
import importlib.util
import os
import sys
import unittest

# Load the service advisor module under a unique name to avoid collisions
# with other service_advisor.py files in sibling service directories.
_SA_PATH = os.path.normpath(os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    '..', '..', 'main', 'resources', 'addon-services',
    'AZURE_AUTOSCALER', '1.0.0', 'service_advisor.py'
))
_spec = importlib.util.spec_from_file_location('azure_autoscaler_service_advisor', _SA_PATH)
_mod = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = _mod
_spec.loader.exec_module(_mod)
AzureAutoscalerServiceAdvisor = _mod.AzureAutoscalerServiceAdvisor


def _build_services(autoscaler_site=None, autoscaler_env=None):
    """Build a services dict in the Ambari format expected by _get_service_configs."""
    configurations = []
    if autoscaler_site is not None:
        configurations.append({'azure-autoscaler-site': {'properties': autoscaler_site}})
    if autoscaler_env is not None:
        configurations.append({'azure-autoscaler-env': {'properties': autoscaler_env}})
    return {'configurations': configurations}


def _valid_autoscaler_services():
    """Return a services dict representing a fully valid autoscaler configuration."""
    return _build_services(
        autoscaler_site={
            'autoscaler.cpu.scale.out.threshold': '80',
            'autoscaler.cpu.scale.in.threshold': '30',
            'autoscaler.memory.scale.out.threshold': '80',
            'autoscaler.memory.scale.in.threshold': '30',
            'autoscaler.evaluation.interval.seconds': '60',
            'autoscaler.cooldown.scale.out.seconds': '300',
            'autoscaler.cooldown.scale.in.seconds': '600',
        },
        autoscaler_env={
            'autoscaler_tls_enabled': 'true',
        },
    )


class TestAutoscalerServiceAdvisorValidation(unittest.TestCase):
    """Tests for getServiceConfigurationsValidationItems."""

    def setUp(self):
        self.advisor = AzureAutoscalerServiceAdvisor()

    # --- CPU thresholds ---

    def test_cpu_threshold_out_must_exceed_in(self):
        services = _build_services(autoscaler_site={
            'autoscaler.cpu.scale.out.threshold': '30',
            'autoscaler.cpu.scale.in.threshold': '80',
            'autoscaler.memory.scale.out.threshold': '80',
            'autoscaler.memory.scale.in.threshold': '30',
            'autoscaler.evaluation.interval.seconds': '60',
            'autoscaler.cooldown.scale.out.seconds': '300',
            'autoscaler.cooldown.scale.in.seconds': '600',
        })
        items = self.advisor.getServiceConfigurationsValidationItems({}, {}, services, {})
        error_names = [i['config-name'] for i in items if i['level'] == 'ERROR']
        self.assertIn('autoscaler.cpu.scale.out.threshold', error_names)

    # --- Memory thresholds ---

    def test_memory_threshold_out_must_exceed_in(self):
        services = _build_services(autoscaler_site={
            'autoscaler.cpu.scale.out.threshold': '80',
            'autoscaler.cpu.scale.in.threshold': '30',
            'autoscaler.memory.scale.out.threshold': '30',
            'autoscaler.memory.scale.in.threshold': '80',
            'autoscaler.evaluation.interval.seconds': '60',
            'autoscaler.cooldown.scale.out.seconds': '300',
            'autoscaler.cooldown.scale.in.seconds': '600',
        })
        items = self.advisor.getServiceConfigurationsValidationItems({}, {}, services, {})
        error_names = [i['config-name'] for i in items if i['level'] == 'ERROR']
        self.assertIn('autoscaler.memory.scale.out.threshold', error_names)

    # --- Cooldown vs evaluation interval ---

    def test_cooldown_less_than_interval(self):
        services = _build_services(autoscaler_site={
            'autoscaler.cpu.scale.out.threshold': '80',
            'autoscaler.cpu.scale.in.threshold': '30',
            'autoscaler.memory.scale.out.threshold': '80',
            'autoscaler.memory.scale.in.threshold': '30',
            'autoscaler.evaluation.interval.seconds': '600',
            'autoscaler.cooldown.scale.out.seconds': '30',
            'autoscaler.cooldown.scale.in.seconds': '30',
        })
        items = self.advisor.getServiceConfigurationsValidationItems({}, {}, services, {})
        warn_names = [i['config-name'] for i in items if i['level'] == 'WARN']
        self.assertIn('autoscaler.cooldown.scale.out.seconds', warn_names)
        self.assertIn('autoscaler.cooldown.scale.in.seconds', warn_names)

    # --- Fully valid config ---

    def test_valid_config_no_errors(self):
        services = _valid_autoscaler_services()
        items = self.advisor.getServiceConfigurationsValidationItems({}, {}, services, {})
        errors = [i for i in items if i['level'] == 'ERROR']
        warnings = [i for i in items if i['level'] == 'WARN']
        self.assertEqual(len(errors), 0)
        self.assertEqual(len(warnings), 0)


if __name__ == '__main__':
    unittest.main()
