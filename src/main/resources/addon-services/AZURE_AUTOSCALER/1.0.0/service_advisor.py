#!/usr/bin/env python3
"""Service advisor for AZURE_AUTOSCALER. Validates thresholds, cooldowns, and dependencies."""
import os, sys, traceback

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
        class ServiceAdvisor(object):
            def getServiceComponentLayoutValidations(self, services, hosts): return []
            def getServiceConfigurationRecommendations(self, configurations, clusterData, services, hosts): pass
            def getServiceConfigurationsValidationItems(self, configurations, recommendedDefaults, services, hosts): return []
        service_advisor = type(sys)('service_advisor')
        service_advisor.ServiceAdvisor = ServiceAdvisor
except Exception:
    traceback.print_exc()
    class ServiceAdvisor(object):
        def getServiceComponentLayoutValidations(self, services, hosts): return []
        def getServiceConfigurationRecommendations(self, configurations, clusterData, services, hosts): pass
        def getServiceConfigurationsValidationItems(self, configurations, recommendedDefaults, services, hosts): return []
    service_advisor = type(sys)('service_advisor')
    service_advisor.ServiceAdvisor = ServiceAdvisor


class AzureAutoscalerServiceAdvisor(service_advisor.ServiceAdvisor):

    def getServiceConfigurationsValidationItems(self, configurations, recommendedDefaults, services, hosts):
        items = []
        props = self._get_service_configs(services)
        if not props:
            return items

        site = props.get('azure-autoscaler-site', {})

        # Scale-out threshold must be > scale-in threshold
        cpu_out = int(site.get('autoscaler.cpu.scale.out.threshold', '80'))
        cpu_in = int(site.get('autoscaler.cpu.scale.in.threshold', '30'))
        if cpu_out <= cpu_in:
            items.append({
                'type': 'configuration', 'level': 'ERROR',
                'message': 'CPU scale-out threshold ({0}%) must be greater than scale-in threshold ({1}%).'.format(
                    cpu_out, cpu_in),
                'config-type': 'azure-autoscaler-site',
                'config-name': 'autoscaler.cpu.scale.out.threshold'
            })

        mem_out = int(site.get('autoscaler.memory.scale.out.threshold', '80'))
        mem_in = int(site.get('autoscaler.memory.scale.in.threshold', '30'))
        if mem_out <= mem_in:
            items.append({
                'type': 'configuration', 'level': 'ERROR',
                'message': 'Memory scale-out threshold ({0}%) must be greater than scale-in threshold ({1}%).'.format(
                    mem_out, mem_in),
                'config-type': 'azure-autoscaler-site',
                'config-name': 'autoscaler.memory.scale.out.threshold'
            })

        # Cooldown must be >= evaluation interval
        interval = int(site.get('autoscaler.evaluation.interval.seconds', '60'))
        cooldown_out = int(site.get('autoscaler.cooldown.scale.out.seconds', '300'))
        cooldown_in = int(site.get('autoscaler.cooldown.scale.in.seconds', '600'))

        if cooldown_out < interval:
            items.append({
                'type': 'configuration', 'level': 'WARN',
                'message': 'Scale-out cooldown ({0}s) should be >= evaluation interval ({1}s).'.format(
                    cooldown_out, interval),
                'config-type': 'azure-autoscaler-site',
                'config-name': 'autoscaler.cooldown.scale.out.seconds'
            })

        if cooldown_in < interval:
            items.append({
                'type': 'configuration', 'level': 'WARN',
                'message': 'Scale-in cooldown ({0}s) should be >= evaluation interval ({1}s).'.format(
                    cooldown_in, interval),
                'config-type': 'azure-autoscaler-site',
                'config-name': 'autoscaler.cooldown.scale.in.seconds'
            })

        # Worker count bounds
        schedule_site = props.get('azure-autoscaler-schedule-site', {})
        rules_str = schedule_site.get('schedule.rules', '[]')
        try:
            import json
            rules = json.loads(rules_str)
            for i, rule in enumerate(rules):
                cron = rule.get('cron', '')
                if cron and len(cron.split()) != 5:
                    items.append({
                        'type': 'configuration', 'level': 'ERROR',
                        'message': 'Schedule rule #{0}: cron expression must have 5 fields '
                                   '(minute hour dom month dow), got: "{1}"'.format(i + 1, cron),
                        'config-type': 'azure-autoscaler-schedule-site',
                        'config-name': 'schedule.rules'
                    })
                if not rule.get('target_count'):
                    items.append({
                        'type': 'configuration', 'level': 'ERROR',
                        'message': 'Schedule rule #{0}: target_count is required.'.format(i + 1),
                        'config-type': 'azure-autoscaler-schedule-site',
                        'config-name': 'schedule.rules'
                    })
        except (ValueError, TypeError):
            items.append({
                'type': 'configuration', 'level': 'ERROR',
                'message': 'schedule.rules must be valid JSON array.',
                'config-type': 'azure-autoscaler-schedule-site',
                'config-name': 'schedule.rules'
            })

        # TLS warning
        env = props.get('azure-autoscaler-env', {})
        tls_enabled = env.get('autoscaler_tls_enabled', 'false')
        if tls_enabled != 'true':
            items.append({
                'type': 'configuration', 'level': 'WARN',
                'message': 'TLS is disabled for the Autoscaler REST API. '
                           'Strongly recommended for production deployments.',
                'config-type': 'azure-autoscaler-env',
                'config-name': 'autoscaler_tls_enabled'
            })

        return items

    def getServiceComponentLayoutValidations(self, services, hosts):
        return []

    def getServiceConfigurationRecommendations(self, configurations, clusterData, services, hosts):
        pass

    def _get_service_configs(self, services):
        if not services or 'configurations' not in services:
            return {}
        configs = {}
        for config_type in services['configurations']:
            for key, value in config_type.items():
                if isinstance(value, dict) and 'properties' in value:
                    configs[key] = value['properties']
        return configs
