#!/usr/bin/env python3
"""Linux-specific parameter extraction for AZURE_AUTOSCALER service."""
import json
from resource_management.libraries.script.script import Script
from resource_management.libraries.functions.default import default

config = Script.get_config()

# ---- Environment ----
autoscaler_user = config['configurations'].get('azure-autoscaler-env', {}).get('autoscaler_user', 'azurehdp')
autoscaler_group = config['configurations'].get('azure-autoscaler-env', {}).get('autoscaler_group', 'azurehdp')
autoscaler_log_dir = config['configurations'].get('azure-autoscaler-env', {}).get('autoscaler_log_dir', '/var/log/azure-autoscaler')
autoscaler_pid_dir = config['configurations'].get('azure-autoscaler-env', {}).get('autoscaler_pid_dir', '/var/run/azure-autoscaler')
autoscaler_port = int(config['configurations'].get('azure-autoscaler-env', {}).get('autoscaler_port', '8471'))
autoscaler_pid_file = '{0}/azure-autoscaler.pid'.format(autoscaler_pid_dir)

# ---- Scaling config ----
site = config['configurations'].get('azure-autoscaler-site', {})
autoscaler_enabled = site.get('autoscaler.enabled', 'true') == 'true'
autoscaler_mode = site.get('autoscaler.mode', 'load_based')
evaluation_interval = int(site.get('autoscaler.evaluation.interval.seconds', '60'))
scale_out_trigger_duration = int(site.get('autoscaler.scale.out.trigger.duration.seconds', '300'))
scale_in_trigger_duration = int(site.get('autoscaler.scale.in.trigger.duration.seconds', '300'))
cooldown_scale_out = int(site.get('autoscaler.cooldown.scale.out.seconds', '300'))
cooldown_scale_in = int(site.get('autoscaler.cooldown.scale.in.seconds', '600'))
scale_out_increment = int(site.get('autoscaler.scale.out.increment', '1'))
scale_in_decrement = int(site.get('autoscaler.scale.in.decrement', '1'))
cpu_scale_out_threshold = int(site.get('autoscaler.cpu.scale.out.threshold', '80'))
cpu_scale_in_threshold = int(site.get('autoscaler.cpu.scale.in.threshold', '30'))
memory_scale_out_threshold = int(site.get('autoscaler.memory.scale.out.threshold', '80'))
memory_scale_in_threshold = int(site.get('autoscaler.memory.scale.in.threshold', '30'))
yarn_pending_containers_threshold = int(site.get('autoscaler.yarn.pending.containers.scale.out.threshold', '10'))
yarn_available_memory_scale_in_pct = int(site.get('autoscaler.yarn.available.memory.scale.in.threshold.pct', '60'))
graceful_decommission_timeout = int(site.get('autoscaler.graceful.decommission.timeout.seconds', '3600'))
scale_only_task_nodes = site.get('autoscaler.scale.only.task.nodes', 'true') == 'true'
yarn_rm_url = site.get('autoscaler.yarn.resourcemanager.url', '')
vm_manager_url = site.get('autoscaler.vm.manager.url', 'http://localhost:8470')
metrics_source = site.get('autoscaler.metrics.source', 'yarn_and_system')

# ---- Schedule ----
schedule_site = config['configurations'].get('azure-autoscaler-schedule-site', {})
schedule_timezone = schedule_site.get('schedule.timezone', 'UTC')
try:
    schedule_rules = json.loads(schedule_site.get('schedule.rules', '[]'))
except (json.JSONDecodeError, TypeError):
    schedule_rules = []

# ---- Worker pool bounds (from VM Manager) ----
pool = config['configurations'].get('azure-vm-pool-site', {})
worker_min_count = int(pool.get('azure.vm.pool.worker.min.count', '1'))
worker_max_count = int(pool.get('azure.vm.pool.worker.max.count', '20'))

# ---- Computed ----
hostname = config.get('agentLevelParams', {}).get('hostname', 'localhost')

# Auto-detect YARN RM URL if not configured
if not yarn_rm_url:
    yarn_rm_host = default('/clusterHostInfo/resourcemanager_hosts', ['localhost'])[0]
    yarn_rm_port = default('/configurations/yarn-site/yarn.resourcemanager.webapp.address',
                           '{0}:8088'.format(yarn_rm_host)).split(':')[-1]
    yarn_rm_url = 'http://{0}:{1}'.format(yarn_rm_host, yarn_rm_port)
