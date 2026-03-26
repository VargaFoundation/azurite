#!/usr/bin/env python3
"""Status parameters for AZURE_AUTOSCALER service."""
from resource_management.libraries.script.script import Script

config = Script.get_config()

autoscaler_user = config['configurations']['azure-autoscaler-env']['autoscaler_user']
autoscaler_pid_dir = config['configurations']['azure-autoscaler-env']['autoscaler_pid_dir']
autoscaler_pid_file = '{0}/azure-autoscaler.pid'.format(autoscaler_pid_dir)
