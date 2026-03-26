#!/usr/bin/env python3
"""
Status parameters for AZURE_HADOOP_CLOUD service.
CLIENT components do not have a running process to monitor.
"""
from resource_management.libraries.script.script import Script

config = Script.get_config()

azure_cloud_user = config['configurations']['azure-cloud-env']['azure_cloud_user']
azure_cloud_pid_dir = config['configurations']['azure-cloud-env']['azure_cloud_pid_dir']
