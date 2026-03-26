#!/usr/bin/env python3
"""
Status parameters for AZURE_VM_MANAGER service.
"""
from resource_management.libraries.script.script import Script

config = Script.get_config()

vm_manager_user = config['configurations']['azure-vm-manager-env']['vm_manager_user']
vm_manager_pid_dir = config['configurations']['azure-vm-manager-env']['vm_manager_pid_dir']
vm_manager_pid_file = '{0}/azure-vm-manager.pid'.format(vm_manager_pid_dir)
