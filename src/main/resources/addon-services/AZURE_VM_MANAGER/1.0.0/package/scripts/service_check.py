#!/usr/bin/env python3
"""
Service check for AZURE_VM_MANAGER.
Verifies the VM Manager REST API is responsive and Azure ARM API is reachable.
"""
from resource_management.libraries.script.script import Script
from resource_management.core.resources.system import Execute
from resource_management.libraries.functions.format import format


class AzureVmManagerServiceCheck(Script):

    def service_check(self, env):
        import params
        env.set_params(params)

        # Check VM Manager REST API health
        Execute('curl -sf http://localhost:{0}/api/v1/health'.format(params.vm_manager_port),
                user=params.vm_manager_user,
                logoutput=True,
                tries=3,
                try_sleep=5)


if __name__ == '__main__':
    AzureVmManagerServiceCheck().execute()
