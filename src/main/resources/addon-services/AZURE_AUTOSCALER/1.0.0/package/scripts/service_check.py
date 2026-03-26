#!/usr/bin/env python3
"""Service check for AZURE_AUTOSCALER."""
from resource_management.libraries.script.script import Script
from resource_management.core.resources.system import Execute


class AzureAutoscalerServiceCheck(Script):

    def service_check(self, env):
        import params
        env.set_params(params)
        Execute('curl -sf http://localhost:{0}/api/v1/health'.format(params.autoscaler_port),
                user=params.autoscaler_user, logoutput=True, tries=3, try_sleep=5)


if __name__ == '__main__':
    AzureAutoscalerServiceCheck().execute()
