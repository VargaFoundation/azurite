#!/usr/bin/env python3
"""
AZURE_HADOOP_CLOUD_CLIENT component handler.
Installs Azure Hadoop dependencies and configures storage backend properties.
"""
from resource_management.core.exceptions import ClientComponentHasNoStatus
from resource_management.core.resources.system import Directory, Execute
from resource_management.libraries.script.script import Script
from resource_management.core.resources.system import File
from resource_management.libraries.functions.format import format
import os


class AzureHadoopCloudClient(Script):

    def install(self, env):
        import params
        env.set_params(params)
        self.install_packages(env)

        # Create service directories
        Directory(params.azure_cloud_log_dir,
                  owner=params.azure_cloud_user,
                  group=params.azure_cloud_group,
                  create_parents=True,
                  mode=0o755)

        Directory(params.azure_cloud_pid_dir,
                  owner=params.azure_cloud_user,
                  group=params.azure_cloud_group,
                  create_parents=True,
                  mode=0o755)

        self.configure(env)

    def configure(self, env):
        import params
        env.set_params(params)
        # Configuration is managed by service_advisor.py via Ambari's config push mechanism.
        # No manual core-site.xml injection needed here.

    def status(self, env):
        raise ClientComponentHasNoStatus()

    def start(self, env):
        self.configure(env)

    def stop(self, env):
        pass


if __name__ == '__main__':
    AzureHadoopCloudClient().execute()
