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
        self._create_cloud_storage_dirs(env)

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

    def _create_cloud_storage_dirs(self, env):
        """Create standard Hadoop directory tree on cloud storage (idempotent)."""
        import params
        if params.azure_storage_backend not in ('adls_gen2', 'wasb'):
            return

        # Check if directories already exist (idempotent guard)
        try:
            Execute('hdfs dfs -test -d /apps',
                    user=params.azure_cloud_user,
                    logoutput=False)
            return
        except:
            pass

        cloud_dirs = [
            '/tmp',
            '/user',
            '/apps',
            '/apps/hive/warehouse',
            '/apps/spark/warehouse',
            '/app-logs',
            '/mr-history/tmp',
            '/mr-history/done',
            '/spark2-history',
            '/tmp/hive',
            '/tmp/tez-staging',
        ]
        for d in cloud_dirs:
            Execute(format('hdfs dfs -mkdir -p {d}'),
                    user=params.azure_cloud_user,
                    logoutput=True,
                    ignore_failures=True)

        # Set appropriate permissions
        perms = [
            ('/tmp', '1777'),
            ('/user', '755'),
            ('/apps/hive/warehouse', '733'),
            ('/app-logs', '1777'),
            ('/mr-history', '755'),
            ('/spark2-history', '1777'),
            ('/tmp/hive', '733'),
        ]
        for path, mode in perms:
            Execute(format('hdfs dfs -chmod {mode} {path}'),
                    user=params.azure_cloud_user,
                    logoutput=True,
                    ignore_failures=True)


if __name__ == '__main__':
    AzureHadoopCloudClient().execute()
