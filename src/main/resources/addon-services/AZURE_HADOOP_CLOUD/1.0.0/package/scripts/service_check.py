#!/usr/bin/env python3
"""
Service check for AZURE_HADOOP_CLOUD.
Validates Azure storage connectivity based on the configured backend.
"""
from resource_management.libraries.script.script import Script
from resource_management.core.resources.system import Execute
from resource_management.libraries.functions.format import format


class AzureHadoopCloudServiceCheck(Script):

    def service_check(self, env):
        import params
        env.set_params(params)

        if params.azure_storage_backend == 'adls_gen2':
            fqdn = '{0}.dfs.{1}'.format(params.storage_account_name, params.storage_endpoint_suffix)
            adls_scheme = 'abfss' if params.adls_secure_mode == 'true' else 'abfs'
            test_uri = '{0}://{1}@{2}/'.format(adls_scheme, params.storage_container_name, fqdn)
            Execute(format('hdfs dfs -ls {test_uri}'),
                    user=params.azure_cloud_user,
                    logoutput=False,
                    tries=3,
                    try_sleep=10)

        elif params.azure_storage_backend == 'wasb':
            fqdn = '{0}.blob.{1}'.format(params.storage_account_name, params.storage_endpoint_suffix)
            scheme = 'wasbs' if params.wasb_secure_mode == 'true' else 'wasb'
            test_uri = '{0}://{1}@{2}/'.format(scheme, params.storage_container_name, fqdn)
            Execute(format('hdfs dfs -ls {test_uri}'),
                    user=params.azure_cloud_user,
                    logoutput=False,
                    tries=3,
                    try_sleep=10)

        elif params.azure_storage_backend == 'hdfs':
            Execute(format('hdfs dfs -ls /'),
                    user=params.azure_cloud_user,
                    logoutput=False,
                    tries=3,
                    try_sleep=10)


if __name__ == '__main__':
    AzureHadoopCloudServiceCheck().execute()
