#!/usr/bin/env python3
"""
Alert script: checks that hadoop-azure and azure-storage JARs are present
on the Hadoop classpath.
"""
import glob
import os
import traceback

RESULT_STATE_OK = 'OK'
RESULT_STATE_WARNING = 'WARNING'
RESULT_STATE_CRITICAL = 'CRITICAL'
RESULT_STATE_UNKNOWN = 'UNKNOWN'

HADOOP_LIB_DIRS = [
    '/usr/hdp/current/hadoop-client/lib',
    '/usr/lib/hadoop/lib',
    '/opt/hadoop/share/hadoop/tools/lib',
]

REQUIRED_JARS = [
    'hadoop-azure',
    'azure-storage',
]


def get_tokens():
    return ()


def execute(configurations={}, parameters={}, host_name=None):
    try:
        backend = configurations.get('{{azure-cloud-env/azure_storage_backend}}', 'hdfs')
        if backend == 'hdfs':
            return (RESULT_STATE_OK, ['HDFS backend: Azure JARs not required.'])

        missing = []
        found = []
        for jar_prefix in REQUIRED_JARS:
            jar_found = False
            for lib_dir in HADOOP_LIB_DIRS:
                pattern = os.path.join(lib_dir, '{0}*.jar'.format(jar_prefix))
                if glob.glob(pattern):
                    jar_found = True
                    found.append(jar_prefix)
                    break
            if not jar_found:
                missing.append(jar_prefix)

        if missing:
            return (RESULT_STATE_CRITICAL,
                    ['Missing JARs on classpath: {0}. '
                     'Searched in: {1}'.format(', '.join(missing), ', '.join(HADOOP_LIB_DIRS))])

        return (RESULT_STATE_OK,
                ['All required Azure JARs found: {0}'.format(', '.join(found))])

    except Exception:
        return (RESULT_STATE_UNKNOWN, [traceback.format_exc()])
