# Azurite - Azure Hadoop Cloud Ambari Management Pack

Ambari Management Pack (mpack) for deploying and managing Hadoop clusters on Microsoft Azure.

## Features

- **Azure Storage Integration**: ADLS Gen2, WASB, or HDFS with multiple authentication methods (Managed Identity, Storage Key, SAS Token, OAuth2)
- **VM Lifecycle Management**: Provision and manage Azure VMs directly from Ambari, or use existing VMs
- **Autoscaling**: Automatic cluster scaling based on YARN/CPU/memory metrics with graceful YARN decommission
- **Cost Tracking**: Real-time cluster cost estimation and budget alerts
- **Node Auto-Recovery**: Automatic detection and replacement of failed or evicted VMs
- **Monitoring Dashboard**: Ambari view with VM status, scaling activity, and storage health

## Architecture

| Service | Role | Required |
|---------|------|----------|
| **AZURE_HADOOP_CLOUD** | Storage config, credentials, core-site injection | Yes |
| **AZURE_VM_MANAGER** | VM provisioning/deletion, REST API on port 8470 | Optional |
| **AZURE_AUTOSCALER** | Scaling daemon, REST API on port 8471 | Optional |
| **AZURE_CLUSTER_VIEW** | Ambari dashboard | Optional |

## Quick Start

### Option A: Bootstrap from scratch

```bash
cd azure-hadoop-cloud-mpack/bootstrap
./deploy.sh -g my-resource-group -l eastus -p parameters.json
```

### Option B: Install on existing cluster

```bash
# Build the mpack
cd azure-hadoop-cloud-mpack && mvn clean package

# Install on Ambari
ambari-server install-mpack --mpack=target/azure-hadoop-cloud-mpack-1.0.0.0.tar.gz
ambari-server restart
```

Then add the AZURE_HADOOP_CLOUD service via the Ambari UI.

## Build & Test

```bash
cd azure-hadoop-cloud-mpack

# Run tests (110 tests)
python3 -m unittest discover -s src/test/python -p 'Test*.py' -v

# Build tarball
mvn clean package
```

## Documentation

Full operational documentation is available on the [Varga docs site](https://docs.varga.dev/azurite):

- [Deployment Guide](https://docs.varga.dev/azurite/deployment)
- [Operations Guide](https://docs.varga.dev/azurite/operations)
- [Troubleshooting](https://docs.varga.dev/azurite/troubleshooting)
- [Networking](https://docs.varga.dev/azurite/networking)
- [Disaster Recovery](https://docs.varga.dev/azurite/disaster-recovery)
- [Cost Guide](https://docs.varga.dev/azurite/cost-guide)

## License

Apache License 2.0
