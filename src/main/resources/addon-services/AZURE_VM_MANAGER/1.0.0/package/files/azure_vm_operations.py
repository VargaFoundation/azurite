#!/usr/bin/env python3
"""
Azure VM Operations - Core Azure SDK integration for VM lifecycle management.
Runs as a REST daemon when invoked directly, or importable for library use.
"""
import argparse
import functools
import json
import logging
import logging.handlers
import os
import signal
import ssl
import sys
import threading
import time as _time
import time
from http.server import HTTPServer, BaseHTTPRequestHandler

logger = logging.getLogger('azure_vm_operations')


def _retry_azure(max_attempts=3, base_delay=2):
    """Retry decorator with exponential backoff for Azure SDK calls."""
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_error = None
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_error = e
                    if attempt < max_attempts - 1:
                        delay = base_delay * (2 ** attempt)
                        logger.warning('Azure call %s failed (attempt %d/%d): %s. Retrying in %ds...',
                                       func.__name__, attempt + 1, max_attempts, e, delay)
                        _time.sleep(delay)
            raise last_error
        return wrapper
    return decorator


class AzureVmOperations:
    """Manages Azure VM lifecycle using Azure SDK."""

    def __init__(self, config):
        self.config = config
        self.subscription_id = config['subscription_id']
        self.resource_group = config['resource_group']
        self.region = config['region']

        # Initialize Azure clients
        self._credential = self._get_credential()
        self._compute_client = None
        self._network_client = None
        self._resource_client = None
        self._init_clients()

        # VM inventory tracking
        self._inventory_file = os.path.join(config.get('data_dir', '/tmp'), 'vm_inventory.json')
        self._inventory_lock = threading.Lock()
        self._inventory = self._load_inventory()

        # Reconcile inventory with Azure on startup
        self.reconcile()

    def _get_credential(self):
        """Get Azure credential based on identity provider configuration."""
        identity = self.config.get('identity', {})
        provider = identity.get('provider', 'managed_identity')

        if provider == 'managed_identity':
            from azure.identity import ManagedIdentityCredential
            client_id = identity.get('client_id', '')
            if client_id:
                return ManagedIdentityCredential(client_id=client_id)
            return ManagedIdentityCredential()
        else:
            from azure.identity import ClientSecretCredential
            return ClientSecretCredential(
                tenant_id=identity['tenant_id'],
                client_id=identity['client_id'],
                client_secret=identity.get('client_secret', '')
            )

    def _init_clients(self):
        """Initialize Azure management clients. Raises on failure."""
        from azure.mgmt.compute import ComputeManagementClient
        from azure.mgmt.network import NetworkManagementClient
        from azure.mgmt.resource import ResourceManagementClient

        self._compute_client = ComputeManagementClient(self._credential, self.subscription_id)
        self._network_client = NetworkManagementClient(self._credential, self.subscription_id)
        self._resource_client = ResourceManagementClient(self._credential, self.subscription_id)
        logger.info('Azure SDK clients initialized successfully')

    def _load_inventory(self):
        """Load VM inventory from state file."""
        if os.path.exists(self._inventory_file):
            with open(self._inventory_file, 'r') as f:
                return json.load(f)
        return {'vms': []}

    def _save_inventory(self):
        """Save VM inventory atomically (write to temp, then rename)."""
        import tempfile
        tmp_fd, tmp_path = tempfile.mkstemp(dir=os.path.dirname(self._inventory_file), suffix='.tmp')
        try:
            with os.fdopen(tmp_fd, 'w') as f:
                json.dump(self._inventory, f, indent=2)
            os.replace(tmp_path, self._inventory_file)  # Atomic on POSIX
        except Exception:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise

    @_retry_azure()
    def create_vm(self, vm_name, vm_size, role='worker', data_disk_count=0, data_disk_size_gb=512,
                  disk_type='Standard_LRS', os_disk_size_gb=128, spot=False, spot_max_price=-1):
        """
        Create a single Azure VM with NIC and optional data disks.
        Returns the VM creation result.
        """
        if not self._compute_client:
            raise RuntimeError('Azure compute client not initialized')

        net_cfg = self.config.get('networking', {})
        vnet_rg = net_cfg.get('vnet_resource_group', self.resource_group)
        subnet_id = (
            '/subscriptions/{sub}/resourceGroups/{rg}/providers/Microsoft.Network/virtualNetworks/{vnet}'
            '/subnets/{subnet}'
        ).format(
            sub=self.subscription_id,
            rg=vnet_rg,
            vnet=net_cfg.get('vnet_name', ''),
            subnet=net_cfg.get('subnet_name', 'default')
        )

        # Create NIC
        nic_name = '{0}-nic'.format(vm_name)
        nic_params = {
            'location': self.region,
            'ip_configurations': [{
                'name': 'ipconfig1',
                'subnet': {'id': subnet_id},
                'private_ip_allocation_method': 'Dynamic'
            }],
            'tags': self.config.get('tags', {})
        }

        nsg_name = net_cfg.get('nsg_name', '')
        if nsg_name:
            nsg_id = (
                '/subscriptions/{sub}/resourceGroups/{rg}/providers/Microsoft.Network'
                '/networkSecurityGroups/{nsg}'
            ).format(sub=self.subscription_id, rg=self.resource_group, nsg=nsg_name)
            nic_params['network_security_group'] = {'id': nsg_id}

        logger.info('Creating NIC %s', nic_name)
        nic_poller = self._network_client.network_interfaces.begin_create_or_update(
            self.resource_group, nic_name, nic_params)
        nic = nic_poller.result(timeout=300)

        # Build VM parameters
        admin_cfg = self.config.get('admin', {})
        image_cfg = self.config.get('vm_image', {})
        ambari_url = self.config.get('ambari_server_url', '')

        # Cloud-init script to install and register Ambari agent
        user_data = self._build_cloud_init(ambari_url, vm_name)

        vm_params = {
            'location': self.region,
            'tags': {**self.config.get('tags', {}), 'role': role, 'vm-name': vm_name},
            'hardware_profile': {'vm_size': vm_size},
            'storage_profile': {
                'image_reference': {
                    'publisher': image_cfg.get('publisher', 'Canonical'),
                    'offer': image_cfg.get('offer', '0001-com-ubuntu-server-focal'),
                    'sku': image_cfg.get('sku', '20_04-lts-gen2'),
                    'version': image_cfg.get('version', 'latest'),
                },
                'os_disk': {
                    'name': '{0}-osdisk'.format(vm_name),
                    'caching': 'ReadWrite',
                    'create_option': 'FromImage',
                    'managed_disk': {'storage_account_type': disk_type},
                    'disk_size_gb': os_disk_size_gb,
                },
                'data_disks': [
                    {
                        'lun': i,
                        'name': '{0}-datadisk-{1}'.format(vm_name, i),
                        'create_option': 'Empty',
                        'disk_size_gb': data_disk_size_gb,
                        'managed_disk': {'storage_account_type': disk_type},
                    }
                    for i in range(data_disk_count)
                ]
            },
            'os_profile': {
                'computer_name': vm_name,
                'admin_username': admin_cfg.get('username', 'azureadmin'),
                'linux_configuration': {
                    'disable_password_authentication': True,
                    'ssh': {
                        'public_keys': [{
                            'path': '/home/{0}/.ssh/authorized_keys'.format(admin_cfg.get('username', 'azureadmin')),
                            'key_data': admin_cfg.get('ssh_public_key', ''),
                        }]
                    }
                },
                'custom_data': user_data,
            },
            'network_profile': {
                'network_interfaces': [{'id': nic.id}]
            },
        }

        # Spot VM configuration
        if spot:
            vm_params['priority'] = 'Spot'
            vm_params['eviction_policy'] = 'Deallocate'
            if spot_max_price != -1:
                vm_params['billing_profile'] = {'max_price': spot_max_price}

        # Availability zone
        az = self.config.get('availability_zone', '')
        if az:
            vm_params['zones'] = [str(az)]

        logger.info('Creating VM %s (size=%s, role=%s, spot=%s)', vm_name, vm_size, role, spot)
        vm_poller = self._compute_client.virtual_machines.begin_create_or_update(
            self.resource_group, vm_name, vm_params)
        vm = vm_poller.result(timeout=900)

        # Update inventory
        with self._inventory_lock:
            self._inventory['vms'].append({
                'name': vm_name,
                'role': role,
                'size': vm_size,
                'status': 'running',
                'nic_name': nic_name,
                'created_at': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
            })
            self._save_inventory()

        logger.info('VM %s created successfully', vm_name)
        return vm

    @_retry_azure()
    def delete_vm(self, vm_name):
        """Delete a VM and its associated resources (NIC, OS disk, data disks)."""
        if not self._compute_client:
            raise RuntimeError('Azure compute client not initialized')

        logger.info('Deleting VM %s', vm_name)

        # Get VM details for cleanup
        try:
            vm = self._compute_client.virtual_machines.get(self.resource_group, vm_name)
        except Exception:
            logger.warning('VM %s not found, skipping', vm_name)
            self._remove_from_inventory(vm_name)
            return

        all_success = True

        # Delete VM
        try:
            vm_poller = self._compute_client.virtual_machines.begin_delete(self.resource_group, vm_name)
            vm_poller.result(timeout=600)
        except Exception as e:
            logger.warning('Failed to delete VM %s: %s', vm_name, e)
            all_success = False

        # Delete NIC
        nic_name = '{0}-nic'.format(vm_name)
        try:
            nic_poller = self._network_client.network_interfaces.begin_delete(self.resource_group, nic_name)
            nic_poller.result(timeout=300)
        except Exception as e:
            logger.warning('Failed to delete NIC %s: %s', nic_name, e)
            all_success = False

        # Delete OS disk
        os_disk_name = '{0}-osdisk'.format(vm_name)
        try:
            disk_poller = self._compute_client.disks.begin_delete(self.resource_group, os_disk_name)
            disk_poller.result(timeout=300)
        except Exception as e:
            logger.warning('Failed to delete OS disk %s: %s', os_disk_name, e)
            all_success = False

        # Delete data disks
        for disk in vm.storage_profile.data_disks or []:
            try:
                disk_poller = self._compute_client.disks.begin_delete(self.resource_group, disk.name)
                disk_poller.result(timeout=300)
            except Exception as e:
                logger.warning('Failed to delete data disk %s: %s', disk.name, e)
                all_success = False

        if all_success:
            self._remove_from_inventory(vm_name)
            logger.info('VM %s deleted successfully', vm_name)
        else:
            with self._inventory_lock:
                for v in self._inventory['vms']:
                    if v['name'] == vm_name:
                        v['status'] = 'delete_failed'
                self._save_inventory()
            logger.warning('VM %s partially deleted, marked as delete_failed', vm_name)

    def list_vms(self, role_filter=None):
        """List managed VMs, optionally filtered by role."""
        with self._inventory_lock:
            vms = list(self._inventory.get('vms', []))
        if role_filter:
            vms = [v for v in vms if v.get('role') == role_filter]
        return vms

    def get_worker_count(self):
        """Get current number of worker VMs."""
        return len(self.list_vms(role_filter='worker'))

    def get_scale_in_candidates(self, count):
        """Select worker VMs to remove (prefer newest task nodes)."""
        workers = self.list_vms(role_filter='worker')
        # Sort by creation time descending (newest first)
        workers.sort(key=lambda v: v.get('created_at', ''), reverse=True)
        return [w['name'] for w in workers[:count]]

    def _remove_from_inventory(self, vm_name):
        """Remove VM from inventory."""
        with self._inventory_lock:
            self._inventory['vms'] = [v for v in self._inventory['vms'] if v.get('name') != vm_name]
            self._save_inventory()

    def reconcile(self):
        """Reconcile local inventory with actual Azure VMs."""
        if not self._compute_client:
            return
        try:
            azure_vms = list(self._compute_client.virtual_machines.list(self.resource_group))
            managed_vms = [vm for vm in azure_vms
                           if vm.tags and vm.tags.get('managed-by') == 'ambari-azure-mpack']

            azure_names = {vm.name for vm in managed_vms}
            local_names = {v['name'] for v in self._inventory.get('vms', [])}

            # Add VMs that exist in Azure but not locally
            for vm in managed_vms:
                if vm.name not in local_names:
                    logger.info('Reconciliation: adding missing VM %s to inventory', vm.name)
                    self._inventory['vms'].append({
                        'name': vm.name,
                        'role': vm.tags.get('role', 'unknown'),
                        'size': vm.hardware_profile.vm_size if vm.hardware_profile else 'unknown',
                        'status': 'running',
                        'nic_name': '{0}-nic'.format(vm.name),
                        'created_at': 'reconciled',
                    })

            # Mark VMs that exist locally but not in Azure
            for entry in self._inventory['vms']:
                if entry['name'] not in azure_names:
                    logger.warning('Reconciliation: VM %s in inventory but not in Azure, marking as deleted',
                                   entry['name'])
                    entry['status'] = 'deleted'

            # Remove deleted entries
            self._inventory['vms'] = [v for v in self._inventory['vms'] if v.get('status') != 'deleted']
            self._save_inventory()
            logger.info('Reconciliation complete: %d VMs tracked', len(self._inventory['vms']))
        except Exception as e:
            logger.error('Reconciliation failed: %s', e)

    def _build_cloud_init(self, ambari_url, hostname):
        """Build cloud-init script for Ambari agent registration."""
        import base64
        import shlex
        # Sanitize the URL - only allow valid URL characters
        safe_url = ''.join(c for c in ambari_url if c.isalnum() or c in '.:/-_@')
        script = """#!/bin/bash
set -e
apt-get update -y
apt-get install -y curl

AMBARI_URL='{safe_url}'

if [ -n "$AMBARI_URL" ]; then
    # Extract hostname from URL
    AMBARI_HOST=$(echo "$AMBARI_URL" | sed -e 's|^[^/]*//||' -e 's|[:/].*||')

    wget -O /etc/apt/sources.list.d/ambari.list "${{AMBARI_URL}}/ambari.list" 2>/dev/null || true
    apt-get update
    apt-get install -y ambari-agent || true

    if [ -f /etc/ambari-agent/conf/ambari-agent.ini ]; then
        sed -i "s/hostname=localhost/hostname=$AMBARI_HOST/" /etc/ambari-agent/conf/ambari-agent.ini
        systemctl enable ambari-agent
        systemctl start ambari-agent
    fi
fi
""".format(safe_url=safe_url)
        return base64.b64encode(script.encode()).decode()


class VmManagerRequestHandler(BaseHTTPRequestHandler):
    """REST API handler for VM Manager daemon."""

    vm_ops = None
    api_token = None
    cost_tracker = None
    health_monitor = None

    def _check_auth(self):
        if not self.api_token:
            self._respond(403, {'error': 'No API token configured. '
                                'Authentication is mandatory.'})
            return False
        auth = self.headers.get('Authorization', '')
        if auth == 'Bearer {}'.format(self.api_token):
            return True
        self._respond(401, {'error': 'Unauthorized'})
        return False

    def do_GET(self):
        if not self._check_auth():
            return
        if self.path == '/api/v1/health':
            healthy = not self.vm_ops or self.vm_ops._compute_client is not None
            health = {'status': 'healthy' if healthy else 'degraded',
                      'mode': 'managed' if self.vm_ops else 'existing'}
            if self.vm_ops:
                health['vm_count'] = self.vm_ops.get_worker_count()
                health['azure_sdk'] = self.vm_ops._compute_client is not None
            self._respond(200, health)
            return
        if self.path == '/api/v1/vms':
            vms = self.vm_ops.list_vms() if self.vm_ops else []
            self._respond(200, {'vms': vms, 'count': len(vms)})
        elif self.path == '/api/v1/workers/count':
            count = self.vm_ops.get_worker_count() if self.vm_ops else 0
            self._respond(200, {'worker_count': count})
        elif self.path == '/api/v1/cost':
            if self.cost_tracker:
                self._respond(200, self.cost_tracker.get_cost_summary())
            else:
                self._respond(200, {'error': 'Cost tracking not configured'})
        elif self.path == '/api/v1/nodes/health':
            if self.health_monitor and self.vm_ops:
                self._respond(200, self.health_monitor.get_health_summary(self.vm_ops._inventory))
            else:
                self._respond(200, {'error': 'Health monitor not available'})
        else:
            self._respond(404, {'error': 'Not found'})

    def do_POST(self):
        if not self._check_auth():
            return
        if self.path == '/api/v1/workers/provision':
            self._handle_provision()
        elif self.path == '/api/v1/workers/decommission':
            self._handle_decommission()
        else:
            self._respond(404, {'error': 'Not found'})

    def _handle_provision(self):
        """Provision new worker VMs."""
        if not self.vm_ops:
            self._respond(500, {'error': 'VM operations not initialized'})
            return

        content_length = int(self.headers.get('Content-Length', 0))
        body = json.loads(self.rfile.read(content_length)) if content_length > 0 else {}
        count = body.get('count', 1)
        pool_cfg = self.vm_ops.config.get('pools', {}).get('worker', {})

        results = []
        for i in range(count):
            vm_name = 'azr-worker-{0}'.format(int(time.time() * 1000) + i)
            try:
                self.vm_ops.create_vm(
                    vm_name=vm_name,
                    vm_size=pool_cfg.get('size', 'Standard_D4_v3'),
                    role='worker',
                    data_disk_count=pool_cfg.get('data_disks', 4),
                    data_disk_size_gb=pool_cfg.get('data_disk_size_gb', 512),
                    disk_type=pool_cfg.get('disk_type', 'Standard_LRS'),
                    os_disk_size_gb=pool_cfg.get('disk_size_gb', 128),
                    spot=pool_cfg.get('spot_enabled', False),
                    spot_max_price=pool_cfg.get('spot_max_price', -1),
                )
                results.append({'name': vm_name, 'status': 'created'})
            except Exception as e:
                results.append({'name': vm_name, 'status': 'failed', 'error': str(e)})

        self._respond(200, {'results': results})

    def _handle_decommission(self):
        """Decommission worker VMs."""
        if not self.vm_ops:
            self._respond(500, {'error': 'VM operations not initialized'})
            return

        content_length = int(self.headers.get('Content-Length', 0))
        body = json.loads(self.rfile.read(content_length)) if content_length > 0 else {}
        count = body.get('count', 1)
        hostnames = body.get('hostnames', [])

        if not hostnames:
            hostnames = self.vm_ops.get_scale_in_candidates(count)

        results = []
        for hostname in hostnames:
            try:
                self.vm_ops.delete_vm(hostname)
                results.append({'name': hostname, 'status': 'deleted'})
            except Exception as e:
                results.append({'name': hostname, 'status': 'failed', 'error': str(e)})

        self._respond(200, {'results': results})

    def _respond(self, status, body):
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(body).encode())

    def log_message(self, format, *args):
        logger.info(format, *args)


def main():
    parser = argparse.ArgumentParser(description='Azure VM Manager REST daemon')
    parser.add_argument('--config', required=True, help='Path to VM manager config JSON')
    parser.add_argument('--port', type=int, default=8470, help='REST API port')
    parser.add_argument('--pid-file', default='', help='Path to write PID file')
    args = parser.parse_args()

    with open(args.config, 'r') as f:
        config = json.load(f)

    # Setup logging with rotation
    log_dir = config.get('log_dir', '/var/log/azure-vm-manager')
    log_file = os.path.join(log_dir, 'vm_manager.log')
    handler = logging.handlers.RotatingFileHandler(
        log_file, maxBytes=50 * 1024 * 1024, backupCount=5)
    handler.setFormatter(logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s'))
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(handler)

    # Write PID file from the daemon process itself
    if args.pid_file:
        with open(args.pid_file, 'w') as pf:
            pf.write(str(os.getpid()))

    mode = config.get('mode', 'managed')
    if mode == 'managed':
        vm_ops = AzureVmOperations(config)
    else:
        vm_ops = None
        logger.info('VM Manager running in "existing" mode - VM operations disabled')

    VmManagerRequestHandler.vm_ops = vm_ops
    VmManagerRequestHandler.api_token = config.get('api_token', '')

    # Initialize cost tracker
    try:
        from cost_tracker import CostTracker
        inventory_file = os.path.join(config.get('data_dir', '/tmp'), 'vm_inventory.json')
        budget = float(config.get('daily_budget', 0))
        VmManagerRequestHandler.cost_tracker = CostTracker(
            inventory_file=inventory_file, budget_limit=budget)
        logger.info('Cost tracker initialized (budget=$%.2f/day)', budget)
    except ImportError:
        logger.warning('cost_tracker module not found, cost tracking disabled')
    except Exception as e:
        logger.warning('Failed to initialize cost tracker: %s', e)

    # Initialize node health monitor
    if vm_ops and vm_ops._compute_client:
        try:
            from node_health_monitor import NodeHealthMonitor
            VmManagerRequestHandler.health_monitor = NodeHealthMonitor(
                compute_client=vm_ops._compute_client,
                resource_group=config.get('resource_group', ''),
                vm_operations=vm_ops)
            logger.info('Node health monitor initialized')
        except ImportError:
            logger.warning('node_health_monitor module not found')
        except Exception as e:
            logger.warning('Failed to initialize health monitor: %s', e)

    bind_address = config.get('bind_address', '127.0.0.1')
    server = HTTPServer((bind_address, args.port), VmManagerRequestHandler)

    # TLS support
    tls_cert = config.get('tls_cert_path', '')
    tls_key = config.get('tls_key_path', '')
    if tls_cert and tls_key and os.path.exists(tls_cert) and os.path.exists(tls_key):
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(tls_cert, tls_key)
        server.socket = context.wrap_socket(server.socket, server_side=True)
        logger.info('TLS enabled for VM Manager daemon')

    if not config.get('api_token', ''):
        logger.error('No API token configured. Set api_token in the config file.')
        sys.exit(1)

    def _shutdown_handler(signum, frame):
        logger.info('Received signal %d, shutting down gracefully...', signum)
        server.shutdown()

    signal.signal(signal.SIGTERM, _shutdown_handler)
    signal.signal(signal.SIGINT, _shutdown_handler)

    logger.info('VM Manager daemon started on %s:%d (mode=%s)', bind_address, args.port, mode)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        server.shutdown()
    finally:
        if args.pid_file and os.path.exists(args.pid_file):
            os.remove(args.pid_file)


if __name__ == '__main__':
    main()
