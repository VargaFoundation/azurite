#!/usr/bin/env python3
"""
AZURE_AUTOSCALER_MASTER component handler.
Manages the autoscaler daemon lifecycle and custom scaling commands.
"""
import json
import os
import secrets

from resource_management.core.resources.system import Directory, Execute, File
from resource_management.libraries.functions.check_process_status import check_process_status
from resource_management.libraries.script.script import Script


class AzureAutoscalerMaster(Script):

    def install(self, env):
        import params
        env.set_params(params)
        self.install_packages(env)

        for d in [params.autoscaler_log_dir, params.autoscaler_pid_dir]:
            Directory(d,
                      owner=params.autoscaler_user,
                      group=params.autoscaler_group,
                      create_parents=True,
                      mode=0o755)

        self.configure(env)

    def configure(self, env):
        import params
        env.set_params(params)

        autoscaler_config = {
            'enabled': params.autoscaler_enabled,
            'mode': params.autoscaler_mode,
            'port': params.autoscaler_port,
            'evaluation_interval': params.evaluation_interval,
            'scale_out_trigger_duration': params.scale_out_trigger_duration,
            'scale_in_trigger_duration': params.scale_in_trigger_duration,
            'cooldown_scale_out': params.cooldown_scale_out,
            'cooldown_scale_in': params.cooldown_scale_in,
            'scale_out_increment': params.scale_out_increment,
            'scale_in_decrement': params.scale_in_decrement,
            'cpu_scale_out_threshold': params.cpu_scale_out_threshold,
            'cpu_scale_in_threshold': params.cpu_scale_in_threshold,
            'memory_scale_out_threshold': params.memory_scale_out_threshold,
            'memory_scale_in_threshold': params.memory_scale_in_threshold,
            'yarn_pending_containers_threshold': params.yarn_pending_containers_threshold,
            'yarn_available_memory_scale_in_pct': params.yarn_available_memory_scale_in_pct,
            'graceful_decommission_timeout': params.graceful_decommission_timeout,
            'scale_only_task_nodes': params.scale_only_task_nodes,
            'yarn_rm_url': params.yarn_rm_url,
            'vm_manager_url': params.vm_manager_url,
            'metrics_source': params.metrics_source,
            'worker_min_count': params.worker_min_count,
            'worker_max_count': params.worker_max_count,
            'schedule': {
                'timezone': params.schedule_timezone,
                'rules': params.schedule_rules,
            },
            'log_dir': params.autoscaler_log_dir,
        }

        autoscaler_config['api_token'] = secrets.token_hex(32)

        File(os.path.join(params.autoscaler_log_dir, 'autoscaler_config.json'),
             content=json.dumps(autoscaler_config, indent=2),
             owner=params.autoscaler_user,
             group=params.autoscaler_group,
             mode=0o600)

    def start(self, env):
        import params
        env.set_params(params)
        self.configure(env)

        daemon_script = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                                     'files', 'autoscaler_daemon.py')
        config_path = os.path.join(params.autoscaler_log_dir, 'autoscaler_config.json')
        log_file = os.path.join(params.autoscaler_log_dir, 'autoscaler.log')

        cmd = ('nohup python3 -u {daemon} --config {config} --port {port} '
               '--pid-file {pid} >> {log} 2>&1 &').format(
            daemon=daemon_script,
            config=config_path,
            port=params.autoscaler_port,
            pid=params.autoscaler_pid_file,
            log=log_file)

        Execute(cmd, user=params.autoscaler_user, logoutput=True)

    def stop(self, env):
        import params
        env.set_params(params)

        if os.path.isfile(params.autoscaler_pid_file):
            with open(params.autoscaler_pid_file, 'r') as f:
                pid = f.read().strip()
            if pid:
                cmdline_file = '/proc/{0}/cmdline'.format(pid)
                if os.path.exists(cmdline_file):
                    with open(cmdline_file, 'r') as cf:
                        cmdline = cf.read()
                    if 'autoscaler_daemon' in cmdline:
                        Execute('kill {0}'.format(pid), user=params.autoscaler_user)
                    else:
                        from resource_management.core.logger import Logger
                        Logger.warning('PID %s does not belong to autoscaler daemon, skipping kill', pid)
                os.remove(params.autoscaler_pid_file)

    def status(self, env):
        import params
        env.set_params(params)
        check_process_status(params.autoscaler_pid_file)

    def _read_api_token(self, params):
        """Read the API token from the autoscaler config file."""
        config_path = os.path.join(params.autoscaler_log_dir, 'autoscaler_config.json')
        try:
            with open(config_path, 'r') as f:
                cfg = json.load(f)
            return cfg.get('api_token', '')
        except Exception:
            return ''

    def _api_call(self, params, method, path):
        """Make an authenticated API call to the local autoscaler daemon."""
        from resource_management.core.logger import Logger
        try:
            from urllib.request import urlopen, Request
        except ImportError:
            from urllib2 import urlopen, Request
        token = self._read_api_token(params)
        url = 'http://localhost:{port}{path}'.format(port=params.autoscaler_port, path=path)
        req = Request(url, method=method, data=b'')
        req.add_header('Authorization', 'Bearer {0}'.format(token))
        response = urlopen(req, timeout=30)
        result = response.read().decode()
        Logger.info(result)

    def force_scale_out(self, env):
        import params
        env.set_params(params)
        self._api_call(params, 'POST', '/api/v1/scale/out')

    def force_scale_in(self, env):
        import params
        env.set_params(params)
        self._api_call(params, 'POST', '/api/v1/scale/in')

    def pause_autoscaling(self, env):
        import params
        env.set_params(params)
        self._api_call(params, 'POST', '/api/v1/pause')

    def resume_autoscaling(self, env):
        import params
        env.set_params(params)
        self._api_call(params, 'POST', '/api/v1/resume')


if __name__ == '__main__':
    AzureAutoscalerMaster().execute()
