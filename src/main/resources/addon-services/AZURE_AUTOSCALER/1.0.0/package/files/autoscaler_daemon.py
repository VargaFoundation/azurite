#!/usr/bin/env python3
"""
Autoscaler daemon - Main control loop.
Collects metrics, evaluates scaling policies, and executes scaling actions.
Also exposes a REST API for manual control and status queries.
"""
import argparse
import json
import logging
import logging.handlers
import os
import signal
import ssl
import sys
import threading
import time
from http.server import HTTPServer, BaseHTTPRequestHandler

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from metrics_collector import MetricsCollector
from scaling_policy import ScalingDecision, ScalingPolicyEngine
from yarn_decommissioner import YarnDecommissioner

logger = logging.getLogger('autoscaler_daemon')

try:
    from urllib.request import urlopen, Request
    from urllib.error import URLError
except ImportError:
    from urllib2 import urlopen, Request, URLError

_ssl_context = ssl.create_default_context()


def _http_retry(func, max_attempts=3, base_delay=2):
    """Execute a function with retry and exponential backoff."""
    last_error = None
    for attempt in range(max_attempts):
        try:
            return func()
        except Exception as e:
            last_error = e
            if attempt < max_attempts - 1:
                delay = base_delay * (2 ** attempt)
                logger.warning('HTTP call failed (attempt %d/%d): %s. Retrying in %ds...',
                               attempt + 1, max_attempts, e, delay)
                time.sleep(delay)
    raise last_error


class AutoscalerDaemon:
    """Main autoscaler control loop."""

    def __init__(self, config):
        self.config = config
        self.running = True
        self.paused = not config.get('enabled', True)

        self.metrics_collector = MetricsCollector(
            yarn_rm_url=config.get('yarn_rm_url', 'http://localhost:8088'),
            metrics_source=config.get('metrics_source', 'yarn_and_system')
        )
        self.policy_engine = ScalingPolicyEngine(config)
        self.decommissioner = YarnDecommissioner(
            yarn_rm_url=config.get('yarn_rm_url', 'http://localhost:8088')
        )
        self.vm_manager_url = config.get('vm_manager_url', 'http://localhost:8470').rstrip('/')
        self.vm_manager_api_token = config.get('vm_manager_api_token', '')
        self.api_token = config.get('api_token', '')
        self.min_workers = config.get('worker_min_count', 1)
        self.max_workers = config.get('worker_max_count', 20)
        self.evaluation_interval = config.get('evaluation_interval', 60)
        self.graceful_timeout = config.get('graceful_decommission_timeout', 3600)

        # Scaling concurrency
        self._scaling_lock = threading.Lock()
        self._scale_in_thread = None

        # Schedule idempotency
        self._last_schedule_trigger = {}

        # Status tracking
        self.last_metrics = {}
        self.last_decision = 'NO_ACTION'
        self.last_decision_reason = ''
        self.last_decision_time = 0
        self.scale_out_events = 0
        self.scale_in_events = 0
        self.current_worker_count = 0

    def run(self):
        """Main evaluation loop."""
        logger.info('Autoscaler daemon started (mode=%s, interval=%ds)',
                     self.config.get('mode', 'load_based'), self.evaluation_interval)

        while self.running:
            if not self.paused:
                try:
                    self._evaluation_cycle()
                except Exception as e:
                    logger.error('Autoscaler evaluation error: %s', e, exc_info=True)

            time.sleep(self.evaluation_interval)

    def _evaluation_cycle(self):
        """Single evaluation cycle."""
        # Collect metrics
        metrics = self.metrics_collector.get_aggregated_metrics()
        self.last_metrics = metrics or {}

        # Get current worker count from VM Manager
        worker_count = self._get_worker_count()
        if worker_count is None:
            logger.warning('Failed to get worker count, skipping evaluation')
            return
        self.current_worker_count = worker_count

        if metrics is None:
            logger.warning('Failed to collect metrics, skipping evaluation')
            return

        # Evaluate scaling policy
        mode = self.config.get('mode', 'load_based')

        if mode in ('load_based', 'hybrid'):
            decision, target, reason = self.policy_engine.evaluate(
                metrics, self.current_worker_count, self.min_workers, self.max_workers)

            self.last_decision = decision.name
            self.last_decision_reason = reason
            self.last_decision_time = time.time()

            if decision == ScalingDecision.SCALE_OUT:
                self._execute_scale_out(self.current_worker_count, target, reason)
            elif decision == ScalingDecision.SCALE_IN:
                self._execute_scale_in(self.current_worker_count, target, reason)

        if mode in ('schedule_based', 'hybrid'):
            self._check_schedule_rules()

    def _execute_scale_out(self, current, target, reason):
        """Execute a scale-out action via VM Manager."""
        if not self._scaling_lock.acquire(blocking=False):
            logger.info('Scaling operation in progress, skipping scale-out')
            return
        try:
            count = target - current
            logger.info('SCALE OUT: Adding %d workers (%d -> %d). Reason: %s', count, current, target, reason)

            data = json.dumps({'count': count}).encode()

            def _do_provision():
                req = Request('{0}/api/v1/workers/provision'.format(self.vm_manager_url),
                              data=data, method='POST')
                req.add_header('Content-Type', 'application/json')
                if self.vm_manager_api_token:
                    req.add_header('Authorization', 'Bearer {}'.format(self.vm_manager_api_token))
                with urlopen(req, timeout=60, context=_ssl_context) as resp:
                    return json.loads(resp.read().decode())

            result = _http_retry(_do_provision)
            logger.info('Scale-out result: %s', result)
            self.policy_engine.record_scale_out()
            self.scale_out_events += 1
        except Exception as e:
            logger.error('Scale-out failed: %s', e)
        finally:
            self._scaling_lock.release()

    def _execute_scale_in(self, current, target, reason):
        """Execute a scale-in action with graceful YARN decommission (async)."""
        if self._scale_in_thread and self._scale_in_thread.is_alive():
            logger.info('Scale-in already in progress, skipping')
            return

        def _do_scale_in():
            count = current - target
            logger.info('SCALE IN: Removing %d workers (%d -> %d). Reason: %s', count, current, target, reason)
            try:
                candidates = self._get_scale_in_candidates(count)
                if candidates is None:
                    logger.error('Failed to get scale-in candidates, aborting scale-in')
                    return
                if not candidates:
                    logger.warning('No scale-in candidates available')
                    return

                decommissioned = self.decommissioner.graceful_decommission(
                    candidates, self.graceful_timeout)

                if decommissioned:
                    decom_data = json.dumps({'hostnames': decommissioned}).encode()

                    def _do_decommission():
                        req = Request('{0}/api/v1/workers/decommission'.format(self.vm_manager_url),
                                      data=decom_data, method='POST')
                        req.add_header('Content-Type', 'application/json')
                        if self.vm_manager_api_token:
                            req.add_header('Authorization', 'Bearer {0}'.format(self.vm_manager_api_token))
                        with urlopen(req, timeout=60, context=_ssl_context) as resp:
                            return json.loads(resp.read().decode())

                    result = _http_retry(_do_decommission)
                    logger.info('Scale-in result: %s', result)

                self.policy_engine.record_scale_in()
                self.scale_in_events += 1
            except Exception as e:
                logger.error('Scale-in failed: %s', e)

        self._scale_in_thread = threading.Thread(target=_do_scale_in, daemon=True)
        self._scale_in_thread.start()

    def _check_schedule_rules(self):
        """Evaluate cron-based schedule rules."""
        rules = self.config.get('schedule', {}).get('rules', [])
        if not rules:
            return

        try:
            from datetime import datetime
            import pytz
            tz_name = self.config.get('schedule', {}).get('timezone', 'UTC')
            tz = pytz.timezone(tz_name)
            now = datetime.now(tz)

            for rule in rules:
                cron_expr = rule.get('cron', '')
                target_count = rule.get('target_count', 0)
                if not cron_expr or not target_count:
                    continue

                if self._cron_matches(cron_expr, now):
                    # Check if this rule was already triggered recently
                    rule_key = '{cron}:{target}'.format(cron=cron_expr, target=target_count)
                    last_trigger = self._last_schedule_trigger.get(rule_key, 0)
                    if (time.time() - last_trigger) < self.evaluation_interval * 2:
                        continue  # Already triggered recently

                    if self.current_worker_count != target_count:
                        target_count = max(self.min_workers, min(target_count, self.max_workers))
                        if target_count > self.current_worker_count:
                            self._execute_scale_out(self.current_worker_count, target_count,
                                                    'Schedule rule: {0}'.format(rule.get('label', cron_expr)))
                        elif target_count < self.current_worker_count:
                            self._execute_scale_in(self.current_worker_count, target_count,
                                                   'Schedule rule: {0}'.format(rule.get('label', cron_expr)))
                    self._last_schedule_trigger[rule_key] = time.time()
        except ImportError:
            logger.warning('pytz not available, schedule-based scaling disabled')
        except Exception as e:
            logger.error('Schedule evaluation error: %s', e)

    def _cron_matches(self, cron_expr, dt):
        """Simple cron expression matcher (minute hour dom month dow)."""
        parts = cron_expr.split()
        if len(parts) != 5:
            return False

        # Convert isoweekday (1=Mon..7=Sun) to cron dow (0=Sun..6=Sat)
        cron_dow = dt.isoweekday() % 7  # Mon=1..Sat=6, Sun=7->0

        checks = [
            (parts[0], dt.minute),
            (parts[1], dt.hour),
            (parts[2], dt.day),
            (parts[3], dt.month),
            (parts[4], cron_dow),
        ]

        for expr, value in checks:
            if expr == '*':
                continue
            # Handle named days
            day_map = {'SUN': '0', 'MON': '1', 'TUE': '2', 'WED': '3',
                       'THU': '4', 'FRI': '5', 'SAT': '6'}
            for name, num in day_map.items():
                expr = expr.replace(name, num)

            if '-' in expr:
                low, high = expr.split('-', 1)
                if not (int(low) <= value <= int(high)):
                    return False
            elif ',' in expr:
                if value not in [int(v) for v in expr.split(',')]:
                    return False
            elif int(expr) != value:
                return False

        return True

    def _get_worker_count(self):
        """Get current worker count from VM Manager. Returns None on failure."""
        try:
            url = '{0}/api/v1/workers/count'.format(self.vm_manager_url)
            req = Request(url)
            if self.vm_manager_api_token:
                req.add_header('Authorization', 'Bearer {}'.format(self.vm_manager_api_token))
            with urlopen(req, timeout=10, context=_ssl_context) as response:
                data = json.loads(response.read().decode())
            return data.get('worker_count', 0)
        except Exception as e:
            logger.error('Failed to get worker count: %s', e)
            return None

    def _get_scale_in_candidates(self, count):
        """Get hostnames of workers to remove. Returns None on failure."""
        try:
            url = '{0}/api/v1/vms'.format(self.vm_manager_url)
            req = Request(url)
            if self.vm_manager_api_token:
                req.add_header('Authorization', 'Bearer {}'.format(self.vm_manager_api_token))
            with urlopen(req, timeout=10, context=_ssl_context) as response:
                data = json.loads(response.read().decode())
            workers = [v for v in data.get('vms', []) if v.get('role') == 'worker']
            workers.sort(key=lambda v: v.get('created_at', ''), reverse=True)
            return [w['name'] for w in workers[:count]]
        except Exception as e:
            logger.error('Failed to get scale-in candidates: %s', e)
            return None

    def get_status(self):
        """Get current autoscaler status."""
        return {
            'running': self.running,
            'paused': self.paused,
            'current_worker_count': self.current_worker_count,
            'last_decision': self.last_decision,
            'last_decision_reason': self.last_decision_reason,
            'last_decision_time': self.last_decision_time,
            'scale_out_events': self.scale_out_events,
            'scale_in_events': self.scale_in_events,
            'last_metrics': self.last_metrics,
        }


class AutoscalerRequestHandler(BaseHTTPRequestHandler):
    """REST API handler for the autoscaler daemon."""

    daemon_instance = None
    api_token = None

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
            self._respond(200, {'status': 'healthy', 'paused': self.daemon_instance.paused
                                if self.daemon_instance else False})
            return
        if not self._check_auth():
            return
        if self.path == '/api/v1/status':
            status = self.daemon_instance.get_status() if self.daemon_instance else {}
            self._respond(200, status)
        else:
            self._respond(404, {'error': 'Not found'})

    def do_POST(self):
        if not self._check_auth():
            return
        if self.path == '/api/v1/pause':
            if self.daemon_instance:
                self.daemon_instance.paused = True
            self._respond(200, {'paused': True})
        elif self.path == '/api/v1/resume':
            if self.daemon_instance:
                self.daemon_instance.paused = False
            self._respond(200, {'paused': False})
        elif self.path == '/api/v1/scale/out':
            if self.daemon_instance:
                current = self.daemon_instance.current_worker_count
                target = min(current + self.daemon_instance.config.get('scale_out_increment', 1),
                             self.daemon_instance.max_workers)
                self.daemon_instance._execute_scale_out(current, target, 'Manual force scale-out')
            self._respond(200, {'action': 'scale_out'})
        elif self.path == '/api/v1/scale/in':
            if self.daemon_instance:
                current = self.daemon_instance.current_worker_count
                target = max(current - self.daemon_instance.config.get('scale_in_decrement', 1),
                             self.daemon_instance.min_workers)
                self.daemon_instance._execute_scale_in(current, target, 'Manual force scale-in')
            self._respond(200, {'action': 'scale_in'})
        else:
            self._respond(404, {'error': 'Not found'})

    def _respond(self, status, body):
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(body).encode())

    def log_message(self, format, *args):
        logger.info(format, *args)


def main():
    parser = argparse.ArgumentParser(description='Azure Autoscaler daemon')
    parser.add_argument('--config', required=True, help='Path to autoscaler config JSON')
    parser.add_argument('--port', type=int, default=8471, help='REST API port')
    parser.add_argument('--pid-file', default='', help='Path to write PID file')
    args = parser.parse_args()

    with open(args.config, 'r') as f:
        config = json.load(f)

    # Setup logging with rotation
    log_dir = config.get('log_dir', '/var/log/azure-autoscaler')
    log_file = os.path.join(log_dir, 'autoscaler.log')
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

    if not config.get('api_token', ''):
        logger.error('No API token configured. Set api_token in the config file.')
        sys.exit(1)

    daemon = AutoscalerDaemon(config)
    AutoscalerRequestHandler.daemon_instance = daemon
    AutoscalerRequestHandler.api_token = config.get('api_token', '')

    # Start REST API in background thread
    bind_address = config.get('bind_address', '127.0.0.1')
    server = HTTPServer((bind_address, args.port), AutoscalerRequestHandler)

    # TLS support
    tls_cert = config.get('tls_cert_path', '')
    tls_key = config.get('tls_key_path', '')
    if tls_cert and tls_key and os.path.exists(tls_cert) and os.path.exists(tls_key):
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(tls_cert, tls_key)
        server.socket = context.wrap_socket(server.socket, server_side=True)
        logger.info('TLS enabled for Autoscaler daemon')

    def _shutdown_handler(signum, frame):
        logger.info('Received signal %d, shutting down gracefully...', signum)
        daemon.running = False
        server.shutdown()

    signal.signal(signal.SIGTERM, _shutdown_handler)
    signal.signal(signal.SIGINT, _shutdown_handler)

    api_thread = threading.Thread(target=server.serve_forever, daemon=True)
    api_thread.start()
    logger.info('Autoscaler REST API started on %s:%d', bind_address, args.port)

    # Run main evaluation loop
    try:
        daemon.run()
    except KeyboardInterrupt:
        daemon.running = False
        server.shutdown()
    finally:
        if args.pid_file and os.path.exists(args.pid_file):
            os.remove(args.pid_file)


if __name__ == '__main__':
    main()
