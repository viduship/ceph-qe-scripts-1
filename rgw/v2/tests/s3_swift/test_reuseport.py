"""
test_reuseport - Validate cephadm RGW allow_port_reuse / so_reuseport

Feature: mgr/cephadm support for RGW frontend so_reuseport via RGWSpec.allow_port_reuse
Tracker: https://tracker.ceph.com/issues/78118
Upstream PR: https://github.com/ceph/ceph/pull/70093

Usage: test_reuseport.py -c <input_yaml>

<input_yaml>
    test_reuseport_enable.yaml
    test_reuseport_ports_increment.yaml
    test_reuseport_toggle_false_to_true.yaml
    test_reuseport_toggle_true_to_false.yaml

Operation:
    Apply a temporary RGW service with count_per_host > 1
    With allow_port_reuse=true:
        - rgw_frontends must include so_reuseport=1
        - daemons on the same host must share the same frontend port
        - run S3 IO (create/put/get/delete) against that shared endpoint
    With allow_port_reuse=false:
        - so_reuseport must not be set
        - daemons on the same host must use distinct ports
        - run S3 IO against one of the deployed endpoints
    Toggle scenarios (test_ops.toggle_allow_port_reuse set):
        - Deploy with initial allow_port_reuse
        - Re-apply the same service with the toggled value
        - Assert EVERY daemon's rgw_frontends matches the new policy
          (ports + so_reuseport) without requiring a manual orch redeploy
    Remove the temporary RGW service and purge leftover client.rgw.*
    config keys (rgw_frontends / zonegroup / rgw_run_sync_thread, …)
"""

import argparse
import json
import logging
import os
import socket
import sys
import time
import traceback

import yaml

sys.path.append(os.path.abspath(os.path.join(__file__, "../../../..")))

import v2.lib.resource_op as s3lib
import v2.utils.utils as utils
from v2.lib.exceptions import RGWBaseException, TestExecError
from v2.lib.resource_op import Config
from v2.lib.s3.auth import Auth
from v2.lib.s3.write_io_info import BasicIOInfoStructure, IOInfoInitialize
from v2.tests.s3_swift import reusable
from v2.utils.log import configure_logging
from v2.utils.test_desc import AddTestInfo

log = logging.getLogger()
TEST_DATA_PATH = None

# Local import: shared config-DB cleanup (same directory as this file).
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import reuseport_cleanup as _rp_cleanup  # noqa: E402


def _test_ops(config):
    return config.test_ops if isinstance(config.test_ops, dict) else {}


def _run_cmd(cmd):
    """Adapter so reuseport_cleanup can call utils.exec_shell_cmd."""
    out = utils.exec_shell_cmd(cmd)
    return "" if out is None else str(out)


def _normalize_rgw_service_name(service_name):
    return _rp_cleanup.normalize_rgw_service_name(service_name)


def _is_protected_rgw_service(service_name):
    return _rp_cleanup.is_protected_rgw_service(service_name)


def cleanup_rgw_service_config(service_name, extra_daemons=None):
    """Remove leftover client.rgw.* keys after orch rm of a temp service."""
    return _rp_cleanup.cleanup_rgw_service_config(
        service_name,
        _run_cmd,
        extra_daemons=extra_daemons,
        log_fn=log.info,
    )


def get_cluster_hosts():
    """Return list of {hostname, addr} from ceph orch host ls."""
    log.info("Step: listing cluster hosts via ceph orch host ls")
    raw = utils.exec_shell_cmd("ceph orch host ls -f json")
    hosts = json.loads(raw)
    host_list = []
    for h in hosts:
        hostname = h.get("hostname")
        addr = h.get("addr") or hostname
        host_list.append({"hostname": hostname, "addr": addr})
        log.info(f"  host={hostname} addr={addr}")
    if not host_list:
        raise TestExecError("No hosts found from ceph orch host ls")
    return host_list


def wait_for_port_listen(port, timeout=120, poll=5):
    """Wait until something is listening on the given TCP port."""
    log.info(f"Step: waiting for TCP port {port} to be listening (timeout={timeout}s)")
    deadline = time.time() + timeout
    while time.time() < deadline:
        out = utils.exec_shell_cmd(f"ss -lntp | grep ':{port} ' || true")
        log.info(f"  ss probe for :{port}: {out}")
        if out and str(port) in str(out):
            log.info(f"Port {port} is listening")
            return
        time.sleep(poll)
    raise TestExecError(f"Nothing listening on TCP port {port} after {timeout}s")


def ensure_firewall_port(port):
    """Open frontend port in firewalld if firewalld is active."""
    log.info(f"Step: ensuring firewall allows TCP/{port}")
    active = utils.exec_shell_cmd(
        "firewall-cmd --state 2>/dev/null || echo not_running"
    )
    log.info(f"  firewalld state: {active}")
    if active and "running" in str(active):
        utils.exec_shell_cmd(f"firewall-cmd --add-port={port}/tcp --permanent || true")
        utils.exec_shell_cmd("firewall-cmd --reload || true")
        ports = utils.exec_shell_cmd("firewall-cmd --list-ports || true")
        log.info(f"  firewall ports after update: {ports}")
    else:
        log.info("  firewalld not active; skipping")


def wait_for_rgw_service(service_name, expected_running, timeout=300, poll=10):
    """Poll orch until RGW service reports expected running count."""
    log.info(
        f"Step: waiting for service {service_name} to reach "
        f"running={expected_running} (timeout={timeout}s)"
    )
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        raw = utils.exec_shell_cmd(
            f"ceph orch ls --service-name {service_name} -f json"
        )
        if not raw:
            time.sleep(poll)
            continue
        data = json.loads(raw)
        if not data:
            time.sleep(poll)
            continue
        status = data[0].get("status", {})
        running = status.get("running", 0)
        size = status.get("size", 0)
        last = f"running={running} size={size} ports={status.get('ports')}"
        log.info(f"  service status: {last}")
        if running >= expected_running:
            log.info(f"Service {service_name} is up: {last}")
            return data[0]
        time.sleep(poll)
    raise TestExecError(f"Timed out waiting for {service_name}; last status: {last}")


def get_rgw_daemons(service_name):
    """Return orch ps JSON list for the RGW service."""
    log.info(f"Step: fetching daemons for {service_name}")
    raw = utils.exec_shell_cmd(f"ceph orch ps --service-name {service_name} -f json")
    daemons = json.loads(raw) if raw else []
    for d in daemons:
        log.info(
            f"  daemon={d.get('daemon_name')} host={d.get('hostname')} "
            f"ports={d.get('ports')} status={d.get('status_desc')}"
        )
    return daemons


def verify_frontends(daemons, expect_reuseport, expected_port):
    """Assert rgw_frontends content for each daemon."""
    log.info(
        f"Step: verifying rgw_frontends "
        f"(expect so_reuseport={expect_reuseport}, port={expected_port})"
    )
    for d in daemons:
        daemon_name = d.get("daemon_name")
        who = f"client.{daemon_name}"
        frontend = utils.exec_shell_cmd(f"ceph config get {who} rgw_frontends")
        if frontend is None:
            frontend = ""
        frontend = str(frontend).strip()
        log.info(f"  {who} rgw_frontends='{frontend}'")
        if f"port={expected_port}" not in frontend and expect_reuseport:
            # With reuseport all should share expected_port; without reuseport
            # base port may still appear on first daemon only.
            log.warning(f"  port={expected_port} not found in frontend for {who}")
        if expect_reuseport:
            if "so_reuseport=1" not in frontend:
                raise TestExecError(
                    f"Expected so_reuseport=1 in rgw_frontends for {who}, got: {frontend}"
                )
            if f"port={expected_port}" not in frontend:
                raise TestExecError(
                    f"Expected port={expected_port} in rgw_frontends for {who}, got: {frontend}"
                )
        else:
            if "so_reuseport=1" in frontend:
                raise TestExecError(
                    f"Did not expect so_reuseport=1 for {who}, got: {frontend}"
                )
    log.info("rgw_frontends verification passed")


def verify_ports_per_host(daemons, expect_same_port):
    """Group daemon ports by hostname and assert same/unique as configured."""
    log.info(f"Step: verifying ports per host (expect_same_port={expect_same_port})")
    by_host = {}
    for d in daemons:
        host = d.get("hostname")
        ports = d.get("ports") or []
        by_host.setdefault(host, []).append(list(ports))
    for host, port_lists in by_host.items():
        log.info(f"  host={host} daemon_ports={port_lists}")
        if len(port_lists) < 2:
            log.warning(f"  host {host} has fewer than 2 daemons; skip compare")
            continue
        if expect_same_port:
            first = port_lists[0]
            for pl in port_lists[1:]:
                if pl != first:
                    raise TestExecError(
                        f"Expected identical ports on {host}, got {port_lists}"
                    )
        else:
            # No two daemons on the same host should share an identical port list
            serialized = [tuple(p) for p in port_lists]
            if len(set(serialized)) != len(serialized):
                raise TestExecError(
                    f"Expected distinct ports on {host} without reuseport, got {port_lists}"
                )
    log.info("per-host port verification passed")


def collect_frontend_snapshot(daemons):
    """Return {daemon_name: rgw_frontends} for bug-report / assertion logging."""
    snapshot = {}
    for d in daemons:
        daemon_name = d.get("daemon_name")
        who = f"client.{daemon_name}"
        frontend = utils.exec_shell_cmd(f"ceph config get {who} rgw_frontends")
        frontend = str(frontend or "").strip()
        snapshot[daemon_name] = {
            "hostname": d.get("hostname"),
            "ports": d.get("ports"),
            "status": d.get("status_desc"),
            "rgw_frontends": frontend,
        }
        log.info(
            f"  snapshot {daemon_name}: ports={d.get('ports')} "
            f"rgw_frontends='{frontend}' status={d.get('status_desc')}"
        )
    return snapshot


def write_and_apply_rgw_spec(ops, hosts, allow_port_reuse=None):
    """Write RGW service spec YAML and apply via ceph orch."""
    service_id = ops.get("service_id", "reuseport-qe")
    port = int(ops.get("rgw_frontend_port", 8000))
    count_per_host = int(ops.get("count_per_host", 2))
    if allow_port_reuse is None:
        allow_port_reuse = bool(ops.get("allow_port_reuse", False))
    else:
        allow_port_reuse = bool(allow_port_reuse)
    # Use first host by default for a contained test footprint; override via YAML
    placement_hosts = ops.get("placement_hosts")
    if not placement_hosts:
        placement_hosts = [hosts[0]["hostname"]]
    log.info(
        f"Step: building RGW spec service_id={service_id} "
        f"port={port} count_per_host={count_per_host} "
        f"allow_port_reuse={allow_port_reuse} hosts={placement_hosts}"
    )
    spec = {
        "service_type": "rgw",
        "service_id": service_id,
        "placement": {
            "hosts": placement_hosts,
            "count_per_host": count_per_host,
        },
        "spec": {
            "rgw_frontend_port": port,
            "allow_port_reuse": allow_port_reuse,
        },
    }
    # Optional multisite / role fields (ignored when unset)
    for key in (
        "rgw_realm",
        "rgw_zonegroup",
        "rgw_zone",
        "rgw_exit_timeout_secs",
    ):
        if ops.get(key) is not None:
            spec["spec"][key] = ops[key]
    if "disable_multisite_sync_traffic" in ops:
        spec["spec"]["disable_multisite_sync_traffic"] = bool(
            ops.get("disable_multisite_sync_traffic")
        )
    if ops.get("rgw_frontend_extra_args") is not None:
        spec["spec"]["rgw_frontend_extra_args"] = list(ops.get("rgw_frontend_extra_args"))
    if ops.get("ssl") is not None:
        spec["spec"]["ssl"] = bool(ops.get("ssl"))
    spec_path = ops.get("spec_path", "/tmp/rgw_reuseport_spec.yaml")
    with open(spec_path, "w") as fh:
        yaml.safe_dump(spec, fh, default_flow_style=False)
    content = utils.exec_shell_cmd(f"cat {spec_path}")
    log.info(f"RGW spec content:\n{content}")
    log.info(f"Step: applying RGW spec via ceph orch apply -i {spec_path}")
    apply_out = utils.exec_shell_cmd(f"ceph orch apply -i {spec_path}")
    log.info(f"orch apply output: {apply_out}")
    expected = count_per_host * len(placement_hosts)
    service_name = f"rgw.{service_id}"
    return {
        "service_id": service_id,
        "service_name": service_name,
        "port": port,
        "count_per_host": count_per_host,
        "allow_port_reuse": allow_port_reuse,
        "placement_hosts": placement_hosts,
        "expected_running": expected,
        "spec_path": spec_path,
    }


def wait_for_toggle_convergence(
    service_name,
    expect_reuseport,
    expected_port,
    expected_running,
    timeout=240,
    poll=15,
):
    """
    After re-applying allow_port_reuse, wait until all daemons match the new
    frontend policy. Returns (daemons, frontend_snapshot) on success.
    Raises TestExecError with full before/after-style evidence on timeout.
    """
    log.info(
        f"Step: waiting for toggle convergence on {service_name} "
        f"(expect so_reuseport={expect_reuseport}, port={expected_port}, "
        f"running>={expected_running}, timeout={timeout}s)"
    )
    deadline = time.time() + timeout
    last_snap = {}
    last_error = None
    while time.time() < deadline:
        try:
            wait_for_rgw_service(
                service_name,
                expected_running=expected_running,
                timeout=min(45, max(10, int(deadline - time.time()))),
                poll=5,
            )
        except TestExecError as exc:
            last_error = str(exc)
            log.warning(f"Service not fully up during toggle wait: {exc}")
            time.sleep(poll)
            continue
        daemons = get_rgw_daemons(service_name)
        last_snap = collect_frontend_snapshot(daemons)
        try:
            verify_frontends(
                daemons,
                expect_reuseport=expect_reuseport,
                expected_port=expected_port,
            )
            verify_ports_per_host(daemons, expect_same_port=expect_reuseport)
            log.info("Toggle convergence achieved — all daemons match new policy")
            return daemons, last_snap
        except TestExecError as exc:
            last_error = str(exc)
            remaining = int(deadline - time.time())
            log.warning(
                f"Toggle not yet converged ({exc}); retrying "
                f"({remaining}s remaining)..."
            )
            time.sleep(poll)

    # Explicit bug-report block for JIRA
    log.error("=" * 60)
    log.error("BUG EVIDENCE: allow_port_reuse toggle did not converge")
    log.error(f"  service={service_name}")
    log.error(f"  expect_reuseport={expect_reuseport}")
    log.error(f"  expected_port={expected_port}")
    log.error(f"  timeout={timeout}s")
    log.error(f"  last_error={last_error}")
    for name, info in last_snap.items():
        log.error(
            f"  daemon={name} ports={info.get('ports')} "
            f"rgw_frontends='{info.get('rgw_frontends')}'"
        )
    orch = utils.exec_shell_cmd(f"ceph orch ls --service-name {service_name} -f json")
    log.error(f"  orch ls: {orch}")
    log.error("=" * 60)
    raise TestExecError(
        f"allow_port_reuse toggle did not converge within {timeout}s. "
        f"last_error={last_error}. Frontend snapshot: {json.dumps(last_snap)}. "
        "Likely cephadm only rewrites rgw_frontends for daemons whose port "
        "changed; daemons already on the base port keep stale so_reuseport."
    )


def resolve_endpoint_for_io(hosts, placement_hosts, port):
    """Pick host addr for the first placement host and return (ip, port, url)."""
    host_map = {h["hostname"]: h["addr"] for h in hosts}
    target_host = placement_hosts[0]
    addr = host_map.get(target_host, target_host)
    # Prefer IP if hostname does not resolve from this node
    try:
        socket.gethostbyname(addr)
        endpoint_ip = addr
    except socket.gaierror:
        endpoint_ip = host_map.get(target_host, addr)
    endpoint_url = f"http://{endpoint_ip}:{port}"
    log.info(
        f"Step: selected IO endpoint host={target_host} "
        f"ip={endpoint_ip} port={port} url={endpoint_url}"
    )
    return endpoint_ip, port, endpoint_url


def run_s3_io_on_endpoint(config, ssh_con, endpoint_ip, endpoint_port, endpoint_url):
    """Create user, bucket, upload/download/delete objects via the given endpoint."""
    log.info(f"Step: starting S3 IO against endpoint {endpoint_url}")
    log.info("Checking endpoint reachability")
    curl_out = utils.exec_shell_cmd(
        f"curl -k --connect-timeout 10 {endpoint_ip}:{endpoint_port}"
    )
    log.info(f"curl response (truncated): {str(curl_out)[:200]}")

    log.info("Creating S3 user for reuseport IO")
    users = s3lib.create_users(no_of_users_to_create=config.user_count or 1)
    if not users:
        raise TestExecError("Failed to create users for S3 IO")
    user_info = users[0]
    log.info(f"Created user: {user_info['user_id']}")

    log.info(
        f"Authenticating boto3 against explicit endpoint "
        f"{endpoint_ip}:{endpoint_port}"
    )
    auth = Auth(
        user_info,
        ssh_con,
        ssl=getattr(config, "ssl", False),
        endpoint_ip=endpoint_ip,
        endpoint_port=endpoint_port,
    )
    rgw_conn = auth.do_auth()
    s3_client = auth.do_auth_using_client()
    log.info(f"Auth endpoint_url={auth.endpoint_url}")

    bucket_name = utils.gen_bucket_name_from_userid(user_info["user_id"], rand_no=0)
    log.info(f"Creating bucket {bucket_name} on reuseport endpoint {endpoint_url}")
    # Use sync_init helper to avoid multisite remote SSH during local endpoint IO
    log.info("Checking endpoint reachability before bucket create")
    for attempt in range(1, 4):
        curl_probe = utils.exec_shell_cmd(
            f"curl -k --connect-timeout 10 {endpoint_ip}:{endpoint_port}"
        )
        if curl_probe:
            log.info(f"Endpoint reachable on attempt {attempt}")
            break
        log.warning(f"Endpoint not reachable on attempt {attempt}, retrying...")
        time.sleep(5)
    bucket = reusable.create_bucket_sync_init(bucket_name, rgw_conn, user_info)

    objects_count = config.objects_count or 5
    config.mapped_sizes = utils.make_mapped_sizes(config)
    log.info(f"Uploading {objects_count} objects to s3://{bucket_name}")
    uploaded = []
    for i in range(objects_count):
        config.obj_size = config.mapped_sizes[i]
        s3_object_name = utils.gen_s3_object_name(bucket_name, i)
        log.info(
            f"  PUT object={s3_object_name} size={config.obj_size} via {endpoint_url}"
        )
        reusable.upload_object(
            s3_object_name, bucket, TEST_DATA_PATH, config, user_info
        )
        uploaded.append(s3_object_name)

    if _test_ops(config).get("download_object", True):
        log.info(f"Downloading {len(uploaded)} objects from s3://{bucket_name}")
        for s3_object_name in uploaded:
            s3_object_path = os.path.join(TEST_DATA_PATH, s3_object_name + ".download")
            log.info(f"  GET object={s3_object_name} -> {s3_object_path}")
            s3_client.download_file(bucket_name, s3_object_name, s3_object_path)
            if not os.path.exists(s3_object_path):
                raise TestExecError(f"Download failed for {s3_object_name}")
            log.info(f"  downloaded ok: {s3_object_name}")

    if _test_ops(config).get("delete_bucket_object", True):
        log.info(f"Deleting objects and bucket {bucket_name}")
        for s3_object_name in uploaded:
            log.info(f"  DELETE object={s3_object_name}")
            s3_client.delete_object(Bucket=bucket_name, Key=s3_object_name)
        log.info(f"  DELETE bucket={bucket_name}")
        s3_client.delete_bucket(Bucket=bucket_name)
        log.info("Bucket and objects deleted")

    log.info(f"S3 IO against {endpoint_url} completed successfully")
    return user_info, bucket_name


def cleanup_service(service_name, enabled=True, timeout=180):
    """Remove temporary RGW service, wait until gone, purge leftover config."""
    if not enabled:
        log.info(f"Step: cleanup skipped for {service_name}")
        return
    svc = _normalize_rgw_service_name(service_name)
    if _is_protected_rgw_service(svc):
        raise TestExecError(f"Refusing to remove protected RGW service {svc}")

    # Capture daemon names before orch rm so config cleanup can target them.
    daemons = []
    try:
        daemons = get_rgw_daemons(svc)
    except Exception as exc:
        log.warning(f"Could not list daemons before rm of {svc}: {exc}")

    log.info(f"Step: removing temporary service {svc}")
    out = utils.exec_shell_cmd(f"ceph orch rm {svc} --force")
    log.info(f"orch rm output: {out}")
    deadline = time.time() + timeout
    removed = False
    while time.time() < deadline:
        remaining = utils.exec_shell_cmd(
            f"ceph orch ls --service-name {svc} -f json"
        )
        text = str(remaining or "").strip()
        if (not text) or text.startswith("No services") or text in ("[]", "null"):
            log.info(f"Service {svc} fully removed")
            removed = True
            break
        # empty JSON list
        try:
            data = json.loads(text)
            if not data:
                log.info(f"Service {svc} fully removed")
                removed = True
                break
        except (json.JSONDecodeError, TypeError):
            if "No services" in text:
                log.info(f"Service {svc} fully removed")
                removed = True
                break
        log.info(f"  waiting for {svc} removal; orch still reports: {text[:200]}")
        time.sleep(5)
    if not removed:
        log.warning(
            f"Service {svc} still visible after {timeout}s; continuing config cleanup"
        )

    # orch rm does not scrub client.rgw.* keys from the config DB.
    cleanup_rgw_service_config(svc, extra_daemons=daemons)


def run_phase_verify_and_io(
    config, ssh_con, hosts, applied, allow_port_reuse, phase_label
):
    """Verify frontends/ports for a phase and optionally run S3 IO."""
    service_name = applied["service_name"]
    log.info(f"===== PHASE: {phase_label} (allow_port_reuse={allow_port_reuse}) =====")
    wait_for_rgw_service(
        service_name,
        expected_running=applied["expected_running"],
        timeout=int(_test_ops(config).get("wait_timeout", 300)),
    )
    daemons = get_rgw_daemons(service_name)
    if len(daemons) < applied["expected_running"]:
        raise TestExecError(
            f"Expected {applied['expected_running']} daemons, found {len(daemons)}"
        )

    snap = collect_frontend_snapshot(daemons)
    log.info(f"Frontend snapshot ({phase_label}): {json.dumps(snap)}")

    verify_frontends(
        daemons,
        expect_reuseport=allow_port_reuse,
        expected_port=applied["port"],
    )
    verify_ports_per_host(daemons, expect_same_port=allow_port_reuse)

    ops = _test_ops(config)
    perform_io = ops.get("perform_s3_io", True)
    if not perform_io:
        log.info(f"Step: S3 IO skipped for phase {phase_label}")
        return daemons, snap

    io_port = applied["port"]
    if not allow_port_reuse and daemons:
        dports = daemons[0].get("ports") or [io_port]
        io_port = dports[0] if dports else io_port
        log.info(f"Non-reuseport phase: using daemon-reported port {io_port} for IO")
    ensure_firewall_port(io_port)
    # Open firewall before listen wait; reload can race with freshly started daemons
    wait_for_port_listen(io_port, timeout=int(ops.get("port_wait_timeout", 180)))
    endpoint_ip, endpoint_port, endpoint_url = resolve_endpoint_for_io(
        hosts, applied["placement_hosts"], io_port
    )
    run_s3_io_on_endpoint(config, ssh_con, endpoint_ip, endpoint_port, endpoint_url)
    return daemons, snap


def test_exec(config, ssh_con):
    """
    Execute RGW reuseport validation including S3 IO on the target endpoint.
    Supports optional allow_port_reuse toggle via test_ops.toggle_allow_port_reuse.
    """
    io_info_initialize = IOInfoInitialize()
    basic_io_structure = BasicIOInfoStructure()
    io_info_initialize.initialize(basic_io_structure.initial())

    ops = _test_ops(config)
    initial_reuse = bool(ops.get("allow_port_reuse", False))
    toggle_to = ops.get("toggle_allow_port_reuse", None)
    if toggle_to is not None:
        toggle_to = bool(toggle_to)

    log.info("=" * 60)
    log.info(
        f"Starting reuseport test allow_port_reuse={initial_reuse} "
        f"toggle_allow_port_reuse={toggle_to} test_ops={ops}"
    )
    log.info("=" * 60)

    log.info("Step: cluster version / health snapshot")
    log.info(utils.exec_shell_cmd("ceph version"))
    log.info(utils.exec_shell_cmd("ceph orch ls rgw"))

    hosts = get_cluster_hosts()
    # Pre-open firewall for the planned frontend port(s)
    base_port = int(ops.get("rgw_frontend_port", 8000))
    ensure_firewall_port(base_port)
    if not initial_reuse or (toggle_to is False):
        ensure_firewall_port(base_port + 1)

    applied = write_and_apply_rgw_spec(ops, hosts, allow_port_reuse=initial_reuse)
    service_name = applied["service_name"]

    try:
        run_phase_verify_and_io(
            config,
            ssh_con,
            hosts,
            applied,
            allow_port_reuse=initial_reuse,
            phase_label="initial",
        )

        if toggle_to is not None:
            if toggle_to == initial_reuse:
                raise TestExecError(
                    "toggle_allow_port_reuse must differ from allow_port_reuse"
                )
            log.info("=" * 60)
            log.info(
                f"Step: toggling allow_port_reuse "
                f"{initial_reuse} -> {toggle_to} via orch apply"
            )
            log.info("=" * 60)
            before_daemons = get_rgw_daemons(service_name)
            before_snap = collect_frontend_snapshot(before_daemons)
            log.info(f"BEFORE toggle snapshot: {json.dumps(before_snap)}")

            applied = write_and_apply_rgw_spec(
                ops, hosts, allow_port_reuse=toggle_to
            )
            # Give scheduler a moment to start reconfiguring
            settle = int(ops.get("toggle_settle_secs", 30))
            log.info(f"Waiting {settle}s for orch to start reconciling toggle...")
            time.sleep(settle)

            daemons, after_snap = wait_for_toggle_convergence(
                service_name,
                expect_reuseport=toggle_to,
                expected_port=applied["port"],
                expected_running=applied["expected_running"],
                timeout=int(ops.get("toggle_wait_timeout", 240)),
                poll=int(ops.get("toggle_poll_secs", 15)),
            )
            log.info(f"AFTER toggle snapshot: {json.dumps(after_snap)}")
            log.info(
                "Toggle frontend comparison:\n"
                f"  BEFORE={json.dumps(before_snap)}\n"
                f"  AFTER ={json.dumps(after_snap)}"
            )

            # Optional post-toggle IO against the new policy endpoint
            if ops.get("perform_s3_io_after_toggle", ops.get("perform_s3_io", True)):
                io_port = applied["port"]
                if not toggle_to and daemons:
                    dports = daemons[0].get("ports") or [io_port]
                    io_port = dports[0] if dports else io_port
                ensure_firewall_port(io_port)
                wait_for_port_listen(
                    io_port, timeout=int(ops.get("port_wait_timeout", 120))
                )
                endpoint_ip, endpoint_port, endpoint_url = resolve_endpoint_for_io(
                    hosts, applied["placement_hosts"], io_port
                )
                run_s3_io_on_endpoint(
                    config, ssh_con, endpoint_ip, endpoint_port, endpoint_url
                )

    finally:
        cleanup_service(service_name, enabled=bool(ops.get("cleanup_service", True)))

    crash_info = reusable.check_for_crash()
    if crash_info:
        raise TestExecError("ceph daemon crash found!")
    log.info("Reuseport test completed successfully")


if __name__ == "__main__":
    test_info = AddTestInfo("RGW cephadm allow_port_reuse / so_reuseport")
    test_info.started_info()
    try:
        project_dir = os.path.abspath(os.path.join(__file__, "../../.."))
        test_data_dir = "test_data"
        TEST_DATA_PATH = os.path.join(project_dir, test_data_dir)
        log.info(f"TEST_DATA_PATH: {TEST_DATA_PATH}")
        if not os.path.exists(TEST_DATA_PATH):
            log.info("test data dir not exists, creating.. ")
            os.makedirs(TEST_DATA_PATH)
        parser = argparse.ArgumentParser(
            description="RGW reuseport (allow_port_reuse) automation"
        )
        parser.add_argument("-c", dest="config", help="RGW Test yaml configuration")
        parser.add_argument(
            "-log_level",
            dest="log_level",
            help="Set Log Level [DEBUG, INFO, WARNING, ERROR, CRITICAL]",
            default="info",
        )
        parser.add_argument(
            "--rgw-node", dest="rgw_node", help="RGW Node", default="127.0.0.1"
        )
        args = parser.parse_args()
        yaml_file = args.config
        rgw_node = args.rgw_node
        ssh_con = None
        if rgw_node != "127.0.0.1":
            ssh_con = utils.connect_remote(rgw_node)
        log_f_name = os.path.basename(os.path.splitext(yaml_file)[0])
        configure_logging(f_name=log_f_name, set_level=args.log_level.upper())
        config = Config(yaml_file)
        config.read(ssh_con)
        test_exec(config, ssh_con)
        test_info.success_status("test passed")
        sys.exit(0)
    except (RGWBaseException, Exception) as e:
        log.error(e)
        log.error(traceback.format_exc())
        test_info.failed_status("test failed")
        sys.exit(1)
