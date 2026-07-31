"""
test_reuseport_edge - Edge / negative cases for allow_port_reuse (PR #70093)

Targets code-path gaps found while reviewing:
  - prepare_create appends so_reuseport=1 then rgw_frontend_extra_args
  - skip_port_check allows a second service on the same port
  - multi-host count_per_host docs example

Usage: test_reuseport_edge.py -c <yaml>

Configs:
  test_reuseport_extra_args_duplicate.yaml
  test_reuseport_extra_args_conflict.yaml
  test_reuseport_cross_service_same_port.yaml
  test_reuseport_multi_host_enable.yaml
"""

import argparse
import json
import logging
import os
import re
import sys
import time
import traceback

sys.path.append(os.path.abspath(os.path.join(__file__, "../../../..")))

import v2.utils.utils as utils
from v2.lib.exceptions import RGWBaseException, TestExecError
from v2.lib.resource_op import Config
from v2.lib.s3.write_io_info import BasicIOInfoStructure, IOInfoInitialize
from v2.tests.s3_swift import reusable
from v2.tests.s3_swift import test_reuseport as rp
from v2.utils.log import configure_logging
from v2.utils.test_desc import AddTestInfo

log = logging.getLogger()


def _ops(config):
    return config.test_ops if isinstance(config.test_ops, dict) else {}


def assert_no_duplicate_so_reuseport(snap):
    """Each frontend must contain so_reuseport=1 at most once."""
    log.info("Step: asserting no duplicate so_reuseport tokens")
    bad = []
    for name, info in snap.items():
        fe = info.get("rgw_frontends") or ""
        count = len(re.findall(r"so_reuseport=", fe))
        log.info(f"  {name}: so_reuseport_count={count} frontend='{fe}'")
        if count > 1:
            bad.append((name, fe, count))
    if bad:
        log.error("=" * 60)
        log.error("BUG EVIDENCE: duplicate so_reuseport in rgw_frontends")
        for name, fe, count in bad:
            log.error(f"  daemon={name} count={count} frontend='{fe}'")
        log.error("=" * 60)
        raise TestExecError(
            "rgw_frontends contains duplicate so_reuseport tokens "
            f"(allow_port_reuse + rgw_frontend_extra_args both inject it): {bad}"
        )
    log.info("No duplicate so_reuseport tokens")


def assert_no_conflicting_so_reuseport(snap):
    """Frontend must not contain both so_reuseport=1 and so_reuseport=0."""
    log.info("Step: asserting no conflicting so_reuseport=1 and =0")
    bad = []
    for name, info in snap.items():
        fe = info.get("rgw_frontends") or ""
        has1 = "so_reuseport=1" in fe
        has0 = "so_reuseport=0" in fe
        log.info(f"  {name}: has1={has1} has0={has0} frontend='{fe}'")
        if has1 and has0:
            bad.append((name, fe))
    if bad:
        log.error("=" * 60)
        log.error("BUG EVIDENCE: conflicting so_reuseport=1 and so_reuseport=0")
        for name, fe in bad:
            log.error(f"  daemon={name} frontend='{fe}'")
        log.error(
            "Root cause: prepare_create appends so_reuseport=1 when "
            "allow_port_reuse=true, then extends rgw_frontend_extra_args "
            "which may include so_reuseport=0"
        )
        log.error("=" * 60)
        raise TestExecError(
            "rgw_frontends has both so_reuseport=1 and so_reuseport=0: "
            f"{bad}"
        )
    # Also require a single coherent policy matching allow_port_reuse
    for name, info in snap.items():
        fe = info.get("rgw_frontends") or ""
        if "so_reuseport=0" in fe and "so_reuseport=1" not in fe:
            # allow_port_reuse was true in these scenarios — =0 alone means override
            log.warning(
                f"  {name}: only so_reuseport=0 present while allow_port_reuse "
                "was requested — policy overridden by extra_args"
            )
    log.info("No conflicting so_reuseport tokens")


def assert_cross_service_not_sharing_port(svc_a, svc_b, port):
    """
    Two different RGW service_ids should not both run on the same host port
    via skip_port_check, even if both set allow_port_reuse.
    """
    log.info(
        f"Step: asserting services {svc_a} and {svc_b} do not both own port {port}"
    )
    info_a = utils.exec_shell_cmd(f"ceph orch ls --service-name {svc_a} -f json")
    info_b = utils.exec_shell_cmd(f"ceph orch ls --service-name {svc_b} -f json")
    log.info(f"  orch A: {info_a}")
    log.info(f"  orch B: {info_b}")

    def _running(raw):
        text = str(raw or "").strip()
        if not text or text.startswith("No services"):
            return 0
        try:
            data = json.loads(text)
            if isinstance(data, list) and data:
                return data[0].get("status", {}).get("running", 0)
        except json.JSONDecodeError:
            return 0
        return 0

    run_a = _running(info_a)
    run_b = _running(info_b)
    daemons_a = rp.get_rgw_daemons(svc_a)
    daemons_b = rp.get_rgw_daemons(svc_b)
    ports_a = [p for d in daemons_a for p in (d.get("ports") or [])]
    ports_b = [p for d in daemons_b for p in (d.get("ports") or [])]
    log.info(f"  running A={run_a} ports={ports_a}")
    log.info(f"  running B={run_b} ports={ports_b}")

    both_on_port = run_a >= 1 and run_b >= 1 and port in ports_a and port in ports_b
    if both_on_port:
        ss = utils.exec_shell_cmd(f"ss -lntp | grep ':{port}' || true")
        log.error("=" * 60)
        log.error("BUG EVIDENCE: cross-service same-port bind via skip_port_check")
        log.error(f"  {svc_a} and {svc_b} both running on port {port}")
        log.error(f"  ss: {ss}")
        log.error(
            "Root cause: serve.py sets skip_port_check from each service's "
            "allow_port_reuse independently — no check for foreign RGW listeners"
        )
        log.error("=" * 60)
        raise TestExecError(
            f"Independent services {svc_a} and {svc_b} both bound port {port} "
            "with allow_port_reuse — skip_port_check cross-service footgun"
        )
    log.info("Cross-service same-port sharing not observed (good)")


def scenario_extra_args_frontend(config, ssh_con):
    """Deploy with allow_port_reuse + rgw_frontend_extra_args; validate frontend string."""
    ops = _ops(config)
    hosts = rp.get_cluster_hosts()
    port = int(ops.get("rgw_frontend_port", 8700))
    rp.ensure_firewall_port(port)
    applied = rp.write_and_apply_rgw_spec(ops, hosts)
    svc = applied["service_name"]
    try:
        # extra_args changes may need redeploy to refresh existing daemons
        if ops.get("force_redeploy_after_apply"):
            log.info(f"Step: ceph orch redeploy {svc}")
            utils.exec_shell_cmd(f"ceph orch redeploy {svc}")
            time.sleep(20)
        rp.wait_for_rgw_service(
            svc,
            expected_running=applied["expected_running"],
            timeout=int(ops.get("wait_timeout", 300)),
        )
        daemons = rp.get_rgw_daemons(svc)
        snap = rp.collect_frontend_snapshot(daemons)
        log.info(f"Frontend snapshot: {json.dumps(snap)}")

        mode = ops.get("frontend_assert", "no_duplicate")
        if mode == "no_duplicate":
            # Still expect so_reuseport present for allow_port_reuse=true
            rp.verify_frontends(
                daemons, expect_reuseport=True, expected_port=port
            )
            assert_no_duplicate_so_reuseport(snap)
        elif mode == "no_conflict":
            assert_no_conflicting_so_reuseport(snap)
            # After conflict resolution, policy must match allow_port_reuse
            rp.verify_frontends(
                daemons,
                expect_reuseport=bool(ops.get("allow_port_reuse", True)),
                expected_port=port,
            )
        else:
            raise TestExecError(f"Unknown frontend_assert={mode}")

        if ops.get("perform_s3_io"):
            rp.run_phase_verify_and_io(
                config, ssh_con, hosts, applied, True, "extra_args"
            )
    finally:
        rp.cleanup_service(svc, enabled=bool(ops.get("cleanup_service", True)))


def scenario_cross_service(config, ssh_con):
    """Deploy two services with allow_port_reuse on the same port."""
    ops = _ops(config)
    hosts = rp.get_cluster_hosts()
    port = int(ops.get("rgw_frontend_port", 8710))
    rp.ensure_firewall_port(port)

    ops_a = dict(ops)
    ops_a["service_id"] = ops.get("service_id_a", "reuseport-edge-a")
    ops_a["spec_path"] = "/tmp/rgw_reuseport_edge_a.yaml"
    ops_b = dict(ops)
    ops_b["service_id"] = ops.get("service_id_b", "reuseport-edge-b")
    ops_b["spec_path"] = "/tmp/rgw_reuseport_edge_b.yaml"

    applied_a = rp.write_and_apply_rgw_spec(ops_a, hosts, allow_port_reuse=True)
    svc_a = applied_a["service_name"]
    svc_b = None
    try:
        rp.wait_for_rgw_service(
            svc_a,
            expected_running=applied_a["expected_running"],
            timeout=int(ops.get("wait_timeout", 300)),
        )
        rp.wait_for_port_listen(port, timeout=int(ops.get("port_wait_timeout", 120)))
        snap_a = rp.collect_frontend_snapshot(rp.get_rgw_daemons(svc_a))
        log.info(f"Service A snapshot: {json.dumps(snap_a)}")

        applied_b = rp.write_and_apply_rgw_spec(ops_b, hosts, allow_port_reuse=True)
        svc_b = applied_b["service_name"]
        # Give B time to either fail or come up incorrectly
        time.sleep(int(ops.get("second_service_settle_secs", 45)))
        try:
            rp.wait_for_rgw_service(
                svc_b,
                expected_running=applied_b["expected_running"],
                timeout=int(ops.get("second_service_wait", 120)),
            )
            b_up = True
        except TestExecError as exc:
            log.info(f"Service B did not reach expected running (may be OK): {exc}")
            b_up = False

        snap_b = rp.collect_frontend_snapshot(rp.get_rgw_daemons(svc_b)) if b_up else {}
        log.info(f"Service B up={b_up} snapshot: {json.dumps(snap_b)}")
        assert_cross_service_not_sharing_port(svc_a, svc_b, port)
    finally:
        if svc_b:
            rp.cleanup_service(svc_b, enabled=bool(ops.get("cleanup_service", True)))
        rp.cleanup_service(svc_a, enabled=bool(ops.get("cleanup_service", True)))


def scenario_multi_host(config, ssh_con):
    """Docs example: count_per_host>1 on multiple hosts."""
    ops = _ops(config)
    hosts = rp.get_cluster_hosts()
    placement = ops.get("placement_hosts")
    if not placement:
        placement = [h["hostname"] for h in hosts[:2]]
        ops["placement_hosts"] = placement
    if len(placement) < 2:
        raise TestExecError(
            f"multi-host scenario needs >=2 hosts, got {placement}"
        )
    port = int(ops.get("rgw_frontend_port", 8720))
    rp.ensure_firewall_port(port)
    applied = rp.write_and_apply_rgw_spec(ops, hosts, allow_port_reuse=True)
    svc = applied["service_name"]
    try:
        rp.run_phase_verify_and_io(
            config,
            ssh_con,
            hosts,
            applied,
            allow_port_reuse=True,
            phase_label="multi_host",
        )
    finally:
        rp.cleanup_service(svc, enabled=bool(ops.get("cleanup_service", True)))


def test_exec(config, ssh_con):
    io_info_initialize = IOInfoInitialize()
    basic_io_structure = BasicIOInfoStructure()
    io_info_initialize.initialize(basic_io_structure.initial())

    ops = _ops(config)
    scenario = ops.get("scenario", "extra_args_frontend")
    log.info("=" * 60)
    log.info(f"Starting reuseport EDGE test scenario={scenario} test_ops={ops}")
    log.info("=" * 60)
    log.info(utils.exec_shell_cmd("ceph version"))
    log.info(utils.exec_shell_cmd("ceph orch ls rgw"))

    if scenario == "extra_args_frontend":
        scenario_extra_args_frontend(config, ssh_con)
    elif scenario == "cross_service_same_port":
        scenario_cross_service(config, ssh_con)
    elif scenario == "multi_host_enable":
        scenario_multi_host(config, ssh_con)
    else:
        raise TestExecError(f"Unknown scenario: {scenario}")

    crash_info = reusable.check_for_crash()
    if crash_info:
        raise TestExecError("ceph daemon crash found!")
    log.info("Reuseport edge test completed successfully")


if __name__ == "__main__":
    test_info = AddTestInfo("RGW allow_port_reuse edge / negative cases")
    test_info.started_info()
    try:
        parser = argparse.ArgumentParser(description="RGW reuseport edge cases")
        parser.add_argument("-c", dest="config", help="YAML config")
        parser.add_argument("-log_level", dest="log_level", default="info")
        parser.add_argument("--rgw-node", dest="rgw_node", default="127.0.0.1")
        args = parser.parse_args()
        ssh_con = None
        if args.rgw_node != "127.0.0.1":
            ssh_con = utils.connect_remote(args.rgw_node)
        log_f_name = os.path.basename(os.path.splitext(args.config)[0])
        configure_logging(f_name=log_f_name, set_level=args.log_level.upper())
        config = Config(args.config)
        config.read(ssh_con)
        test_exec(config, ssh_con)
        test_info.success_status("test passed")
        sys.exit(0)
    except (RGWBaseException, Exception) as e:
        log.error(e)
        log.error(traceback.format_exc())
        test_info.failed_status("test failed")
        sys.exit(1)
