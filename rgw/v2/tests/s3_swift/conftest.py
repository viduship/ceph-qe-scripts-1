"""
Pytest conftest for RGW dedup tests.

Provides shared fixtures for S3 client setup, bucket management,
and cluster configuration used across all dedup pytest tests.

Usage:
  pytest test_dedup_pytest.py -c <config.yaml> -v
  pytest test_dedup_pytest.py -c <config.yaml> --rgw-node <hostname>
"""

import json
import logging
import os
import random
import sys
import time

import pytest

sys.path.append(os.path.abspath(os.path.join(__file__, "../../../..")))

import v2.lib.resource_op as s3lib
import v2.utils.utils as utils
from v2.lib.resource_op import Config
from v2.lib.s3.write_io_info import BasicIOInfoStructure, IOInfoInitialize
from v2.tests.s3_swift import reusable
from v2.tests.s3_swift.reusables import dedup as dedup_utils
from v2.utils.log import configure_logging

log = logging.getLogger()

# ---------------------------------------------------------------------------
# Per-test resource tracking for cleanup and reporting
# ---------------------------------------------------------------------------

_test_context = {}
_test_passed = {}


def _get_ctx(node_id):
    if node_id not in _test_context:
        _test_context[node_id] = {
            "users": [],
            "buckets": [],
            "bucket_markers": {},
            "recorder": None,
        }
    return _test_context[node_id]


DEDUP_TEST_SCENARIOS = {
    "test_s1_sanity_large_objects": {
        "id": "S1",
        "category": "Sanity",
        "scenario": "Upload 50 identical 5KB objects, run dedup estimate+exec, verify deduped_count>0 and data integrity",
    },
    "test_s2_sanity_small_objects": {
        "id": "S2",
        "category": "Sanity",
        "scenario": "Upload identical objects at 5KB/8KB/10KB sizes, run dedup estimate+exec, verify ratio and integrity",
    },
    "test_s3_admin_ops_api": {
        "id": "S3",
        "category": "Sanity",
        "scenario": "Verify dedup Admin OPS REST API endpoints (estimate, stats, exec) with S3 V1 auth",
    },
    "test_s4_estimate_dry_run": {
        "id": "S4",
        "category": "Sanity",
        "scenario": "Run estimate only (no exec), verify ETags unchanged and no data modification",
    },
    "test_s5_data_integrity": {
        "id": "S5",
        "category": "Sanity",
        "scenario": "Upload 100 duplicates with known MD5, run exec, verify all MD5s match post-dedup",
    },
    "test_s6_multipart_objects": {
        "id": "S6",
        "category": "Feature",
        "scenario": "Upload 5 identical 20MB multipart objects, dedup, verify range GETs work post-dedup",
    },
    "test_s7_session_lifecycle": {
        "id": "S7",
        "category": "Feature",
        "scenario": "Test dedup pause/resume/abort controls during exec, verify data survives each action",
    },
    "test_s8_ssec_exclusion": {
        "id": "S8",
        "category": "Feature",
        "scenario": "SSE-C encrypted objects excluded from dedup, plain objects deduped, both accessible",
    },
    "test_s9_storage_class_dedup": {
        "id": "S9",
        "category": "Feature",
        "scenario": "Create custom storage class with data pool, upload objects, dedup with storage class filter",
    },
    "test_s10_lc_expiration": {
        "id": "S10",
        "category": "Feature",
        "scenario": "Dedup objects then apply lifecycle expiration, verify LC works on deduped objects",
    },
    "test_s11_versioned_objects": {
        "id": "S11",
        "category": "Feature",
        "scenario": "Upload 10 identical versions of same key, dedup, verify all versions accessible post-dedup",
    },
    "test_s12_s3_copy_dedup": {
        "id": "S12",
        "category": "Feature",
        "scenario": "S3 COPY to create 20 duplicates, dedup, verify all copies intact post-dedup",
    },
    "test_s14_same_content_diff_metadata": {
        "id": "S14",
        "category": "Feature",
        "scenario": "Same content with different metadata/tags across 2 users, dedup, verify metadata preserved",
    },
    "test_s15_concurrent_s3_ops": {
        "id": "S15",
        "category": "Feature",
        "scenario": "Run concurrent S3 workload (puts/gets/deletes) during dedup exec, verify <5% error rate",
    },
    "test_s16_uncompressed_to_compressed": {
        "id": "S16",
        "category": "Compression",
        "scenario": "Upload uncompressed, switch to zlib, upload copies, dedup aligns all to compressed",
    },
    "test_s17_compressed_to_uncompressed": {
        "id": "S17",
        "category": "Compression",
        "scenario": "Upload compressed, switch to none, upload copies, dedup aligns all to uncompressed",
    },
    "test_s18_per_sc_compression_cache": {
        "id": "S18",
        "category": "Compression",
        "scenario": "Two SCs with opposite compression, flip, dedup aligns each SC independently",
    },
    "test_s19_inc_compressed_shared_manifest": {
        "id": "S19",
        "category": "Compression",
        "scenario": "Incremental: start compressed, switch mode, shared_manifest SRC priority preserved",
    },
    "test_s20_inc_uncompressed_shared_manifest": {
        "id": "S20",
        "category": "Compression",
        "scenario": "Incremental: start uncompressed, switch mode, shared_manifest SRC priority preserved",
    },
    "test_s22_filter_bucket_list_parsing": {
        "id": "S22",
        "category": "Filter",
        "scenario": "Bucket filter file parsing: mutual exclusivity, missing file, empty file errors",
    },
    "test_s23_filter_sc_list_parsing": {
        "id": "S23",
        "category": "Filter",
        "scenario": "SC filter file parsing: mutual exclusivity, missing file, empty file errors",
    },
    "test_s24_filter_sc_estimate": {
        "id": "S24",
        "category": "Filter",
        "scenario": "Allow STANDARD estimate finds duplicates, deny STANDARD skips all via ingress_skip_filtered_sc",
    },
    "test_s25_filter_sc_exec": {
        "id": "S25",
        "category": "Filter",
        "scenario": "Deny STANDARD exec dedupes 0, allow STANDARD exec dedupes normally",
    },
    "test_s26_compression_stats_counters": {
        "id": "S26",
        "category": "Compression",
        "scenario": "Validate compressed_objs, compressed_bytes, deduped_compressed_objects stat counters",
    },
    "test_s27_cross_user_compressed_to_uncompressed": {
        "id": "S27",
        "category": "Compression",
        "scenario": "Cross-user: user1 compressed bucket + user2 uncompressed bucket, dedup aligns all to uncompressed",
    },
    "test_s28_cross_user_uncompressed_to_compressed": {
        "id": "S28",
        "category": "Compression",
        "scenario": "Cross-user: user1 uncompressed bucket + user2 compressed bucket, dedup aligns all to compressed",
    },
    "test_b1_overwrite_deduped_object": {
        "id": "B1",
        "category": "Bug",
        "scenario": "Overwrite one deduped object with new content, verify sibling objects survive",
    },
    "test_b2_delete_dedup_source": {
        "id": "B2",
        "category": "Bug",
        "scenario": "Delete deduped objects one-by-one, verify each remaining object survives",
    },
    "test_b3_s3_copy_then_delete_deduped": {
        "id": "B3",
        "category": "Bug",
        "scenario": "S3 COPY deduped object (same + cross bucket), delete originals, verify copies survive",
    },
    "test_b4_cross_bucket_source_delete": {
        "id": "B4",
        "category": "Bug",
        "scenario": "Cross-bucket dedup, delete source bucket entirely, verify target bucket objects survive",
    },
    "test_b5_dedup_idempotency": {
        "id": "B5",
        "category": "Bug",
        "scenario": "Run dedup exec 3 times, verify run1 dedupes, runs 2-3 dedup 0 (no double-counting)",
    },
    "test_e1_128_limit_boundary": {
        "id": "E1",
        "category": "Enhancement",
        "scenario": "200 identical objects, verify deduped<=127 (128-limit), skipped_too_many_copies>0",
    },
    "test_e2_versioned_boundary": {
        "id": "E2",
        "category": "Enhancement",
        "scenario": "130 versions of same key (past 128 boundary), verify all versions accessible post-dedup",
    },
    "test_e3_multi_cycle_no_progress": {
        "id": "E3",
        "category": "Enhancement",
        "scenario": "200 objects, 3 exec cycles: cycle1 dedupes, cycles 2-3 dedup 0, stable skip counts",
    },
    "test_e4_split_head_small_objects": {
        "id": "E4",
        "category": "Enhancement",
        "scenario": "50 identical 5KB objects, verify split-head mechanism used (split_head_src>0, tgt>0)",
    },
    "test_e5_stats_validation": {
        "id": "E5",
        "category": "Enhancement",
        "scenario": "Validate stats fields: ingress_count for estimate, total_processed for exec, ratios, skipped fields",
    },
    "test_r1_rgw_restart_mid_exec": {
        "id": "R1",
        "category": "Restart",
        "scenario": "Restart RGW service mid-exec, verify no data corruption and dedup completes on re-run",
    },
    "test_r2_rgw_sigkill_mid_exec": {
        "id": "R2",
        "category": "Restart",
        "scenario": "SIGKILL RGW process during split-head, verify no orphan tail objects and data intact",
    },
    "test_r3_osd_restart_mid_exec": {
        "id": "R3",
        "category": "Restart",
        "scenario": "Restart OSD hosting bucket PG during dedup exec, verify dedup completes and data intact",
    },
    "test_r4_mon_restart_mid_exec": {
        "id": "R4",
        "category": "Restart",
        "scenario": "Restart MON leader during dedup exec, verify dedup completes after MON election",
    },
    "test_r5_rapid_rgw_restarts": {
        "id": "R5",
        "category": "Restart",
        "scenario": "Restart RGW 3 times in 30 seconds during exec, verify shard tokens re-acquired on re-run",
    },
    "test_c1_exec_during_estimate": {
        "id": "C1",
        "category": "Concurrent",
        "scenario": "Fire dedup exec while estimate is still running, verify concurrent requests handled gracefully",
    },
    "test_c2_delete_during_exec": {
        "id": "C2",
        "category": "Concurrent",
        "scenario": "Delete objects during dedup exec, verify surviving objects intact and no crash",
    },
    "test_c3_upload_during_exec": {
        "id": "C3",
        "category": "Concurrent",
        "scenario": "Upload new objects during dedup exec, verify new objects intact, re-run dedupes them",
    },
    "test_c4_rapid_pause_resume": {
        "id": "C4",
        "category": "Concurrent",
        "scenario": "Rapidly toggle pause/resume 10 times during exec, verify dedup completes without deadlock",
    },
    "test_c5_double_abort": {
        "id": "C5",
        "category": "Concurrent",
        "scenario": "Send abort twice in quick succession, verify no crash and fresh exec works normally",
    },
    "test_d1_empty_bucket": {
        "id": "D1",
        "category": "Edge Case",
        "scenario": "Run dedup estimate and exec on empty bucket, verify clean completion with all-zero counters",
    },
    "test_d2_singletons_only": {
        "id": "D2",
        "category": "Edge Case",
        "scenario": "Upload 50 objects with unique content, verify dedup finds 0 duplicates and skips all",
    },
    "test_d3_min_size_boundary": {
        "id": "D3",
        "category": "Edge Case",
        "scenario": "Objects at exactly min_obj_size boundary, verify threshold enforced correctly",
    },
    "test_d4_single_object": {
        "id": "D4",
        "category": "Edge Case",
        "scenario": "Single object in bucket, verify dedup completes with 0 deduped (nothing to match)",
    },
    "test_d5_max_versions_500": {
        "id": "D5",
        "category": "Edge Case",
        "scenario": "500 versions of same key, verify deduped<=127 (128 limit), all versions accessible",
    },
    "test_f1_pool_near_full": {
        "id": "F1",
        "category": "Infrastructure",
        "scenario": "Set pool quota to simulate near-full, run dedup, verify graceful failure and recovery",
    },
    "test_f2_reshard_during_exec": {
        "id": "F2",
        "category": "Infrastructure",
        "scenario": "Trigger bucket reshard during dedup exec, verify dedup handles stale index gracefully",
    },
    "test_f3_network_partition": {
        "id": "F3",
        "category": "Infrastructure",
        "scenario": "Block MON traffic on one RGW node during dedup, verify recovery after restore",
    },
    "test_f4_corrupt_rados_object": {
        "id": "F4",
        "category": "Infrastructure",
        "scenario": "Corrupt a RADOS object mid-dedup, verify hash mismatch detected and other objects intact",
    },
    "test_f5_time_jump": {
        "id": "F5",
        "category": "Infrastructure",
        "scenario": "Jump system clock forward 1 hour during dedup, verify completion despite stale heartbeats",
    },
    "test_m1_100_buckets_cross_dedup": {
        "id": "M1",
        "category": "Scale",
        "scenario": "100 buckets with identical objects, verify cross-bucket dedup and source bucket deletion survival",
    },
    "test_m2_bucket_allow_deny_filters": {
        "id": "M2",
        "category": "Scale",
        "scenario": "3 buckets with allow/deny filter lists, verify only allowed buckets are deduped",
    },
    "test_m3_mixed_sizes_threshold": {
        "id": "M3",
        "category": "Scale",
        "scenario": "Mix of objects above and below min_obj_size_for_dedup, verify threshold filtering",
    },
    # -------------------------------------------------------------------------
    # Scale & Extended Suite (test_dedup_scale_pytest.py) — SC01-SC08
    # Only tests NOT covered by test_dedup_pytest.py
    # -------------------------------------------------------------------------
    "test_sc01_large_objects_above_4mb": {
        "id": "SC01",
        "category": "Sanity",
        "scenario": "Large objects >4MB (non-split-head dedup path), 50 x 5MB objects",
    },
    "test_sc02_multipart_large_objects": {
        "id": "SC02",
        "category": "Feature",
        "scenario": "Multipart objects 50MB/500MB with range GETs (larger than S6's 20MB)",
    },
    "test_sc03_s3_copy_large_objects": {
        "id": "SC03",
        "category": "Regression",
        "scenario": "S3 COPY of 10MB objects (>5MB triggers multipart copy path, differs from S12)",
    },
    "test_sc04_10k_objects_multi_bucket": {
        "id": "SC04",
        "category": "Scale",
        "scenario": "10K objects across 10 buckets at 50% dup rate (10x M1 scale)",
    },
    "test_sc05_multi_rgw_scaleout": {
        "id": "SC05",
        "category": "Scale",
        "scenario": "Multi-RGW scaleout — completion time and work distribution measurement",
    },
    "test_sc06_high_duplication_rate": {
        "id": "SC06",
        "category": "Scale",
        "scenario": "1000 objects with 90% duplication rate, verify high storage savings",
    },
    "test_sc07_throttle_control": {
        "id": "SC07",
        "category": "Adhoc",
        "scenario": "Throttle 50 vs 500 ops/sec comparison — speed vs foreground impact",
    },
    "test_sc08_lc_transition_deduped_objects": {
        "id": "SC08",
        "category": "Adhoc",
        "scenario": "LC transition (not expiration) of deduped objects to different SC, ref counts preserved",
    },
}


def pytest_addoption(parser):
    parser.addoption(
        "--config",
        "-C",
        dest="config",
        required=True,
        help="Path to RGW test YAML configuration file",
    )
    parser.addoption(
        "--rgw-node",
        dest="rgw_node",
        default="",
        help="RGW node hostname for SSH connection",
    )


@pytest.fixture(scope="session", autouse=True)
def setup_logging():
    configure_logging(f_name="test_dedup_pytest")


@pytest.fixture(scope="session")
def rgw_config(request):
    yaml_path = request.config.getoption("config")
    config = Config(yaml_path)
    config.read()
    return config


@pytest.fixture(scope="session")
def ssh_con(request):
    rgw_node = request.config.getoption("rgw_node")
    if rgw_node:
        return utils.connect_remote(rgw_node)
    return None


@pytest.fixture(scope="session")
def io_info():
    io_info_initialize = IOInfoInitialize()
    basic_io_structure = BasicIOInfoStructure()
    io_info_initialize.initialize(basic_io_structure.initial())


@pytest.fixture
def s3_client(rgw_config, ssh_con, io_info, request):
    user_info = s3lib.create_users(1)[0]
    ctx = _get_ctx(request.node.nodeid)
    ctx["users"].append(user_info)
    auth = reusable.get_auth(
        user_info,
        ssh_con,
        rgw_config.ssl,
        getattr(rgw_config, "haproxy", False),
    )
    return auth.do_auth_using_client()


@pytest.fixture
def s3_clients(rgw_config, ssh_con, io_info, request):
    all_users = s3lib.create_users(2)
    ctx = _get_ctx(request.node.nodeid)
    for user_info in all_users:
        ctx["users"].append(user_info)
    clients = []
    for user_info in all_users:
        auth = reusable.get_auth(
            user_info,
            ssh_con,
            rgw_config.ssl,
            getattr(rgw_config, "haproxy", False),
        )
        clients.append(auth.do_auth_using_client())
    return clients


@pytest.fixture
def bucket(s3_client, request):
    name = f"dedup-pytest-{random.randint(1, 9999)}"
    s3_client.create_bucket(Bucket=name)
    ctx = _get_ctx(request.node.nodeid)
    ctx["buckets"].append(name)
    marker = dedup_utils.get_bucket_marker(name)
    if marker:
        ctx["bucket_markers"][name] = marker
    yield name
    dedup_utils.cleanup_bucket(s3_client, name)


@pytest.fixture
def bucket_factory(s3_client, request):
    created = []

    def _create(prefix="dedup-pytest"):
        name = f"{prefix}-{random.randint(1, 9999)}"
        s3_client.create_bucket(Bucket=name)
        created.append(name)
        ctx = _get_ctx(request.node.nodeid)
        ctx["buckets"].append(name)
        marker = dedup_utils.get_bucket_marker(name)
        if marker:
            ctx["bucket_markers"][name] = marker
        return name

    yield _create

    for name in created:
        dedup_utils.cleanup_bucket(s3_client, name)


@pytest.fixture(scope="session", autouse=True)
def dedup_min_size_4k():
    dedup_utils.set_dedup_config("rgw_dedup_min_obj_size_for_dedup", "4096")
    yield
    dedup_utils.reset_dedup_config("rgw_dedup_min_obj_size_for_dedup")


@pytest.fixture(autouse=True)
def dedup_session_reset():
    """Abort any leftover dedup session before each test."""
    try:
        dedup_utils.run_dedup_abort()
    except (AssertionError, Exception):
        pass
    time.sleep(2)
    yield


@pytest.fixture
def admin_user(rgw_config, ssh_con, io_info, request):
    import json as _json

    admin_user = utils.exec_shell_cmd(
        "radosgw-admin user create --uid=dedup-admin --display-name='Dedup Admin' "
        "--caps='dedup=*'"
    )
    if admin_user is False:
        admin_user = utils.exec_shell_cmd("radosgw-admin user info --uid=dedup-admin")
    admin_info = _json.loads(admin_user)
    user_info = {
        "user_id": "dedup-admin",
        "access_key": admin_info["keys"][0]["access_key"],
        "secret_key": admin_info["keys"][0]["secret_key"],
    }
    ctx = _get_ctx(request.node.nodeid)
    ctx["users"].append(user_info)
    return user_info


@pytest.fixture
def endpoint_url(rgw_config, ssh_con, io_info, request):
    user_info = s3lib.create_users(1)[0]
    ctx = _get_ctx(request.node.nodeid)
    ctx["users"].append(user_info)
    auth = reusable.get_auth(
        user_info,
        ssh_con,
        rgw_config.ssl,
        getattr(rgw_config, "haproxy", False),
    )
    return auth.endpoint_url


@pytest.fixture(autouse=True)
def step_recorder(request):
    test_name = request.node.name
    scenario_info = DEDUP_TEST_SCENARIOS.get(test_name, {})
    test_id = scenario_info.get("id", "?")
    goal = scenario_info.get("scenario", request.node.obj.__doc__ or "No description")
    recorder = dedup_utils.TestStepRecorder(test_id, goal)
    ctx = _get_ctx(request.node.nodeid)
    ctx["recorder"] = recorder
    dedup_utils._active_recorder = recorder
    yield recorder
    dedup_utils._active_recorder = None


@pytest.fixture
def test_context(request):
    return _get_ctx(request.node.nodeid)


def _do_post_pass_cleanup(node_id):
    ctx = _test_context.get(node_id)
    if not ctx:
        return
    log.info("=" * 60)
    log.info("POST-PASS CLEANUP")
    log.info("=" * 60)

    for bkt_name, marker in ctx.get("bucket_markers", {}).items():
        log.info(f"RADOS cleanup for bucket {bkt_name} (marker={marker})")
        dedup_utils.cleanup_rados_objects_by_marker(marker)

    purged_uids = set()
    for user_info in ctx.get("users", []):
        uid = user_info.get("user_id", "")
        if uid and uid not in purged_uids:
            dedup_utils.purge_user(uid)
            purged_uids.add(uid)

    log.info("POST-PASS CLEANUP COMPLETE")
    log.info("=" * 60)
    _test_context.pop(node_id, None)


# ---------------------------------------------------------------------------
# Pytest hooks for test scenario logging and summary
# ---------------------------------------------------------------------------

_test_results = []


def pytest_runtest_setup(item):
    test_name = item.name
    scenario_info = DEDUP_TEST_SCENARIOS.get(test_name, {})
    test_id = scenario_info.get("id", "?")
    category = scenario_info.get("category", "Unknown")
    scenario = scenario_info.get("scenario", item.obj.__doc__ or "No description")
    log.info("=" * 80)
    log.info(f"TEST START: [{test_id}] {test_name}")
    log.info(f"  Category : {category}")
    log.info(f"  Scenario : {scenario}")
    log.info("=" * 80)


def pytest_runtest_makereport(item, call):
    if call.when == "call":
        test_name = item.name
        node_id = item.nodeid
        scenario_info = DEDUP_TEST_SCENARIOS.get(test_name, {})
        test_id = scenario_info.get("id", "?")
        category = scenario_info.get("category", "Unknown")
        scenario = scenario_info.get("scenario", "")
        duration = round(call.duration, 2)

        if call.excinfo is None:
            status = "PASSED"
            error_msg = ""
            _test_passed[node_id] = True
        else:
            status = "FAILED"
            error_msg = str(call.excinfo.value)[:120]
            _test_passed[node_id] = False

        _test_results.append(
            {
                "id": test_id,
                "name": test_name,
                "category": category,
                "scenario": scenario,
                "status": status,
                "duration": duration,
                "error": error_msg,
            }
        )

        ctx = _test_context.get(node_id, {})
        recorder = ctx.get("recorder")
        if recorder:
            for line in recorder.get_report_lines(status, duration, error_msg):
                log.info(line)

        log.info("-" * 80)
        log.info(f"TEST RESULT: [{test_id}] {test_name} => {status} ({duration}s)")
        if error_msg:
            log.info(f"  Error: {error_msg}")
        log.info("-" * 80)

    elif call.when == "teardown":
        node_id = item.nodeid
        if _test_passed.get(node_id, False):
            _do_post_pass_cleanup(node_id)
        else:
            _test_context.pop(node_id, None)
        _test_passed.pop(node_id, None)


DEDUP_REPORT_PATH = "/tmp/dedup_test_report.json"


def pytest_sessionfinish(session, exitstatus):
    """Write per-test results to JSON for cephci to pick up."""
    if not _test_results:
        return

    report = {
        "results": [],
        "savings": {},
        "exit_status": exitstatus,
    }

    for r in _test_results:
        entry = dict(r)
        node_id = f"test_dedup_pytest.py::{r['name']}"
        ctx = _test_context.get(node_id, {})
        recorder = ctx.get("recorder")
        if recorder:
            entry["steps"] = list(recorder.steps)
        report["results"].append(entry)

    for test_id, s in dedup_utils._savings_registry.items():
        report["savings"][test_id] = s

    try:
        with open(DEDUP_REPORT_PATH, "w") as f:
            json.dump(report, f, indent=2, default=str)
        log.info("Wrote dedup test report to %s", DEDUP_REPORT_PATH)
    except Exception as e:
        log.warning("Failed to write dedup test report JSON: %s", e)


def pytest_terminal_summary(terminalreporter, exitstatus, config):
    if not _test_results:
        return

    passed = [r for r in _test_results if r["status"] == "PASSED"]
    failed = [r for r in _test_results if r["status"] == "FAILED"]

    summary_lines = [
        "",
        "=" * 100,
        "DEDUP TEST SUITE SUMMARY",
        "=" * 100,
        f"{'ID':<5} {'Category':<14} {'Test Name':<42} {'Status':<8} {'Duration':<10} {'Error'}",
        "-" * 100,
    ]
    for r in _test_results:
        err_short = r["error"][:40] + "..." if len(r["error"]) > 40 else r["error"]
        summary_lines.append(
            f"{r['id']:<5} {r['category']:<14} {r['name']:<42} {r['status']:<8} {r['duration']:<10} {err_short}"
        )
    summary_lines.append("-" * 100)
    summary_lines.append(
        f"Total: {len(_test_results)} | Passed: {len(passed)} | Failed: {len(failed)}"
    )
    summary_lines.append("=" * 100)

    if failed:
        summary_lines.append("")
        summary_lines.append("FAILED TEST DETAILS:")
        summary_lines.append("-" * 100)
        for r in failed:
            summary_lines.append(f"  [{r['id']}] {r['name']}")
            summary_lines.append(f"       Scenario: {r['scenario']}")
            summary_lines.append(f"       Error   : {r['error']}")
            summary_lines.append("")

    if dedup_utils._savings_registry:
        summary_lines.append("")
        summary_lines.append("DEDUP STORAGE SAVINGS:")
        summary_lines.append("-" * 100)
        summary_lines.append(
            f"{'ID':<5} {'Uploaded':<14} {'Deduped Bytes':<14} {'Savings %':<12} "
            f"{'Ratio':<10} {'Deduped Objs':<14} {'Skipped Comp'}"
        )
        summary_lines.append("-" * 100)
        for test_id, s in sorted(dedup_utils._savings_registry.items()):
            ratio_str = f"{s['dedup_ratio']:.2f}x" if s["dedup_ratio"] > 0 else "N/A"
            comp_str = (
                str(s["skipped_compressed"]) if s["skipped_compressed"] > 0 else "-"
            )
            summary_lines.append(
                f"{s['test_id']:<5} "
                f"{dedup_utils._format_bytes(s['total_uploaded']):<14} "
                f"{dedup_utils._format_bytes(s['deduped_bytes']):<14} "
                f"{s['savings_pct']:<11.1f}% "
                f"{ratio_str:<10} "
                f"{s['deduped_count']:<14} "
                f"{comp_str}"
            )
        summary_lines.append("=" * 100)

    summary_text = "\n".join(summary_lines)
    log.info(summary_text)
    terminalreporter.write_line(summary_text)
