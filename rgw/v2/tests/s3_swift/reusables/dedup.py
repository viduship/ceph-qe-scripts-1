"""
Reusable helper functions for RGW dedup (deduplication) test automation.

Provides wrappers around radosgw-admin dedup commands, Admin OPS API calls,
identical object upload utilities, and integrity verification helpers.
"""

import base64
import hashlib
import json
import logging
import os
import random
import string
import subprocess
import tempfile
import time
from threading import Thread

import v2.utils.utils as utils

log = logging.getLogger()

DEDUP_POLL_INTERVAL = 10
DEDUP_DEFAULT_TIMEOUT = 600

# ---------------------------------------------------------------------------
# Step recorder for per-test structured reports
# ---------------------------------------------------------------------------

_active_recorder = None


def _record_step(action, command="", result=""):
    if _active_recorder is not None:
        _active_recorder.record(action, command, result)


class TestStepRecorder:
    def __init__(self, test_id, goal):
        self.test_id = test_id
        self.goal = goal
        self.steps = []

    def record(self, action, command="", result=""):
        self.steps.append(
            {
                "action": action,
                "command": command,
                "result": result,
            }
        )

    def get_report_lines(self, status, duration, error=""):
        lines = [
            "=" * 90,
            f"TEST REPORT: [{self.test_id}] — {status} ({duration}s)",
            f"  Goal: {self.goal}",
            "-" * 90,
            "  Steps:",
        ]
        for i, s in enumerate(self.steps, 1):
            lines.append(f"    {i}. {s['action']}")
            if s["command"]:
                lines.append(f"       cmd: {s['command']}")
            if s["result"]:
                lines.append(f"       result: {s['result']}")
        lines.append("-" * 90)
        if error:
            lines.append(f"  FAILURE: {error}")
        else:
            lines.append("  Result: All assertions passed — EXPECTED")
        lines.append("=" * 90)
        return lines


# ---------------------------------------------------------------------------
# Post-test cleanup helpers
# ---------------------------------------------------------------------------


def get_bucket_marker(bucket_name):
    cmd = f"radosgw-admin bucket stats --bucket={bucket_name}"
    out = utils.exec_shell_cmd(cmd)
    if out is False:
        log.warning(f"Could not get marker for bucket {bucket_name}")
        return None
    try:
        info = json.loads(out)
        marker = info.get("marker", "")
        log.info(f"Bucket {bucket_name} marker: {marker}")
        return marker
    except json.JSONDecodeError:
        log.warning(f"Could not parse bucket stats for {bucket_name}")
        return None


def cleanup_rados_objects_by_marker(marker, pool="default.rgw.buckets.data"):
    if not marker:
        return
    objects = get_rados_objects(pool, prefix=marker)
    if not objects:
        log.info(f"No RADOS objects found for marker {marker}")
        return
    log.info(f"Removing {len(objects)} RADOS objects for marker {marker}")
    for oid in objects:
        cmd = f"rados -p {pool} rm {oid}"
        utils.exec_shell_cmd(cmd)
    log.info(f"RADOS cleanup done for marker {marker}")


def purge_user(user_id):
    cmd = f"radosgw-admin user rm --purge-keys --purge-data --uid={user_id}"
    log.info(f"Purging user: {cmd}")
    out = utils.exec_shell_cmd(cmd)
    if out is False:
        log.warning(f"Failed to purge user {user_id} (may already be gone)")
    else:
        log.info(f"User {user_id} purged successfully")


def _abort_previous_session():
    """Abort any leftover dedup session so stats are reset for the new run."""
    log.info("Aborting any previous dedup session to reset stats")
    utils.exec_shell_cmd("radosgw-admin dedup abort")
    time.sleep(2)


def run_dedup_estimate(
    allow_bucket_file=None,
    deny_bucket_file=None,
    allow_sc_file=None,
    deny_sc_file=None,
):
    _abort_previous_session()
    cmd = "radosgw-admin dedup estimate"
    if allow_bucket_file:
        cmd += f" --allow-bucket-list {allow_bucket_file}"
    if deny_bucket_file:
        cmd += f" --deny-bucket-list {deny_bucket_file}"
    if allow_sc_file:
        cmd += f" --allow-storage-class-list {allow_sc_file}"
    if deny_sc_file:
        cmd += f" --deny-storage-class-list {deny_sc_file}"
    log.info(f"Running dedup estimate: {cmd}")
    _record_step("Run dedup estimate", cmd)
    out = utils.exec_shell_cmd(cmd)
    if out is False:
        raise AssertionError("dedup estimate command failed")
    return out


def run_dedup_execute(
    allow_bucket_file=None,
    deny_bucket_file=None,
    allow_sc_file=None,
    deny_sc_file=None,
):
    _abort_previous_session()
    cmd = "radosgw-admin dedup exec --yes-i-really-mean-it"
    if allow_bucket_file:
        cmd += f" --allow-bucket-list {allow_bucket_file}"
    if deny_bucket_file:
        cmd += f" --deny-bucket-list {deny_bucket_file}"
    if allow_sc_file:
        cmd += f" --allow-storage-class-list {allow_sc_file}"
    if deny_sc_file:
        cmd += f" --deny-storage-class-list {deny_sc_file}"
    log.info(f"Running dedup exec: {cmd}")
    _record_step("Run dedup exec", cmd)
    out = utils.exec_shell_cmd(cmd)
    if out is False:
        raise AssertionError("dedup exec command failed")
    return out


def run_dedup_pause():
    log.info("Pausing dedup session")
    _record_step("Pause dedup session", "radosgw-admin dedup pause")
    out = utils.exec_shell_cmd("radosgw-admin dedup pause")
    if out is False:
        raise AssertionError("dedup pause command failed")
    return out


def run_dedup_resume():
    log.info("Resuming dedup session")
    _record_step("Resume dedup session", "radosgw-admin dedup resume")
    out = utils.exec_shell_cmd("radosgw-admin dedup resume")
    if out is False:
        raise AssertionError("dedup resume command failed")
    return out


def run_dedup_abort():
    log.info("Aborting dedup session")
    _record_step("Abort dedup session", "radosgw-admin dedup abort")
    out = utils.exec_shell_cmd("radosgw-admin dedup abort")
    if out is False:
        raise AssertionError("dedup abort command failed")
    return out


def get_dedup_stats():
    log.info("Getting dedup stats")
    out = utils.exec_shell_cmd("radosgw-admin dedup stats")
    if out is False:
        raise AssertionError("dedup stats command failed")
    try:
        stats = json.loads(out)
    except json.JSONDecodeError:
        log.warning(f"Could not parse dedup stats as JSON: {out}")
        return out
    log.info(f"Dedup stats: {json.dumps(stats, indent=2)}")
    return stats


def set_dedup_throttle(max_bucket_index_ops=None, max_metadata_ops=None):
    cmd = "radosgw-admin dedup throttle"
    if max_bucket_index_ops is not None:
        cmd += f" --max-bucket-index-ops={max_bucket_index_ops}"
    if max_metadata_ops is not None:
        cmd += f" --max-metadata-ops={max_metadata_ops}"
    log.info(f"Setting dedup throttle: {cmd}")
    out = utils.exec_shell_cmd(cmd)
    if out is False:
        raise AssertionError("dedup throttle set command failed")
    return out


def get_dedup_throttle():
    log.info("Getting dedup throttle settings")
    out = utils.exec_shell_cmd("radosgw-admin dedup throttle --stat")
    if out is False:
        raise AssertionError("dedup throttle stat command failed")
    return out


def wait_for_dedup_completion(
    timeout=DEDUP_DEFAULT_TIMEOUT, poll_interval=DEDUP_POLL_INTERVAL
):
    log.info(f"Waiting for dedup completion (timeout={timeout}s)")
    start = time.time()

    # Phase 1: Wait for scan to start (completed=false means scan is running)
    log.info("Phase 1: Waiting for dedup scan to start...")
    scan_started = False
    while time.time() - start < timeout:
        stats = get_dedup_stats()
        if isinstance(stats, dict):
            completed = stats.get("completed", False)
            worker = stats.get("worker_stats", {}).get("main", {})
            md5 = stats.get("md5_stats", {}).get("main", {})
            ingress = worker.get("Ingress Objs count", 0)
            processed = md5.get("Total processed objects", 0)

            if not completed:
                log.info(
                    f"Scan started (completed=false): "
                    f"ingress={ingress}, processed={processed}"
                )
                scan_started = True
                break

            if completed and (ingress > 0 or processed > 0):
                log.info(
                    f"Scan already completed with data: "
                    f"ingress={ingress}, processed={processed}"
                )
                scan_started = True
                break

            elapsed_phase1 = time.time() - start
            if elapsed_phase1 > 30:
                log.info(
                    "Scan shows completed=true with 0 objects after 30s — "
                    "accepting as valid (empty bucket or instant completion)"
                )
                scan_started = True
                break

            log.info(
                "Scan not started yet (completed=true, 0 objects) — "
                "waiting for async scan to kick in..."
            )
        time.sleep(poll_interval)

    if not scan_started:
        raise AssertionError(f"Dedup scan did not start within {timeout}s")

    # Phase 2: Wait for scan to complete (completed=true with data)
    log.info("Phase 2: Waiting for dedup scan to complete...")
    while time.time() - start < timeout:
        stats = get_dedup_stats()
        if isinstance(stats, dict):
            completed = stats.get("completed", False)
            worker = stats.get("worker_stats", {}).get("main", {})
            md5 = stats.get("md5_stats", {}).get("main", {})
            ingress = worker.get("Ingress Objs count", 0)
            processed = md5.get("Total processed objects", 0)
            deduped = md5.get("Deduped Obj (this cycle)", 0)

            if completed:
                elapsed = int(time.time() - start)
                log.info(
                    f"Dedup completed (took {elapsed}s): "
                    f"ingress={ingress}, processed={processed}, "
                    f"deduped={deduped}"
                )
                _record_step(
                    "Dedup completed",
                    "radosgw-admin dedup stats",
                    f"ingress={ingress}, processed={processed}, deduped={deduped} ({elapsed}s)",
                )
                return stats

            log.info(
                f"Dedup in progress: ingress={ingress}, "
                f"processed={processed}, deduped={deduped}"
            )
        time.sleep(poll_interval)
    raise AssertionError(f"Dedup did not complete within {timeout}s")


def generate_identical_data(size_bytes):
    random.seed(42)
    return random.randbytes(size_bytes)


def upload_identical_objects(
    s3_client, bucket_name, count, size_bytes, prefix="dedup-obj"
):
    data = generate_identical_data(size_bytes)
    md5_hash = hashlib.md5(data).hexdigest()
    log.info(
        f"Uploading {count} identical objects of size {size_bytes} bytes "
        f"(MD5: {md5_hash}) to bucket {bucket_name}"
    )
    _record_step(
        f"Upload {count} identical {size_bytes}B objects to {bucket_name}",
        f"s3_client.put_object x{count}",
        f"MD5={md5_hash}",
    )
    keys = []
    for i in range(count):
        key = f"{prefix}-{i}"
        s3_client.put_object(Bucket=bucket_name, Key=key, Body=data)
        keys.append(key)
        log.info(f"Uploaded {key}")
    return keys, md5_hash, data


def upload_identical_multipart_objects(
    s3_client,
    bucket_name,
    count,
    size_bytes,
    prefix="dedup-mp",
    part_size=5 * 1024 * 1024,
):
    data = generate_identical_data(size_bytes)
    md5_hash = hashlib.md5(data).hexdigest()
    log.info(
        f"Uploading {count} identical multipart objects of size {size_bytes} bytes "
        f"to bucket {bucket_name}"
    )
    _record_step(
        f"Upload {count} identical multipart {size_bytes}B objects to {bucket_name}",
        f"s3_client.create_multipart_upload x{count}",
        f"MD5={md5_hash}",
    )
    keys = []
    for i in range(count):
        key = f"{prefix}-{i}"
        mpu = s3_client.create_multipart_upload(Bucket=bucket_name, Key=key)
        upload_id = mpu["UploadId"]

        parts = []
        offset = 0
        part_number = 1
        while offset < size_bytes:
            end = min(offset + part_size, size_bytes)
            part_data = data[offset:end]
            resp = s3_client.upload_part(
                Bucket=bucket_name,
                Key=key,
                UploadId=upload_id,
                PartNumber=part_number,
                Body=part_data,
            )
            parts.append({"ETag": resp["ETag"], "PartNumber": part_number})
            offset = end
            part_number += 1

        s3_client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts},
        )
        keys.append(key)
        log.info(f"Uploaded multipart {key} ({part_number - 1} parts)")
    return keys, md5_hash, data


def upload_ssec_objects(s3_client, bucket_name, count, size_bytes, prefix="dedup-ssec"):
    data = generate_identical_data(size_bytes)
    sse_key = os.urandom(32)
    sse_key_b64 = base64.b64encode(sse_key).decode("utf-8")
    sse_key_md5 = base64.b64encode(hashlib.md5(sse_key).digest()).decode("utf-8")
    log.info(f"Uploading {count} SSE-C encrypted objects to bucket {bucket_name}")
    _record_step(
        f"Upload {count} SSE-C encrypted objects to {bucket_name}",
        f"s3_client.put_object x{count} (SSE-C AES256)",
    )
    keys = []
    for i in range(count):
        key = f"{prefix}-{i}"
        s3_client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=data,
            SSECustomerAlgorithm="AES256",
            SSECustomerKey=sse_key_b64,
            SSECustomerKeyMD5=sse_key_md5,
        )
        keys.append(key)
        log.info(f"Uploaded SSE-C encrypted {key}")
    return keys, sse_key_b64, sse_key_md5


def verify_object_integrity(s3_client, bucket_name, key, expected_md5):
    log.info(f"Verifying integrity of {bucket_name}/{key}")
    resp = s3_client.get_object(Bucket=bucket_name, Key=key)
    body = resp["Body"].read()
    actual_md5 = hashlib.md5(body).hexdigest()
    if actual_md5 != expected_md5:
        raise AssertionError(
            f"MD5 mismatch for {key}: expected {expected_md5}, got {actual_md5}"
        )
    log.info(f"Integrity OK for {key}: MD5={actual_md5}")
    return resp.get("ETag", "").strip('"')


def verify_all_objects_accessible(s3_client, bucket_name, keys):
    log.info(f"Verifying all {len(keys)} objects are accessible in {bucket_name}")
    _record_step(f"Verify {len(keys)} objects accessible in {bucket_name}")
    for key in keys:
        resp = s3_client.head_object(Bucket=bucket_name, Key=key)
        status = resp["ResponseMetadata"]["HTTPStatusCode"]
        if status != 200:
            raise AssertionError(f"Object {key} not accessible, status: {status}")
    log.info(f"All {len(keys)} objects accessible")


def verify_all_objects_integrity(s3_client, bucket_name, keys, expected_md5):
    log.info(f"Verifying integrity of {len(keys)} objects in {bucket_name}")
    _record_step(f"Verify MD5 integrity for {len(keys)} objects in {bucket_name}")
    etags = {}
    for key in keys:
        etag = verify_object_integrity(s3_client, bucket_name, key, expected_md5)
        etags[key] = etag
    log.info(f"All {len(keys)} objects passed integrity check")
    return etags


def verify_range_get(
    s3_client, bucket_name, key, original_data, range_start, range_end
):
    log.info(f"Verifying range GET for {key} bytes={range_start}-{range_end}")
    resp = s3_client.get_object(
        Bucket=bucket_name, Key=key, Range=f"bytes={range_start}-{range_end}"
    )
    range_data = resp["Body"].read()
    expected_data = original_data[range_start : range_end + 1]
    if range_data != expected_data:
        raise AssertionError(
            f"Range GET mismatch for {key}: got {len(range_data)} bytes, "
            f"expected {len(expected_data)} bytes"
        )
    log.info(f"Range GET OK for {key}")


def create_filter_list_file(items, filepath=None):
    if filepath is None:
        fd, filepath = tempfile.mkstemp(suffix=".txt", prefix="dedup_filter_")
        os.close(fd)
    with open(filepath, "w") as f:
        for item in items:
            f.write(f"{item}\n")
    log.info(f"Created filter list file at {filepath} with {len(items)} entries")
    return filepath


def ensure_dedup_caps(uid):
    cmd = f"radosgw-admin caps add --uid={uid} --caps='dedup=*'"
    log.info(f"Granting dedup caps: {cmd}")
    out = utils.exec_shell_cmd(cmd)
    if out is False:
        log.warning(f"Failed to add dedup caps to {uid}, may already exist")


def dedup_api_request(
    endpoint_url, op, method=None, access_key=None, secret_key=None, params=None
):
    import requests
    from botocore.auth import HmacV1Auth
    from botocore.awsrequest import AWSRequest
    from botocore.credentials import Credentials

    if method is None:
        method = "GET" if op in ("stats",) else "POST"
    if op == "throttle" and method is None:
        method = "GET"

    url = f"{endpoint_url}/admin/dedup"
    if params is None:
        params = {}
    params["op"] = op

    log.info(f"Dedup API request: {method} {url} params={params}")
    _record_step(f"Dedup API {method} op={op}", f"{method} {url} params={params}")

    credentials = Credentials(access_key, secret_key)
    full_url = f"{url}?{requests.compat.urlencode(params)}"
    request = AWSRequest(method=method, url=full_url)
    HmacV1Auth(credentials).add_auth(request)

    response = requests.request(
        method=method,
        url=url,
        headers=dict(request.headers),
        params=params,
        verify=False,
    )
    log.info(f"API response status: {response.status_code}")
    log.info(f"API response body: {response.text[:500]}")
    return response


def run_concurrent_s3_workload(
    s3_client, bucket_name, duration_secs=30, prefix="concurrent"
):
    results = {"puts": 0, "gets": 0, "deletes": 0, "errors": 0}
    stop_flag = {"stop": False}

    def workload():
        obj_counter = 0
        created_keys = []
        while not stop_flag["stop"]:
            try:
                key = f"{prefix}-{obj_counter}"
                data = os.urandom(random.randint(1024, 10240))
                s3_client.put_object(Bucket=bucket_name, Key=key, Body=data)
                results["puts"] += 1
                created_keys.append(key)
                obj_counter += 1

                if created_keys:
                    get_key = random.choice(created_keys)
                    resp = s3_client.get_object(Bucket=bucket_name, Key=get_key)
                    resp["Body"].read()
                    resp["Body"].close()
                    results["gets"] += 1

                if len(created_keys) > 10:
                    del_key = created_keys.pop(0)
                    s3_client.delete_object(Bucket=bucket_name, Key=del_key)
                    results["deletes"] += 1

                time.sleep(0.05)
            except Exception as e:
                log.warning(f"Concurrent workload error: {e}")
                results["errors"] += 1

    thread = Thread(target=workload, daemon=True)
    thread.start()
    time.sleep(duration_secs)
    stop_flag["stop"] = True
    thread.join(timeout=10)

    log.info(
        f"Concurrent workload results: {results['puts']} puts, "
        f"{results['gets']} gets, {results['deletes']} deletes, "
        f"{results['errors']} errors"
    )
    return results


def set_lifecycle_expiration(s3_client, bucket_name, days=1, prefix=""):
    lc_config = {
        "Rules": [
            {
                "ID": "dedup-lc-expiration",
                "Filter": {"Prefix": prefix},
                "Status": "Enabled",
                "Expiration": {"Days": days},
            }
        ]
    }
    s3_client.put_bucket_lifecycle_configuration(
        Bucket=bucket_name, LifecycleConfiguration=lc_config
    )
    _record_step(f"Set lifecycle expiration ({days} days) on {bucket_name}")
    log.info(f"Set LC expiration of {days} day(s) on bucket {bucket_name}")


def enable_bucket_versioning(s3_client, bucket_name):
    s3_client.put_bucket_versioning(
        Bucket=bucket_name,
        VersioningConfiguration={"Status": "Enabled"},
    )
    _record_step(f"Enable versioning on {bucket_name}")
    log.info(f"Enabled versioning on bucket {bucket_name}")


def get_all_versions(s3_client, bucket_name, key):
    resp = s3_client.list_object_versions(Bucket=bucket_name, Prefix=key)
    versions = resp.get("Versions", [])
    log.info(f"Found {len(versions)} versions of {key} in {bucket_name}")
    return versions


def cleanup_bucket(s3_client, bucket_name):
    log.info(f"Cleaning up bucket {bucket_name}")
    try:
        resp = s3_client.list_object_versions(Bucket=bucket_name)
        versions = resp.get("Versions", [])
        delete_markers = resp.get("DeleteMarkers", [])
        objects_to_delete = []
        for v in versions:
            objects_to_delete.append({"Key": v["Key"], "VersionId": v["VersionId"]})
        for dm in delete_markers:
            objects_to_delete.append({"Key": dm["Key"], "VersionId": dm["VersionId"]})
        if objects_to_delete:
            for i in range(0, len(objects_to_delete), 1000):
                batch = objects_to_delete[i : i + 1000]
                s3_client.delete_objects(Bucket=bucket_name, Delete={"Objects": batch})
    except Exception:
        try:
            resp = s3_client.list_objects_v2(Bucket=bucket_name)
            if "Contents" in resp:
                for obj in resp["Contents"]:
                    s3_client.delete_object(Bucket=bucket_name, Key=obj["Key"])
        except Exception as e:
            log.warning(f"Cleanup list/delete failed: {e}")
    try:
        s3_client.delete_bucket(Bucket=bucket_name)
        log.info(f"Deleted bucket {bucket_name}")
    except Exception as e:
        log.warning(f"Failed to delete bucket {bucket_name}: {e}")


# --- Compression helpers ---


def get_zone_name():
    out = utils.exec_shell_cmd("radosgw-admin zone get")
    if out is False:
        raise AssertionError("Failed to get zone info")
    return json.loads(out).get("name")


def enable_zone_compression(compression_type="zlib", ssh_con=None):
    zone_name = get_zone_name()
    cmd = (
        f"radosgw-admin zone placement modify --rgw-zone={zone_name} "
        f"--placement-id=default-placement "
        f"--storage-class=STANDARD "
        f"--compression={compression_type}"
    )
    log.info(f"Enabling compression: {cmd}")
    _record_step(f"Enable {compression_type} compression on zone {zone_name}", cmd)
    out = utils.exec_shell_cmd(cmd)
    if out is False:
        raise AssertionError(f"Failed to enable {compression_type} compression")
    _commit_period()
    _restart_rgw(ssh_con)
    return zone_name


def disable_zone_compression(ssh_con=None):
    zone_name = get_zone_name()
    cmd = (
        f"radosgw-admin zone placement modify --rgw-zone={zone_name} "
        f"--placement-id=default-placement "
        f"--storage-class=STANDARD "
        f"--compression=none"
    )
    log.info(f"Disabling compression: {cmd}")
    _record_step("Disable compression", cmd)
    out = utils.exec_shell_cmd(cmd)
    if out is False:
        raise AssertionError("Failed to disable compression")
    _commit_period()
    _restart_rgw(ssh_con)


def _commit_period():
    out = utils.exec_shell_cmd("radosgw-admin period update --commit")
    if out is False:
        log.warning("period update --commit failed (may be non-multisite, ignoring)")
    else:
        log.info("Period updated and committed")


def _restart_rgw(ssh_con=None):
    out = utils.exec_shell_cmd("ceph orch ls --service-type rgw --format json")
    if out is False:
        raise AssertionError("Failed to list RGW services")
    services = json.loads(out)
    svc_names = [s.get("service_name", "") for s in services]
    total_expected = sum(s.get("status", {}).get("size", 0) for s in services)

    ps_out = utils.exec_shell_cmd("ceph orch ps --daemon_type rgw --format json")
    old_started = {}
    if ps_out and ps_out is not False:
        for d in json.loads(ps_out):
            old_started[d["daemon_name"]] = d.get("started", "")

    log.info(f"Restarting all RGW services: {svc_names} ({total_expected} daemons)")
    for svc in svc_names:
        utils.exec_shell_cmd(f"ceph orch restart {svc}")

    timeout = max(180, total_expected * 20)
    poll_interval = 15
    deadline = time.time() + timeout
    while time.time() < deadline:
        time.sleep(poll_interval)
        ps_out = utils.exec_shell_cmd("ceph orch ps --daemon_type rgw --format json")
        if ps_out is False:
            continue
        daemons = json.loads(ps_out)
        running = [d for d in daemons if d.get("status_desc") == "running"]
        restarted = 0
        for d in running:
            name = d["daemon_name"]
            cur_started = d.get("started", "")
            if cur_started and cur_started != old_started.get(name, ""):
                restarted += 1
        log.info(
            f"RGW restart progress: {restarted}/{total_expected} "
            f"daemons restarted, {len(running)} running"
        )
        if restarted >= total_expected and len(running) >= total_expected:
            log.info("All RGW daemons restarted successfully")
            time.sleep(5)
            return
    log.warning(f"RGW restart timed out after {timeout}s — proceeding anyway")


# --- Storage class helpers ---


def get_zonegroup_name():
    out = utils.exec_shell_cmd("radosgw-admin zonegroup get")
    if out is False:
        raise AssertionError("Failed to get zonegroup info")
    return json.loads(out).get("name")


def setup_storage_class(sc_name, pool_name, ssh_con=None):
    zone = get_zone_name()
    zonegroup = get_zonegroup_name()

    log.info(f"Creating pool {pool_name}")
    out = utils.exec_shell_cmd(f"ceph osd pool create {pool_name}")
    if out is False:
        raise AssertionError(f"Failed to create pool {pool_name}")

    log.info(f"Enabling rgw application on pool {pool_name}")
    out = utils.exec_shell_cmd(f"ceph osd pool application enable {pool_name} rgw")
    if out is False:
        raise AssertionError(f"Failed to enable rgw app on pool {pool_name}")

    log.info(f"Adding storage class {sc_name} to zonegroup {zonegroup} placement")
    out = utils.exec_shell_cmd(
        f"radosgw-admin zonegroup placement add "
        f"--rgw-zonegroup {zonegroup} "
        f"--placement-id default-placement "
        f"--storage-class {sc_name}"
    )
    if out is False:
        raise AssertionError(
            f"Failed to add storage class {sc_name} to zonegroup placement"
        )

    log.info(
        f"Adding storage class {sc_name} to zone {zone} placement "
        f"with data-pool {pool_name}"
    )
    out = utils.exec_shell_cmd(
        f"radosgw-admin zone placement add "
        f"--rgw-zone {zone} "
        f"--placement-id default-placement "
        f"--storage-class {sc_name} "
        f"--data-pool {pool_name} "
        f"--compression none"
    )
    if out is False:
        raise AssertionError(f"Failed to add storage class {sc_name} to zone placement")

    _commit_period()
    _restart_rgw(ssh_con)
    _record_step(f"Setup storage class {sc_name} with pool {pool_name}")
    log.info(f"Storage class {sc_name} setup complete (pool={pool_name})")


def teardown_storage_class(sc_name, pool_name, ssh_con=None):
    zone = get_zone_name()
    zonegroup = get_zonegroup_name()

    log.info(f"Removing storage class {sc_name} from zone {zone} placement")
    utils.exec_shell_cmd(
        f"radosgw-admin zone placement rm "
        f"--rgw-zone {zone} "
        f"--placement-id default-placement "
        f"--storage-class {sc_name}"
    )

    log.info(f"Removing storage class {sc_name} from zonegroup {zonegroup} placement")
    utils.exec_shell_cmd(
        f"radosgw-admin zonegroup placement rm "
        f"--rgw-zonegroup {zonegroup} "
        f"--placement-id default-placement "
        f"--storage-class {sc_name}"
    )

    log.info(f"Removing pool {pool_name}")
    utils.exec_shell_cmd("ceph config set mon mon_allow_pool_delete true")
    utils.exec_shell_cmd(
        f"ceph osd pool rm {pool_name} {pool_name} " f"--yes-i-really-really-mean-it"
    )
    utils.exec_shell_cmd("ceph config set mon mon_allow_pool_delete false")

    _commit_period()
    _restart_rgw(ssh_con)
    _record_step(f"Teardown storage class {sc_name}, remove pool {pool_name}")
    log.info(f"Storage class {sc_name} teardown complete")


def set_storage_class_compression(sc_name, compression_type, ssh_con=None):
    zone = get_zone_name()
    cmd = (
        f"radosgw-admin zone placement modify --rgw-zone={zone} "
        f"--placement-id=default-placement --storage-class={sc_name} "
        f"--compression={compression_type}"
    )
    log.info(f"Setting {sc_name} compression to {compression_type}")
    _record_step(f"Set {sc_name} compression to {compression_type}", cmd)
    out = utils.exec_shell_cmd(cmd)
    if out is False:
        raise AssertionError(
            f"Failed to set compression {compression_type} on SC {sc_name}"
        )
    _commit_period()
    _restart_rgw(ssh_con)


def get_zone_compression_setting(sc_name="STANDARD"):
    out = utils.exec_shell_cmd("radosgw-admin zone get")
    if out is False:
        raise AssertionError("Failed to get zone info")
    zone_info = json.loads(out)
    for placement in zone_info.get("placement_pools", []):
        if placement.get("key") == "default-placement":
            sc_dict = placement.get("val", {}).get("storage_classes", {})
            if sc_name in sc_dict:
                return sc_dict[sc_name].get("compression_type", "")
    return ""


def is_object_compressed(bucket_name, object_key):
    cmd = (
        f"radosgw-admin object stat " f"--bucket={bucket_name} --object='{object_key}'"
    )
    out = utils.exec_shell_cmd(cmd)
    if out is False:
        raise AssertionError(f"object stat failed for {bucket_name}/{object_key}")
    obj_info = json.loads(out)
    comp_type = obj_info.get("compression", {}).get("compression_type", "")
    return bool(comp_type)


def assert_all_objects_compression(bucket_name, keys, expect_compressed, tag=""):
    compressed_count = sum(1 for k in keys if is_object_compressed(bucket_name, k))
    uncompressed_count = len(keys) - compressed_count
    log.info(
        f"{tag} {bucket_name}: compressed={compressed_count}, "
        f"uncompressed={uncompressed_count}"
    )
    if expect_compressed:
        assert compressed_count == len(keys) and uncompressed_count == 0, (
            f"{tag} {bucket_name} should be all compressed: "
            f"compressed={compressed_count}, uncompressed={uncompressed_count}"
        )
    else:
        assert uncompressed_count == len(keys) and compressed_count == 0, (
            f"{tag} {bucket_name} should be all uncompressed: "
            f"compressed={compressed_count}, uncompressed={uncompressed_count}"
        )


def upload_identical_objects_with_sc(
    s3_client, bucket_name, count, size_bytes, storage_class, prefix="sc-obj"
):
    data = generate_identical_data(size_bytes)
    md5_hash = hashlib.md5(data).hexdigest()
    log.info(
        f"Uploading {count} identical objects ({size_bytes}B, SC={storage_class}) "
        f"to bucket {bucket_name}"
    )
    _record_step(
        f"Upload {count} identical {size_bytes}B objects (SC={storage_class}) "
        f"to {bucket_name}"
    )
    keys = []
    for i in range(count):
        key = f"{prefix}-{i}"
        s3_client.put_object(
            Bucket=bucket_name, Key=key, Body=data, StorageClass=storage_class
        )
        keys.append(key)
    return keys, md5_hash, data


def run_dedup_cmd_with_rc(cmd):
    log.info(f"Running dedup cmd (with rc): {cmd}")
    _record_step("Run dedup cmd (with rc)", cmd)
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    return result.stdout + result.stderr, result.returncode


def get_rgw_service_name():
    out = utils.exec_shell_cmd("ceph orch ls --service-type rgw --format json")
    if out is False:
        raise AssertionError("Failed to get RGW service list")
    services = json.loads(out)
    if not services:
        raise AssertionError("No RGW service found")
    svc_name = services[0].get("service_name", "")
    log.info(f"RGW service name: {svc_name}")
    return svc_name


def set_dedup_config(key, value):
    svc_name = get_rgw_service_name()
    config_section = f"client.{svc_name}"
    cmd = f"ceph config set {config_section} {key} {value}"
    log.info(f"Setting dedup config: {cmd}")
    _record_step(f"Set dedup config {key}={value}", cmd)
    out = utils.exec_shell_cmd(cmd)
    if out is False:
        raise AssertionError(f"Failed to set config {key}={value}")
    log.info("Restarting RGW after config change")
    utils.exec_shell_cmd(f"ceph orch restart {svc_name}")
    time.sleep(15)


def reset_dedup_config(key):
    svc_name = get_rgw_service_name()
    config_section = f"client.{svc_name}"
    cmd = f"ceph config rm {config_section} {key}"
    log.info(f"Removing dedup config override: {cmd}")
    _record_step(f"Reset dedup config {key}", cmd)
    utils.exec_shell_cmd(cmd)
    log.info("Restarting RGW after config removal")
    utils.exec_shell_cmd(f"ceph orch restart {svc_name}")
    time.sleep(15)


def get_object_content_length(s3_client, bucket_name, key):
    resp = s3_client.head_object(Bucket=bucket_name, Key=key)
    return resp["ContentLength"]


def get_rados_objects(pool="default.rgw.buckets.data", prefix=None):
    cmd = f"rados -p {pool} ls"
    out = utils.exec_shell_cmd(cmd)
    if out is False:
        raise AssertionError(f"Failed to list RADOS objects in pool {pool}")
    objects = [line.strip() for line in out.strip().split("\n") if line.strip()]
    if prefix:
        objects = [o for o in objects if prefix in o]
    log.info(f"Found {len(objects)} RADOS objects in {pool} (prefix={prefix})")
    return objects


def get_rados_object_stat(pool, oid):
    cmd = f"rados -p {pool} stat {oid}"
    out = utils.exec_shell_cmd(cmd)
    if out is False:
        raise AssertionError(f"Failed to stat RADOS object {oid}")
    return out


def parse_dedup_stats(stats):
    if not isinstance(stats, dict):
        return {}
    md5_main = stats.get("md5_stats", {}).get("main", {})
    md5_skipped = stats.get("md5_stats", {}).get("skipped", {})
    return {
        "deduped_count": md5_main.get("Deduped Obj (this cycle)", 0),
        "deduped_bytes": md5_main.get("Deduped Bytes(this cycle)", 0),
        "unique_count": md5_main.get("Unique Obj", 0),
        "duplicate_count": md5_main.get("Duplicate Obj", 0),
        "already_deduped_bytes": md5_main.get("Already Deduped bytes (prev cycles)", 0),
        "skipped_shared_manifest": md5_skipped.get("Skipped shared_manifest", 0),
        "completed": stats.get("completed", False),
    }


def parse_dedup_stats_full(stats):
    if not isinstance(stats, dict):
        return {}
    worker_main = stats.get("worker_stats", {}).get("main", {})
    worker_skipped = stats.get("worker_stats", {}).get("skipped", {})
    md5_main = stats.get("md5_stats", {}).get("main", {})
    md5_skipped = stats.get("md5_stats", {}).get("skipped", {})
    md5_notify = stats.get("md5_stats", {}).get("notify", {})
    return {
        "ingress_count": worker_main.get("Ingress Objs count", 0),
        "total_processed": md5_main.get("Total processed objects", 0),
        "deduped_count": md5_main.get("Deduped Obj (this cycle)", 0),
        "deduped_bytes": md5_main.get("Deduped Bytes(this cycle)", 0),
        "unique_count": md5_main.get("Unique Obj", 0),
        "duplicate_count": md5_main.get("Duplicate Obj", 0),
        "already_deduped_bytes": md5_main.get("Already Deduped bytes (prev cycles)", 0),
        "skipped_shared_manifest": md5_skipped.get("Skipped shared_manifest", 0),
        "skipped_too_many_copies": md5_skipped.get("Skipped Too Many Copies", 0),
        "skipped_source_record": md5_skipped.get("Skipped source record", 0),
        "skipped_compressed": worker_main.get("Skipped Compressed objs", 0),
        "split_head_src": md5_notify.get("Split-Head Src OBJ", 0),
        "split_head_tgt": md5_notify.get("Split-Head Tgt OBJ", 0),
        "valid_hash_attrs": md5_notify.get("Valid HASH attrs", 0),
        "compressed_objs": md5_notify.get("Compressed objs", 0),
        "compressed_bytes": md5_notify.get("Compressed Bytes", 0),
        "deduped_compressed_objects": md5_notify.get("Deduped Compressed objs", 0),
        "set_compression_on_tgt": md5_notify.get("Set Compression on TGT", 0),
        "clear_compression_on_tgt": md5_notify.get("Clear Compression on TGT", 0),
        "ingress_skip_filtered_sc": worker_skipped.get(
            "Ingress skipped filtered storage class, num objects skipped", 0
        ),
        "ingress_skip_filtered_bucket": worker_skipped.get(
            "Ingress skip: filtered bucket", 0
        ),
        "dedup_ratio_estimate": stats.get("dedup_ratio_estimate", {}).get(
            "dedup_ratio", 0
        ),
        "dedup_ratio_actual": stats.get("dedup_ratio_actual", {}).get("dedup_ratio", 0),
        "completed": stats.get("completed", False),
    }


_savings_registry = {}


def _format_bytes(size_bytes):
    for unit in ["B", "KB", "MB", "GB"]:
        if abs(size_bytes) < 1024.0:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.1f} TB"


def log_dedup_savings(exec_stats, total_uploaded_bytes, test_id):
    parsed = parse_dedup_stats_full(exec_stats)
    deduped_bytes = parsed["deduped_bytes"]
    deduped_count = parsed["deduped_count"]
    dedup_ratio = parsed.get("dedup_ratio_actual", 0)
    skipped_compressed = parsed.get("skipped_compressed", 0)

    if total_uploaded_bytes > 0 and deduped_bytes > 0:
        savings_pct = (deduped_bytes / total_uploaded_bytes) * 100
        effective_bytes = total_uploaded_bytes - deduped_bytes
    else:
        savings_pct = 0.0
        effective_bytes = total_uploaded_bytes

    savings = {
        "test_id": test_id,
        "total_uploaded": total_uploaded_bytes,
        "deduped_count": deduped_count,
        "deduped_bytes": deduped_bytes,
        "savings_pct": savings_pct,
        "effective_bytes": effective_bytes,
        "dedup_ratio": dedup_ratio,
        "skipped_compressed": skipped_compressed,
    }
    _savings_registry[test_id] = savings

    _record_step(
        f"Log dedup savings for {test_id}",
        "",
        f"savings={savings_pct:.1f}%, deduped={deduped_count} objs / {_format_bytes(deduped_bytes)}",
    )
    log.info(f"--- DEDUP STORAGE SAVINGS [{test_id}] ---")
    log.info(f"  Total uploaded   : {_format_bytes(total_uploaded_bytes)}")
    log.info(f"  Deduped objects   : {deduped_count}")
    log.info(f"  Deduped bytes     : {_format_bytes(deduped_bytes)}")
    log.info(f"  Storage savings   : {savings_pct:.1f}%")
    log.info(f"  Effective storage : {_format_bytes(effective_bytes)}")
    if dedup_ratio > 0:
        log.info(f"  Dedup ratio       : {dedup_ratio:.2f}x")
    if skipped_compressed > 0:
        log.info(f"  Skipped compressed: {skipped_compressed}")
    log.info(f"--- END SAVINGS [{test_id}] ---")

    return savings


def validate_estimate_exec_ratio(estimate_stats, exec_stats, tolerance=0.1):
    est = parse_dedup_stats_full(estimate_stats)
    exe = parse_dedup_stats_full(exec_stats)
    est_ratio = est.get("dedup_ratio_estimate", 0)
    act_ratio = exe.get("dedup_ratio_actual", 0)
    log.info(f"Ratio validation: estimate={est_ratio:.4f}, actual={act_ratio:.4f}")
    if est_ratio > 0 and act_ratio > 0:
        diff = abs(est_ratio - act_ratio) / max(est_ratio, act_ratio)
        log.info(f"Ratio difference: {diff:.4f} (tolerance={tolerance})")
        if diff > tolerance:
            log.warning(
                f"Estimate/exec ratio mismatch: estimate={est_ratio:.4f}, "
                f"actual={act_ratio:.4f}, diff={diff:.4f}"
            )
    return est_ratio, act_ratio


def upload_identical_versions(s3_client, bucket_name, key, count, size_bytes):
    enable_bucket_versioning(s3_client, bucket_name)
    data = generate_identical_data(size_bytes)
    md5_hash = hashlib.md5(data).hexdigest()
    log.info(
        f"Uploading {count} identical versions of '{key}' ({size_bytes} bytes, "
        f"MD5: {md5_hash}) to versioned bucket {bucket_name}"
    )
    version_ids = []
    for i in range(count):
        resp = s3_client.put_object(Bucket=bucket_name, Key=key, Body=data)
        version_ids.append(resp["VersionId"])
        if (i + 1) % 50 == 0:
            log.info(f"Uploaded {i + 1}/{count} versions")
    log.info(f"Uploaded all {count} versions")
    return version_ids, md5_hash, data


# ---------------------------------------------------------------------------
# Infrastructure disruption helpers (for negative/edge-case tests)
# ---------------------------------------------------------------------------


def restart_rgw_service(service_name=None):
    """Restart all RGW daemons via ceph orch."""
    if service_name is None:
        service_name = get_rgw_service_name()
    cmd = f"ceph orch restart {service_name}"
    log.info(f"Restarting RGW service: {cmd}")
    _record_step("Restart RGW service", cmd)
    out = utils.exec_shell_cmd(cmd)
    if out is False:
        log.warning("RGW restart command returned false (may still succeed)")
    time.sleep(15)


def kill_rgw_process(node=None):
    """Send SIGKILL to radosgw process. If node is given, uses SSH."""
    kill_cmd = "pkill -9 radosgw"
    if node:
        cmd = f"ssh {node} '{kill_cmd}'"
    else:
        cmd = kill_cmd
    log.info(f"Killing RGW process: {cmd}")
    _record_step("Kill RGW process (SIGKILL)", cmd)
    utils.exec_shell_cmd(cmd)
    time.sleep(5)


def restart_osd(osd_id):
    """Restart a specific OSD daemon via ceph orch."""
    cmd = f"ceph orch daemon restart osd.{osd_id}"
    log.info(f"Restarting OSD: {cmd}")
    _record_step(f"Restart OSD {osd_id}", cmd)
    out = utils.exec_shell_cmd(cmd)
    if out is False:
        log.warning(f"OSD {osd_id} restart command returned false")
    time.sleep(10)


def get_mon_leader():
    """Get the current MON leader name."""
    cmd = "ceph mon stat"
    out = utils.exec_shell_cmd(cmd)
    if out is False:
        raise AssertionError("Failed to get mon stat")
    # Output contains "leader: <name>" or we parse the quorum
    if "leader" in out:
        for part in out.split(","):
            if "leader" in part:
                # format: "..., leader: mon.hostname ..."
                leader = part.strip().split()[-1]
                log.info(f"MON leader: {leader}")
                return leader
    # Fallback: parse JSON if available
    try:
        cmd2 = "ceph mon dump --format json"
        out2 = utils.exec_shell_cmd(cmd2)
        data = json.loads(out2)
        quorum = data.get("quorum", [])
        mons = data.get("mons", [])
        if quorum and mons:
            leader_rank = quorum[0]
            for m in mons:
                if m.get("rank") == leader_rank:
                    log.info(f"MON leader (from dump): {m['name']}")
                    return m["name"]
    except Exception:
        pass
    raise AssertionError("Could not determine MON leader")


def restart_mon(mon_name):
    """Restart a specific MON daemon via ceph orch."""
    cmd = f"ceph orch daemon restart mon.{mon_name}"
    log.info(f"Restarting MON: {cmd}")
    _record_step(f"Restart MON {mon_name}", cmd)
    out = utils.exec_shell_cmd(cmd)
    if out is False:
        log.warning(f"MON {mon_name} restart command returned false")
    time.sleep(15)


def get_osd_for_bucket(bucket_name, pool="default.rgw.buckets.data"):
    """Find an OSD that hosts PGs for the given bucket's pool."""
    marker = get_bucket_marker(bucket_name)
    if not marker:
        raise AssertionError(f"Cannot get marker for bucket {bucket_name}")
    cmd = f"ceph osd map {pool} {marker}"
    out = utils.exec_shell_cmd(cmd)
    if out is False:
        raise AssertionError(f"Failed to map OSD for {pool}/{marker}")
    # Output: "osdmap ... pool '...' (N) object '...' -> pg M.xxx ... -> up ([A,B,C], pA) ..."
    for part in out.split():
        if part.startswith("osd."):
            osd_id = part.replace("osd.", "").rstrip(",).:")
            if osd_id.isdigit():
                log.info(f"Bucket {bucket_name} mapped to OSD {osd_id}")
                return int(osd_id)
    # Fallback: parse the "up" list
    import re

    match = re.search(r"up\s*\(\[(\d+)", out)
    if match:
        osd_id = int(match.group(1))
        log.info(f"Bucket {bucket_name} mapped to OSD {osd_id} (from up list)")
        return osd_id
    raise AssertionError(f"Could not find OSD for bucket {bucket_name}: {out}")


def wait_for_health_ok(timeout=120):
    """Poll ceph health until HEALTH_OK or timeout."""
    log.info(f"Waiting for cluster health OK (timeout={timeout}s)")
    start = time.time()
    while time.time() - start < timeout:
        out = utils.exec_shell_cmd("ceph health")
        if out and "HEALTH_OK" in out:
            log.info("Cluster health is OK")
            return True
        time.sleep(5)
    log.warning(f"Cluster did not reach HEALTH_OK within {timeout}s")
    return False


def wait_for_rgw_ready(endpoint_url=None, timeout=60):
    """Poll RGW endpoint until it responds with 200."""
    if endpoint_url is None:
        endpoint_url = "http://localhost:80"
    log.info(f"Waiting for RGW ready at {endpoint_url} (timeout={timeout}s)")
    import requests

    start = time.time()
    while time.time() - start < timeout:
        try:
            resp = requests.get(endpoint_url, timeout=5)
            if resp.status_code in (200, 403):
                log.info(f"RGW is ready (status={resp.status_code})")
                return True
        except Exception:
            pass
        time.sleep(3)
    log.warning(f"RGW not ready within {timeout}s")
    return False


def check_orphan_objects(pool="default.rgw.buckets.data", pattern="_sh_"):
    """Check for orphaned split-head tail objects in a RADOS pool."""
    objects = get_rados_objects(pool)
    orphans = [o for o in objects if pattern in o]
    log.info(f"Found {len(orphans)} potential orphan objects matching '{pattern}'")
    return orphans


def upload_unique_objects(
    s3_client, bucket_name, count, size_bytes, prefix="unique-obj"
):
    """Upload objects each with unique random content (no duplicates)."""
    log.info(
        f"Uploading {count} unique objects of size {size_bytes} bytes "
        f"to bucket {bucket_name}"
    )
    _record_step(
        f"Upload {count} unique {size_bytes}B objects to {bucket_name}",
        f"s3_client.put_object x{count} (unique content each)",
    )
    keys = []
    md5s = {}
    for i in range(count):
        data = os.urandom(size_bytes)
        key = f"{prefix}-{i}"
        s3_client.put_object(Bucket=bucket_name, Key=key, Body=data)
        md5s[key] = hashlib.md5(data).hexdigest()
        keys.append(key)
    log.info(f"Uploaded {count} unique objects")
    return keys, md5s


def run_dedup_estimate_async():
    """Fire dedup estimate without waiting for completion. Returns immediately."""
    cmd = "radosgw-admin dedup estimate"
    log.info(f"Running dedup estimate (async/fire-and-forget): {cmd}")
    _record_step("Run dedup estimate (async)", cmd)
    import subprocess

    subprocess.Popen(
        cmd, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )
    time.sleep(1)


def run_dedup_execute_async():
    """Fire dedup exec without waiting for completion. Returns immediately."""
    cmd = "radosgw-admin dedup exec --yes-i-really-mean-it"
    log.info(f"Running dedup exec (async/fire-and-forget): {cmd}")
    _record_step("Run dedup exec (async)", cmd)
    import subprocess

    subprocess.Popen(
        cmd, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )
    time.sleep(1)


def trigger_bucket_reshard(bucket_name, num_shards):
    """Trigger manual bucket resharding."""
    cmd = (
        f"radosgw-admin bucket reshard --bucket={bucket_name} "
        f"--num-shards={num_shards} --yes-i-really-mean-it"
    )
    log.info(f"Triggering bucket reshard: {cmd}")
    _record_step(f"Reshard bucket {bucket_name} to {num_shards} shards", cmd)
    out = utils.exec_shell_cmd(cmd)
    if out is False:
        log.warning(f"Bucket reshard command returned false for {bucket_name}")
    return out


def set_pool_quota(pool, max_bytes):
    """Set a byte quota on a RADOS pool to simulate near-full conditions."""
    cmd = f"ceph osd pool set-quota {pool} max_bytes {max_bytes}"
    log.info(f"Setting pool quota: {cmd}")
    _record_step(f"Set pool {pool} quota to {max_bytes} bytes", cmd)
    out = utils.exec_shell_cmd(cmd)
    if out is False:
        raise AssertionError(f"Failed to set quota on pool {pool}")
    return out


def remove_pool_quota(pool):
    """Remove byte quota from a RADOS pool."""
    cmd = f"ceph osd pool set-quota {pool} max_bytes 0"
    log.info(f"Removing pool quota: {cmd}")
    _record_step(f"Remove pool {pool} quota", cmd)
    out = utils.exec_shell_cmd(cmd)
    if out is False:
        log.warning(f"Failed to remove quota on pool {pool}")
    return out


def verify_rgw_daemons_running():
    """Verify all RGW daemons are running via ceph orch ps."""
    cmd = "ceph orch ps --daemon-type rgw --format json"
    out = utils.exec_shell_cmd(cmd)
    if out is False:
        raise AssertionError("Failed to get RGW daemon status")
    daemons = json.loads(out)
    running = [d for d in daemons if "running" in d.get("status_desc", "")]
    log.info(f"RGW daemons running: {len(running)}/{len(daemons)}")
    return len(running), len(daemons)


def corrupt_rados_object(pool, oid, data=b"CORRUPTED"):
    """Overwrite a RADOS object with garbage data."""
    import tempfile

    fd, tmpfile = tempfile.mkstemp()
    try:
        os.write(fd, data)
        os.close(fd)
        cmd = f"rados -p {pool} put {oid} {tmpfile}"
        log.info(f"Corrupting RADOS object: {cmd}")
        _record_step(f"Corrupt RADOS object {oid} in {pool}", cmd)
        out = utils.exec_shell_cmd(cmd)
        if out is False:
            raise AssertionError(f"Failed to corrupt object {oid}")
    finally:
        os.unlink(tmpfile)
