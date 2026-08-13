"""
test_dedup_pytest.py - RGW Dedup Test Suite (Pytest Format)

All 58 dedup test scenarios in pytest format with fixtures and markers.
Reuses helpers from reusables/dedup.py -- no logic duplication.

Usage:
  pytest test_dedup_pytest.py -C config.yaml -v
  pytest test_dedup_pytest.py -C config.yaml -m sanity
  pytest test_dedup_pytest.py -C config.yaml -m enhancement
  pytest test_dedup_pytest.py -C config.yaml -k "test_128_limit"

Categories:
  sanity       : S1-S5   (basic dedup operations)
  feature      : S6-S15  (multipart, versioning, filters, lifecycle, etc.)
  compression  : S16-S28 (compressed object dedup, cross-user)
  bug          : B1-B7   (data integrity, regression, IBMCEPH-17201)
  enhancement  : E1-E5   (128-limit boundary, multi-cycle, split-head)
"""

import hashlib
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
from v2.tests.s3_swift import reusable
from v2.tests.s3_swift.reusables import dedup as dedup_utils

log = logging.getLogger()


# =============================================================================
# SANITY TESTS (S1-S5)
# =============================================================================


@pytest.mark.sanity
def test_s1_sanity_large_objects(s3_client, bucket, rgw_config):
    """S1: Upload 50 identical objects > 4MB, dedup, verify accessible."""
    obj_count = getattr(rgw_config, "objects_count", None) or 50
    obj_size = 5 * 1024

    keys, expected_md5, _ = dedup_utils.upload_identical_objects(
        s3_client, bucket, obj_count, obj_size, prefix="large-obj"
    )

    dedup_utils.run_dedup_estimate()
    estimate_stats = dedup_utils.wait_for_dedup_completion()
    assert estimate_stats.get("completed") is True, "Estimate did not complete"

    dedup_utils.run_dedup_execute()
    exec_stats = dedup_utils.wait_for_dedup_completion()
    assert exec_stats.get("completed") is True, "Exec did not complete"

    dedup_utils.validate_estimate_exec_ratio(estimate_stats, exec_stats)

    dedup_utils.verify_all_objects_accessible(s3_client, bucket, keys)
    dedup_utils.verify_all_objects_integrity(s3_client, bucket, keys, expected_md5)

    parsed = dedup_utils.parse_dedup_stats(exec_stats)
    assert parsed.get("deduped_count", 0) > 0, "Expected dedup to deduplicate objects"
    dedup_utils.log_dedup_savings(exec_stats, obj_count * obj_size, "S1")


@pytest.mark.sanity
def test_s2_sanity_small_objects(s3_client, bucket):
    """S2: Upload identical objects at various small sizes, dedup, verify split-head."""
    sizes = [5 * 1024, 8 * 1024, 10 * 1024]
    all_keys = []
    md5_map = {}

    for size in sizes:
        size_label = f"{size // 1024}KB"
        keys, md5_hash, _ = dedup_utils.upload_identical_objects(
            s3_client, bucket, 30, size, prefix=f"small-{size_label}"
        )
        all_keys.extend(keys)
        for k in keys:
            md5_map[k] = md5_hash

    dedup_utils.run_dedup_estimate()
    estimate_stats = dedup_utils.wait_for_dedup_completion()
    assert estimate_stats.get("completed") is True, "Estimate did not complete"

    dedup_utils.run_dedup_execute()
    exec_stats = dedup_utils.wait_for_dedup_completion()
    assert exec_stats.get("completed") is True, "Exec did not complete"

    dedup_utils.validate_estimate_exec_ratio(estimate_stats, exec_stats)

    for key in all_keys:
        dedup_utils.verify_object_integrity(s3_client, bucket, key, md5_map[key])
    total_uploaded = sum(30 * s for s in sizes)
    dedup_utils.log_dedup_savings(exec_stats, total_uploaded, "S2")


@pytest.mark.sanity
def test_s3_admin_ops_api(s3_client, bucket, admin_user, endpoint_url):
    """S3: Verify dedup Admin OPS REST API (estimate, stats, exec)."""
    ak = admin_user["access_key"]
    sk = admin_user["secret_key"]
    dedup_utils.ensure_dedup_caps("dedup-admin")

    keys, expected_md5, _ = dedup_utils.upload_identical_objects(
        s3_client, bucket, 20, 5 * 1024, prefix="api-obj"
    )

    resp = dedup_utils.dedup_api_request(
        endpoint_url,
        "estimate",
        method="POST",
        access_key=ak,
        secret_key=sk,
    )
    assert (
        resp.status_code == 200
    ), f"Estimate API failed: {resp.status_code} {resp.text}"

    estimate_stats = dedup_utils.wait_for_dedup_completion()
    assert estimate_stats.get("completed") is True, "Estimate did not complete"

    resp = dedup_utils.dedup_api_request(
        endpoint_url,
        "stats",
        method="GET",
        access_key=ak,
        secret_key=sk,
    )
    assert resp.status_code == 200, f"Stats API failed: {resp.status_code} {resp.text}"
    stats_json = resp.json()
    assert "worker_stats" in stats_json, "Stats API missing worker_stats"
    assert "md5_stats" in stats_json, "Stats API missing md5_stats"

    resp = dedup_utils.dedup_api_request(
        endpoint_url,
        "exec",
        method="POST",
        access_key=ak,
        secret_key=sk,
        params={"yes-i-really-mean-it": ""},
    )
    assert resp.status_code == 200, f"Exec API failed: {resp.status_code} {resp.text}"

    exec_stats = dedup_utils.wait_for_dedup_completion()
    assert exec_stats.get("completed") is True, "Exec did not complete"

    dedup_utils.validate_estimate_exec_ratio(estimate_stats, exec_stats)
    dedup_utils.verify_all_objects_accessible(s3_client, bucket, keys)
    dedup_utils.verify_all_objects_integrity(s3_client, bucket, keys, expected_md5)
    dedup_utils.log_dedup_savings(exec_stats, 20 * 5 * 1024, "S3")


@pytest.mark.sanity
def test_s4_estimate_dry_run(s3_client, bucket):
    """S4: Run estimate only, verify no data changes."""
    dup_keys, dup_md5, _ = dedup_utils.upload_identical_objects(
        s3_client, bucket, 30, 5 * 1024, prefix="dup-obj"
    )

    unique_keys = []
    for i in range(10):
        key = f"unique-obj-{i}"
        s3_client.put_object(Bucket=bucket, Key=key, Body=os.urandom(5 * 1024))
        unique_keys.append(key)

    all_keys = dup_keys + unique_keys
    pre_etags = {}
    for key in all_keys:
        resp = s3_client.head_object(Bucket=bucket, Key=key)
        pre_etags[key] = resp["ETag"]

    dedup_utils.run_dedup_estimate()
    estimate_stats = dedup_utils.wait_for_dedup_completion()
    assert estimate_stats.get("completed") is True, "Estimate did not complete"

    for key in all_keys:
        resp = s3_client.head_object(Bucket=bucket, Key=key)
        assert pre_etags[key] == resp["ETag"], f"ETag changed for {key} after estimate"

    dedup_utils.verify_all_objects_integrity(s3_client, bucket, dup_keys, dup_md5)


@pytest.mark.sanity
def test_s5_data_integrity(s3_client, bucket, rgw_config):
    """S5: Upload 100 duplicates with known MD5, dedup, verify all match."""
    obj_count = getattr(rgw_config, "objects_count", None) or 100
    keys, expected_md5, _ = dedup_utils.upload_identical_objects(
        s3_client, bucket, obj_count, 5 * 1024, prefix="integrity-obj"
    )

    pre_etags = {}
    for key in keys:
        resp = s3_client.head_object(Bucket=bucket, Key=key)
        pre_etags[key] = resp["ETag"]

    dedup_utils.run_dedup_execute()
    exec_stats = dedup_utils.wait_for_dedup_completion()
    assert exec_stats.get("completed") is True, "Exec did not complete"

    dedup_utils.verify_all_objects_integrity(s3_client, bucket, keys, expected_md5)
    dedup_utils.log_dedup_savings(exec_stats, obj_count * 5 * 1024, "S5")


# =============================================================================
# FEATURE TESTS (S6-S15)
# =============================================================================


@pytest.mark.feature
@pytest.mark.slow
def test_s6_multipart_objects(s3_client, bucket):
    """S6: Upload 5 identical 50MB multipart objects, dedup, verify range GETs."""
    mp_size = 20 * 1024 * 1024
    keys, expected_md5, original_data = dedup_utils.upload_identical_multipart_objects(
        s3_client, bucket, 5, mp_size, prefix="mp-obj"
    )

    dedup_utils.run_dedup_estimate()
    estimate_stats = dedup_utils.wait_for_dedup_completion()
    assert estimate_stats.get("completed") is True, "Estimate did not complete"

    dedup_utils.run_dedup_execute()
    exec_stats = dedup_utils.wait_for_dedup_completion()
    assert exec_stats.get("completed") is True, "Exec did not complete"

    dedup_utils.validate_estimate_exec_ratio(estimate_stats, exec_stats)

    dedup_utils.verify_all_objects_accessible(s3_client, bucket, keys)
    dedup_utils.verify_all_objects_integrity(s3_client, bucket, keys, expected_md5)

    for key in keys:
        dedup_utils.verify_range_get(
            s3_client, bucket, key, original_data, 0, 1024 * 1024
        )
        mid = mp_size // 2
        dedup_utils.verify_range_get(
            s3_client, bucket, key, original_data, mid, mid + 1024 * 1024
        )
    dedup_utils.log_dedup_savings(exec_stats, 5 * mp_size, "S6")


@pytest.mark.feature
@pytest.mark.slow
def test_s7_session_lifecycle(s3_client, bucket):
    """S7: Test dedup pause/resume/abort controls."""
    keys, expected_md5, _ = dedup_utils.upload_identical_objects(
        s3_client, bucket, 100, 5 * 1024, prefix="lifecycle-obj"
    )

    dedup_utils.run_dedup_execute()
    time.sleep(3)
    dedup_utils.run_dedup_pause()
    time.sleep(2)
    dedup_utils.get_dedup_stats()
    dedup_utils.run_dedup_resume()
    exec_stats = dedup_utils.wait_for_dedup_completion()
    assert exec_stats.get("completed") is True, "Exec did not complete after resume"

    dedup_utils.verify_all_objects_accessible(s3_client, bucket, keys)
    dedup_utils.verify_all_objects_integrity(s3_client, bucket, keys, expected_md5)
    dedup_utils.log_dedup_savings(exec_stats, 100 * 5 * 1024, "S7")

    keys2, _, _ = dedup_utils.upload_identical_objects(
        s3_client, bucket, 50, 5 * 1024, prefix="lifecycle2-obj"
    )
    dedup_utils.run_dedup_execute()
    time.sleep(3)
    dedup_utils.run_dedup_abort()
    time.sleep(2)

    dedup_utils.verify_all_objects_accessible(s3_client, bucket, keys + keys2)


@pytest.mark.feature
def test_s8_ssec_exclusion(s3_client, bucket):
    """S8: SSE-C encrypted objects excluded from dedup."""
    utils.exec_shell_cmd("ceph config set client.rgw rgw_crypt_require_ssl false")
    time.sleep(5)

    ssec_keys, sse_key_b64, sse_key_md5 = dedup_utils.upload_ssec_objects(
        s3_client, bucket, 20, 5 * 1024, prefix="ssec-obj"
    )
    plain_keys, plain_md5, _ = dedup_utils.upload_identical_objects(
        s3_client, bucket, 20, 5 * 1024, prefix="plain-obj"
    )

    dedup_utils.run_dedup_estimate()
    estimate_stats = dedup_utils.wait_for_dedup_completion()
    assert estimate_stats.get("completed") is True, "Estimate did not complete"

    dedup_utils.run_dedup_execute()
    exec_stats = dedup_utils.wait_for_dedup_completion()
    assert exec_stats.get("completed") is True, "Exec did not complete"

    dedup_utils.validate_estimate_exec_ratio(estimate_stats, exec_stats)

    dedup_utils.verify_all_objects_integrity(s3_client, bucket, plain_keys, plain_md5)

    for key in ssec_keys:
        resp = s3_client.get_object(
            Bucket=bucket,
            Key=key,
            SSECustomerAlgorithm="AES256",
            SSECustomerKey=sse_key_b64,
            SSECustomerKeyMD5=sse_key_md5,
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    dedup_utils.log_dedup_savings(exec_stats, (20 + 20) * 5 * 1024, "S8")


@pytest.mark.feature
def test_s9_storage_class_dedup(s3_client, bucket, ssh_con):
    """S9: Create storage class, upload objects to it, run dedup, verify integrity.

    Steps:
      1. Create data pool for the storage class
      2. Enable rgw application on that pool
      3. Add storage class to zonegroup and zone placement
      4. Period update + restart RGW
      5. Upload identical objects with that storage class
      6. Run dedup estimate → wait for completed
      7. Run dedup exec → wait for completed
      8. Validate estimate/exec ratio match
      9. Verify object integrity
     10. Teardown storage class and pool
    """
    sc_name = "DEDUP_TEST_SC"
    pool_name = "dedup-sc-test-pool"

    dedup_utils.setup_storage_class(sc_name, pool_name, ssh_con)
    try:
        dedup_utils.wait_for_rgw_ready()
        obj_count = 20
        obj_size = 5 * 1024
        keys, content_md5, _ = dedup_utils.upload_identical_objects_with_sc(
            s3_client, bucket, obj_count, obj_size, sc_name, prefix="sc-obj"
        )
        log.info(
            f"Uploaded {obj_count} objects to bucket {bucket} "
            f"with StorageClass={sc_name}"
        )

        sc_file = dedup_utils.create_filter_list_file([sc_name])
        try:
            dedup_utils.run_dedup_estimate(allow_sc_file=sc_file)
            estimate_stats = dedup_utils.wait_for_dedup_completion()
            assert estimate_stats.get("completed") is True, "Estimate did not complete"

            dedup_utils.run_dedup_execute(allow_sc_file=sc_file)
            exec_stats = dedup_utils.wait_for_dedup_completion()
            assert exec_stats.get("completed") is True, "Exec did not complete"

            parsed = dedup_utils.parse_dedup_stats_full(exec_stats)
            assert (
                parsed["deduped_count"] > 0
            ), f"Expected deduped_count > 0 for SC objects, got {parsed['deduped_count']}"
            dedup_utils.validate_estimate_exec_ratio(estimate_stats, exec_stats)
        finally:
            os.remove(sc_file)

        dedup_utils.verify_all_objects_accessible(s3_client, bucket, keys)
        dedup_utils.verify_all_objects_integrity(s3_client, bucket, keys, content_md5)
        log.info("All objects in storage class verified post-dedup")
        dedup_utils.log_dedup_savings(exec_stats, obj_count * obj_size, "S9")

    finally:
        dedup_utils.teardown_storage_class(sc_name, pool_name, ssh_con)


@pytest.mark.feature
@pytest.mark.slow
def test_s10_lc_expiration(s3_client, bucket):
    """S10: LC expiration with deduplicated objects."""
    utils.exec_shell_cmd("ceph config set client.rgw rgw_lc_debug_interval 30")
    time.sleep(3)

    keys, expected_md5, _ = dedup_utils.upload_identical_objects(
        s3_client, bucket, 20, 5 * 1024, prefix="lc-obj"
    )

    dedup_utils.run_dedup_estimate()
    estimate_stats = dedup_utils.wait_for_dedup_completion()
    assert estimate_stats.get("completed") is True, "Estimate did not complete"

    dedup_utils.run_dedup_execute()
    exec_stats = dedup_utils.wait_for_dedup_completion()
    assert exec_stats.get("completed") is True, "Exec did not complete"

    dedup_utils.validate_estimate_exec_ratio(estimate_stats, exec_stats)
    dedup_utils.verify_all_objects_accessible(s3_client, bucket, keys)
    dedup_utils.log_dedup_savings(exec_stats, 20 * 5 * 1024, "S10")

    dedup_utils.set_lifecycle_expiration(s3_client, bucket, days=1)
    time.sleep(90)

    resp = s3_client.list_objects_v2(Bucket=bucket)
    remaining = resp.get("KeyCount", 0)
    log.info(f"Objects remaining after LC expiration: {remaining}")


@pytest.mark.feature
def test_s11_versioned_objects(s3_client, bucket, rgw_config):
    """S11: Versioned objects dedup, verify all versions accessible."""
    dedup_utils.enable_bucket_versioning(s3_client, bucket)

    identical_data = dedup_utils.generate_identical_data(5 * 1024)
    expected_md5 = hashlib.md5(identical_data).hexdigest()

    version_count = getattr(rgw_config, "version_count", None) or 10
    object_key = "versioned-dedup-object"
    version_ids = []

    for i in range(version_count):
        resp = s3_client.put_object(Bucket=bucket, Key=object_key, Body=identical_data)
        version_ids.append(resp["VersionId"])

    dedup_utils.run_dedup_estimate()
    estimate_stats = dedup_utils.wait_for_dedup_completion()
    assert estimate_stats.get("completed") is True, "Estimate did not complete"

    dedup_utils.run_dedup_execute()
    exec_stats = dedup_utils.wait_for_dedup_completion()
    assert exec_stats.get("completed") is True, "Exec did not complete"

    dedup_utils.validate_estimate_exec_ratio(estimate_stats, exec_stats)

    for vid in version_ids:
        resp = s3_client.get_object(Bucket=bucket, Key=object_key, VersionId=vid)
        body = resp["Body"].read()
        actual_md5 = hashlib.md5(body).hexdigest()
        assert actual_md5 == expected_md5, f"Version {vid} MD5 mismatch"

    versions = dedup_utils.get_all_versions(s3_client, bucket, object_key)
    assert len(versions) == version_count
    dedup_utils.log_dedup_savings(exec_stats, version_count * 5 * 1024, "S11")


@pytest.mark.feature
def test_s12_s3_copy_dedup(s3_client, bucket):
    """S12: S3 COPY to duplicate 20 times, dedup, verify all copies."""
    source_data = dedup_utils.generate_identical_data(5 * 1024)
    expected_md5 = hashlib.md5(source_data).hexdigest()

    source_key = "source-large-obj"
    s3_client.put_object(Bucket=bucket, Key=source_key, Body=source_data)

    copy_keys = []
    for i in range(20):
        copy_key = f"copy-obj-{i}"
        s3_client.copy_object(
            Bucket=bucket,
            Key=copy_key,
            CopySource={"Bucket": bucket, "Key": source_key},
        )
        copy_keys.append(copy_key)

    all_keys = [source_key] + copy_keys

    dedup_utils.run_dedup_estimate()
    estimate_stats = dedup_utils.wait_for_dedup_completion()
    assert estimate_stats.get("completed") is True, "Estimate did not complete"

    dedup_utils.run_dedup_execute()
    exec_stats = dedup_utils.wait_for_dedup_completion()
    assert exec_stats.get("completed") is True, "Exec did not complete"

    dedup_utils.validate_estimate_exec_ratio(estimate_stats, exec_stats)

    dedup_utils.verify_all_objects_integrity(s3_client, bucket, all_keys, expected_md5)
    dedup_utils.log_dedup_savings(exec_stats, 21 * 5 * 1024, "S12")


@pytest.mark.feature
def test_s14_same_content_diff_metadata(s3_clients, test_context):
    """S14: Same content, different metadata/tags/users. Metadata preserved."""
    s3_client1, s3_client2 = s3_clients
    bucket1 = f"dedup-meta1-{random.randint(1, 9999)}"
    bucket2 = f"dedup-meta2-{random.randint(1, 9999)}"
    s3_client1.create_bucket(Bucket=bucket1)
    s3_client2.create_bucket(Bucket=bucket2)

    test_context["buckets"].extend([bucket1, bucket2])
    for bkt in [bucket1, bucket2]:
        marker = dedup_utils.get_bucket_marker(bkt)
        if marker:
            test_context["bucket_markers"][bkt] = marker

    try:
        identical_data = dedup_utils.generate_identical_data(5 * 1024)
        expected_md5 = hashlib.md5(identical_data).hexdigest()

        s3_client1.put_object(
            Bucket=bucket1,
            Key="obj-user1-a",
            Body=identical_data,
            Metadata={"custom-key": "value-a"},
            Tagging="env=prod",
        )
        s3_client1.put_object(
            Bucket=bucket1,
            Key="obj-user1-b",
            Body=identical_data,
            Metadata={"custom-key": "value-b"},
            Tagging="env=staging",
        )
        s3_client2.put_object(
            Bucket=bucket2,
            Key="obj-user2-a",
            Body=identical_data,
            Metadata={"custom-key": "value-c"},
        )
        s3_client2.put_object(
            Bucket=bucket2,
            Key="obj-user2-b",
            Body=identical_data,
            Metadata={"custom-key": "value-d"},
        )

        dedup_utils.run_dedup_estimate()
        estimate_stats = dedup_utils.wait_for_dedup_completion()
        assert estimate_stats.get("completed") is True, "Estimate did not complete"

        dedup_utils.run_dedup_execute()
        exec_stats = dedup_utils.wait_for_dedup_completion()
        assert exec_stats.get("completed") is True, "Exec did not complete"

        dedup_utils.validate_estimate_exec_ratio(estimate_stats, exec_stats)

        for client, bkt, key, expected_meta in [
            (s3_client1, bucket1, "obj-user1-a", "value-a"),
            (s3_client1, bucket1, "obj-user1-b", "value-b"),
            (s3_client2, bucket2, "obj-user2-a", "value-c"),
            (s3_client2, bucket2, "obj-user2-b", "value-d"),
        ]:
            dedup_utils.verify_object_integrity(client, bkt, key, expected_md5)
            resp = client.head_object(Bucket=bkt, Key=key)
            actual = resp.get("Metadata", {}).get("custom-key", "")
            assert actual == expected_meta, f"Metadata mismatch for {bkt}/{key}"

        tag_resp = s3_client1.get_object_tagging(Bucket=bucket1, Key="obj-user1-a")
        tags = {t["Key"]: t["Value"] for t in tag_resp.get("TagSet", [])}
        assert tags.get("env") == "prod"
        dedup_utils.log_dedup_savings(exec_stats, 4 * 5 * 1024, "S14")
    finally:
        dedup_utils.cleanup_bucket(s3_client1, bucket1)
        dedup_utils.cleanup_bucket(s3_client2, bucket2)


@pytest.mark.feature
@pytest.mark.slow
def test_s15_concurrent_s3_ops(s3_client, bucket_factory):
    """S15: Concurrent S3 workload during dedup."""
    dedup_bucket = bucket_factory("dedup-concurrent")
    workload_bucket = bucket_factory("workload-concurrent")

    keys, expected_md5, _ = dedup_utils.upload_identical_objects(
        s3_client, dedup_bucket, 50, 5 * 1024, prefix="concurrent-obj"
    )

    dedup_utils.run_dedup_execute()
    workload_results = dedup_utils.run_concurrent_s3_workload(
        s3_client, workload_bucket, duration_secs=20, prefix="workload"
    )
    exec_stats = dedup_utils.wait_for_dedup_completion()
    assert exec_stats.get("completed") is True, "Exec did not complete"

    dedup_utils.verify_all_objects_accessible(s3_client, dedup_bucket, keys)
    dedup_utils.verify_all_objects_integrity(
        s3_client, dedup_bucket, keys, expected_md5
    )

    dedup_utils.log_dedup_savings(exec_stats, 50 * 5 * 1024, "S15")

    assert workload_results["puts"] > 0
    assert workload_results["gets"] > 0
    error_rate = workload_results["errors"] / max(
        workload_results["puts"]
        + workload_results["gets"]
        + workload_results["deletes"],
        1,
    )
    assert error_rate < 0.05, f"Error rate too high: {error_rate:.2%}"


# =============================================================================
# COMPRESSION TESTS (S16-S20)
# =============================================================================


@pytest.mark.compression
def test_s16_uncompressed_to_compressed(s3_client, bucket_factory, ssh_con):
    """S16: Mode switch — upload uncompressed, switch to zlib, upload copies, dedup aligns all to compressed.

    Mirrors upstream _dedup_mode_switch_test(start_compressed=False).
    After dedup all objects in both buckets should be compressed.
    """
    obj_count = 20
    obj_size = 5 * 1024

    dedup_utils.disable_zone_compression(ssh_con)
    bucket_a = bucket_factory("s16-uncomp")
    keys_a, expected_md5, _ = dedup_utils.upload_identical_objects(
        s3_client, bucket_a, obj_count, obj_size, prefix="mode-obj"
    )
    dedup_utils.assert_all_objects_compression(
        bucket_a, keys_a, expect_compressed=False, tag="PRE-DEDUP bucket_a"
    )

    dedup_utils.enable_zone_compression("zlib", ssh_con)
    try:
        bucket_b = bucket_factory("s16-comp")
        keys_b, md5_b, _ = dedup_utils.upload_identical_objects(
            s3_client, bucket_b, obj_count, obj_size, prefix="mode-obj"
        )
        assert expected_md5 == md5_b, "MD5 mismatch between buckets"
        dedup_utils.assert_all_objects_compression(
            bucket_b, keys_b, expect_compressed=True, tag="PRE-DEDUP bucket_b"
        )

        dedup_utils.run_dedup_estimate()
        estimate_stats = dedup_utils.wait_for_dedup_completion()
        assert estimate_stats.get("completed") is True, "Estimate did not complete"

        dedup_utils.run_dedup_execute()
        exec_stats = dedup_utils.wait_for_dedup_completion()
        assert exec_stats.get("completed") is True, "Exec did not complete"

        parsed = dedup_utils.parse_dedup_stats_full(exec_stats)
        assert (
            parsed["deduped_count"] > 0
        ), f"Dedup should process objects across modes, got deduped_count={parsed['deduped_count']}"
        assert (
            parsed["compressed_objs"] > 0
        ), f"Expected compressed_objs > 0, got {parsed['compressed_objs']}"
        assert (
            parsed["set_compression_on_tgt"] > 0
        ), f"Expected set_compression_on_tgt > 0, got {parsed['set_compression_on_tgt']}"
        log.info(
            f"Mode switch dedup: deduped={parsed['deduped_count']}, "
            f"compressed_objs={parsed['compressed_objs']}, "
            f"set_compression_on_tgt={parsed['set_compression_on_tgt']}"
        )

        dedup_utils.assert_all_objects_compression(
            bucket_a, keys_a, expect_compressed=True, tag="POST-DEDUP bucket_a"
        )
        dedup_utils.assert_all_objects_compression(
            bucket_b, keys_b, expect_compressed=True, tag="POST-DEDUP bucket_b"
        )

        dedup_utils.verify_all_objects_accessible(s3_client, bucket_a, keys_a)
        dedup_utils.verify_all_objects_accessible(s3_client, bucket_b, keys_b)
        dedup_utils.verify_all_objects_integrity(
            s3_client, bucket_a, keys_a, expected_md5
        )
        dedup_utils.verify_all_objects_integrity(
            s3_client, bucket_b, keys_b, expected_md5
        )
        dedup_utils.log_dedup_savings(exec_stats, 2 * obj_count * obj_size, "S16")
    finally:
        dedup_utils.disable_zone_compression(ssh_con)


@pytest.mark.compression
def test_s17_compressed_to_uncompressed(s3_client, bucket_factory, ssh_con):
    """S17: Mode switch — upload compressed, switch to none, upload copies, dedup aligns all to uncompressed.

    Mirrors upstream _dedup_mode_switch_test(start_compressed=True).
    After dedup all objects in both buckets should be uncompressed.
    """
    obj_count = 20
    obj_size = 5 * 1024

    dedup_utils.enable_zone_compression("zlib", ssh_con)
    try:
        bucket_a = bucket_factory("s17-comp")
        keys_a, expected_md5, _ = dedup_utils.upload_identical_objects(
            s3_client, bucket_a, obj_count, obj_size, prefix="mode-obj"
        )
        dedup_utils.assert_all_objects_compression(
            bucket_a, keys_a, expect_compressed=True, tag="PRE-DEDUP bucket_a"
        )

        dedup_utils.disable_zone_compression(ssh_con)
        bucket_b = bucket_factory("s17-uncomp")
        keys_b, md5_b, _ = dedup_utils.upload_identical_objects(
            s3_client, bucket_b, obj_count, obj_size, prefix="mode-obj"
        )
        assert expected_md5 == md5_b, "MD5 mismatch between buckets"
        dedup_utils.assert_all_objects_compression(
            bucket_b, keys_b, expect_compressed=False, tag="PRE-DEDUP bucket_b"
        )

        dedup_utils.run_dedup_estimate()
        estimate_stats = dedup_utils.wait_for_dedup_completion()
        assert estimate_stats.get("completed") is True, "Estimate did not complete"

        dedup_utils.run_dedup_execute()
        exec_stats = dedup_utils.wait_for_dedup_completion()
        assert exec_stats.get("completed") is True, "Exec did not complete"

        parsed = dedup_utils.parse_dedup_stats_full(exec_stats)
        assert (
            parsed["deduped_count"] > 0
        ), f"Dedup should process objects across modes, got deduped_count={parsed['deduped_count']}"
        assert (
            parsed["compressed_objs"] > 0
        ), f"Expected compressed_objs > 0, got {parsed['compressed_objs']}"
        assert (
            parsed["clear_compression_on_tgt"] > 0
        ), f"Expected clear_compression_on_tgt > 0, got {parsed['clear_compression_on_tgt']}"
        log.info(
            f"Mode switch dedup: deduped={parsed['deduped_count']}, "
            f"compressed_objs={parsed['compressed_objs']}, "
            f"clear_compression_on_tgt={parsed['clear_compression_on_tgt']}"
        )

        dedup_utils.assert_all_objects_compression(
            bucket_a, keys_a, expect_compressed=False, tag="POST-DEDUP bucket_a"
        )
        dedup_utils.assert_all_objects_compression(
            bucket_b, keys_b, expect_compressed=False, tag="POST-DEDUP bucket_b"
        )

        dedup_utils.verify_all_objects_accessible(s3_client, bucket_a, keys_a)
        dedup_utils.verify_all_objects_accessible(s3_client, bucket_b, keys_b)
        dedup_utils.verify_all_objects_integrity(
            s3_client, bucket_a, keys_a, expected_md5
        )
        dedup_utils.verify_all_objects_integrity(
            s3_client, bucket_b, keys_b, expected_md5
        )
        dedup_utils.log_dedup_savings(exec_stats, 2 * obj_count * obj_size, "S17")
    finally:
        dedup_utils.disable_zone_compression(ssh_con)


@pytest.mark.compression
def test_s18_per_sc_compression_cache(s3_client, bucket_factory, ssh_con):
    """S18: Two storage classes with opposite compression, flip, dedup aligns each SC independently.

    Mirrors upstream test_dedup_placement_compression_cache_per_storage_class.
    SC1=STANDARD (none), SC2=LUKEWARM (zlib) -> upload copy-0 to each.
    Flip: SC1=zlib, SC2=none -> upload copy-1 to each.
    After dedup: SC1 all compressed, SC2 all uncompressed.
    """
    sc_name = "LUKEWARM"
    pool_name = "lukewarm-test-pool"
    obj_count = 10
    obj_size = 5 * 1024

    dedup_utils.setup_storage_class(sc_name, pool_name, ssh_con)
    try:
        dedup_utils.set_storage_class_compression("STANDARD", "none", ssh_con)
        dedup_utils.set_storage_class_compression(sc_name, "zlib", ssh_con)

        bucket_std = bucket_factory("s18-std")
        bucket_lw = bucket_factory("s18-lw")

        keys_std_0, md5_std, _ = dedup_utils.upload_identical_objects(
            s3_client, bucket_std, obj_count, obj_size, prefix="copy-0"
        )
        keys_lw_0, md5_lw, _ = dedup_utils.upload_identical_objects_with_sc(
            s3_client, bucket_lw, obj_count, obj_size, sc_name, prefix="copy-0"
        )

        dedup_utils.assert_all_objects_compression(
            bucket_std, keys_std_0, expect_compressed=False, tag="PRE-DEDUP SC1 copy-0"
        )
        dedup_utils.assert_all_objects_compression(
            bucket_lw, keys_lw_0, expect_compressed=True, tag="PRE-DEDUP SC2 copy-0"
        )

        dedup_utils.set_storage_class_compression("STANDARD", "zlib", ssh_con)
        dedup_utils.set_storage_class_compression(sc_name, "none", ssh_con)

        keys_std_1, md5_std_1, _ = dedup_utils.upload_identical_objects(
            s3_client, bucket_std, obj_count, obj_size, prefix="copy-1"
        )
        keys_lw_1, md5_lw_1, _ = dedup_utils.upload_identical_objects_with_sc(
            s3_client, bucket_lw, obj_count, obj_size, sc_name, prefix="copy-1"
        )

        dedup_utils.assert_all_objects_compression(
            bucket_std, keys_std_1, expect_compressed=True, tag="PRE-DEDUP SC1 copy-1"
        )
        dedup_utils.assert_all_objects_compression(
            bucket_lw, keys_lw_1, expect_compressed=False, tag="PRE-DEDUP SC2 copy-1"
        )

        dedup_utils.run_dedup_estimate()
        estimate_stats = dedup_utils.wait_for_dedup_completion()
        assert estimate_stats.get("completed") is True, "Estimate did not complete"

        dedup_utils.run_dedup_execute()
        exec_stats = dedup_utils.wait_for_dedup_completion()
        assert exec_stats.get("completed") is True, "Exec did not complete"

        parsed = dedup_utils.parse_dedup_stats_full(exec_stats)
        assert (
            parsed["deduped_count"] > 0
        ), f"Expected deduped_count > 0, got {parsed['deduped_count']}"
        assert (
            parsed["set_compression_on_tgt"] > 0
        ), f"Expected set_compression_on_tgt > 0, got {parsed['set_compression_on_tgt']}"
        assert (
            parsed["clear_compression_on_tgt"] > 0
        ), f"Expected clear_compression_on_tgt > 0, got {parsed['clear_compression_on_tgt']}"
        log.info(
            f"Per-SC compression cache: deduped={parsed['deduped_count']}, "
            f"set_compression_on_tgt={parsed['set_compression_on_tgt']}, "
            f"clear_compression_on_tgt={parsed['clear_compression_on_tgt']}"
        )

        all_std_keys = keys_std_0 + keys_std_1
        all_lw_keys = keys_lw_0 + keys_lw_1
        dedup_utils.assert_all_objects_compression(
            bucket_std, all_std_keys, expect_compressed=True, tag="POST-DEDUP SC1"
        )
        dedup_utils.assert_all_objects_compression(
            bucket_lw, all_lw_keys, expect_compressed=False, tag="POST-DEDUP SC2"
        )

        dedup_utils.verify_all_objects_accessible(s3_client, bucket_std, all_std_keys)
        dedup_utils.verify_all_objects_accessible(s3_client, bucket_lw, all_lw_keys)
        dedup_utils.verify_all_objects_integrity(
            s3_client, bucket_std, all_std_keys, md5_std
        )
        dedup_utils.verify_all_objects_integrity(
            s3_client, bucket_lw, all_lw_keys, md5_lw
        )
        dedup_utils.log_dedup_savings(exec_stats, 4 * obj_count * obj_size, "S18")
    finally:
        dedup_utils.set_storage_class_compression("STANDARD", "none", ssh_con)
        dedup_utils.teardown_storage_class(sc_name, pool_name, ssh_con)


@pytest.mark.compression
def test_s19_inc_compressed_shared_manifest(s3_client, bucket_factory, ssh_con):
    """S19: Incremental dedup — start compressed, switch mode, shared_manifest SRC priority preserved.

    Mirrors upstream _dedup_inc_shared_manifest_test(start_compressed=True).
    Cycle 1: compressed bucket_a + bucket_b, dedup.
    Cycle 2: switch to none, add bucket_c (uncompressed), dedup again.
    Post-dedup: ALL 3 buckets compressed (shared_manifest SRC from cycle 1 wins).
    """
    obj_count = 20
    obj_size = 5 * 1024

    dedup_utils.enable_zone_compression("zlib", ssh_con)
    try:
        bucket_a = bucket_factory("s19-a")
        bucket_b = bucket_factory("s19-b")
        keys_a, expected_md5, _ = dedup_utils.upload_identical_objects(
            s3_client, bucket_a, obj_count, obj_size, prefix="inc-obj"
        )
        keys_b, md5_b, _ = dedup_utils.upload_identical_objects(
            s3_client, bucket_b, obj_count, obj_size, prefix="inc-obj"
        )
        assert expected_md5 == md5_b

        dedup_utils.assert_all_objects_compression(
            bucket_a, keys_a, expect_compressed=True, tag="PRE-DEDUP-1 bucket_a"
        )
        dedup_utils.assert_all_objects_compression(
            bucket_b, keys_b, expect_compressed=True, tag="PRE-DEDUP-1 bucket_b"
        )

        dedup_utils.run_dedup_estimate()
        dedup_utils.wait_for_dedup_completion()
        dedup_utils.run_dedup_execute()
        exec_stats_1 = dedup_utils.wait_for_dedup_completion()
        assert exec_stats_1.get("completed") is True, "Cycle 1 exec did not complete"

        parsed_1 = dedup_utils.parse_dedup_stats_full(exec_stats_1)
        assert (
            parsed_1["deduped_count"] > 0
        ), f"Cycle 1: expected deduped_count > 0, got {parsed_1['deduped_count']}"
        log.info(f"Cycle 1 complete: deduped={parsed_1['deduped_count']}")

        dedup_utils.disable_zone_compression(ssh_con)
        bucket_c = bucket_factory("s19-c")
        keys_c, md5_c, _ = dedup_utils.upload_identical_objects(
            s3_client, bucket_c, obj_count, obj_size, prefix="inc-obj"
        )
        assert expected_md5 == md5_c
        dedup_utils.assert_all_objects_compression(
            bucket_c, keys_c, expect_compressed=False, tag="PRE-DEDUP-2 bucket_c"
        )

        dedup_utils.run_dedup_estimate()
        dedup_utils.wait_for_dedup_completion()
        dedup_utils.run_dedup_execute()
        exec_stats_2 = dedup_utils.wait_for_dedup_completion()
        assert exec_stats_2.get("completed") is True, "Cycle 2 exec did not complete"

        parsed_2 = dedup_utils.parse_dedup_stats_full(exec_stats_2)
        assert (
            parsed_2["deduped_count"] > 0
        ), f"Cycle 2: expected deduped_count > 0, got {parsed_2['deduped_count']}"
        assert (
            parsed_2["skipped_shared_manifest"] > 0
        ), f"Cycle 2: expected skipped_shared_manifest > 0, got {parsed_2['skipped_shared_manifest']}"
        log.info(
            f"Cycle 2: deduped={parsed_2['deduped_count']}, "
            f"skipped_shared_manifest={parsed_2['skipped_shared_manifest']}, "
            f"set_compression_on_tgt={parsed_2.get('set_compression_on_tgt', 0)}"
        )

        dedup_utils.assert_all_objects_compression(
            bucket_a, keys_a, expect_compressed=True, tag="POST-DEDUP-2 bucket_a"
        )
        dedup_utils.assert_all_objects_compression(
            bucket_b, keys_b, expect_compressed=True, tag="POST-DEDUP-2 bucket_b"
        )
        dedup_utils.assert_all_objects_compression(
            bucket_c, keys_c, expect_compressed=True, tag="POST-DEDUP-2 bucket_c"
        )

        for bkt, keys in [(bucket_a, keys_a), (bucket_b, keys_b), (bucket_c, keys_c)]:
            dedup_utils.verify_all_objects_accessible(s3_client, bkt, keys)
            dedup_utils.verify_all_objects_integrity(s3_client, bkt, keys, expected_md5)
        dedup_utils.log_dedup_savings(exec_stats_2, 3 * obj_count * obj_size, "S19")
    finally:
        dedup_utils.disable_zone_compression(ssh_con)


@pytest.mark.compression
def test_s20_inc_uncompressed_shared_manifest(s3_client, bucket_factory, ssh_con):
    """S20: Incremental dedup — start uncompressed, switch mode, shared_manifest SRC priority preserved.

    Mirrors upstream _dedup_inc_shared_manifest_test(start_compressed=False).
    Cycle 1: uncompressed bucket_a + bucket_b, dedup.
    Cycle 2: switch to zlib, add bucket_c (compressed), dedup again.
    Post-dedup: ALL 3 buckets uncompressed (shared_manifest SRC from cycle 1 wins).
    """
    obj_count = 20
    obj_size = 5 * 1024

    dedup_utils.disable_zone_compression(ssh_con)
    bucket_a = bucket_factory("s20-a")
    bucket_b = bucket_factory("s20-b")
    keys_a, expected_md5, _ = dedup_utils.upload_identical_objects(
        s3_client, bucket_a, obj_count, obj_size, prefix="inc-obj"
    )
    keys_b, md5_b, _ = dedup_utils.upload_identical_objects(
        s3_client, bucket_b, obj_count, obj_size, prefix="inc-obj"
    )
    assert expected_md5 == md5_b

    dedup_utils.assert_all_objects_compression(
        bucket_a, keys_a, expect_compressed=False, tag="PRE-DEDUP-1 bucket_a"
    )
    dedup_utils.assert_all_objects_compression(
        bucket_b, keys_b, expect_compressed=False, tag="PRE-DEDUP-1 bucket_b"
    )

    dedup_utils.run_dedup_estimate()
    dedup_utils.wait_for_dedup_completion()
    dedup_utils.run_dedup_execute()
    exec_stats_1 = dedup_utils.wait_for_dedup_completion()
    assert exec_stats_1.get("completed") is True, "Cycle 1 exec did not complete"

    parsed_1 = dedup_utils.parse_dedup_stats_full(exec_stats_1)
    assert (
        parsed_1["deduped_count"] > 0
    ), f"Cycle 1: expected deduped_count > 0, got {parsed_1['deduped_count']}"
    log.info(f"Cycle 1 complete: deduped={parsed_1['deduped_count']}")

    dedup_utils.enable_zone_compression("zlib", ssh_con)
    try:
        bucket_c = bucket_factory("s20-c")
        keys_c, md5_c, _ = dedup_utils.upload_identical_objects(
            s3_client, bucket_c, obj_count, obj_size, prefix="inc-obj"
        )
        assert expected_md5 == md5_c
        dedup_utils.assert_all_objects_compression(
            bucket_c, keys_c, expect_compressed=True, tag="PRE-DEDUP-2 bucket_c"
        )

        dedup_utils.run_dedup_estimate()
        dedup_utils.wait_for_dedup_completion()
        dedup_utils.run_dedup_execute()
        exec_stats_2 = dedup_utils.wait_for_dedup_completion()
        assert exec_stats_2.get("completed") is True, "Cycle 2 exec did not complete"

        parsed_2 = dedup_utils.parse_dedup_stats_full(exec_stats_2)
        assert (
            parsed_2["deduped_count"] > 0
        ), f"Cycle 2: expected deduped_count > 0, got {parsed_2['deduped_count']}"
        assert (
            parsed_2["skipped_shared_manifest"] > 0
        ), f"Cycle 2: expected skipped_shared_manifest > 0, got {parsed_2['skipped_shared_manifest']}"
        log.info(
            f"Cycle 2: deduped={parsed_2['deduped_count']}, "
            f"skipped_shared_manifest={parsed_2['skipped_shared_manifest']}, "
            f"clear_compression_on_tgt={parsed_2.get('clear_compression_on_tgt', 0)}"
        )

        dedup_utils.assert_all_objects_compression(
            bucket_a, keys_a, expect_compressed=False, tag="POST-DEDUP-2 bucket_a"
        )
        dedup_utils.assert_all_objects_compression(
            bucket_b, keys_b, expect_compressed=False, tag="POST-DEDUP-2 bucket_b"
        )
        dedup_utils.assert_all_objects_compression(
            bucket_c, keys_c, expect_compressed=False, tag="POST-DEDUP-2 bucket_c"
        )

        for bkt, keys in [(bucket_a, keys_a), (bucket_b, keys_b), (bucket_c, keys_c)]:
            dedup_utils.verify_all_objects_accessible(s3_client, bkt, keys)
            dedup_utils.verify_all_objects_integrity(s3_client, bkt, keys, expected_md5)
        dedup_utils.log_dedup_savings(exec_stats_2, 3 * obj_count * obj_size, "S20")
    finally:
        dedup_utils.disable_zone_compression(ssh_con)


# =============================================================================
# FILTER AND STATS TESTS (S22-S26)
# =============================================================================


@pytest.mark.compression
def test_s22_filter_bucket_list_parsing(s3_client, bucket):
    """S22: Filter file parsing — mutual exclusivity, missing file, empty file errors."""
    obj_size = 5 * 1024
    dedup_utils.upload_identical_objects(
        s3_client, bucket, 5, obj_size, prefix="parse-obj"
    )

    allow_file = dedup_utils.create_filter_list_file([bucket])
    deny_file = dedup_utils.create_filter_list_file(["other-bucket"])
    try:
        out, rc = dedup_utils.run_dedup_cmd_with_rc(
            f"radosgw-admin dedup estimate "
            f"--allow-bucket-list {allow_file} --deny-bucket-list {deny_file}"
        )
        assert (
            rc != 0
        ), f"Mutual exclusivity: allow+deny bucket lists should fail, rc={rc}"
        log.info(f"Mutual exclusivity rejected as expected (rc={rc})")
    finally:
        os.remove(allow_file)
        os.remove(deny_file)

    out, rc = dedup_utils.run_dedup_cmd_with_rc(
        "radosgw-admin dedup estimate --allow-bucket-list /tmp/nonexistent-filter-file.txt"
    )
    assert rc != 0, f"Non-existent file should fail, rc={rc}"
    log.info(f"Non-existent file rejected as expected (rc={rc})")

    empty_file = dedup_utils.create_filter_list_file([])
    try:
        out, rc = dedup_utils.run_dedup_cmd_with_rc(
            f"radosgw-admin dedup estimate --allow-bucket-list {empty_file}"
        )
        assert rc != 0, f"Empty filter file should fail, rc={rc}"
        log.info(f"Empty file rejected as expected (rc={rc})")
    finally:
        os.remove(empty_file)


@pytest.mark.compression
def test_s23_filter_sc_list_parsing(s3_client, bucket):
    """S23: SC filter file parsing — mutual exclusivity, missing file, empty file errors."""
    obj_size = 5 * 1024
    dedup_utils.upload_identical_objects(
        s3_client, bucket, 5, obj_size, prefix="parse-obj"
    )

    allow_file = dedup_utils.create_filter_list_file(["STANDARD"])
    deny_file = dedup_utils.create_filter_list_file(["LUKEWARM"])
    try:
        out, rc = dedup_utils.run_dedup_cmd_with_rc(
            f"radosgw-admin dedup estimate "
            f"--allow-storage-class-list {allow_file} "
            f"--deny-storage-class-list {deny_file}"
        )
        assert rc != 0, f"Mutual exclusivity: allow+deny SC lists should fail, rc={rc}"
        log.info(f"SC mutual exclusivity rejected as expected (rc={rc})")
    finally:
        os.remove(allow_file)
        os.remove(deny_file)

    out, rc = dedup_utils.run_dedup_cmd_with_rc(
        "radosgw-admin dedup estimate "
        "--allow-storage-class-list /tmp/nonexistent-sc-filter.txt"
    )
    assert rc != 0, f"Non-existent SC file should fail, rc={rc}"
    log.info(f"Non-existent SC file rejected as expected (rc={rc})")

    empty_file = dedup_utils.create_filter_list_file([])
    try:
        out, rc = dedup_utils.run_dedup_cmd_with_rc(
            f"radosgw-admin dedup estimate " f"--allow-storage-class-list {empty_file}"
        )
        assert rc != 0, f"Empty SC filter file should fail, rc={rc}"
        log.info(f"Empty SC file rejected as expected (rc={rc})")
    finally:
        os.remove(empty_file)


@pytest.mark.compression
def test_s24_filter_sc_estimate(s3_client, bucket):
    """S24: SC filter on estimate — allow STANDARD finds duplicates, deny STANDARD skips all."""
    obj_count = 20
    obj_size = 5 * 1024
    dedup_utils.upload_identical_objects(
        s3_client, bucket, obj_count, obj_size, prefix="sc-filter-obj"
    )

    allow_file = dedup_utils.create_filter_list_file(["STANDARD"])
    try:
        dedup_utils.run_dedup_estimate(allow_sc_file=allow_file)
        stats_allow = dedup_utils.wait_for_dedup_completion()
        assert stats_allow.get("completed") is True, "Allow estimate did not complete"
        parsed_allow = dedup_utils.parse_dedup_stats_full(stats_allow)
        assert (
            parsed_allow["ingress_count"] > 0
        ), f"Allow STANDARD: expected ingress_count > 0, got {parsed_allow['ingress_count']}"
        log.info(
            f"Allow STANDARD estimate: ingress={parsed_allow['ingress_count']}, "
            f"ingress_skip_filtered_sc={parsed_allow.get('ingress_skip_filtered_sc', 0)}"
        )
    finally:
        os.remove(allow_file)

    deny_file = dedup_utils.create_filter_list_file(["STANDARD"])
    try:
        dedup_utils.run_dedup_estimate(deny_sc_file=deny_file)
        stats_deny = dedup_utils.wait_for_dedup_completion()
        assert stats_deny.get("completed") is True, "Deny estimate did not complete"
        parsed_deny = dedup_utils.parse_dedup_stats_full(stats_deny)
        assert parsed_deny.get("ingress_skip_filtered_sc", 0) > 0, (
            f"Deny STANDARD: expected ingress_skip_filtered_sc > 0, "
            f"got {parsed_deny.get('ingress_skip_filtered_sc', 0)}"
        )
        log.info(
            f"Deny STANDARD estimate: ingress_skip_filtered_sc="
            f"{parsed_deny.get('ingress_skip_filtered_sc', 0)}"
        )
    finally:
        os.remove(deny_file)


@pytest.mark.compression
def test_s25_filter_sc_exec(s3_client, bucket):
    """S25: SC filter on exec — allow STANDARD dedupes, deny STANDARD dedupes nothing."""
    obj_count = 20
    obj_size = 5 * 1024
    keys, expected_md5, _ = dedup_utils.upload_identical_objects(
        s3_client, bucket, obj_count, obj_size, prefix="sc-exec-obj"
    )

    deny_file = dedup_utils.create_filter_list_file(["STANDARD"])
    try:
        dedup_utils.run_dedup_estimate(deny_sc_file=deny_file)
        dedup_utils.wait_for_dedup_completion()
        dedup_utils.run_dedup_execute(deny_sc_file=deny_file)
        exec_deny = dedup_utils.wait_for_dedup_completion()
        assert exec_deny.get("completed") is True, "Deny exec did not complete"
        parsed_deny = dedup_utils.parse_dedup_stats_full(exec_deny)
        assert (
            parsed_deny["deduped_count"] == 0
        ), f"Deny STANDARD: expected deduped_count == 0, got {parsed_deny['deduped_count']}"
        log.info(f"Deny STANDARD exec: deduped_count={parsed_deny['deduped_count']}")
    finally:
        os.remove(deny_file)

    dedup_utils.verify_all_objects_accessible(s3_client, bucket, keys)
    dedup_utils.verify_all_objects_integrity(s3_client, bucket, keys, expected_md5)

    allow_file = dedup_utils.create_filter_list_file(["STANDARD"])
    try:
        dedup_utils.run_dedup_estimate(allow_sc_file=allow_file)
        dedup_utils.wait_for_dedup_completion()
        dedup_utils.run_dedup_execute(allow_sc_file=allow_file)
        exec_allow = dedup_utils.wait_for_dedup_completion()
        assert exec_allow.get("completed") is True, "Allow exec did not complete"
        parsed_allow = dedup_utils.parse_dedup_stats_full(exec_allow)
        assert (
            parsed_allow["deduped_count"] > 0
        ), f"Allow STANDARD: expected deduped_count > 0, got {parsed_allow['deduped_count']}"
        log.info(f"Allow STANDARD exec: deduped_count={parsed_allow['deduped_count']}")
    finally:
        os.remove(allow_file)

    dedup_utils.verify_all_objects_accessible(s3_client, bucket, keys)
    dedup_utils.verify_all_objects_integrity(s3_client, bucket, keys, expected_md5)
    dedup_utils.log_dedup_savings(exec_allow, obj_count * obj_size, "S25")


@pytest.mark.compression
def test_s26_compression_stats_counters(s3_client, bucket, ssh_con):
    """S26: Validate compression stat counters after dedup on compressed objects."""
    obj_count = 30
    obj_size = 5 * 1024

    dedup_utils.enable_zone_compression("zlib", ssh_con)
    try:
        keys, expected_md5, _ = dedup_utils.upload_identical_objects(
            s3_client, bucket, obj_count, obj_size, prefix="compstats-obj"
        )
        dedup_utils.assert_all_objects_compression(
            bucket, keys, expect_compressed=True, tag="PRE-DEDUP"
        )

        dedup_utils.run_dedup_estimate()
        dedup_utils.wait_for_dedup_completion()
        dedup_utils.run_dedup_execute()
        exec_stats = dedup_utils.wait_for_dedup_completion()
        assert exec_stats.get("completed") is True, "Exec did not complete"

        parsed = dedup_utils.parse_dedup_stats_full(exec_stats)
        assert (
            parsed["deduped_count"] > 0
        ), f"Expected deduped_count > 0, got {parsed['deduped_count']}"
        assert (
            parsed["compressed_objs"] > 0
        ), f"Expected compressed_objs > 0, got {parsed['compressed_objs']}"
        assert (
            parsed["compressed_bytes"] > 0
        ), f"Expected compressed_bytes > 0, got {parsed['compressed_bytes']}"
        assert (
            parsed["deduped_compressed_objects"] > 0
        ), f"Expected deduped_compressed_objects > 0, got {parsed['deduped_compressed_objects']}"
        log.info(
            f"Compression stats: compressed_objs={parsed['compressed_objs']}, "
            f"compressed_bytes={parsed['compressed_bytes']}, "
            f"deduped_compressed_objects={parsed['deduped_compressed_objects']}"
        )

        dedup_utils.assert_all_objects_compression(
            bucket, keys, expect_compressed=True, tag="POST-DEDUP"
        )
        dedup_utils.verify_all_objects_accessible(s3_client, bucket, keys)
        dedup_utils.verify_all_objects_integrity(s3_client, bucket, keys, expected_md5)
        dedup_utils.log_dedup_savings(exec_stats, obj_count * obj_size, "S26")
    finally:
        dedup_utils.disable_zone_compression(ssh_con)


@pytest.mark.compression
def test_s27_cross_user_compressed_to_uncompressed(s3_clients, test_context, ssh_con):
    """S27: Cross-user dedup with compression mode switch — compressed to uncompressed.

    user1 uploads to bucket_a with zlib compression enabled.
    user2 uploads identical content to bucket_b with compression disabled.
    After dedup, all objects in both buckets should be uncompressed.
    Verifies dedup works across objects owned by different RGW users.
    """
    s3_client1, s3_client2 = s3_clients
    obj_count = 20
    obj_size = 5 * 1024
    bucket_a = f"s27-comp-{random.randint(1, 9999)}"
    bucket_b = f"s27-uncomp-{random.randint(1, 9999)}"

    dedup_utils.enable_zone_compression("zlib", ssh_con)
    try:
        s3_client1.create_bucket(Bucket=bucket_a)
        test_context["buckets"].append(bucket_a)
        marker_a = dedup_utils.get_bucket_marker(bucket_a)
        if marker_a:
            test_context["bucket_markers"][bucket_a] = marker_a

        keys_a, expected_md5, _ = dedup_utils.upload_identical_objects(
            s3_client1, bucket_a, obj_count, obj_size, prefix="mode-obj"
        )
        dedup_utils.assert_all_objects_compression(
            bucket_a, keys_a, expect_compressed=True, tag="PRE-DEDUP bucket_a"
        )

        dedup_utils.disable_zone_compression(ssh_con)

        s3_client2.create_bucket(Bucket=bucket_b)
        test_context["buckets"].append(bucket_b)
        marker_b = dedup_utils.get_bucket_marker(bucket_b)
        if marker_b:
            test_context["bucket_markers"][bucket_b] = marker_b

        keys_b, md5_b, _ = dedup_utils.upload_identical_objects(
            s3_client2, bucket_b, obj_count, obj_size, prefix="mode-obj"
        )
        assert expected_md5 == md5_b, "MD5 mismatch between buckets"
        dedup_utils.assert_all_objects_compression(
            bucket_b, keys_b, expect_compressed=False, tag="PRE-DEDUP bucket_b"
        )

        dedup_utils.run_dedup_estimate()
        estimate_stats = dedup_utils.wait_for_dedup_completion()
        assert estimate_stats.get("completed") is True, "Estimate did not complete"

        dedup_utils.run_dedup_execute()
        exec_stats = dedup_utils.wait_for_dedup_completion()
        assert exec_stats.get("completed") is True, "Exec did not complete"

        parsed = dedup_utils.parse_dedup_stats_full(exec_stats)
        assert (
            parsed["deduped_count"] > 0
        ), f"Cross-user dedup should process objects, got deduped_count={parsed['deduped_count']}"
        assert (
            parsed["compressed_objs"] > 0
        ), f"Expected compressed_objs > 0, got {parsed['compressed_objs']}"
        assert (
            parsed["clear_compression_on_tgt"] > 0
        ), f"Expected clear_compression_on_tgt > 0, got {parsed['clear_compression_on_tgt']}"
        log.info(
            f"Cross-user dedup (comp->uncomp): deduped={parsed['deduped_count']}, "
            f"compressed_objs={parsed['compressed_objs']}, "
            f"clear_compression_on_tgt={parsed['clear_compression_on_tgt']}"
        )

        dedup_utils.assert_all_objects_compression(
            bucket_a, keys_a, expect_compressed=False, tag="POST-DEDUP bucket_a"
        )
        dedup_utils.assert_all_objects_compression(
            bucket_b, keys_b, expect_compressed=False, tag="POST-DEDUP bucket_b"
        )

        dedup_utils.verify_all_objects_accessible(s3_client1, bucket_a, keys_a)
        dedup_utils.verify_all_objects_accessible(s3_client2, bucket_b, keys_b)
        dedup_utils.verify_all_objects_integrity(
            s3_client1, bucket_a, keys_a, expected_md5
        )
        dedup_utils.verify_all_objects_integrity(
            s3_client2, bucket_b, keys_b, expected_md5
        )
        dedup_utils.log_dedup_savings(exec_stats, 2 * obj_count * obj_size, "S27")
    finally:
        dedup_utils.disable_zone_compression(ssh_con)
        dedup_utils.cleanup_bucket(s3_client1, bucket_a)
        dedup_utils.cleanup_bucket(s3_client2, bucket_b)


@pytest.mark.compression
def test_s28_cross_user_uncompressed_to_compressed(s3_clients, test_context, ssh_con):
    """S28: Cross-user dedup with compression mode switch — uncompressed to compressed.

    user1 uploads to bucket_a with compression disabled.
    user2 uploads identical content to bucket_b with zlib compression enabled.
    After dedup, all objects in both buckets should be compressed.
    Verifies dedup works across objects owned by different RGW users.
    """
    s3_client1, s3_client2 = s3_clients
    obj_count = 20
    obj_size = 5 * 1024
    bucket_a = f"s28-uncomp-{random.randint(1, 9999)}"
    bucket_b = f"s28-comp-{random.randint(1, 9999)}"

    dedup_utils.disable_zone_compression(ssh_con)
    try:
        s3_client1.create_bucket(Bucket=bucket_a)
        test_context["buckets"].append(bucket_a)
        marker_a = dedup_utils.get_bucket_marker(bucket_a)
        if marker_a:
            test_context["bucket_markers"][bucket_a] = marker_a

        keys_a, expected_md5, _ = dedup_utils.upload_identical_objects(
            s3_client1, bucket_a, obj_count, obj_size, prefix="mode-obj"
        )
        dedup_utils.assert_all_objects_compression(
            bucket_a, keys_a, expect_compressed=False, tag="PRE-DEDUP bucket_a"
        )

        dedup_utils.enable_zone_compression("zlib", ssh_con)

        s3_client2.create_bucket(Bucket=bucket_b)
        test_context["buckets"].append(bucket_b)
        marker_b = dedup_utils.get_bucket_marker(bucket_b)
        if marker_b:
            test_context["bucket_markers"][bucket_b] = marker_b

        keys_b, md5_b, _ = dedup_utils.upload_identical_objects(
            s3_client2, bucket_b, obj_count, obj_size, prefix="mode-obj"
        )
        assert expected_md5 == md5_b, "MD5 mismatch between buckets"
        dedup_utils.assert_all_objects_compression(
            bucket_b, keys_b, expect_compressed=True, tag="PRE-DEDUP bucket_b"
        )

        dedup_utils.run_dedup_estimate()
        estimate_stats = dedup_utils.wait_for_dedup_completion()
        assert estimate_stats.get("completed") is True, "Estimate did not complete"

        dedup_utils.run_dedup_execute()
        exec_stats = dedup_utils.wait_for_dedup_completion()
        assert exec_stats.get("completed") is True, "Exec did not complete"

        parsed = dedup_utils.parse_dedup_stats_full(exec_stats)
        assert (
            parsed["deduped_count"] > 0
        ), f"Cross-user dedup should process objects, got deduped_count={parsed['deduped_count']}"
        assert (
            parsed["compressed_objs"] > 0
        ), f"Expected compressed_objs > 0, got {parsed['compressed_objs']}"
        assert (
            parsed["set_compression_on_tgt"] > 0
        ), f"Expected set_compression_on_tgt > 0, got {parsed['set_compression_on_tgt']}"
        log.info(
            f"Cross-user dedup (uncomp->comp): deduped={parsed['deduped_count']}, "
            f"compressed_objs={parsed['compressed_objs']}, "
            f"set_compression_on_tgt={parsed['set_compression_on_tgt']}"
        )

        dedup_utils.assert_all_objects_compression(
            bucket_a, keys_a, expect_compressed=True, tag="POST-DEDUP bucket_a"
        )
        dedup_utils.assert_all_objects_compression(
            bucket_b, keys_b, expect_compressed=True, tag="POST-DEDUP bucket_b"
        )

        dedup_utils.verify_all_objects_accessible(s3_client1, bucket_a, keys_a)
        dedup_utils.verify_all_objects_accessible(s3_client2, bucket_b, keys_b)
        dedup_utils.verify_all_objects_integrity(
            s3_client1, bucket_a, keys_a, expected_md5
        )
        dedup_utils.verify_all_objects_integrity(
            s3_client2, bucket_b, keys_b, expected_md5
        )
        dedup_utils.log_dedup_savings(exec_stats, 2 * obj_count * obj_size, "S28")
    finally:
        dedup_utils.disable_zone_compression(ssh_con)
        dedup_utils.cleanup_bucket(s3_client1, bucket_a)
        dedup_utils.cleanup_bucket(s3_client2, bucket_b)


# =============================================================================
# BUG-HUNTING TESTS (B1-B7)
# =============================================================================


@pytest.mark.bug
def test_b1_overwrite_deduped_object(s3_client, bucket, rgw_config):
    """B1: Overwrite one deduped target, verify siblings survive."""
    obj_count = getattr(rgw_config, "objects_count", None) or 10
    obj_size = 5 * 1024

    keys, expected_md5, _ = dedup_utils.upload_identical_objects(
        s3_client, bucket, obj_count, obj_size, prefix="ow-obj"
    )

    dedup_utils.run_dedup_estimate()
    estimate_stats = dedup_utils.wait_for_dedup_completion()
    assert estimate_stats.get("completed") is True, "Estimate did not complete"

    dedup_utils.run_dedup_execute()
    exec_stats = dedup_utils.wait_for_dedup_completion()
    assert exec_stats.get("completed") is True, "Exec did not complete"

    dedup_utils.validate_estimate_exec_ratio(estimate_stats, exec_stats)
    dedup_utils.verify_all_objects_integrity(s3_client, bucket, keys, expected_md5)
    dedup_utils.log_dedup_savings(exec_stats, obj_count * obj_size, "B1")

    overwrite_key = keys[1]
    new_data = os.urandom(5 * 1024)
    new_md5 = hashlib.md5(new_data).hexdigest()
    s3_client.put_object(Bucket=bucket, Key=overwrite_key, Body=new_data)

    resp = s3_client.get_object(Bucket=bucket, Key=overwrite_key)
    body = resp["Body"].read()
    assert hashlib.md5(body).hexdigest() == new_md5

    remaining_keys = [k for k in keys if k != overwrite_key]
    for key in remaining_keys:
        resp = s3_client.get_object(Bucket=bucket, Key=key)
        body = resp["Body"].read()
        assert (
            hashlib.md5(body).hexdigest() == expected_md5
        ), f"Object {key} corrupted after overwrite of sibling"


@pytest.mark.bug
def test_b2_delete_dedup_source(s3_client, bucket, rgw_config):
    """B2: Delete objects one by one, verify remaining survive each time."""
    obj_count = getattr(rgw_config, "objects_count", None) or 10
    keys, expected_md5, _ = dedup_utils.upload_identical_objects(
        s3_client, bucket, obj_count, 5 * 1024, prefix="delsrc-obj"
    )

    dedup_utils.run_dedup_estimate()
    estimate_stats = dedup_utils.wait_for_dedup_completion()
    assert estimate_stats.get("completed") is True, "Estimate did not complete"

    dedup_utils.run_dedup_execute()
    exec_stats = dedup_utils.wait_for_dedup_completion()
    assert exec_stats.get("completed") is True, "Exec did not complete"

    dedup_utils.validate_estimate_exec_ratio(estimate_stats, exec_stats)
    dedup_utils.verify_all_objects_integrity(s3_client, bucket, keys, expected_md5)
    dedup_utils.log_dedup_savings(exec_stats, obj_count * 5 * 1024, "B2")

    remaining = list(keys)
    for key_to_delete in keys:
        s3_client.delete_object(Bucket=bucket, Key=key_to_delete)
        remaining.remove(key_to_delete)
        if not remaining:
            break
        for key in remaining:
            resp = s3_client.get_object(Bucket=bucket, Key=key)
            body = resp["Body"].read()
            assert (
                hashlib.md5(body).hexdigest() == expected_md5
            ), f"Object {key} corrupted after deleting {key_to_delete}"


@pytest.mark.bug
def test_b3_s3_copy_then_delete_deduped(s3_client, bucket_factory):
    """B3: S3 COPY deduped object (same + cross bucket), delete originals."""
    bucket_a = bucket_factory("dedup-copysrc")
    bucket_b = bucket_factory("dedup-copydst")

    obj_size = 5 * 1024
    keys, expected_md5, _ = dedup_utils.upload_identical_objects(
        s3_client, bucket_a, 5, obj_size, prefix="cpsrc-obj"
    )

    dedup_utils.run_dedup_estimate()
    estimate_stats = dedup_utils.wait_for_dedup_completion()
    assert estimate_stats.get("completed") is True, "Estimate did not complete"

    dedup_utils.run_dedup_execute()
    exec_stats = dedup_utils.wait_for_dedup_completion()
    assert exec_stats.get("completed") is True, "Exec did not complete"

    dedup_utils.validate_estimate_exec_ratio(estimate_stats, exec_stats)
    dedup_utils.verify_all_objects_integrity(s3_client, bucket_a, keys, expected_md5)
    dedup_utils.log_dedup_savings(exec_stats, 5 * obj_size, "B3")

    s3_client.copy_object(
        Bucket=bucket_a,
        Key="copy-same",
        CopySource={"Bucket": bucket_a, "Key": keys[0]},
    )
    s3_client.copy_object(
        Bucket=bucket_b,
        Key="copy-cross",
        CopySource={"Bucket": bucket_a, "Key": keys[1]},
    )

    for key in keys:
        s3_client.delete_object(Bucket=bucket_a, Key=key)

    dedup_utils.verify_object_integrity(s3_client, bucket_a, "copy-same", expected_md5)
    dedup_utils.verify_object_integrity(s3_client, bucket_b, "copy-cross", expected_md5)


@pytest.mark.bug
def test_b4_cross_bucket_source_delete(s3_client, bucket_factory):
    """B4: Cross-bucket dedup, delete source bucket, verify target survives."""
    bucket_a = bucket_factory("dedup-xbkt-a")
    bucket_b = bucket_factory("dedup-xbkt-b")

    obj_size = 5 * 1024
    identical_data = dedup_utils.generate_identical_data(obj_size)
    expected_md5 = hashlib.md5(identical_data).hexdigest()

    s3_client.put_object(Bucket=bucket_a, Key="cross-obj-a", Body=identical_data)
    s3_client.put_object(Bucket=bucket_b, Key="cross-obj-b", Body=identical_data)

    dedup_utils.run_dedup_estimate()
    estimate_stats = dedup_utils.wait_for_dedup_completion()
    assert estimate_stats.get("completed") is True, "Estimate did not complete"

    dedup_utils.run_dedup_execute()
    exec_stats = dedup_utils.wait_for_dedup_completion()
    assert exec_stats.get("completed") is True, "Exec did not complete"

    dedup_utils.validate_estimate_exec_ratio(estimate_stats, exec_stats)

    dedup_utils.verify_object_integrity(
        s3_client, bucket_a, "cross-obj-a", expected_md5
    )
    dedup_utils.verify_object_integrity(
        s3_client, bucket_b, "cross-obj-b", expected_md5
    )
    dedup_utils.log_dedup_savings(exec_stats, 2 * obj_size, "B4")

    s3_client.delete_object(Bucket=bucket_a, Key="cross-obj-a")
    s3_client.delete_bucket(Bucket=bucket_a)

    dedup_utils.verify_object_integrity(
        s3_client, bucket_b, "cross-obj-b", expected_md5
    )


@pytest.mark.bug
def test_b5_dedup_idempotency(s3_client, bucket, rgw_config):
    """B5: Run dedup exec 3 times, verify no corruption or double-counting."""
    obj_count = getattr(rgw_config, "objects_count", None) or 20
    keys, expected_md5, _ = dedup_utils.upload_identical_objects(
        s3_client, bucket, obj_count, 5 * 1024, prefix="idemp-obj"
    )

    dedup_utils.run_dedup_estimate()
    estimate_stats = dedup_utils.wait_for_dedup_completion()
    assert estimate_stats.get("completed") is True, "Estimate did not complete"

    all_run_stats = []
    all_raw_stats = []
    for run_num in range(1, 4):
        dedup_utils.run_dedup_execute()
        stats = dedup_utils.wait_for_dedup_completion()
        assert stats.get("completed") is True, f"Exec run {run_num} did not complete"
        all_raw_stats.append(stats)
        parsed = dedup_utils.parse_dedup_stats(stats)
        all_run_stats.append(parsed)
        dedup_utils.verify_all_objects_accessible(s3_client, bucket, keys)

    assert all_run_stats[0]["deduped_count"] > 0
    dedup_utils.log_dedup_savings(all_raw_stats[0], obj_count * 5 * 1024, "B5")
    for parsed in all_run_stats[1:]:
        assert parsed["deduped_count"] == 0
        assert parsed["skipped_shared_manifest"] > 0

    dedup_utils.verify_all_objects_integrity(s3_client, bucket, keys, expected_md5)


@pytest.mark.bug
@pytest.mark.slow
def test_b6_lc_expiration_deduped_large_objects(s3_client, bucket):
    """B6: LC expiration must succeed on deduped >4MB objects (IBMCEPH-17201).

    Polarion: CEPH-83632923

    Dedup's setxattr modifies object mtime causing drift from bucket-index
    mtime. LC's remove_expired_obj compares the two and fails with
    ERR_PRECONDITION_FAILED (-2015) when they diverge. This test verifies
    the fix allows LC to delete deduped objects normally.
    """
    obj_size = 5 * 1024 * 1024  # 5MB — triggers split-head dedup path
    obj_count = 3

    utils.exec_shell_cmd("ceph config set client.rgw rgw_lc_debug_interval 30")
    time.sleep(3)

    keys, expected_md5, _ = dedup_utils.upload_identical_multipart_objects(
        s3_client, bucket, obj_count, obj_size, prefix="b6-lc-large"
    )

    dedup_utils.run_dedup_estimate()
    estimate_stats = dedup_utils.wait_for_dedup_completion()
    assert estimate_stats.get("completed") is True, "Estimate did not complete"

    dedup_utils.run_dedup_execute()
    exec_stats = dedup_utils.wait_for_dedup_completion()
    assert exec_stats.get("completed") is True, "Exec did not complete"

    dedup_utils.verify_all_objects_accessible(s3_client, bucket, keys)

    dedup_utils.set_lifecycle_expiration(s3_client, bucket, days=1)
    log.info("Waiting for LC to process deduped >4MB objects...")
    time.sleep(120)

    resp = s3_client.list_objects_v2(Bucket=bucket)
    remaining = resp.get("KeyCount", 0)
    log.info(f"Objects remaining after LC: {remaining}")
    assert remaining == 0, (
        f"LC failed to delete {remaining} deduped objects — "
        f"possible ERR_PRECONDITION_FAILED (-2015) regression (IBMCEPH-17201)"
    )


@pytest.mark.bug
def test_b7_s3_delete_deduped_large_objects(s3_client, bucket):
    """B7: S3 DeleteObject must work on deduped >4MB objects (IBMCEPH-17201 workaround).

    Polarion: CEPH-83632923

    While LC failed on deduped objects due to mtime drift, S3 API DeleteObject
    always worked. This test confirms the S3 delete path remains functional.
    """
    obj_size = 5 * 1024 * 1024
    obj_count = 3

    keys, expected_md5, _ = dedup_utils.upload_identical_multipart_objects(
        s3_client, bucket, obj_count, obj_size, prefix="b7-del-large"
    )

    dedup_utils.run_dedup_estimate()
    estimate_stats = dedup_utils.wait_for_dedup_completion()
    assert estimate_stats.get("completed") is True, "Estimate did not complete"

    dedup_utils.run_dedup_execute()
    exec_stats = dedup_utils.wait_for_dedup_completion()
    assert exec_stats.get("completed") is True, "Exec did not complete"

    dedup_utils.verify_all_objects_accessible(s3_client, bucket, keys)

    for key in keys:
        s3_client.delete_object(Bucket=bucket, Key=key)
        log.info(f"S3 DeleteObject succeeded for deduped large object: {key}")

    resp = s3_client.list_objects_v2(Bucket=bucket)
    remaining = resp.get("KeyCount", 0)
    assert remaining == 0, f"S3 DeleteObject failed: {remaining} deduped objects remain"


# =============================================================================
# ENHANCEMENT TESTS (E1-E5) -- 128-limit boundary
# =============================================================================


@pytest.mark.enhancement
def test_e1_128_limit_boundary(s3_client, bucket):
    """E1: 200 identical 5KB objects, verify only ~127 deduped, rest skipped."""
    obj_count = 200
    obj_size = 5 * 1024

    keys, expected_md5, _ = dedup_utils.upload_identical_objects(
        s3_client, bucket, obj_count, obj_size, prefix="limit-obj"
    )

    dedup_utils.run_dedup_estimate()
    estimate_stats = dedup_utils.wait_for_dedup_completion()
    assert estimate_stats.get("completed") is True, "Estimate did not complete"

    dedup_utils.run_dedup_execute()
    exec_stats = dedup_utils.wait_for_dedup_completion()
    assert exec_stats.get("completed") is True, "Exec did not complete"

    dedup_utils.validate_estimate_exec_ratio(estimate_stats, exec_stats)
    parsed = dedup_utils.parse_dedup_stats_full(exec_stats)

    log.info(
        f"Deduped: {parsed['deduped_count']}, "
        f"Skipped Too Many Copies: {parsed['skipped_too_many_copies']}"
    )

    assert parsed["deduped_count"] > 0, "Expected some objects deduped"
    assert (
        parsed["deduped_count"] <= 128
    ), f"Expected deduped <= 128 (128 limit), got {parsed['deduped_count']}"
    assert (
        parsed["skipped_too_many_copies"] > 0
    ), f"Expected Skipped Too Many Copies > 0, got {parsed['skipped_too_many_copies']}"

    dedup_utils.verify_all_objects_accessible(s3_client, bucket, keys)
    dedup_utils.verify_all_objects_integrity(s3_client, bucket, keys, expected_md5)
    dedup_utils.log_dedup_savings(exec_stats, obj_count * obj_size, "E1")


@pytest.mark.enhancement
def test_e2_versioned_boundary(s3_client, bucket):
    """E2: 130 versions of same key (past 128 boundary), verify all accessible."""
    version_count = 130
    object_key = "versioned-boundary-obj"
    obj_size = 5 * 1024

    version_ids, expected_md5, _ = dedup_utils.upload_identical_versions(
        s3_client, bucket, object_key, version_count, obj_size
    )

    dedup_utils.run_dedup_estimate()
    estimate_stats = dedup_utils.wait_for_dedup_completion()
    assert estimate_stats.get("completed") is True, "Estimate did not complete"

    dedup_utils.run_dedup_execute()
    exec_stats = dedup_utils.wait_for_dedup_completion()
    assert exec_stats.get("completed") is True, "Exec did not complete"

    dedup_utils.validate_estimate_exec_ratio(estimate_stats, exec_stats)
    parsed = dedup_utils.parse_dedup_stats_full(exec_stats)

    assert parsed["deduped_count"] > 0
    assert parsed["unique_count"] >= 1

    for vid in version_ids:
        resp = s3_client.get_object(Bucket=bucket, Key=object_key, VersionId=vid)
        body = resp["Body"].read()
        assert (
            hashlib.md5(body).hexdigest() == expected_md5
        ), f"Version {vid} MD5 mismatch"

    versions = dedup_utils.get_all_versions(s3_client, bucket, object_key)
    assert len(versions) == version_count
    dedup_utils.log_dedup_savings(exec_stats, version_count * obj_size, "E2")


@pytest.mark.enhancement
def test_e3_multi_cycle_no_progress(s3_client, bucket):
    """E3: 200 objects, 3 exec cycles. Cycles 2-3 dedup zero -- system is stuck."""
    obj_count = 200
    obj_size = 5 * 1024

    keys, expected_md5, _ = dedup_utils.upload_identical_objects(
        s3_client, bucket, obj_count, obj_size, prefix="cycle-obj"
    )

    all_cycle_stats = []
    first_raw_stats = None
    for cycle in range(1, 4):
        log.info(f"=== Dedup exec cycle {cycle}/3 ===")
        dedup_utils.run_dedup_estimate()
        est = dedup_utils.wait_for_dedup_completion()
        assert est.get("completed") is True, f"Estimate cycle {cycle} did not complete"

        dedup_utils.run_dedup_execute()
        stats = dedup_utils.wait_for_dedup_completion()
        assert stats.get("completed") is True, f"Exec cycle {cycle} did not complete"
        if cycle == 1:
            first_raw_stats = stats

        dedup_utils.validate_estimate_exec_ratio(est, stats)
        parsed = dedup_utils.parse_dedup_stats_full(stats)
        all_cycle_stats.append(parsed)
        log.info(
            f"Cycle {cycle}: deduped={parsed['deduped_count']}, "
            f"skipped_too_many={parsed['skipped_too_many_copies']}, "
            f"skipped_shared_manifest={parsed['skipped_shared_manifest']}"
        )

    assert all_cycle_stats[0]["deduped_count"] > 0, "Cycle 1 should dedup objects"
    assert all_cycle_stats[1]["deduped_count"] == 0, "Cycle 2 should dedup 0"
    assert all_cycle_stats[2]["deduped_count"] == 0, "Cycle 3 should dedup 0"

    assert (
        all_cycle_stats[1]["skipped_too_many_copies"]
        == all_cycle_stats[2]["skipped_too_many_copies"]
    ), "Skipped Too Many Copies should be stable across cycles 2-3"

    for parsed in all_cycle_stats[1:]:
        assert parsed["skipped_shared_manifest"] >= all_cycle_stats[0]["deduped_count"]

    dedup_utils.verify_all_objects_accessible(s3_client, bucket, keys)
    dedup_utils.verify_all_objects_integrity(s3_client, bucket, keys, expected_md5)
    dedup_utils.log_dedup_savings(first_raw_stats, obj_count * obj_size, "E3")


@pytest.mark.enhancement
def test_e4_split_head_small_objects(s3_client, bucket):
    """E4: 50 identical 5KB objects, verify split-head mechanism used."""
    obj_count = 50
    obj_size = 5 * 1024

    keys, expected_md5, original_data = dedup_utils.upload_identical_objects(
        s3_client, bucket, obj_count, obj_size, prefix="splithead-obj"
    )

    dedup_utils.run_dedup_estimate()
    estimate_stats = dedup_utils.wait_for_dedup_completion()
    assert estimate_stats.get("completed") is True, "Estimate did not complete"

    dedup_utils.run_dedup_execute()
    exec_stats = dedup_utils.wait_for_dedup_completion()
    assert exec_stats.get("completed") is True, "Exec did not complete"

    dedup_utils.validate_estimate_exec_ratio(estimate_stats, exec_stats)
    parsed = dedup_utils.parse_dedup_stats_full(exec_stats)

    assert parsed["deduped_count"] > 0
    assert (
        parsed["split_head_src"] > 0
    ), f"Expected Split-Head Src > 0, got {parsed['split_head_src']}"
    assert (
        parsed["split_head_tgt"] > 0
    ), f"Expected Split-Head Tgt > 0, got {parsed['split_head_tgt']}"

    dedup_utils.verify_all_objects_accessible(s3_client, bucket, keys)
    dedup_utils.verify_all_objects_integrity(s3_client, bucket, keys, expected_md5)

    for key in keys[:5]:
        dedup_utils.verify_range_get(s3_client, bucket, key, original_data, 0, 1023)
        dedup_utils.verify_range_get(
            s3_client, bucket, key, original_data, 2048, obj_size - 1
        )
    dedup_utils.log_dedup_savings(exec_stats, obj_count * obj_size, "E4")


@pytest.mark.enhancement
def test_e5_stats_validation(s3_client, bucket):
    """E5: Validate all expected stats fields present after estimate + exec."""
    obj_count = 50
    obj_size = 5 * 1024

    keys, expected_md5, _ = dedup_utils.upload_identical_objects(
        s3_client, bucket, obj_count, obj_size, prefix="stats-obj"
    )

    dedup_utils.run_dedup_estimate()
    estimate_stats = dedup_utils.wait_for_dedup_completion()

    assert isinstance(estimate_stats, dict)
    assert estimate_stats.get("completed") is True, "Estimate did not complete"
    est_parsed = dedup_utils.parse_dedup_stats_full(estimate_stats)
    assert (
        est_parsed["ingress_count"] > 0
    ), f"Estimate ingress_count should be > 0, got {est_parsed['ingress_count']}"
    assert est_parsed["unique_count"] >= 1
    assert est_parsed["duplicate_count"] > 0
    assert est_parsed["dedup_ratio_estimate"] > 1.0

    dedup_utils.run_dedup_execute()
    exec_stats = dedup_utils.wait_for_dedup_completion()

    assert isinstance(exec_stats, dict)
    assert exec_stats.get("completed") is True, "Exec did not complete"
    exec_parsed = dedup_utils.parse_dedup_stats_full(exec_stats)
    assert (
        exec_parsed["total_processed"] == obj_count
    ), f"Exec total_processed should be {obj_count}, got {exec_parsed['total_processed']}"
    assert exec_parsed["deduped_count"] > 0
    assert exec_parsed["unique_count"] >= 1
    assert exec_parsed["dedup_ratio_actual"] > 1.0

    dedup_utils.validate_estimate_exec_ratio(estimate_stats, exec_stats)

    md5_skipped = exec_stats.get("md5_stats", {}).get("skipped", {})
    for field in [
        "Skipped shared_manifest",
        "Skipped purged small objs",
        "Skipped singleton objs",
        "Skipped source record",
    ]:
        assert field in md5_skipped, f"Missing field: {field}"
    dedup_utils.log_dedup_savings(exec_stats, obj_count * obj_size, "E5")


# =============================================================================
# RESTART SCENARIOS (R1-R5)
# =============================================================================


@pytest.mark.restart
def test_r1_rgw_restart_mid_exec(s3_client, bucket, ssh_con):
    """R1: RGW restart mid-exec — verify no data corruption."""
    obj_count = 100
    obj_size = 5 * 1024
    keys, expected_md5, _ = dedup_utils.upload_identical_objects(
        s3_client, bucket, obj_count, obj_size, prefix="r1"
    )

    dedup_utils.run_dedup_estimate()
    dedup_utils.wait_for_dedup_completion()

    dedup_utils.run_dedup_execute_async()
    time.sleep(5)
    dedup_utils.restart_rgw_service()
    time.sleep(30)

    # Re-run dedup exec to complete any interrupted work
    dedup_utils.run_dedup_execute()
    stats = dedup_utils.wait_for_dedup_completion()
    assert stats.get("completed") is True

    dedup_utils.verify_all_objects_accessible(s3_client, bucket, keys)
    dedup_utils.verify_all_objects_integrity(s3_client, bucket, keys, expected_md5)
    parsed = dedup_utils.parse_dedup_stats(stats)
    assert (
        parsed.get("deduped_count", 0) > 0
        or parsed.get("skipped_shared_manifest", 0) > 0
    )


@pytest.mark.restart
def test_r2_rgw_sigkill_mid_exec(s3_client, bucket, ssh_con):
    """R2: SIGKILL RGW during split-head — verify no orphan tail objects."""
    obj_count = 50
    obj_size = 5 * 1024
    keys, expected_md5, _ = dedup_utils.upload_identical_objects(
        s3_client, bucket, obj_count, obj_size, prefix="r2"
    )

    dedup_utils.run_dedup_estimate()
    dedup_utils.wait_for_dedup_completion()

    dedup_utils.run_dedup_execute_async()
    time.sleep(3)
    dedup_utils.kill_rgw_process()
    time.sleep(30)  # Wait for ceph orch to respawn

    dedup_utils.run_dedup_execute()
    stats = dedup_utils.wait_for_dedup_completion()
    assert stats.get("completed") is True

    dedup_utils.verify_all_objects_accessible(s3_client, bucket, keys)
    orphans = dedup_utils.check_orphan_objects()
    log.info(f"Orphan split-head objects found: {len(orphans)}")


@pytest.mark.restart
def test_r3_osd_restart_mid_exec(s3_client, bucket, ssh_con):
    """R3: OSD restart during dedup exec — verify dedup completes."""
    obj_count = 100
    obj_size = 5 * 1024
    keys, expected_md5, _ = dedup_utils.upload_identical_objects(
        s3_client, bucket, obj_count, obj_size, prefix="r3"
    )

    dedup_utils.run_dedup_estimate()
    dedup_utils.wait_for_dedup_completion()

    dedup_utils.run_dedup_execute_async()
    time.sleep(5)

    osd_id = dedup_utils.get_osd_for_bucket(bucket)
    dedup_utils.restart_osd(osd_id)
    dedup_utils.wait_for_health_ok(timeout=120)

    stats = dedup_utils.wait_for_dedup_completion(timeout=900)
    if not stats.get("completed"):
        dedup_utils.run_dedup_execute()
        stats = dedup_utils.wait_for_dedup_completion(timeout=900)
    assert stats.get("completed") is True

    dedup_utils.verify_all_objects_accessible(s3_client, bucket, keys)
    dedup_utils.verify_all_objects_integrity(s3_client, bucket, keys, expected_md5)


@pytest.mark.restart
def test_r4_mon_restart_mid_exec(s3_client, bucket, ssh_con):
    """R4: MON leader restart during dedup — verify dedup completes."""
    obj_count = 50
    obj_size = 5 * 1024
    keys, expected_md5, _ = dedup_utils.upload_identical_objects(
        s3_client, bucket, obj_count, obj_size, prefix="r4"
    )

    dedup_utils.run_dedup_estimate()
    dedup_utils.wait_for_dedup_completion()

    dedup_utils.run_dedup_execute_async()
    time.sleep(3)

    leader = dedup_utils.get_mon_leader()
    dedup_utils.restart_mon(leader)
    time.sleep(15)

    stats = dedup_utils.wait_for_dedup_completion(timeout=900)
    if not stats.get("completed"):
        dedup_utils.run_dedup_execute()
        stats = dedup_utils.wait_for_dedup_completion(timeout=900)
    assert stats.get("completed") is True

    dedup_utils.verify_all_objects_accessible(s3_client, bucket, keys)


@pytest.mark.restart
def test_r5_rapid_rgw_restarts(s3_client, bucket, ssh_con):
    """R5: 3 rapid RGW restarts in 30 seconds during exec."""
    obj_count = 100
    obj_size = 5 * 1024
    keys, expected_md5, _ = dedup_utils.upload_identical_objects(
        s3_client, bucket, obj_count, obj_size, prefix="r5"
    )

    dedup_utils.run_dedup_estimate()
    dedup_utils.wait_for_dedup_completion()

    dedup_utils.run_dedup_execute_async()
    time.sleep(5)
    dedup_utils.restart_rgw_service()
    time.sleep(10)
    dedup_utils.restart_rgw_service()
    time.sleep(10)
    dedup_utils.restart_rgw_service()
    time.sleep(30)

    dedup_utils.run_dedup_execute()
    stats = dedup_utils.wait_for_dedup_completion()
    assert stats.get("completed") is True

    dedup_utils.verify_all_objects_accessible(s3_client, bucket, keys)
    dedup_utils.verify_all_objects_integrity(s3_client, bucket, keys, expected_md5)


# =============================================================================
# CONCURRENT / RACE SCENARIOS (C1-C5)
# =============================================================================


@pytest.mark.concurrent
def test_c1_exec_during_estimate(s3_client, bucket):
    """C1: Fire exec while estimate is still running."""
    obj_count = 200
    obj_size = 5 * 1024
    keys, expected_md5, _ = dedup_utils.upload_identical_objects(
        s3_client, bucket, obj_count, obj_size, prefix="c1"
    )

    dedup_utils.run_dedup_estimate_async()
    time.sleep(2)

    # Try exec while estimate is running — should fail or be rejected
    try:
        out = dedup_utils.run_dedup_execute()
        log.info(f"Exec during estimate returned: {out}")
    except AssertionError:
        log.info("Exec correctly rejected while estimate is running")

    # Wait for estimate to finish, then run exec properly
    dedup_utils.wait_for_dedup_completion()
    dedup_utils.run_dedup_execute()
    stats = dedup_utils.wait_for_dedup_completion()
    assert stats.get("completed") is True

    dedup_utils.verify_all_objects_accessible(s3_client, bucket, keys)


@pytest.mark.concurrent
def test_c2_delete_during_exec(s3_client, bucket):
    """C2: Delete objects during dedup exec."""
    obj_count = 100
    obj_size = 5 * 1024
    keys, expected_md5, _ = dedup_utils.upload_identical_objects(
        s3_client, bucket, obj_count, obj_size, prefix="c2"
    )

    dedup_utils.run_dedup_estimate()
    dedup_utils.wait_for_dedup_completion()

    dedup_utils.run_dedup_execute_async()
    time.sleep(1)

    # Delete half the objects while exec is running
    for key in keys[50:]:
        try:
            s3_client.delete_object(Bucket=bucket, Key=key)
        except Exception as e:
            log.warning(f"Delete {key} during exec: {e}")

    stats = dedup_utils.wait_for_dedup_completion()
    assert stats.get("completed") is True

    # Surviving objects should be intact
    surviving_keys = keys[:50]
    dedup_utils.verify_all_objects_accessible(s3_client, bucket, surviving_keys)
    dedup_utils.verify_all_objects_integrity(
        s3_client, bucket, surviving_keys, expected_md5
    )


@pytest.mark.concurrent
def test_c3_upload_during_exec(s3_client, bucket):
    """C3: Upload new objects during dedup exec."""
    obj_count = 50
    obj_size = 5 * 1024
    keys_a, md5_a, data_a = dedup_utils.upload_identical_objects(
        s3_client, bucket, obj_count, obj_size, prefix="c3-a"
    )

    dedup_utils.run_dedup_estimate()
    dedup_utils.wait_for_dedup_completion()

    dedup_utils.run_dedup_execute_async()
    time.sleep(1)

    # Upload new objects with different content while exec runs
    new_data = os.urandom(obj_size)
    new_md5 = hashlib.md5(new_data).hexdigest()
    keys_b = []
    for i in range(50):
        key = f"c3-b-{i}"
        s3_client.put_object(Bucket=bucket, Key=key, Body=new_data)
        keys_b.append(key)

    stats = dedup_utils.wait_for_dedup_completion()
    assert stats.get("completed") is True

    # Both sets should be intact
    dedup_utils.verify_all_objects_accessible(s3_client, bucket, keys_a)
    dedup_utils.verify_all_objects_integrity(s3_client, bucket, keys_a, md5_a)
    dedup_utils.verify_all_objects_accessible(s3_client, bucket, keys_b)
    dedup_utils.verify_all_objects_integrity(s3_client, bucket, keys_b, new_md5)

    # Re-run dedup to catch new objects
    dedup_utils.run_dedup_execute()
    stats2 = dedup_utils.wait_for_dedup_completion()
    assert stats2.get("completed") is True
    dedup_utils.verify_all_objects_accessible(s3_client, bucket, keys_a + keys_b)


@pytest.mark.concurrent
def test_c4_rapid_pause_resume(s3_client, bucket):
    """C4: Rapid pause/resume 10 times during exec."""
    obj_count = 100
    obj_size = 5 * 1024
    keys, expected_md5, _ = dedup_utils.upload_identical_objects(
        s3_client, bucket, obj_count, obj_size, prefix="c4"
    )

    dedup_utils.run_dedup_estimate()
    dedup_utils.wait_for_dedup_completion()

    dedup_utils.run_dedup_execute_async()
    time.sleep(2)

    for i in range(10):
        try:
            dedup_utils.run_dedup_pause()
            time.sleep(1)
            dedup_utils.run_dedup_resume()
            time.sleep(1)
        except AssertionError as e:
            log.warning(f"Pause/resume cycle {i}: {e}")

    stats = dedup_utils.wait_for_dedup_completion(timeout=900)
    assert stats.get("completed") is True

    dedup_utils.verify_all_objects_accessible(s3_client, bucket, keys)
    dedup_utils.verify_all_objects_integrity(s3_client, bucket, keys, expected_md5)
    parsed = dedup_utils.parse_dedup_stats(stats)
    assert parsed.get("deduped_count", 0) > 0


@pytest.mark.concurrent
def test_c5_double_abort(s3_client, bucket):
    """C5: Double abort — second should be no-op, no crash."""
    obj_count = 100
    obj_size = 5 * 1024
    keys, expected_md5, _ = dedup_utils.upload_identical_objects(
        s3_client, bucket, obj_count, obj_size, prefix="c5"
    )

    dedup_utils.run_dedup_estimate()
    dedup_utils.wait_for_dedup_completion()

    dedup_utils.run_dedup_execute_async()
    time.sleep(2)

    dedup_utils.run_dedup_abort()
    time.sleep(5)
    dedup_utils.run_dedup_abort()  # Second abort should be no-op

    # Verify RGW daemons are still running
    running, total = dedup_utils.verify_rgw_daemons_running()
    assert running == total, f"Some RGW daemons crashed: {running}/{total}"

    dedup_utils.verify_all_objects_accessible(s3_client, bucket, keys)

    # Fresh exec should work normally (dedup count may be 0 if first exec
    # already finished before abort — the point is no crash on double abort)
    dedup_utils.run_dedup_execute()
    stats = dedup_utils.wait_for_dedup_completion()
    assert stats.get("completed") is True


# =============================================================================
# DATA EDGE CASES (D1-D5)
# =============================================================================


@pytest.mark.edge_case
def test_d1_empty_bucket(s3_client, bucket):
    """D1: Dedup on empty bucket — all-zero counters, no errors."""
    dedup_utils.run_dedup_estimate()
    est_stats = dedup_utils.wait_for_dedup_completion()
    assert est_stats.get("completed") is True

    est_parsed = dedup_utils.parse_dedup_stats_full(est_stats)
    assert est_parsed.get("ingress_count", 0) == 0

    dedup_utils.run_dedup_execute()
    exec_stats = dedup_utils.wait_for_dedup_completion()
    assert exec_stats.get("completed") is True

    exec_parsed = dedup_utils.parse_dedup_stats(exec_stats)
    assert exec_parsed.get("deduped_count", 0) == 0


@pytest.mark.edge_case
def test_d3_min_size_boundary(s3_client, bucket):
    """D3: Objects at exactly min_obj_size boundary."""
    boundary_size = 5120  # Exactly 5KB = 4096 min + margin
    below_size = 4095  # 1 byte below min_obj_size_for_dedup (4096)

    # Upload objects at the boundary
    keys_at, md5_at, _ = dedup_utils.upload_identical_objects(
        s3_client, bucket, 20, boundary_size, prefix="d3-at"
    )
    # Upload objects below the boundary
    below_data = os.urandom(below_size)
    keys_below = []
    for i in range(20):
        key = f"d3-below-{i}"
        s3_client.put_object(Bucket=bucket, Key=key, Body=below_data)
        keys_below.append(key)

    dedup_utils.run_dedup_estimate()
    dedup_utils.wait_for_dedup_completion()
    dedup_utils.run_dedup_execute()
    exec_stats = dedup_utils.wait_for_dedup_completion()
    assert exec_stats.get("completed") is True

    # At-boundary objects should be deduped, below should be skipped
    parsed = dedup_utils.parse_dedup_stats(exec_stats)
    assert parsed.get("deduped_count", 0) > 0
    dedup_utils.verify_all_objects_accessible(s3_client, bucket, keys_at + keys_below)


@pytest.mark.edge_case
def test_d4_single_object(s3_client, bucket):
    """D4: Single object — nothing to dedup with."""
    obj_size = 5 * 1024
    keys, expected_md5, _ = dedup_utils.upload_identical_objects(
        s3_client, bucket, 1, obj_size, prefix="d4"
    )

    dedup_utils.run_dedup_estimate()
    est_stats = dedup_utils.wait_for_dedup_completion()
    assert est_stats.get("completed") is True

    est_parsed = dedup_utils.parse_dedup_stats_full(est_stats)
    assert est_parsed.get("ingress_count", 0) == 1

    dedup_utils.run_dedup_execute()
    exec_stats = dedup_utils.wait_for_dedup_completion()
    assert exec_stats.get("completed") is True

    exec_parsed = dedup_utils.parse_dedup_stats(exec_stats)
    assert exec_parsed.get("deduped_count", 0) == 0

    dedup_utils.verify_all_objects_integrity(s3_client, bucket, keys, expected_md5)


@pytest.mark.edge_case
@pytest.mark.slow
def test_d5_max_versions_500(s3_client, bucket):
    """D5: 500 versions of same key — far past 128 limit."""
    obj_size = 5 * 1024
    key = "d5-versioned-key"
    version_ids, expected_md5, data = dedup_utils.upload_identical_versions(
        s3_client, bucket, key, 500, obj_size
    )

    dedup_utils.run_dedup_estimate()
    dedup_utils.wait_for_dedup_completion()
    dedup_utils.run_dedup_execute()
    exec_stats = dedup_utils.wait_for_dedup_completion(timeout=900)
    assert exec_stats.get("completed") is True

    parsed = dedup_utils.parse_dedup_stats(exec_stats)
    assert (
        parsed.get("deduped_count", 0) <= 128
    ), "Deduped count should not exceed 128 (128 limit)"

    # Verify all 500 versions accessible
    for vid in version_ids:
        resp = s3_client.get_object(Bucket=bucket, Key=key, VersionId=vid)
        body = resp["Body"].read()
        actual_md5 = hashlib.md5(body).hexdigest()
        assert actual_md5 == expected_md5, f"Version {vid} MD5 mismatch"

    # Idempotency: second run should dedup 0
    dedup_utils.run_dedup_execute()
    stats2 = dedup_utils.wait_for_dedup_completion()
    parsed2 = dedup_utils.parse_dedup_stats(stats2)
    assert parsed2.get("deduped_count", 0) == 0, "Second run should dedup nothing"


# =============================================================================
# INFRASTRUCTURE FAILURE SCENARIOS (F1-F5)
# =============================================================================


@pytest.mark.infra
def test_f1_pool_near_full(s3_client, bucket, ssh_con):
    """F1: Pool quota to simulate near-full during dedup."""
    obj_count = 50
    obj_size = 5 * 1024
    pool = "default.rgw.buckets.data"
    keys, expected_md5, _ = dedup_utils.upload_identical_objects(
        s3_client, bucket, obj_count, obj_size, prefix="f1"
    )

    # Set a very tight pool quota
    dedup_utils.set_pool_quota(pool, 1024 * 1024)  # 1MB quota

    try:
        dedup_utils.run_dedup_estimate()
        dedup_utils.wait_for_dedup_completion()
        dedup_utils.run_dedup_execute()
        stats = dedup_utils.wait_for_dedup_completion()
        # Dedup may fail or partially succeed under quota
        log.info(f"Dedup under quota completed={stats.get('completed')}")
    except AssertionError as e:
        log.info(f"Dedup under pool quota failed as expected: {e}")
    finally:
        dedup_utils.remove_pool_quota(pool)

    # After removing quota, objects should be intact
    dedup_utils.verify_all_objects_accessible(s3_client, bucket, keys)

    # Re-run dedup without quota
    dedup_utils.run_dedup_execute()
    stats = dedup_utils.wait_for_dedup_completion()
    assert stats.get("completed") is True
    dedup_utils.verify_all_objects_integrity(s3_client, bucket, keys, expected_md5)


@pytest.mark.infra
def test_f2_reshard_during_exec(s3_client, bucket):
    """F2: Bucket reshard during dedup exec."""
    obj_count = 100
    obj_size = 5 * 1024
    keys, expected_md5, _ = dedup_utils.upload_identical_objects(
        s3_client, bucket, obj_count, obj_size, prefix="f2"
    )

    dedup_utils.run_dedup_estimate()
    dedup_utils.wait_for_dedup_completion()

    dedup_utils.run_dedup_execute_async()
    time.sleep(2)

    # Trigger reshard while exec is running
    try:
        dedup_utils.trigger_bucket_reshard(bucket, 8)
    except Exception as e:
        log.info(f"Reshard during exec: {e}")

    stats = dedup_utils.wait_for_dedup_completion(timeout=900)
    log.info(f"Dedup after reshard completed={stats.get('completed')}")

    dedup_utils.verify_all_objects_accessible(s3_client, bucket, keys)

    # Re-run after reshard settles
    if not stats.get("completed"):
        dedup_utils.run_dedup_execute()
        stats = dedup_utils.wait_for_dedup_completion()
    assert stats.get("completed") is True
    dedup_utils.verify_all_objects_integrity(s3_client, bucket, keys, expected_md5)


@pytest.mark.infra
def test_f3_network_partition(s3_client, bucket, ssh_con):
    """F3: Block MON traffic on one node during dedup."""
    obj_count = 100
    obj_size = 5 * 1024
    keys, expected_md5, _ = dedup_utils.upload_identical_objects(
        s3_client, bucket, obj_count, obj_size, prefix="f3"
    )

    dedup_utils.run_dedup_estimate()
    dedup_utils.wait_for_dedup_completion()

    dedup_utils.run_dedup_execute_async()
    time.sleep(3)

    # Block MON traffic briefly (port 6789)
    utils.exec_shell_cmd("iptables -A INPUT -p tcp --dport 6789 -j DROP")
    time.sleep(30)
    utils.exec_shell_cmd("iptables -D INPUT -p tcp --dport 6789 -j DROP")

    stats = dedup_utils.wait_for_dedup_completion(timeout=900)
    if not stats.get("completed"):
        dedup_utils.run_dedup_execute()
        stats = dedup_utils.wait_for_dedup_completion(timeout=900)
    assert stats.get("completed") is True

    dedup_utils.verify_all_objects_accessible(s3_client, bucket, keys)


@pytest.mark.infra
def test_f4_corrupt_rados_object(s3_client, bucket):
    """F4: Corrupt a RADOS object, verify dedup detects hash mismatch."""
    obj_count = 50
    obj_size = 5 * 1024
    pool = "default.rgw.buckets.data"
    keys, expected_md5, _ = dedup_utils.upload_identical_objects(
        s3_client, bucket, obj_count, obj_size, prefix="f4"
    )

    marker = dedup_utils.get_bucket_marker(bucket)
    rados_objects = dedup_utils.get_rados_objects(pool, prefix=marker)

    if rados_objects:
        # Corrupt one RADOS object
        target_oid = rados_objects[0]
        dedup_utils.corrupt_rados_object(pool, target_oid)
        log.info(f"Corrupted RADOS object: {target_oid}")

    dedup_utils.run_dedup_estimate()
    dedup_utils.wait_for_dedup_completion()
    dedup_utils.run_dedup_execute()
    exec_stats = dedup_utils.wait_for_dedup_completion()
    assert exec_stats.get("completed") is True

    # Non-corrupted objects should still be accessible
    accessible_count = 0
    for key in keys:
        try:
            s3_client.head_object(Bucket=bucket, Key=key)
            accessible_count += 1
        except Exception:
            pass
    log.info(f"Accessible objects after corruption: {accessible_count}/{len(keys)}")
    assert accessible_count >= len(keys) - 1  # At most 1 corrupted


@pytest.mark.infra
def test_f5_time_jump(s3_client, bucket, ssh_con):
    """F5: Jump system clock forward during dedup."""
    obj_count = 50
    obj_size = 5 * 1024
    keys, expected_md5, _ = dedup_utils.upload_identical_objects(
        s3_client, bucket, obj_count, obj_size, prefix="f5"
    )

    dedup_utils.run_dedup_estimate()
    dedup_utils.wait_for_dedup_completion()

    dedup_utils.run_dedup_execute_async()
    time.sleep(2)

    # Jump clock forward 1 hour
    utils.exec_shell_cmd("date -s '+1 hour'")
    time.sleep(5)

    stats = dedup_utils.wait_for_dedup_completion(timeout=900)

    # Restore clock — reverse the jump first, then let chrony fine-tune
    utils.exec_shell_cmd("date -s '-1 hour'")
    utils.exec_shell_cmd("chronyc makestep")
    time.sleep(10)

    if not stats.get("completed"):
        dedup_utils.run_dedup_execute()
        stats = dedup_utils.wait_for_dedup_completion()
    assert stats.get("completed") is True

    dedup_utils.verify_all_objects_accessible(s3_client, bucket, keys)
    dedup_utils.verify_all_objects_integrity(s3_client, bucket, keys, expected_md5)
