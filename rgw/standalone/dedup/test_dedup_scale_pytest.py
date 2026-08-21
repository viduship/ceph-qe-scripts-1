"""
test_dedup_scale_pytest.py - RGW Dedup Scale & Performance Test Suite

This is a STANDALONE pytest suite for scale/performance dedup scenarios that
are too slow or resource-intensive for CI. These tests are NOT part of the
main CI dedup suite (test_dedup_pytest.py with 58 tests) and must be run
manually on a dedicated cluster.

What it tests:
  These 8 tests fill gaps left by the CI suite — large objects, multipart
  uploads, cross-bucket dedup at scale, multi-RGW distribution, throttle
  tuning, and lifecycle transitions. They exercise code paths that only
  trigger at scale or with specific object sizes (e.g., >4MB split-head
  dedup, >5MB multipart copy).

Test matrix:
  SC01 : Large objects >4MB — tests non-split-head dedup path
  SC02 : Multipart objects 50MB/500MB — upload + range GETs after dedup
  SC03 : S3 COPY of >5MB objects — multipart copy path with dedup
  SC04 : 10K objects across multiple buckets at 50% dup rate
  SC05 : Multi-RGW scaleout — completion time + work distribution
  SC06 : High duplication rate (90%) — 1000 objects, measures savings
  SC07 : Throttle control — 50 vs 500 ops/sec comparison
  SC08 : LC transition of deduped objects to different storage class

Prerequisites:
  - A deployed Ceph cluster with RGW and dedup enabled
  - Sufficient cluster capacity (SC02 needs ~1GB, SC04 needs ~5GB stored)
  - The ceph-qe-scripts repo cloned with v2/lib and v2/utils available
  - A config YAML with RGW endpoint, access/secret keys, and bucket info

Usage:
  # Run all scale tests
  pytest test_dedup_scale_pytest.py --config <config.yaml> -v

  # Run a specific test
  pytest test_dedup_scale_pytest.py --config <config.yaml> -k "test_sc01"

  # Run only scale-marked tests
  pytest test_dedup_scale_pytest.py --config <config.yaml> -m scale
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
# SC01: Large objects >4MB (non-split-head dedup path)
# =============================================================================


@pytest.mark.sanity
def test_sc01_large_objects_above_4mb(s3_client, bucket):
    """SC01: Basic dedup for large objects > 4MB.

    First suite (S1/S2) uses 5-10KB objects which go through split-head.
    This test uses 5MB objects which take the regular (non-split-head) dedup path.
    """
    obj_count = 50
    obj_size = 5 * 1024 * 1024  # 5MB

    keys, expected_md5, _ = dedup_utils.upload_identical_objects(
        s3_client, bucket, obj_count, obj_size, prefix="sc01-large"
    )

    dedup_utils.run_dedup_estimate()
    estimate_stats = dedup_utils.wait_for_dedup_completion()
    assert estimate_stats.get("completed") is True, "Estimate did not complete"

    dedup_utils.run_dedup_execute()
    exec_stats = dedup_utils.wait_for_dedup_completion()
    assert exec_stats.get("completed") is True, "Exec did not complete"

    parsed = dedup_utils.parse_dedup_stats(exec_stats)
    assert parsed.get("deduped_count", 0) > 0, "Expected objects to be deduplicated"

    parsed_full = dedup_utils.parse_dedup_stats_full(exec_stats)
    split_head_src = parsed_full.get("split_head_src", 0)
    log.info(f"SC01: split_head_src={split_head_src} (expect 0 for >4MB objects)")

    dedup_utils.verify_all_objects_accessible(s3_client, bucket, keys)
    dedup_utils.verify_all_objects_integrity(s3_client, bucket, keys, expected_md5)

    dedup_utils.log_dedup_savings(exec_stats, obj_count * obj_size, "SC01")


# =============================================================================
# SC02: Multipart objects 50MB/500MB with range GETs
# =============================================================================


@pytest.mark.feature
@pytest.mark.slow
def test_sc02_multipart_large_objects(s3_client, bucket):
    """SC02: Dedup multipart objects at 50MB and 500MB.

    First suite (S6) uses 20MB multipart. This tests much larger objects
    to stress multipart manifest handling in dedup.
    """
    sizes = [50 * 1024 * 1024, 500 * 1024 * 1024]  # 50MB, 500MB
    all_keys = []
    md5_map = {}

    for size in sizes:
        size_label = f"{size // (1024 * 1024)}MB"
        count = 3
        keys, md5_hash, _ = dedup_utils.upload_identical_multipart_objects(
            s3_client, bucket, count, size, prefix=f"sc02-mp-{size_label}"
        )
        all_keys.extend(keys)
        for k in keys:
            md5_map[k] = md5_hash

    dedup_utils.run_dedup_estimate()
    estimate_stats = dedup_utils.wait_for_dedup_completion(timeout=1800)
    assert estimate_stats.get("completed") is True, "Estimate did not complete"

    dedup_utils.run_dedup_execute()
    exec_stats = dedup_utils.wait_for_dedup_completion(timeout=1800)
    assert exec_stats.get("completed") is True, "Exec did not complete"

    parsed = dedup_utils.parse_dedup_stats(exec_stats)
    assert (
        parsed.get("deduped_count", 0) > 0
    ), "Expected multipart objects to be deduplicated"

    for key in all_keys:
        dedup_utils.verify_object_integrity(s3_client, bucket, key, md5_map[key])
        dedup_utils.verify_range_get(s3_client, bucket, key)

    total_uploaded = sum(3 * s for s in sizes)
    dedup_utils.log_dedup_savings(exec_stats, total_uploaded, "SC02")


# =============================================================================
# SC03: S3 COPY of >5MB objects (multipart copy path)
# =============================================================================


@pytest.mark.regression
def test_sc03_s3_copy_large_objects(s3_client, bucket):
    """SC03: S3 COPY of 10MB objects — triggers multipart copy path.

    First suite (S12) copies 5KB objects which use simple copy.
    Objects >5MB use server-side multipart copy internally, producing
    different manifest structures that dedup must handle.
    """
    obj_size = 10 * 1024 * 1024  # 10MB
    source_data = dedup_utils.generate_identical_data(obj_size)
    expected_md5 = hashlib.md5(source_data).hexdigest()

    source_key = "sc03-source-10mb"
    s3_client.put_object(Bucket=bucket, Key=source_key, Body=source_data)

    copy_keys = []
    for i in range(20):
        copy_key = f"sc03-copy-{i}"
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

    parsed = dedup_utils.parse_dedup_stats(exec_stats)
    assert (
        parsed.get("deduped_count", 0) > 0
    ), "Copied >5MB objects should be deduplicated"

    dedup_utils.verify_all_objects_integrity(s3_client, bucket, all_keys, expected_md5)

    for key in all_keys:
        resp = s3_client.head_object(Bucket=bucket, Key=key)
        assert resp["ContentLength"] == obj_size, f"Size mismatch for {key}"

    dedup_utils.log_dedup_savings(exec_stats, 21 * obj_size, "SC03")


# =============================================================================
# SC04: 10K objects across multiple buckets at 50% dup rate
# =============================================================================


@pytest.mark.scale
@pytest.mark.slow
def test_sc04_10k_objects_multi_bucket(s3_client, bucket_factory):
    """SC04: 10K objects across 10 buckets at 50% duplication rate.

    First suite (M1) does 1000 objects in 100 buckets (all identical).
    This tests 10x the object count with mixed dup/unique content.
    """
    obj_size = 5 * 1024  # 5KB
    total_objects = 10000
    objects_per_bucket = 1000
    num_buckets = total_objects // objects_per_bucket

    identical_data = dedup_utils.generate_identical_data(obj_size)
    dup_md5 = hashlib.md5(identical_data).hexdigest()

    bucket_keys = {}
    uploaded = 0

    for b in range(num_buckets):
        bkt = bucket_factory(prefix=f"sc04-bkt-{b}")
        keys = []

        dup_in_bucket = objects_per_bucket // 2
        for i in range(dup_in_bucket):
            key = f"sc04-dup-{i}"
            s3_client.put_object(Bucket=bkt, Key=key, Body=identical_data)
            keys.append(key)

        for i in range(objects_per_bucket - dup_in_bucket):
            key = f"sc04-uniq-{i}"
            s3_client.put_object(Bucket=bkt, Key=key, Body=os.urandom(obj_size))
            keys.append(key)

        bucket_keys[bkt] = keys
        uploaded += objects_per_bucket
        log.info(
            f"SC04: Uploaded {uploaded}/{total_objects} objects "
            f"({b + 1}/{num_buckets} buckets)"
        )

    dedup_utils.run_dedup_estimate()
    estimate_stats = dedup_utils.wait_for_dedup_completion(timeout=3600)
    assert estimate_stats.get("completed") is True, "Estimate did not complete"

    dedup_utils.run_dedup_execute()
    exec_stats = dedup_utils.wait_for_dedup_completion(timeout=3600)
    assert exec_stats.get("completed") is True, "Exec did not complete"

    parsed = dedup_utils.parse_dedup_stats(exec_stats)
    deduped = parsed.get("deduped_count", 0)
    log.info(f"SC04: Deduped {deduped} objects out of {total_objects} (50% dup rate)")
    assert deduped > 0, "Expected dedup to find duplicates at scale"

    sample_buckets = random.sample(list(bucket_keys.keys()), min(3, len(bucket_keys)))
    for bkt in sample_buckets:
        sample_keys = random.sample(bucket_keys[bkt], min(10, len(bucket_keys[bkt])))
        dedup_utils.verify_all_objects_accessible(s3_client, bkt, sample_keys)

    dedup_utils.log_dedup_savings(exec_stats, total_objects * obj_size, "SC04")


# =============================================================================
# SC05: Multi-RGW scaleout — completion time + work distribution
# =============================================================================


@pytest.mark.scale
@pytest.mark.slow
def test_sc05_multi_rgw_scaleout(s3_client, bucket):
    """SC05: Verify dedup with multiple RGW instances.

    Measures completion time and reports work distribution across daemons.
    No equivalent in first suite.
    """
    obj_size = 5 * 1024 * 1024  # 5MB
    obj_count = 100

    keys, expected_md5, _ = dedup_utils.upload_identical_objects(
        s3_client, bucket, obj_count, obj_size, prefix="sc05-scale"
    )

    running, total = dedup_utils.verify_rgw_daemons_running()
    log.info(f"SC05: RGW daemons running: {running}/{total}")

    start_time = time.time()

    dedup_utils.run_dedup_estimate()
    estimate_stats = dedup_utils.wait_for_dedup_completion(timeout=1800)
    assert estimate_stats.get("completed") is True, "Estimate did not complete"

    dedup_utils.run_dedup_execute()
    exec_stats = dedup_utils.wait_for_dedup_completion(timeout=1800)
    assert exec_stats.get("completed") is True, "Exec did not complete"

    elapsed = time.time() - start_time
    log.info(f"SC05: Total dedup time with {running} RGW(s): {elapsed:.1f}s")

    parsed = dedup_utils.parse_dedup_stats(exec_stats)
    assert parsed.get("deduped_count", 0) > 0, "Expected dedup at scale"

    dedup_utils.verify_all_objects_accessible(s3_client, bucket, keys)
    dedup_utils.verify_all_objects_integrity(s3_client, bucket, keys, expected_md5)

    parsed_full = dedup_utils.parse_dedup_stats_full(exec_stats)
    log.info(f"SC05: Full stats:\n{json.dumps(parsed_full, indent=2)}")

    dedup_utils.log_dedup_savings(exec_stats, obj_count * obj_size, "SC05")


# =============================================================================
# SC06: High duplication rate (90%) — 1000 objects
# =============================================================================


@pytest.mark.scale
@pytest.mark.slow
def test_sc06_high_duplication_rate(s3_client, bucket_factory):
    """SC06: 1000 objects with 90% duplication rate.

    No equivalent in first suite. Tests dedup efficiency at high dup ratios.
    """
    obj_size = 5 * 1024  # 5KB
    total_objects = 1000
    dup_count = 900
    unique_count = 100

    identical_data = dedup_utils.generate_identical_data(obj_size)
    dup_md5 = hashlib.md5(identical_data).hexdigest()

    bkt = bucket_factory(prefix="sc06-highdup")
    all_keys = []

    for i in range(dup_count):
        key = f"sc06-dup-{i}"
        s3_client.put_object(Bucket=bkt, Key=key, Body=identical_data)
        all_keys.append(key)
    log.info(f"SC06: Uploaded {dup_count} duplicate objects")

    for i in range(unique_count):
        key = f"sc06-uniq-{i}"
        s3_client.put_object(Bucket=bkt, Key=key, Body=os.urandom(obj_size))
        all_keys.append(key)
    log.info(f"SC06: Uploaded {unique_count} unique objects")

    dedup_utils.run_dedup_estimate()
    estimate_stats = dedup_utils.wait_for_dedup_completion(timeout=1800)
    assert estimate_stats.get("completed") is True, "Estimate did not complete"

    est_parsed = dedup_utils.parse_dedup_stats_full(estimate_stats)
    log.info(f"SC06: Estimate stats:\n{json.dumps(est_parsed, indent=2)}")

    dedup_utils.run_dedup_execute()
    exec_stats = dedup_utils.wait_for_dedup_completion(timeout=1800)
    assert exec_stats.get("completed") is True, "Exec did not complete"

    parsed = dedup_utils.parse_dedup_stats(exec_stats)
    deduped = parsed.get("deduped_count", 0)
    log.info(f"SC06: Deduped {deduped} out of {total_objects} objects (90% dup rate)")
    assert deduped > 0, "Expected high dedup count with 90% duplication"

    sample_dup_keys = random.sample(all_keys[:dup_count], min(20, dup_count))
    for key in sample_dup_keys:
        dedup_utils.verify_object_integrity(s3_client, bkt, key, dup_md5)

    sample_all = random.sample(all_keys, min(50, len(all_keys)))
    dedup_utils.verify_all_objects_accessible(s3_client, bkt, sample_all)

    savings_pct = (deduped / total_objects) * 100 if total_objects > 0 else 0
    log.info(f"SC06: Storage savings: ~{savings_pct:.1f}%")
    dedup_utils.log_dedup_savings(exec_stats, total_objects * obj_size, "SC06")


# =============================================================================
# SC07: Throttle control — 50 vs 500 ops/sec comparison
# =============================================================================


@pytest.mark.adhoc
@pytest.mark.slow
def test_sc07_throttle_control(s3_client, bucket_factory):
    """SC07: Throttle control — compare 50 vs 500 ops/sec during active workload.

    No equivalent in first suite. Measures dedup speed vs foreground impact.
    """
    obj_size = 5 * 1024 * 1024  # 5MB

    # --- Low throttle run ---
    dedup_bucket_low = bucket_factory(prefix="sc07-low")
    keys_low, expected_md5, _ = dedup_utils.upload_identical_objects(
        s3_client, dedup_bucket_low, 100, obj_size, prefix="sc07-obj"
    )

    dedup_utils.set_dedup_throttle(max_bucket_index_ops=50, max_metadata_ops=50)
    throttle_settings = dedup_utils.get_dedup_throttle()
    log.info(f"SC07: Low throttle settings: {throttle_settings}")

    dedup_utils.run_dedup_estimate()
    dedup_utils.wait_for_dedup_completion(timeout=900)

    start_low = time.time()
    dedup_utils.run_dedup_execute()
    exec_stats_low = dedup_utils.wait_for_dedup_completion(timeout=900)
    time_low = time.time() - start_low
    assert exec_stats_low.get("completed") is True, "Low-throttle exec did not complete"

    parsed_low = dedup_utils.parse_dedup_stats(exec_stats_low)
    log.info(
        f"SC07: Low throttle (50 ops/sec) — time: {time_low:.1f}s, "
        f"deduped: {parsed_low.get('deduped_count', 0)}"
    )

    dedup_utils.verify_all_objects_accessible(
        s3_client, dedup_bucket_low, keys_low[:10]
    )

    # --- High throttle run ---
    dedup_bucket_high = bucket_factory(prefix="sc07-high")
    keys_high, _, _ = dedup_utils.upload_identical_objects(
        s3_client, dedup_bucket_high, 100, obj_size, prefix="sc07-obj2"
    )

    dedup_utils.set_dedup_throttle(max_bucket_index_ops=500, max_metadata_ops=500)
    throttle_settings = dedup_utils.get_dedup_throttle()
    log.info(f"SC07: High throttle settings: {throttle_settings}")

    dedup_utils.run_dedup_estimate()
    dedup_utils.wait_for_dedup_completion(timeout=900)

    start_high = time.time()
    dedup_utils.run_dedup_execute()
    exec_stats_high = dedup_utils.wait_for_dedup_completion(timeout=900)
    time_high = time.time() - start_high
    assert (
        exec_stats_high.get("completed") is True
    ), "High-throttle exec did not complete"

    parsed_high = dedup_utils.parse_dedup_stats(exec_stats_high)
    log.info(
        f"SC07: High throttle (500 ops/sec) — time: {time_high:.1f}s, "
        f"deduped: {parsed_high.get('deduped_count', 0)}"
    )

    log.info(
        f"SC07: Throttle comparison — low: {time_low:.1f}s vs high: {time_high:.1f}s"
    )

    dedup_utils.verify_all_objects_accessible(s3_client, dedup_bucket_low, keys_low)
    dedup_utils.verify_all_objects_integrity(
        s3_client, dedup_bucket_low, keys_low, expected_md5
    )

    dedup_utils.log_dedup_savings(exec_stats_low, 100 * obj_size, "SC07")


# =============================================================================
# SC08: LC transition of deduped objects to different storage class
# =============================================================================


@pytest.mark.adhoc
@pytest.mark.slow
def test_sc08_lc_transition_deduped_objects(s3_client, bucket, ssh_con):
    """SC08: LC transition of deduplicated objects to a different storage class.

    First suite (S10) tests LC expiration (delete). This tests LC transition
    (move to another SC) — different code path, ref counts must survive the move.
    """
    sc_name = "DEDUP_TRANSITION_SC"
    pool_name = "dedup-transition-pool"

    dedup_utils.setup_storage_class(sc_name, pool_name, ssh_con)
    try:
        dedup_utils.wait_for_rgw_ready()

        utils.exec_shell_cmd("ceph config set client.rgw rgw_lc_debug_interval 30")
        time.sleep(3)

        obj_size = 5 * 1024 * 1024  # 5MB
        keys, expected_md5, _ = dedup_utils.upload_identical_objects(
            s3_client, bucket, 20, obj_size, prefix="sc08-trans"
        )

        dedup_utils.run_dedup_estimate()
        estimate_stats = dedup_utils.wait_for_dedup_completion()
        assert estimate_stats.get("completed") is True, "Estimate did not complete"

        dedup_utils.run_dedup_execute()
        exec_stats = dedup_utils.wait_for_dedup_completion()
        assert exec_stats.get("completed") is True, "Exec did not complete"

        parsed = dedup_utils.parse_dedup_stats(exec_stats)
        assert parsed.get("deduped_count", 0) > 0, "Expected dedup before transition"

        dedup_utils.verify_all_objects_accessible(s3_client, bucket, keys)

        lc_config = {
            "Rules": [
                {
                    "ID": "sc08-transition",
                    "Status": "Enabled",
                    "Filter": {"Prefix": "sc08-trans"},
                    "Transitions": [
                        {
                            "Days": 0,
                            "StorageClass": sc_name,
                        }
                    ],
                }
            ]
        }
        s3_client.put_bucket_lifecycle_configuration(
            Bucket=bucket, LifecycleConfiguration=lc_config
        )
        log.info(f"SC08: Set LC transition to {sc_name}")

        time.sleep(120)

        transitioned = 0
        for key in keys:
            try:
                resp = s3_client.head_object(Bucket=bucket, Key=key)
                sc = resp.get("StorageClass", "STANDARD")
                if sc == sc_name:
                    transitioned += 1
            except Exception as e:
                log.warning(f"SC08: head_object failed for {key}: {e}")

        log.info(f"SC08: Transitioned {transitioned}/{len(keys)} objects to {sc_name}")

        accessible = 0
        for key in keys:
            try:
                resp = s3_client.get_object(Bucket=bucket, Key=key)
                body = resp["Body"].read()
                dl_md5 = hashlib.md5(body).hexdigest()
                if dl_md5 == expected_md5:
                    accessible += 1
                else:
                    log.warning(f"SC08: MD5 mismatch for {key} after transition")
            except Exception as e:
                log.warning(f"SC08: GET failed for {key}: {e}")

        log.info(
            f"SC08: {accessible}/{len(keys)} objects accessible + intact after transition"
        )
        assert accessible == len(
            keys
        ), f"Not all objects survived transition: {accessible}/{len(keys)}"

        dedup_utils.log_dedup_savings(exec_stats, 20 * obj_size, "SC08")

    finally:
        dedup_utils.teardown_storage_class(sc_name, pool_name, ssh_con)
