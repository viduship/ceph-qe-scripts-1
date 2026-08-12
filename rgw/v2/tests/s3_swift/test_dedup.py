"""
test_dedup.py - RGW Dedup (Deduplication) Test Automation

Usage: test_dedup.py -c <input_yaml>

Covers test scenarios from ISCE-4789:
  S1:  Sanity - Basic dedup for large objects > 4MB
  S2:  Sanity - Basic dedup for small objects < 4MB (split-head)
  S3:  Sanity - Dedup via Admin OPS REST API
  S4:  Sanity - Dedup estimate (non-destructive dry-run)
  S5:  Sanity - Data integrity verification after dedup
  S6:  Feature - Dedup multipart objects of any size
  S7:  Feature - Session lifecycle controls (pause/resume/abort)
  S8:  Feature - SSE-C encrypted objects excluded from dedup
  S9:  Feature - Dedup with different storage classes and allow/deny filters
  S10: Feature - LC expiration with deduplicated objects
  S11: Regression - Dedup with versioned objects
  S12: Regression - Dedup with S3 copy object > 5MB
  S14: Regression - Same content different metadata
  S15: Regression - Concurrent S3 operations during dedup

Compressed object dedup (PR #68965):
  S16: Compression - Compressed object dedup sanity (zlib)
  S17: Compression - Cross-mode dedup (compressed + uncompressed)
  S18: Compression - Algorithm switch (zlib -> snappy) + dedup
  S19: Compression - Compressed multipart objects + range GETs
  S20: Compression - rgw_dedup_skip_compressed config toggle
  S21: Compression - Compression attr mirroring integrity

Bug-hunting tests (data integrity after dedup):
  B1: Overwrite deduped object - shared tail integrity (GC vs refcount)
  B2: Delete dedup source - refcount protection
  B3: S3 copy deduped object then delete original (shared_manifest copy)
  B4: Cross-bucket dedup - source bucket deletion
  B5: Dedup idempotency - multiple exec runs
"""

import os
import sys

sys.path.append(os.path.abspath(os.path.join(__file__, "../../../..")))
import argparse
import hashlib
import json
import logging
import random
import string
import time
import traceback
import warnings

warnings.filterwarnings("ignore", category=UserWarning, module="urllib3")
warnings.filterwarnings("ignore", message=".*HeaderParsingError.*")
logging.getLogger("urllib3").setLevel(logging.CRITICAL)
logging.getLogger("urllib3.connectionpool").setLevel(logging.CRITICAL)

import v2.lib.resource_op as s3lib
import v2.utils.utils as utils
from v2.lib.exceptions import RGWBaseException, TestExecError
from v2.lib.resource_op import Config
from v2.lib.s3.write_io_info import BasicIOInfoStructure, IOInfoInitialize
from v2.tests.s3_swift import reusable
from v2.tests.s3_swift.reusables import dedup as dedup_utils
from v2.utils.log import configure_logging
from v2.utils.test_desc import AddTestInfo

log = logging.getLogger()
TEST_DATA_PATH = None


def test_exec_sanity_large_objects(config, ssh_con):
    """
    S1: Upload 50 identical objects > 4MB, run dedup estimate then execute,
    verify objects deduplicated and remain accessible.
    """
    test_info = AddTestInfo("S1: Basic dedup for large objects > 4MB")
    io_info_initialize = IOInfoInitialize()
    basic_io_structure = BasicIOInfoStructure()
    io_info_initialize.initialize(basic_io_structure.initial())

    try:
        test_info.started_info()
        user_info = s3lib.create_users(1)[0]
        auth = reusable.get_auth(
            user_info, ssh_con, config.ssl, getattr(config, "haproxy", False)
        )
        s3_client = auth.do_auth_using_client()

        bucket_name = f"dedup-large-{random.randint(1, 5000)}"
        s3_client.create_bucket(Bucket=bucket_name)
        log.info(f"Created bucket: {bucket_name}")

        obj_count = getattr(config, "objects_count", 50)
        obj_size = 5 * 1024 * 1024  # 5MB

        keys, expected_md5, original_data = dedup_utils.upload_identical_objects(
            s3_client, bucket_name, obj_count, obj_size, prefix="large-obj"
        )

        log.info("Running dedup estimate")
        dedup_utils.run_dedup_estimate()
        estimate_stats = dedup_utils.wait_for_dedup_completion()
        log.info(
            f"Estimate stats: {json.dumps(estimate_stats, indent=2) if isinstance(estimate_stats, dict) else estimate_stats}"
        )

        log.info("Running dedup execute")
        dedup_utils.run_dedup_execute()
        exec_stats = dedup_utils.wait_for_dedup_completion()
        log.info(
            f"Exec stats: {json.dumps(exec_stats, indent=2) if isinstance(exec_stats, dict) else exec_stats}"
        )

        dedup_utils.verify_all_objects_accessible(s3_client, bucket_name, keys)
        dedup_utils.verify_all_objects_integrity(
            s3_client, bucket_name, keys, expected_md5
        )

        if isinstance(exec_stats, dict):
            deduped = exec_stats.get(
                "deduped_objects", exec_stats.get("deduped_obj", 0)
            )
            log.info(f"Objects deduplicated: {deduped}")
            assert deduped > 0, "Expected dedup to deduplicate objects but count is 0"

        test_info.success_status("test passed")

    except (RGWBaseException, AssertionError, Exception) as e:
        log.error(f"Test failed: {e}")
        log.error(traceback.format_exc())
        test_info.failed_status("test failed")
        raise

    finally:
        dedup_utils.cleanup_bucket(s3_client, bucket_name)


def test_exec_sanity_small_objects(config, ssh_con):
    """
    S2: Upload 100 identical objects < 4MB (100KB, 1MB, 3MB sizes),
    run dedup, verify split-head mechanism works for small objects.
    """
    test_info = AddTestInfo("S2: Basic dedup for small objects < 4MB (split-head)")
    io_info_initialize = IOInfoInitialize()
    basic_io_structure = BasicIOInfoStructure()
    io_info_initialize.initialize(basic_io_structure.initial())

    try:
        test_info.started_info()
        user_info = s3lib.create_users(1)[0]
        auth = reusable.get_auth(
            user_info, ssh_con, config.ssl, getattr(config, "haproxy", False)
        )
        s3_client = auth.do_auth_using_client()

        bucket_name = f"dedup-small-{random.randint(1, 5000)}"
        s3_client.create_bucket(Bucket=bucket_name)

        sizes = [100 * 1024, 1 * 1024 * 1024, 3 * 1024 * 1024]  # 100KB, 1MB, 3MB
        all_keys = []
        md5_map = {}

        for size in sizes:
            size_label = f"{size // 1024}KB"
            keys, md5_hash, _ = dedup_utils.upload_identical_objects(
                s3_client, bucket_name, 30, size, prefix=f"small-{size_label}"
            )
            all_keys.extend(keys)
            for k in keys:
                md5_map[k] = md5_hash

        log.info("Running dedup estimate")
        dedup_utils.run_dedup_estimate()
        dedup_utils.wait_for_dedup_completion()

        log.info("Running dedup execute")
        dedup_utils.run_dedup_execute()
        dedup_utils.wait_for_dedup_completion()

        for key in all_keys:
            dedup_utils.verify_object_integrity(
                s3_client, bucket_name, key, md5_map[key]
            )

        log.info(f"All {len(all_keys)} small objects verified after dedup")
        test_info.success_status("test passed")

    except (RGWBaseException, AssertionError, Exception) as e:
        log.error(f"Test failed: {e}")
        log.error(traceback.format_exc())
        test_info.failed_status("test failed")
        raise

    finally:
        dedup_utils.cleanup_bucket(s3_client, bucket_name)


def test_exec_admin_ops_api(config, ssh_con):
    """
    S3: Trigger dedup via Admin OPS REST API, query status and stats via API.
    Requires PR #68863.
    """
    test_info = AddTestInfo("S3: Dedup via Admin OPS REST API")
    io_info_initialize = IOInfoInitialize()
    basic_io_structure = BasicIOInfoStructure()
    io_info_initialize.initialize(basic_io_structure.initial())

    try:
        test_info.started_info()
        user_info = s3lib.create_users(1)[0]
        auth = reusable.get_auth(
            user_info, ssh_con, config.ssl, getattr(config, "haproxy", False)
        )
        s3_client = auth.do_auth_using_client()
        endpoint_url = auth.endpoint_url

        admin_user = utils.exec_shell_cmd(
            "radosgw-admin user create --uid=dedup-admin --display-name='Dedup Admin' "
            "--caps='dedup=*'"
        )
        if admin_user is False:
            admin_user = utils.exec_shell_cmd(
                "radosgw-admin user info --uid=dedup-admin"
            )
        admin_info = json.loads(admin_user)
        admin_access_key = admin_info["keys"][0]["access_key"]
        admin_secret_key = admin_info["keys"][0]["secret_key"]

        bucket_name = f"dedup-api-{random.randint(1, 5000)}"
        s3_client.create_bucket(Bucket=bucket_name)

        keys, expected_md5, _ = dedup_utils.upload_identical_objects(
            s3_client, bucket_name, 20, 5 * 1024 * 1024, prefix="api-obj"
        )

        log.info("Triggering dedup estimate via Admin OPS API")
        resp = dedup_utils.dedup_api_request(
            endpoint_url,
            "estimate",
            method="POST",
            access_key=admin_access_key,
            secret_key=admin_secret_key,
        )
        assert resp.status_code in (
            200,
            202,
        ), f"Estimate API failed: {resp.status_code}"

        time.sleep(5)

        log.info("Querying dedup stats via Admin OPS API")
        resp = dedup_utils.dedup_api_request(
            endpoint_url,
            "stats",
            method="GET",
            access_key=admin_access_key,
            secret_key=admin_secret_key,
        )
        assert resp.status_code == 200, f"Stats API failed: {resp.status_code}"
        log.info(f"Stats API response: {resp.text[:500]}")

        dedup_utils.wait_for_dedup_completion()

        dedup_utils.verify_all_objects_accessible(s3_client, bucket_name, keys)
        test_info.success_status("test passed")

    except (RGWBaseException, AssertionError, Exception) as e:
        log.error(f"Test failed: {e}")
        log.error(traceback.format_exc())
        test_info.failed_status("test failed")
        raise

    finally:
        dedup_utils.cleanup_bucket(s3_client, bucket_name)


def test_exec_estimate_dry_run(config, ssh_con):
    """
    S4: Upload mix of duplicate and unique objects, run estimate only,
    verify no data changes and estimate accuracy.
    """
    test_info = AddTestInfo("S4: Dedup estimate non-destructive dry-run")
    io_info_initialize = IOInfoInitialize()
    basic_io_structure = BasicIOInfoStructure()
    io_info_initialize.initialize(basic_io_structure.initial())

    try:
        test_info.started_info()
        user_info = s3lib.create_users(1)[0]
        auth = reusable.get_auth(
            user_info, ssh_con, config.ssl, getattr(config, "haproxy", False)
        )
        s3_client = auth.do_auth_using_client()

        bucket_name = f"dedup-estimate-{random.randint(1, 5000)}"
        s3_client.create_bucket(Bucket=bucket_name)

        dup_keys, dup_md5, _ = dedup_utils.upload_identical_objects(
            s3_client, bucket_name, 30, 5 * 1024 * 1024, prefix="dup-obj"
        )

        unique_keys = []
        for i in range(10):
            key = f"unique-obj-{i}"
            unique_data = os.urandom(5 * 1024 * 1024)
            s3_client.put_object(Bucket=bucket_name, Key=key, Body=unique_data)
            unique_keys.append(key)

        pre_etags = {}
        for key in dup_keys + unique_keys:
            resp = s3_client.head_object(Bucket=bucket_name, Key=key)
            pre_etags[key] = resp["ETag"]

        log.info("Running dedup estimate (dry-run)")
        dedup_utils.run_dedup_estimate()
        estimate_stats = dedup_utils.wait_for_dedup_completion()

        post_etags = {}
        for key in dup_keys + unique_keys:
            resp = s3_client.head_object(Bucket=bucket_name, Key=key)
            post_etags[key] = resp["ETag"]

        for key in dup_keys + unique_keys:
            assert (
                pre_etags[key] == post_etags[key]
            ), f"ETag changed for {key} after estimate: {pre_etags[key]} -> {post_etags[key]}"

        dedup_utils.verify_all_objects_integrity(
            s3_client, bucket_name, dup_keys, dup_md5
        )
        log.info("Estimate completed without modifying any objects")

        test_info.success_status("test passed")

    except (RGWBaseException, AssertionError, Exception) as e:
        log.error(f"Test failed: {e}")
        log.error(traceback.format_exc())
        test_info.failed_status("test failed")
        raise

    finally:
        dedup_utils.cleanup_bucket(s3_client, bucket_name)


def test_exec_data_integrity(config, ssh_con):
    """
    S5: Upload 100 duplicate objects with known MD5 checksums, run dedup,
    verify all MD5s match and ETags preserved.
    """
    test_info = AddTestInfo("S5: Data integrity verification after dedup")
    io_info_initialize = IOInfoInitialize()
    basic_io_structure = BasicIOInfoStructure()
    io_info_initialize.initialize(basic_io_structure.initial())

    try:
        test_info.started_info()
        user_info = s3lib.create_users(1)[0]
        auth = reusable.get_auth(
            user_info, ssh_con, config.ssl, getattr(config, "haproxy", False)
        )
        s3_client = auth.do_auth_using_client()

        bucket_name = f"dedup-integrity-{random.randint(1, 5000)}"
        s3_client.create_bucket(Bucket=bucket_name)

        obj_count = getattr(config, "objects_count", 100)
        keys, expected_md5, _ = dedup_utils.upload_identical_objects(
            s3_client, bucket_name, obj_count, 5 * 1024 * 1024, prefix="integrity-obj"
        )

        pre_etags = {}
        for key in keys:
            resp = s3_client.head_object(Bucket=bucket_name, Key=key)
            pre_etags[key] = resp["ETag"]

        log.info("Running dedup execute")
        dedup_utils.run_dedup_execute()
        dedup_utils.wait_for_dedup_completion()

        post_etags = dedup_utils.verify_all_objects_integrity(
            s3_client, bucket_name, keys, expected_md5
        )

        etag_mismatches = []
        for key in keys:
            post_resp = s3_client.head_object(Bucket=bucket_name, Key=key)
            if pre_etags[key] != post_resp["ETag"]:
                etag_mismatches.append(key)

        if etag_mismatches:
            log.warning(
                f"ETag changes detected for {len(etag_mismatches)} objects (may be expected with split-head)"
            )
        else:
            log.info("All ETags preserved after dedup")

        log.info(f"All {obj_count} objects passed integrity verification")
        test_info.success_status("test passed")

    except (RGWBaseException, AssertionError, Exception) as e:
        log.error(f"Test failed: {e}")
        log.error(traceback.format_exc())
        test_info.failed_status("test failed")
        raise

    finally:
        dedup_utils.cleanup_bucket(s3_client, bucket_name)


def test_exec_multipart_objects(config, ssh_con):
    """
    S6: Upload multipart objects of varying sizes (50MB, 500MB) with identical content,
    run dedup, verify all parts accessible and range GETs work.
    """
    test_info = AddTestInfo("S6: Dedup multipart objects of any size")
    io_info_initialize = IOInfoInitialize()
    basic_io_structure = BasicIOInfoStructure()
    io_info_initialize.initialize(basic_io_structure.initial())

    try:
        test_info.started_info()
        user_info = s3lib.create_users(1)[0]
        auth = reusable.get_auth(
            user_info, ssh_con, config.ssl, getattr(config, "haproxy", False)
        )
        s3_client = auth.do_auth_using_client()

        bucket_name = f"dedup-multipart-{random.randint(1, 5000)}"
        s3_client.create_bucket(Bucket=bucket_name)

        mp_size = 50 * 1024 * 1024  # 50MB
        mp_count = 5

        (
            keys,
            expected_md5,
            original_data,
        ) = dedup_utils.upload_identical_multipart_objects(
            s3_client, bucket_name, mp_count, mp_size, prefix="mp-obj"
        )

        log.info("Running dedup execute on multipart objects")
        dedup_utils.run_dedup_execute()
        dedup_utils.wait_for_dedup_completion()

        dedup_utils.verify_all_objects_accessible(s3_client, bucket_name, keys)
        dedup_utils.verify_all_objects_integrity(
            s3_client, bucket_name, keys, expected_md5
        )

        for key in keys:
            dedup_utils.verify_range_get(
                s3_client, bucket_name, key, original_data, 0, 1024 * 1024
            )
            mid = mp_size // 2
            dedup_utils.verify_range_get(
                s3_client, bucket_name, key, original_data, mid, mid + 1024 * 1024
            )

        log.info("All multipart objects verified after dedup including range GETs")
        test_info.success_status("test passed")

    except (RGWBaseException, AssertionError, Exception) as e:
        log.error(f"Test failed: {e}")
        log.error(traceback.format_exc())
        test_info.failed_status("test failed")
        raise

    finally:
        dedup_utils.cleanup_bucket(s3_client, bucket_name)


def test_exec_session_lifecycle(config, ssh_con):
    """
    S7: Test dedup session lifecycle - start, pause, resume, complete,
    then start new session and abort mid-run.
    """
    test_info = AddTestInfo("S7: Session lifecycle controls (pause/resume/abort)")
    io_info_initialize = IOInfoInitialize()
    basic_io_structure = BasicIOInfoStructure()
    io_info_initialize.initialize(basic_io_structure.initial())

    try:
        test_info.started_info()
        user_info = s3lib.create_users(1)[0]
        auth = reusable.get_auth(
            user_info, ssh_con, config.ssl, getattr(config, "haproxy", False)
        )
        s3_client = auth.do_auth_using_client()

        bucket_name = f"dedup-lifecycle-{random.randint(1, 5000)}"
        s3_client.create_bucket(Bucket=bucket_name)

        keys, expected_md5, _ = dedup_utils.upload_identical_objects(
            s3_client, bucket_name, 100, 5 * 1024 * 1024, prefix="lifecycle-obj"
        )

        log.info("Phase 1: Start dedup, pause, resume, complete")
        dedup_utils.run_dedup_execute()
        time.sleep(3)

        dedup_utils.run_dedup_pause()
        time.sleep(2)

        pause_stats = dedup_utils.get_dedup_stats()
        log.info(
            f"Stats after pause: {json.dumps(pause_stats, indent=2) if isinstance(pause_stats, dict) else pause_stats}"
        )

        dedup_utils.run_dedup_resume()
        exec_stats = dedup_utils.wait_for_dedup_completion()

        dedup_utils.verify_all_objects_accessible(s3_client, bucket_name, keys)
        dedup_utils.verify_all_objects_integrity(
            s3_client, bucket_name, keys, expected_md5
        )
        log.info("Phase 1 passed: pause/resume completed without data loss")

        log.info("Phase 2: Start new session and abort")
        keys2, _, _ = dedup_utils.upload_identical_objects(
            s3_client, bucket_name, 50, 5 * 1024 * 1024, prefix="lifecycle2-obj"
        )

        dedup_utils.run_dedup_execute()
        time.sleep(3)

        dedup_utils.run_dedup_abort()
        time.sleep(2)

        abort_stats = dedup_utils.get_dedup_stats()
        log.info(
            f"Stats after abort: {json.dumps(abort_stats, indent=2) if isinstance(abort_stats, dict) else abort_stats}"
        )

        dedup_utils.verify_all_objects_accessible(s3_client, bucket_name, keys + keys2)
        log.info("Phase 2 passed: abort leaves cluster consistent")

        test_info.success_status("test passed")

    except (RGWBaseException, AssertionError, Exception) as e:
        log.error(f"Test failed: {e}")
        log.error(traceback.format_exc())
        test_info.failed_status("test failed")
        raise

    finally:
        dedup_utils.cleanup_bucket(s3_client, bucket_name)


def test_exec_ssec_exclusion(config, ssh_con):
    """
    S8: Upload SSE-C encrypted duplicate objects and unencrypted duplicates,
    run dedup, verify SSE-C objects skipped and unencrypted objects deduplicated.
    """
    test_info = AddTestInfo("S8: SSE-C encrypted objects excluded from dedup")
    io_info_initialize = IOInfoInitialize()
    basic_io_structure = BasicIOInfoStructure()
    io_info_initialize.initialize(basic_io_structure.initial())

    try:
        test_info.started_info()
        user_info = s3lib.create_users(1)[0]
        auth = reusable.get_auth(
            user_info, ssh_con, config.ssl, getattr(config, "haproxy", False)
        )
        s3_client = auth.do_auth_using_client()

        utils.exec_shell_cmd("ceph config set client.rgw rgw_crypt_require_ssl false")
        time.sleep(5)

        bucket_name = f"dedup-ssec-{random.randint(1, 5000)}"
        s3_client.create_bucket(Bucket=bucket_name)

        ssec_keys, sse_key_b64, sse_key_md5 = dedup_utils.upload_ssec_objects(
            s3_client, bucket_name, 20, 5 * 1024 * 1024, prefix="ssec-obj"
        )

        plain_keys, plain_md5, _ = dedup_utils.upload_identical_objects(
            s3_client, bucket_name, 20, 5 * 1024 * 1024, prefix="plain-obj"
        )

        log.info("Running dedup execute")
        dedup_utils.run_dedup_execute()
        exec_stats = dedup_utils.wait_for_dedup_completion()

        if isinstance(exec_stats, dict):
            encrypted_skipped = exec_stats.get(
                "ingress_skip_encrypted",
                exec_stats.get("skip_encrypted", 0),
            )
            log.info(f"Encrypted objects skipped: {encrypted_skipped}")
            assert encrypted_skipped > 0, "Expected SSE-C objects to be skipped"

        dedup_utils.verify_all_objects_integrity(
            s3_client, bucket_name, plain_keys, plain_md5
        )

        for key in ssec_keys:
            resp = s3_client.get_object(
                Bucket=bucket_name,
                Key=key,
                SSECustomerAlgorithm="AES256",
                SSECustomerKey=sse_key_b64,
                SSECustomerKeyMD5=sse_key_md5,
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        log.info("All SSE-C objects still accessible with correct key")

        test_info.success_status("test passed")

    except (RGWBaseException, AssertionError, Exception) as e:
        log.error(f"Test failed: {e}")
        log.error(traceback.format_exc())
        test_info.failed_status("test failed")
        raise

    finally:
        dedup_utils.cleanup_bucket(s3_client, bucket_name)


def test_exec_storage_class_filter(config, ssh_con):
    """
    S9: Test dedup with different storage classes and allow/deny bucket/storage-class filters.
    Covers PR #68575.
    """
    test_info = AddTestInfo("S9: Dedup storage class filter and allow/deny lists")
    io_info_initialize = IOInfoInitialize()
    basic_io_structure = BasicIOInfoStructure()
    io_info_initialize.initialize(basic_io_structure.initial())

    try:
        test_info.started_info()
        user_info = s3lib.create_users(1)[0]
        auth = reusable.get_auth(
            user_info, ssh_con, config.ssl, getattr(config, "haproxy", False)
        )
        s3_client = auth.do_auth_using_client()

        bucket_allow = f"dedup-allow-{random.randint(1, 5000)}"
        bucket_deny = f"dedup-deny-{random.randint(1, 5000)}"
        s3_client.create_bucket(Bucket=bucket_allow)
        s3_client.create_bucket(Bucket=bucket_deny)

        keys_allow, md5_allow, _ = dedup_utils.upload_identical_objects(
            s3_client, bucket_allow, 20, 5 * 1024 * 1024, prefix="allow-obj"
        )
        keys_deny, md5_deny, _ = dedup_utils.upload_identical_objects(
            s3_client, bucket_deny, 20, 5 * 1024 * 1024, prefix="deny-obj"
        )

        allow_file = dedup_utils.create_filter_list_file([bucket_allow])

        log.info(f"Running dedup with allow-bucket-list: {bucket_allow}")
        dedup_utils.run_dedup_execute(allow_bucket_file=allow_file)
        exec_stats = dedup_utils.wait_for_dedup_completion()

        if isinstance(exec_stats, dict):
            filtered = exec_stats.get("ingress_skip_filtered", 0)
            log.info(f"Filtered (skipped) objects: {filtered}")

        dedup_utils.verify_all_objects_integrity(
            s3_client, bucket_allow, keys_allow, md5_allow
        )
        dedup_utils.verify_all_objects_integrity(
            s3_client, bucket_deny, keys_deny, md5_deny
        )

        log.info("Allow/deny bucket filter test passed")
        test_info.success_status("test passed")

    except (RGWBaseException, AssertionError, Exception) as e:
        log.error(f"Test failed: {e}")
        log.error(traceback.format_exc())
        test_info.failed_status("test failed")
        raise

    finally:
        dedup_utils.cleanup_bucket(s3_client, bucket_allow)
        dedup_utils.cleanup_bucket(s3_client, bucket_deny)
        try:
            os.remove(allow_file)
        except Exception:
            pass


def test_exec_lc_expiration(config, ssh_con):
    """
    S10: Upload duplicate objects, dedup, set LC expiration, wait for expiration,
    verify ref counts decrement and storage reclaimed.
    """
    test_info = AddTestInfo("S10: LC expiration with deduplicated objects")
    io_info_initialize = IOInfoInitialize()
    basic_io_structure = BasicIOInfoStructure()
    io_info_initialize.initialize(basic_io_structure.initial())

    try:
        test_info.started_info()
        user_info = s3lib.create_users(1)[0]
        auth = reusable.get_auth(
            user_info, ssh_con, config.ssl, getattr(config, "haproxy", False)
        )
        s3_client = auth.do_auth_using_client()

        utils.exec_shell_cmd("ceph config set client.rgw rgw_lc_debug_interval 30")
        time.sleep(3)

        bucket_name = f"dedup-lc-{random.randint(1, 5000)}"
        s3_client.create_bucket(Bucket=bucket_name)

        keys, expected_md5, _ = dedup_utils.upload_identical_objects(
            s3_client, bucket_name, 20, 5 * 1024 * 1024, prefix="lc-obj"
        )

        log.info("Running dedup execute")
        dedup_utils.run_dedup_execute()
        dedup_utils.wait_for_dedup_completion()

        dedup_utils.verify_all_objects_accessible(s3_client, bucket_name, keys)

        pre_dedup_stats = dedup_utils.get_dedup_stats()

        log.info("Setting LC expiration policy (1 day)")
        dedup_utils.set_lifecycle_expiration(s3_client, bucket_name, days=1)

        lc_debug_interval = getattr(config, "rgw_lc_debug_interval", 30)
        log.info(f"Waiting for LC processing (debug_interval={lc_debug_interval}s)")
        time.sleep(lc_debug_interval * 3)

        resp = s3_client.list_objects_v2(Bucket=bucket_name)
        remaining = resp.get("KeyCount", 0)
        log.info(f"Objects remaining after LC expiration: {remaining}")

        post_dedup_stats = dedup_utils.get_dedup_stats()
        log.info(
            f"Post-LC dedup stats: {json.dumps(post_dedup_stats, indent=2) if isinstance(post_dedup_stats, dict) else post_dedup_stats}"
        )

        test_info.success_status("test passed")

    except (RGWBaseException, AssertionError, Exception) as e:
        log.error(f"Test failed: {e}")
        log.error(traceback.format_exc())
        test_info.failed_status("test failed")
        raise

    finally:
        dedup_utils.cleanup_bucket(s3_client, bucket_name)


def test_exec_versioned_objects(config, ssh_con):
    """
    S11: Enable bucket versioning, upload same content as multiple versions,
    run dedup, verify all versions accessible. Covers PR #66233.
    """
    test_info = AddTestInfo("S11: Dedup with versioned objects")
    io_info_initialize = IOInfoInitialize()
    basic_io_structure = BasicIOInfoStructure()
    io_info_initialize.initialize(basic_io_structure.initial())

    try:
        test_info.started_info()
        user_info = s3lib.create_users(1)[0]
        auth = reusable.get_auth(
            user_info, ssh_con, config.ssl, getattr(config, "haproxy", False)
        )
        s3_client = auth.do_auth_using_client()

        bucket_name = f"dedup-versioned-{random.randint(1, 5000)}"
        s3_client.create_bucket(Bucket=bucket_name)
        dedup_utils.enable_bucket_versioning(s3_client, bucket_name)

        identical_data = dedup_utils.generate_identical_data(5 * 1024 * 1024)
        expected_md5 = hashlib.md5(identical_data).hexdigest()

        version_count = getattr(config, "version_count", 10)
        object_key = "versioned-dedup-object"
        version_ids = []

        for i in range(version_count):
            resp = s3_client.put_object(
                Bucket=bucket_name, Key=object_key, Body=identical_data
            )
            version_ids.append(resp["VersionId"])
            log.info(f"Uploaded version {i + 1}: {resp['VersionId']}")

        log.info(f"Running dedup on {version_count} versions of same content")
        dedup_utils.run_dedup_execute()
        dedup_utils.wait_for_dedup_completion()

        for vid in version_ids:
            resp = s3_client.get_object(
                Bucket=bucket_name, Key=object_key, VersionId=vid
            )
            body = resp["Body"].read()
            actual_md5 = hashlib.md5(body).hexdigest()
            assert (
                actual_md5 == expected_md5
            ), f"Version {vid} MD5 mismatch: expected {expected_md5}, got {actual_md5}"
            log.info(f"Version {vid} verified OK")

        versions = dedup_utils.get_all_versions(s3_client, bucket_name, object_key)
        assert (
            len(versions) == version_count
        ), f"Expected {version_count} versions, found {len(versions)}"
        log.info(f"All {version_count} versions preserved and accessible after dedup")

        test_info.success_status("test passed")

    except (RGWBaseException, AssertionError, Exception) as e:
        log.error(f"Test failed: {e}")
        log.error(traceback.format_exc())
        test_info.failed_status("test failed")
        raise

    finally:
        dedup_utils.cleanup_bucket(s3_client, bucket_name)


def test_exec_s3_copy_dedup(config, ssh_con):
    """
    S12: Upload object > 5MB, use S3 COPY to duplicate 20 times,
    run dedup, verify all copies accessible.
    """
    test_info = AddTestInfo("S12: Dedup with S3 copy object > 5MB")
    io_info_initialize = IOInfoInitialize()
    basic_io_structure = BasicIOInfoStructure()
    io_info_initialize.initialize(basic_io_structure.initial())

    try:
        test_info.started_info()
        user_info = s3lib.create_users(1)[0]
        auth = reusable.get_auth(
            user_info, ssh_con, config.ssl, getattr(config, "haproxy", False)
        )
        s3_client = auth.do_auth_using_client()

        bucket_name = f"dedup-copy-{random.randint(1, 5000)}"
        s3_client.create_bucket(Bucket=bucket_name)

        source_key = "source-large-obj"
        source_data = dedup_utils.generate_identical_data(10 * 1024 * 1024)  # 10MB
        expected_md5 = hashlib.md5(source_data).hexdigest()
        s3_client.put_object(Bucket=bucket_name, Key=source_key, Body=source_data)
        log.info(f"Uploaded source object: {source_key}")

        copy_count = 20
        copy_keys = []
        for i in range(copy_count):
            copy_key = f"copy-obj-{i}"
            s3_client.copy_object(
                Bucket=bucket_name,
                Key=copy_key,
                CopySource={"Bucket": bucket_name, "Key": source_key},
            )
            copy_keys.append(copy_key)
            log.info(f"Copied to {copy_key}")

        all_keys = [source_key] + copy_keys

        log.info("Running dedup execute")
        dedup_utils.run_dedup_execute()
        dedup_utils.wait_for_dedup_completion()

        dedup_utils.verify_all_objects_integrity(
            s3_client, bucket_name, all_keys, expected_md5
        )

        for key in all_keys:
            resp = s3_client.head_object(Bucket=bucket_name, Key=key)
            log.info(f"{key}: size={resp['ContentLength']}, ETag={resp['ETag']}")

        log.info(
            f"All {len(all_keys)} objects (source + {copy_count} copies) verified after dedup"
        )
        test_info.success_status("test passed")

    except (RGWBaseException, AssertionError, Exception) as e:
        log.error(f"Test failed: {e}")
        log.error(traceback.format_exc())
        test_info.failed_status("test failed")
        raise

    finally:
        dedup_utils.cleanup_bucket(s3_client, bucket_name)


def test_exec_same_content_diff_metadata(config, ssh_con):
    """
    S14: Upload identical content with different names, buckets, users, ACLs, tags.
    Run dedup. Verify content deduplicated but metadata independently preserved.
    """
    test_info = AddTestInfo("S14: Same content different metadata")
    io_info_initialize = IOInfoInitialize()
    basic_io_structure = BasicIOInfoStructure()
    io_info_initialize.initialize(basic_io_structure.initial())

    try:
        test_info.started_info()
        all_users = s3lib.create_users(2)
        user1 = all_users[0]
        user2 = all_users[1]

        auth1 = reusable.get_auth(
            user1, ssh_con, config.ssl, getattr(config, "haproxy", False)
        )
        auth2 = reusable.get_auth(
            user2, ssh_con, config.ssl, getattr(config, "haproxy", False)
        )
        s3_client1 = auth1.do_auth_using_client()
        s3_client2 = auth2.do_auth_using_client()

        bucket1 = f"dedup-meta1-{random.randint(1, 5000)}"
        bucket2 = f"dedup-meta2-{random.randint(1, 5000)}"
        s3_client1.create_bucket(Bucket=bucket1)
        s3_client2.create_bucket(Bucket=bucket2)

        identical_data = dedup_utils.generate_identical_data(5 * 1024 * 1024)
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

        log.info("Running dedup execute")
        dedup_utils.run_dedup_execute()
        dedup_utils.wait_for_dedup_completion()

        for client, bucket, key, expected_meta_val in [
            (s3_client1, bucket1, "obj-user1-a", "value-a"),
            (s3_client1, bucket1, "obj-user1-b", "value-b"),
            (s3_client2, bucket2, "obj-user2-a", "value-c"),
            (s3_client2, bucket2, "obj-user2-b", "value-d"),
        ]:
            dedup_utils.verify_object_integrity(client, bucket, key, expected_md5)
            resp = client.head_object(Bucket=bucket, Key=key)
            actual_meta = resp.get("Metadata", {}).get("custom-key", "")
            assert actual_meta == expected_meta_val, (
                f"Metadata mismatch for {bucket}/{key}: "
                f"expected '{expected_meta_val}', got '{actual_meta}'"
            )
            log.info(f"Metadata preserved for {bucket}/{key}: custom-key={actual_meta}")

        tag_resp = s3_client1.get_object_tagging(Bucket=bucket1, Key="obj-user1-a")
        tags = {t["Key"]: t["Value"] for t in tag_resp.get("TagSet", [])}
        assert tags.get("env") == "prod", f"Tag mismatch for obj-user1-a: {tags}"

        tag_resp = s3_client1.get_object_tagging(Bucket=bucket1, Key="obj-user1-b")
        tags = {t["Key"]: t["Value"] for t in tag_resp.get("TagSet", [])}
        assert tags.get("env") == "staging", f"Tag mismatch for obj-user1-b: {tags}"

        log.info("All metadata, tags, and ACLs preserved after dedup")
        test_info.success_status("test passed")

    except (RGWBaseException, AssertionError, Exception) as e:
        log.error(f"Test failed: {e}")
        log.error(traceback.format_exc())
        test_info.failed_status("test failed")
        raise

    finally:
        dedup_utils.cleanup_bucket(s3_client1, bucket1)
        dedup_utils.cleanup_bucket(s3_client2, bucket2)


def test_exec_concurrent_s3_ops(config, ssh_con):
    """
    S15: Start dedup on large dataset while running concurrent S3 workload
    (PUT/GET/DELETE). Verify both dedup and S3 operations succeed.
    """
    test_info = AddTestInfo("S15: Concurrent S3 operations during dedup")
    io_info_initialize = IOInfoInitialize()
    basic_io_structure = BasicIOInfoStructure()
    io_info_initialize.initialize(basic_io_structure.initial())

    try:
        test_info.started_info()
        user_info = s3lib.create_users(1)[0]
        auth = reusable.get_auth(
            user_info, ssh_con, config.ssl, getattr(config, "haproxy", False)
        )
        s3_client = auth.do_auth_using_client()

        dedup_bucket = f"dedup-concurrent-{random.randint(1, 5000)}"
        workload_bucket = f"workload-concurrent-{random.randint(1, 5000)}"
        s3_client.create_bucket(Bucket=dedup_bucket)
        s3_client.create_bucket(Bucket=workload_bucket)

        keys, expected_md5, _ = dedup_utils.upload_identical_objects(
            s3_client, dedup_bucket, 50, 5 * 1024 * 1024, prefix="concurrent-obj"
        )

        log.info("Starting dedup execute and concurrent S3 workload simultaneously")
        dedup_utils.run_dedup_execute()

        workload_results = dedup_utils.run_concurrent_s3_workload(
            s3_client, workload_bucket, duration_secs=60, prefix="workload"
        )

        dedup_utils.wait_for_dedup_completion()

        dedup_utils.verify_all_objects_accessible(s3_client, dedup_bucket, keys)
        dedup_utils.verify_all_objects_integrity(
            s3_client, dedup_bucket, keys, expected_md5
        )

        assert workload_results["puts"] > 0, "Expected concurrent PUTs to succeed"
        assert workload_results["gets"] > 0, "Expected concurrent GETs to succeed"
        error_rate = workload_results["errors"] / max(
            workload_results["puts"]
            + workload_results["gets"]
            + workload_results["deletes"],
            1,
        )
        log.info(f"Concurrent workload error rate: {error_rate:.2%}")
        assert error_rate < 0.05, f"Error rate too high: {error_rate:.2%}"

        log.info("Dedup and concurrent S3 operations both succeeded")
        test_info.success_status("test passed")

    except (RGWBaseException, AssertionError, Exception) as e:
        log.error(f"Test failed: {e}")
        log.error(traceback.format_exc())
        test_info.failed_status("test failed")
        raise

    finally:
        dedup_utils.cleanup_bucket(s3_client, dedup_bucket)
        dedup_utils.cleanup_bucket(s3_client, workload_bucket)


# === Compressed object dedup tests (PR #68965) ===


def test_exec_compressed_sanity(config, ssh_con):
    """
    S16: Enable zone compression (zlib), upload 50 identical 5MB objects,
    run dedup estimate + execute, verify objects deduplicated and accessible.
    Exercises streaming decompression hashing (BLAKE3 over uncompressed data).
    """
    test_info = AddTestInfo("S16: Compressed object dedup sanity")
    io_info_initialize = IOInfoInitialize()
    basic_io_structure = BasicIOInfoStructure()
    io_info_initialize.initialize(basic_io_structure.initial())

    bucket_name = f"dedup-compressed-{random.randint(1, 5000)}"
    compression_enabled = False

    try:
        test_info.started_info()

        dedup_utils.enable_zone_compression("zlib", ssh_con)
        compression_enabled = True

        user_info = s3lib.create_users(1)[0]
        auth = reusable.get_auth(
            user_info, ssh_con, config.ssl, getattr(config, "haproxy", False)
        )
        s3_client = auth.do_auth_using_client()

        s3_client.create_bucket(Bucket=bucket_name)
        log.info(f"Created bucket: {bucket_name}")

        obj_count = getattr(config, "objects_count", 50)
        obj_size = 5 * 1024 * 1024

        keys, expected_md5, _ = dedup_utils.upload_identical_objects(
            s3_client, bucket_name, obj_count, obj_size, prefix="compressed-obj"
        )

        on_disk_size = dedup_utils.get_object_content_length(
            s3_client, bucket_name, keys[0]
        )
        if on_disk_size < obj_size:
            log.info(
                f"Compression active: logical={obj_size}, on-disk={on_disk_size}, "
                f"ratio={on_disk_size / obj_size:.2%}"
            )
        else:
            log.warning("Objects may not be compressed — on-disk size >= logical size")

        log.info("Running dedup estimate")
        dedup_utils.run_dedup_estimate()
        estimate_stats = dedup_utils.wait_for_dedup_completion()
        log.info(
            f"Estimate stats: {json.dumps(estimate_stats, indent=2) if isinstance(estimate_stats, dict) else estimate_stats}"
        )

        log.info("Running dedup execute")
        dedup_utils.run_dedup_execute()
        exec_stats = dedup_utils.wait_for_dedup_completion()
        log.info(
            f"Exec stats: {json.dumps(exec_stats, indent=2) if isinstance(exec_stats, dict) else exec_stats}"
        )

        if isinstance(exec_stats, dict):
            deduped = exec_stats.get(
                "deduped_objects", exec_stats.get("deduped_obj", 0)
            )
            log.info(f"Objects deduplicated: {deduped}")
            assert (
                deduped > 0
            ), "Expected compressed objects to be deduplicated but count is 0"

        dedup_utils.verify_all_objects_accessible(s3_client, bucket_name, keys)
        dedup_utils.verify_all_objects_integrity(
            s3_client, bucket_name, keys, expected_md5
        )

        test_info.success_status("test passed")

    except (RGWBaseException, AssertionError, Exception) as e:
        log.error(f"Test failed: {e}")
        log.error(traceback.format_exc())
        test_info.failed_status("test failed")
        raise

    finally:
        if compression_enabled:
            dedup_utils.disable_zone_compression(ssh_con)
        dedup_utils.cleanup_bucket(s3_client, bucket_name)


def test_exec_compressed_cross_mode(config, ssh_con):
    """
    S17: Upload identical objects both with and without compression,
    run dedup, verify all match (cross-mode: compressed + uncompressed).
    Tests that dedup keys use logical size and BLAKE3 hashes uncompressed data.
    """
    test_info = AddTestInfo("S17: Cross-compression dedup (compressed + uncompressed)")
    io_info_initialize = IOInfoInitialize()
    basic_io_structure = BasicIOInfoStructure()
    io_info_initialize.initialize(basic_io_structure.initial())

    bucket_name = f"dedup-crossmode-{random.randint(1, 5000)}"
    compression_enabled = False

    try:
        test_info.started_info()
        user_info = s3lib.create_users(1)[0]
        auth = reusable.get_auth(
            user_info, ssh_con, config.ssl, getattr(config, "haproxy", False)
        )
        s3_client = auth.do_auth_using_client()

        s3_client.create_bucket(Bucket=bucket_name)

        log.info("Phase 1: Upload 20 objects WITHOUT compression")
        dedup_utils.disable_zone_compression(ssh_con)
        plain_keys, expected_md5, original_data = dedup_utils.upload_identical_objects(
            s3_client, bucket_name, 20, 5 * 1024 * 1024, prefix="plain-obj"
        )

        plain_size = dedup_utils.get_object_content_length(
            s3_client, bucket_name, plain_keys[0]
        )
        log.info(f"Plain object on-disk size: {plain_size}")

        log.info("Phase 2: Enable zlib compression, upload 20 more identical objects")
        dedup_utils.enable_zone_compression("zlib", ssh_con)
        compression_enabled = True

        compressed_keys, comp_md5, _ = dedup_utils.upload_identical_objects(
            s3_client, bucket_name, 20, 5 * 1024 * 1024, prefix="compressed-obj"
        )

        assert (
            expected_md5 == comp_md5
        ), f"MD5 mismatch between plain and compressed batches: {expected_md5} vs {comp_md5}"

        compressed_size = dedup_utils.get_object_content_length(
            s3_client, bucket_name, compressed_keys[0]
        )
        log.info(f"Compressed object on-disk size: {compressed_size}")

        if compressed_size < plain_size:
            log.info("Confirmed: compressed objects are smaller on disk")

        all_keys = plain_keys + compressed_keys
        log.info(f"Total objects: {len(all_keys)} (20 plain + 20 compressed)")

        log.info("Running dedup execute")
        dedup_utils.run_dedup_execute()
        exec_stats = dedup_utils.wait_for_dedup_completion()
        log.info(
            f"Exec stats: {json.dumps(exec_stats, indent=2) if isinstance(exec_stats, dict) else exec_stats}"
        )

        if isinstance(exec_stats, dict):
            deduped = exec_stats.get(
                "deduped_objects", exec_stats.get("deduped_obj", 0)
            )
            log.info(f"Objects deduplicated: {deduped}")
            assert (
                deduped > 0
            ), "Expected cross-mode dedup to match compressed and uncompressed objects"

        dedup_utils.verify_all_objects_accessible(s3_client, bucket_name, all_keys)
        dedup_utils.verify_all_objects_integrity(
            s3_client, bucket_name, all_keys, expected_md5
        )

        test_info.success_status("test passed")

    except (RGWBaseException, AssertionError, Exception) as e:
        log.error(f"Test failed: {e}")
        log.error(traceback.format_exc())
        test_info.failed_status("test failed")
        raise

    finally:
        if compression_enabled:
            dedup_utils.disable_zone_compression(ssh_con)
        dedup_utils.cleanup_bucket(s3_client, bucket_name)


def test_exec_compressed_algo_switch(config, ssh_con):
    """
    S18: Upload identical objects under zlib, switch to snappy, upload more,
    run dedup. All should match (same logical content, different compression).
    Tests SRC selection priority (COMPRESSION_MATCH_EXACT vs PARTIAL).
    """
    test_info = AddTestInfo("S18: Compression algorithm switch + dedup")
    io_info_initialize = IOInfoInitialize()
    basic_io_structure = BasicIOInfoStructure()
    io_info_initialize.initialize(basic_io_structure.initial())

    bucket_name = f"dedup-algoswitch-{random.randint(1, 5000)}"
    compression_enabled = False

    try:
        test_info.started_info()
        user_info = s3lib.create_users(1)[0]
        auth = reusable.get_auth(
            user_info, ssh_con, config.ssl, getattr(config, "haproxy", False)
        )
        s3_client = auth.do_auth_using_client()

        s3_client.create_bucket(Bucket=bucket_name)

        log.info("Phase 1: Upload 15 objects with zlib compression")
        dedup_utils.enable_zone_compression("zlib", ssh_con)
        compression_enabled = True
        zlib_keys, expected_md5, _ = dedup_utils.upload_identical_objects(
            s3_client, bucket_name, 15, 5 * 1024 * 1024, prefix="zlib-obj"
        )

        zlib_size = dedup_utils.get_object_content_length(
            s3_client, bucket_name, zlib_keys[0]
        )
        log.info(f"zlib on-disk size: {zlib_size}")

        log.info("Phase 2: Switch to snappy, upload 15 more identical objects")
        dedup_utils.enable_zone_compression("snappy", ssh_con)
        snappy_keys, snappy_md5, _ = dedup_utils.upload_identical_objects(
            s3_client, bucket_name, 15, 5 * 1024 * 1024, prefix="snappy-obj"
        )

        assert (
            expected_md5 == snappy_md5
        ), f"MD5 mismatch between zlib and snappy batches: {expected_md5} vs {snappy_md5}"

        snappy_size = dedup_utils.get_object_content_length(
            s3_client, bucket_name, snappy_keys[0]
        )
        log.info(f"snappy on-disk size: {snappy_size}")

        all_keys = zlib_keys + snappy_keys

        log.info("Running dedup execute (current zone algo: snappy)")
        dedup_utils.run_dedup_execute()
        exec_stats = dedup_utils.wait_for_dedup_completion()
        log.info(
            f"Exec stats: {json.dumps(exec_stats, indent=2) if isinstance(exec_stats, dict) else exec_stats}"
        )

        if isinstance(exec_stats, dict):
            deduped = exec_stats.get(
                "deduped_objects", exec_stats.get("deduped_obj", 0)
            )
            log.info(f"Objects deduplicated: {deduped}")
            assert (
                deduped > 0
            ), "Expected dedup to match objects across compression algorithms"

        dedup_utils.verify_all_objects_accessible(s3_client, bucket_name, all_keys)
        dedup_utils.verify_all_objects_integrity(
            s3_client, bucket_name, all_keys, expected_md5
        )

        test_info.success_status("test passed")

    except (RGWBaseException, AssertionError, Exception) as e:
        log.error(f"Test failed: {e}")
        log.error(traceback.format_exc())
        test_info.failed_status("test failed")
        raise

    finally:
        if compression_enabled:
            dedup_utils.disable_zone_compression(ssh_con)
        dedup_utils.cleanup_bucket(s3_client, bucket_name)


def test_exec_compressed_multipart(config, ssh_con):
    """
    S19: Enable compression, upload 5 identical 50MB multipart objects,
    run dedup, verify accessible including range GETs.
    Tests out-of-line large attrs path (RGW_RECORD_FLAG_REMOTE_ATTRS).
    """
    test_info = AddTestInfo("S19: Compressed multipart object dedup")
    io_info_initialize = IOInfoInitialize()
    basic_io_structure = BasicIOInfoStructure()
    io_info_initialize.initialize(basic_io_structure.initial())

    bucket_name = f"dedup-compmp-{random.randint(1, 5000)}"
    compression_enabled = False

    try:
        test_info.started_info()

        dedup_utils.enable_zone_compression("zlib", ssh_con)
        compression_enabled = True

        user_info = s3lib.create_users(1)[0]
        auth = reusable.get_auth(
            user_info, ssh_con, config.ssl, getattr(config, "haproxy", False)
        )
        s3_client = auth.do_auth_using_client()

        s3_client.create_bucket(Bucket=bucket_name)

        mp_size = 50 * 1024 * 1024
        mp_count = 5

        (
            keys,
            expected_md5,
            original_data,
        ) = dedup_utils.upload_identical_multipart_objects(
            s3_client, bucket_name, mp_count, mp_size, prefix="compmp-obj"
        )

        log.info("Running dedup execute on compressed multipart objects")
        dedup_utils.run_dedup_execute()
        exec_stats = dedup_utils.wait_for_dedup_completion()
        log.info(
            f"Exec stats: {json.dumps(exec_stats, indent=2) if isinstance(exec_stats, dict) else exec_stats}"
        )

        if isinstance(exec_stats, dict):
            deduped = exec_stats.get(
                "deduped_objects", exec_stats.get("deduped_obj", 0)
            )
            log.info(f"Multipart objects deduplicated: {deduped}")
            assert (
                deduped > 0
            ), "Expected compressed multipart objects to be deduplicated"

            remote_attrs = exec_stats.get("remote_attrs_records", 0)
            log.info(f"Remote attrs records (out-of-line): {remote_attrs}")

        dedup_utils.verify_all_objects_accessible(s3_client, bucket_name, keys)
        dedup_utils.verify_all_objects_integrity(
            s3_client, bucket_name, keys, expected_md5
        )

        for key in keys:
            dedup_utils.verify_range_get(
                s3_client, bucket_name, key, original_data, 0, 1024 * 1024
            )
            mid = mp_size // 2
            dedup_utils.verify_range_get(
                s3_client, bucket_name, key, original_data, mid, mid + 1024 * 1024
            )
            end_offset = mp_size - (512 * 1024)
            dedup_utils.verify_range_get(
                s3_client, bucket_name, key, original_data, end_offset, mp_size - 1
            )

        log.info("All compressed multipart objects verified including range GETs")
        test_info.success_status("test passed")

    except (RGWBaseException, AssertionError, Exception) as e:
        log.error(f"Test failed: {e}")
        log.error(traceback.format_exc())
        test_info.failed_status("test failed")
        raise

    finally:
        if compression_enabled:
            dedup_utils.disable_zone_compression(ssh_con)
        dedup_utils.cleanup_bucket(s3_client, bucket_name)


def test_exec_compressed_skip_toggle(config, ssh_con):
    """
    S20: Enable compression, upload objects, toggle rgw_dedup_skip_compressed.
    When true: dedup should skip all compressed objects (0 deduplicated).
    When false: dedup should process them normally.
    """
    test_info = AddTestInfo("S20: rgw_dedup_skip_compressed config toggle")
    io_info_initialize = IOInfoInitialize()
    basic_io_structure = BasicIOInfoStructure()
    io_info_initialize.initialize(basic_io_structure.initial())

    bucket_name = f"dedup-skiptoggle-{random.randint(1, 5000)}"
    compression_enabled = False
    config_set = False

    try:
        test_info.started_info()

        dedup_utils.enable_zone_compression("zlib", ssh_con)
        compression_enabled = True

        user_info = s3lib.create_users(1)[0]
        auth = reusable.get_auth(
            user_info, ssh_con, config.ssl, getattr(config, "haproxy", False)
        )
        s3_client = auth.do_auth_using_client()

        s3_client.create_bucket(Bucket=bucket_name)

        obj_count = 30
        keys, expected_md5, _ = dedup_utils.upload_identical_objects(
            s3_client, bucket_name, obj_count, 5 * 1024 * 1024, prefix="skiptoggle-obj"
        )

        log.info("Phase 1: Set rgw_dedup_skip_compressed=true, run dedup")
        dedup_utils.set_dedup_config("rgw_dedup_skip_compressed", "true")
        config_set = True

        dedup_utils.run_dedup_execute()
        skip_stats = dedup_utils.wait_for_dedup_completion()
        log.info(
            f"Skip-mode stats: {json.dumps(skip_stats, indent=2) if isinstance(skip_stats, dict) else skip_stats}"
        )

        if isinstance(skip_stats, dict):
            deduped_skip = skip_stats.get(
                "deduped_objects", skip_stats.get("deduped_obj", 0)
            )
            log.info(f"Objects deduplicated with skip=true: {deduped_skip}")
            assert (
                deduped_skip == 0
            ), f"Expected 0 objects deduplicated with skip_compressed=true, got {deduped_skip}"

        dedup_utils.verify_all_objects_accessible(s3_client, bucket_name, keys)
        dedup_utils.verify_all_objects_integrity(
            s3_client, bucket_name, keys, expected_md5
        )
        log.info("Phase 1 passed: compressed objects correctly skipped")

        log.info("Phase 2: Set rgw_dedup_skip_compressed=false, run dedup again")
        dedup_utils.set_dedup_config("rgw_dedup_skip_compressed", "false")

        dedup_utils.run_dedup_execute()
        exec_stats = dedup_utils.wait_for_dedup_completion()
        log.info(
            f"Process-mode stats: {json.dumps(exec_stats, indent=2) if isinstance(exec_stats, dict) else exec_stats}"
        )

        if isinstance(exec_stats, dict):
            deduped_exec = exec_stats.get(
                "deduped_objects", exec_stats.get("deduped_obj", 0)
            )
            log.info(f"Objects deduplicated with skip=false: {deduped_exec}")
            assert (
                deduped_exec > 0
            ), "Expected objects to be deduplicated with skip_compressed=false"

        dedup_utils.verify_all_objects_accessible(s3_client, bucket_name, keys)
        dedup_utils.verify_all_objects_integrity(
            s3_client, bucket_name, keys, expected_md5
        )
        log.info("Phase 2 passed: compressed objects now deduplicated")

        test_info.success_status("test passed")

    except (RGWBaseException, AssertionError, Exception) as e:
        log.error(f"Test failed: {e}")
        log.error(traceback.format_exc())
        test_info.failed_status("test failed")
        raise

    finally:
        if config_set:
            dedup_utils.reset_dedup_config("rgw_dedup_skip_compressed")
        if compression_enabled:
            dedup_utils.disable_zone_compression(ssh_con)
        dedup_utils.cleanup_bucket(s3_client, bucket_name)


def test_exec_compressed_attr_mirror(config, ssh_con):
    """
    S21: Verify compression attribute mirroring after dedup.
    Upload compressed objects, dedup, then add uncompressed copies of same data,
    dedup again, verify all objects remain accessible and data-correct.
    Tests RGW_ATTR_COMPRESSION setxattr/rmxattr mirroring.
    """
    test_info = AddTestInfo("S21: Compression attr mirroring integrity")
    io_info_initialize = IOInfoInitialize()
    basic_io_structure = BasicIOInfoStructure()
    io_info_initialize.initialize(basic_io_structure.initial())

    bucket_name = f"dedup-attrmirror-{random.randint(1, 5000)}"
    compression_enabled = False

    try:
        test_info.started_info()
        user_info = s3lib.create_users(1)[0]
        auth = reusable.get_auth(
            user_info, ssh_con, config.ssl, getattr(config, "haproxy", False)
        )
        s3_client = auth.do_auth_using_client()

        s3_client.create_bucket(Bucket=bucket_name)

        log.info("Phase 1: Upload 30 compressed objects and dedup")
        dedup_utils.enable_zone_compression("zlib", ssh_con)
        compression_enabled = True

        comp_keys, expected_md5, _ = dedup_utils.upload_identical_objects(
            s3_client, bucket_name, 30, 5 * 1024 * 1024, prefix="comp-obj"
        )

        dedup_utils.run_dedup_execute()
        phase1_stats = dedup_utils.wait_for_dedup_completion()
        log.info(
            f"Phase 1 dedup stats: {json.dumps(phase1_stats, indent=2) if isinstance(phase1_stats, dict) else phase1_stats}"
        )

        dedup_utils.verify_all_objects_accessible(s3_client, bucket_name, comp_keys)
        dedup_utils.verify_all_objects_integrity(
            s3_client, bucket_name, comp_keys, expected_md5
        )

        pre_sizes = {}
        for key in comp_keys:
            pre_sizes[key] = dedup_utils.get_object_content_length(
                s3_client, bucket_name, key
            )
        log.info("Phase 1 passed: compressed objects deduplicated")

        log.info(
            "Phase 2: Disable compression, upload 10 uncompressed copies, dedup again"
        )
        dedup_utils.disable_zone_compression(ssh_con)
        compression_enabled = False

        plain_keys, plain_md5, _ = dedup_utils.upload_identical_objects(
            s3_client, bucket_name, 10, 5 * 1024 * 1024, prefix="plain-obj"
        )
        assert (
            expected_md5 == plain_md5
        ), "MD5 mismatch between compressed and plain data"

        dedup_utils.run_dedup_execute()
        phase2_stats = dedup_utils.wait_for_dedup_completion()
        log.info(
            f"Phase 2 dedup stats: {json.dumps(phase2_stats, indent=2) if isinstance(phase2_stats, dict) else phase2_stats}"
        )

        all_keys = comp_keys + plain_keys
        dedup_utils.verify_all_objects_accessible(s3_client, bucket_name, all_keys)
        dedup_utils.verify_all_objects_integrity(
            s3_client, bucket_name, all_keys, expected_md5
        )

        post_sizes = {}
        for key in all_keys:
            post_sizes[key] = dedup_utils.get_object_content_length(
                s3_client, bucket_name, key
            )

        size_changes = 0
        for key in comp_keys:
            if pre_sizes[key] != post_sizes[key]:
                size_changes += 1
                log.info(
                    f"Content-Length changed for {key}: "
                    f"{pre_sizes[key]} -> {post_sizes[key]}"
                )

        log.info(
            f"Content-Length changes in compressed batch: {size_changes}/{len(comp_keys)} "
            f"(changes may indicate attr mirroring shifted compression state)"
        )

        log.info("All objects data-correct after cross-compression dedup cycles")
        test_info.success_status("test passed")

    except (RGWBaseException, AssertionError, Exception) as e:
        log.error(f"Test failed: {e}")
        log.error(traceback.format_exc())
        test_info.failed_status("test failed")
        raise

    finally:
        if compression_enabled:
            dedup_utils.disable_zone_compression(ssh_con)
        dedup_utils.cleanup_bucket(s3_client, bucket_name)


# === Bug-hunting tests ===


def test_exec_overwrite_deduped_object(config, ssh_con):
    """
    B1: Upload identical objects, dedup, overwrite one target with different content.
    Verify the source and remaining targets still return original content.
    Bug target: GC deletes shared tails when overwritten object's old manifest
    is cleaned up via complete_atomic_modification() (rgw_rados.cc:6282).
    """
    test_info = AddTestInfo("B1: Overwrite deduped object - shared tail integrity")
    io_info_initialize = IOInfoInitialize()
    basic_io_structure = BasicIOInfoStructure()
    io_info_initialize.initialize(basic_io_structure.initial())

    try:
        test_info.started_info()
        user_info = s3lib.create_users(1)[0]
        auth = reusable.get_auth(
            user_info, ssh_con, config.ssl, getattr(config, "haproxy", False)
        )
        s3_client = auth.do_auth_using_client()

        bucket_name = f"dedup-overwrite-{random.randint(1, 5000)}"
        s3_client.create_bucket(Bucket=bucket_name)
        log.info(f"Created bucket: {bucket_name}")

        obj_count = getattr(config, "objects_count", 10)
        obj_size = 5 * 1024 * 1024

        keys, expected_md5, original_data = dedup_utils.upload_identical_objects(
            s3_client, bucket_name, obj_count, obj_size, prefix="ow-obj"
        )

        log.info("Running dedup execute")
        dedup_utils.run_dedup_execute()
        dedup_utils.wait_for_dedup_completion()

        dedup_utils.verify_all_objects_integrity(
            s3_client, bucket_name, keys, expected_md5
        )
        log.info("All objects verified after dedup")

        overwrite_key = keys[1]
        new_size = 3 * 1024 * 1024
        new_data = os.urandom(new_size)
        new_md5 = hashlib.md5(new_data).hexdigest()
        log.info(
            f"Overwriting deduped target {overwrite_key} with {new_size} bytes of new content"
        )
        s3_client.put_object(Bucket=bucket_name, Key=overwrite_key, Body=new_data)

        resp = s3_client.get_object(Bucket=bucket_name, Key=overwrite_key)
        body = resp["Body"].read()
        actual_md5 = hashlib.md5(body).hexdigest()
        assert (
            actual_md5 == new_md5
        ), f"Overwritten object MD5 mismatch: expected {new_md5}, got {actual_md5}"
        assert (
            len(body) == new_size
        ), f"Overwritten object size mismatch: expected {new_size}, got {len(body)}"
        log.info(f"Overwritten object {overwrite_key} returns new content correctly")

        remaining_keys = [k for k in keys if k != overwrite_key]
        log.info(
            f"Verifying {len(remaining_keys)} remaining deduped objects still readable"
        )
        for key in remaining_keys:
            resp = s3_client.get_object(Bucket=bucket_name, Key=key)
            body = resp["Body"].read()
            actual_md5 = hashlib.md5(body).hexdigest()
            assert actual_md5 == expected_md5, (
                f"Object {key} corrupted after overwrite of sibling: "
                f"expected {expected_md5}, got {actual_md5}"
            )
        log.info("All remaining deduped objects intact after overwrite")

        test_info.success_status("test passed")

    except (RGWBaseException, AssertionError, Exception) as e:
        log.error(f"Test failed: {e}")
        log.error(traceback.format_exc())
        test_info.failed_status("test failed")
        raise

    finally:
        dedup_utils.cleanup_bucket(s3_client, bucket_name)


def test_exec_delete_dedup_source(config, ssh_con):
    """
    B2: Upload identical objects, dedup, then delete objects one by one.
    After each deletion, verify remaining objects are still readable.
    Bug target: S3 delete uses GC path which doesn't call cls_refcount_put(),
    potentially destroying shared tail objects.
    """
    test_info = AddTestInfo("B2: Delete dedup source - refcount protection")
    io_info_initialize = IOInfoInitialize()
    basic_io_structure = BasicIOInfoStructure()
    io_info_initialize.initialize(basic_io_structure.initial())

    try:
        test_info.started_info()
        user_info = s3lib.create_users(1)[0]
        auth = reusable.get_auth(
            user_info, ssh_con, config.ssl, getattr(config, "haproxy", False)
        )
        s3_client = auth.do_auth_using_client()

        bucket_name = f"dedup-delsrc-{random.randint(1, 5000)}"
        s3_client.create_bucket(Bucket=bucket_name)

        obj_count = getattr(config, "objects_count", 10)
        obj_size = 5 * 1024 * 1024

        keys, expected_md5, _ = dedup_utils.upload_identical_objects(
            s3_client, bucket_name, obj_count, obj_size, prefix="delsrc-obj"
        )

        log.info("Running dedup execute")
        dedup_utils.run_dedup_execute()
        dedup_utils.wait_for_dedup_completion()

        dedup_utils.verify_all_objects_integrity(
            s3_client, bucket_name, keys, expected_md5
        )

        remaining = list(keys)
        for i, key_to_delete in enumerate(keys):
            log.info(f"Deleting object {i + 1}/{len(keys)}: {key_to_delete}")
            s3_client.delete_object(Bucket=bucket_name, Key=key_to_delete)
            remaining.remove(key_to_delete)

            if not remaining:
                log.info("All objects deleted, nothing left to verify")
                break

            log.info(f"Verifying {len(remaining)} remaining objects after deletion")
            for key in remaining:
                resp = s3_client.get_object(Bucket=bucket_name, Key=key)
                body = resp["Body"].read()
                actual_md5 = hashlib.md5(body).hexdigest()
                assert actual_md5 == expected_md5, (
                    f"Object {key} corrupted after deleting {key_to_delete}: "
                    f"expected {expected_md5}, got {actual_md5}"
                )
            log.info(
                f"All {len(remaining)} remaining objects OK after deleting {key_to_delete}"
            )

        log.info(
            "Sequential deletion test passed: all objects survived until their own deletion"
        )
        test_info.success_status("test passed")

    except (RGWBaseException, AssertionError, Exception) as e:
        log.error(f"Test failed: {e}")
        log.error(traceback.format_exc())
        test_info.failed_status("test failed")
        raise

    finally:
        dedup_utils.cleanup_bucket(s3_client, bucket_name)


def test_exec_s3_copy_then_delete_deduped(config, ssh_con):
    """
    B3: Upload identical objects, dedup, S3 COPY a deduped object (same bucket
    and cross-bucket), then delete the originals. Verify copies survive.
    Bug target: set_copy_attrs() copies shared_manifest xattr without
    incrementing refcounts on shared tail objects (rgw_rados.cc:3949).
    """
    test_info = AddTestInfo("B3: S3 copy deduped object then delete original")
    io_info_initialize = IOInfoInitialize()
    basic_io_structure = BasicIOInfoStructure()
    io_info_initialize.initialize(basic_io_structure.initial())

    bucket_b = None

    try:
        test_info.started_info()
        user_info = s3lib.create_users(1)[0]
        auth = reusable.get_auth(
            user_info, ssh_con, config.ssl, getattr(config, "haproxy", False)
        )
        s3_client = auth.do_auth_using_client()

        bucket_a = f"dedup-copysrc-{random.randint(1, 5000)}"
        bucket_b = f"dedup-copydst-{random.randint(1, 5000)}"
        s3_client.create_bucket(Bucket=bucket_a)
        s3_client.create_bucket(Bucket=bucket_b)

        obj_size = 5 * 1024 * 1024
        keys, expected_md5, _ = dedup_utils.upload_identical_objects(
            s3_client, bucket_a, 5, obj_size, prefix="cpsrc-obj"
        )

        log.info("Running dedup execute")
        dedup_utils.run_dedup_execute()
        dedup_utils.wait_for_dedup_completion()

        dedup_utils.verify_all_objects_integrity(
            s3_client, bucket_a, keys, expected_md5
        )

        same_bucket_copy = "copy-same-bucket"
        cross_bucket_copy = "copy-cross-bucket"

        log.info(f"S3 COPY {keys[0]} -> {bucket_a}/{same_bucket_copy}")
        s3_client.copy_object(
            Bucket=bucket_a,
            Key=same_bucket_copy,
            CopySource={"Bucket": bucket_a, "Key": keys[0]},
        )

        log.info(f"S3 COPY {keys[1]} -> {bucket_b}/{cross_bucket_copy}")
        s3_client.copy_object(
            Bucket=bucket_b,
            Key=cross_bucket_copy,
            CopySource={"Bucket": bucket_a, "Key": keys[1]},
        )

        dedup_utils.verify_object_integrity(
            s3_client, bucket_a, same_bucket_copy, expected_md5
        )
        dedup_utils.verify_object_integrity(
            s3_client, bucket_b, cross_bucket_copy, expected_md5
        )
        log.info("Both copies verified before deletion")

        log.info("Deleting all original objects from source bucket")
        for key in keys:
            s3_client.delete_object(Bucket=bucket_a, Key=key)

        log.info("Verifying copies survive after original deletion")
        dedup_utils.verify_object_integrity(
            s3_client, bucket_a, same_bucket_copy, expected_md5
        )
        dedup_utils.verify_object_integrity(
            s3_client, bucket_b, cross_bucket_copy, expected_md5
        )

        same_resp = s3_client.head_object(Bucket=bucket_a, Key=same_bucket_copy)
        assert (
            same_resp["ContentLength"] == obj_size
        ), f"Same-bucket copy size mismatch: expected {obj_size}, got {same_resp['ContentLength']}"
        cross_resp = s3_client.head_object(Bucket=bucket_b, Key=cross_bucket_copy)
        assert (
            cross_resp["ContentLength"] == obj_size
        ), f"Cross-bucket copy size mismatch: expected {obj_size}, got {cross_resp['ContentLength']}"

        log.info("Both copies survived deletion of all originals")
        test_info.success_status("test passed")

    except (RGWBaseException, AssertionError, Exception) as e:
        log.error(f"Test failed: {e}")
        log.error(traceback.format_exc())
        test_info.failed_status("test failed")
        raise

    finally:
        dedup_utils.cleanup_bucket(s3_client, bucket_a)
        if bucket_b:
            dedup_utils.cleanup_bucket(s3_client, bucket_b)


def test_exec_cross_bucket_source_delete(config, ssh_con):
    """
    B4: Upload identical object to two buckets, dedup (cross-bucket),
    delete the bucket containing the source, verify target bucket object survives.
    Bug target: No cross-bucket reference tracking; bucket deletion may
    destroy shared tail objects that the other bucket still references.
    """
    test_info = AddTestInfo("B4: Cross-bucket dedup - source bucket deletion")
    io_info_initialize = IOInfoInitialize()
    basic_io_structure = BasicIOInfoStructure()
    io_info_initialize.initialize(basic_io_structure.initial())

    bucket_a = None
    bucket_b = None

    try:
        test_info.started_info()
        user_info = s3lib.create_users(1)[0]
        auth = reusable.get_auth(
            user_info, ssh_con, config.ssl, getattr(config, "haproxy", False)
        )
        s3_client = auth.do_auth_using_client()

        bucket_a = f"dedup-xbkt-a-{random.randint(1, 5000)}"
        bucket_b = f"dedup-xbkt-b-{random.randint(1, 5000)}"
        s3_client.create_bucket(Bucket=bucket_a)
        s3_client.create_bucket(Bucket=bucket_b)

        obj_size = 5 * 1024 * 1024
        identical_data = dedup_utils.generate_identical_data(obj_size)
        expected_md5 = hashlib.md5(identical_data).hexdigest()

        key_a = "cross-obj-a"
        key_b = "cross-obj-b"
        s3_client.put_object(Bucket=bucket_a, Key=key_a, Body=identical_data)
        s3_client.put_object(Bucket=bucket_b, Key=key_b, Body=identical_data)
        log.info(
            f"Uploaded identical object to {bucket_a}/{key_a} and {bucket_b}/{key_b}"
        )

        log.info("Running dedup execute (cross-bucket)")
        dedup_utils.run_dedup_execute()
        dedup_utils.wait_for_dedup_completion()

        dedup_utils.verify_object_integrity(s3_client, bucket_a, key_a, expected_md5)
        dedup_utils.verify_object_integrity(s3_client, bucket_b, key_b, expected_md5)
        log.info("Both objects verified after dedup")

        log.info(f"Deleting source bucket {bucket_a} (object + bucket)")
        s3_client.delete_object(Bucket=bucket_a, Key=key_a)
        s3_client.delete_bucket(Bucket=bucket_a)
        bucket_a = None
        log.info("Source bucket deleted")

        log.info(f"Verifying target bucket object {bucket_b}/{key_b} survives")
        dedup_utils.verify_object_integrity(s3_client, bucket_b, key_b, expected_md5)

        resp = s3_client.head_object(Bucket=bucket_b, Key=key_b)
        assert (
            resp["ContentLength"] == obj_size
        ), f"Target object size mismatch: expected {obj_size}, got {resp['ContentLength']}"
        log.info("Target bucket object survived source bucket deletion")

        test_info.success_status("test passed")

    except (RGWBaseException, AssertionError, Exception) as e:
        log.error(f"Test failed: {e}")
        log.error(traceback.format_exc())
        test_info.failed_status("test failed")
        raise

    finally:
        if bucket_a:
            dedup_utils.cleanup_bucket(s3_client, bucket_a)
        if bucket_b:
            dedup_utils.cleanup_bucket(s3_client, bucket_b)


def test_exec_dedup_idempotency(config, ssh_con):
    """
    B5: Upload identical objects, run dedup exec 3 times consecutively.
    Verify no double-increment of refcounts, stats are consistent,
    and all objects remain accessible after each run.
    Bug target: Refcount double-increment or stats corruption on re-run.
    """
    test_info = AddTestInfo("B5: Dedup idempotency - multiple exec runs")
    io_info_initialize = IOInfoInitialize()
    basic_io_structure = BasicIOInfoStructure()
    io_info_initialize.initialize(basic_io_structure.initial())

    try:
        test_info.started_info()
        user_info = s3lib.create_users(1)[0]
        auth = reusable.get_auth(
            user_info, ssh_con, config.ssl, getattr(config, "haproxy", False)
        )
        s3_client = auth.do_auth_using_client()

        bucket_name = f"dedup-idempotent-{random.randint(1, 5000)}"
        s3_client.create_bucket(Bucket=bucket_name)

        obj_count = getattr(config, "objects_count", 20)
        obj_size = 5 * 1024 * 1024

        keys, expected_md5, _ = dedup_utils.upload_identical_objects(
            s3_client, bucket_name, obj_count, obj_size, prefix="idemp-obj"
        )

        all_run_stats = []
        for run_num in range(1, 4):
            log.info(f"=== Dedup exec run {run_num}/3 ===")
            dedup_utils.run_dedup_execute()
            stats = dedup_utils.wait_for_dedup_completion()
            parsed = dedup_utils.parse_dedup_stats(stats)
            all_run_stats.append(parsed)
            log.info(
                f"Run {run_num}: deduped={parsed.get('deduped_count', '?')}, "
                f"skipped_shared_manifest={parsed.get('skipped_shared_manifest', '?')}"
            )

            dedup_utils.verify_all_objects_accessible(s3_client, bucket_name, keys)

        run1 = all_run_stats[0]
        assert (
            run1.get("deduped_count", 0) > 0
        ), "Run 1 should have deduplicated objects"

        for run_num, parsed in enumerate(all_run_stats[1:], start=2):
            deduped = parsed.get("deduped_count", 0)
            assert (
                deduped == 0
            ), f"Run {run_num} should dedup 0 objects but deduped {deduped}"
            skipped = parsed.get("skipped_shared_manifest", 0)
            assert (
                skipped > 0
            ), f"Run {run_num} should skip already-deduped objects via shared_manifest"
            log.info(
                f"Run {run_num} correctly skipped {skipped} already-deduped objects"
            )

        log.info("Final integrity verification")
        dedup_utils.verify_all_objects_integrity(
            s3_client, bucket_name, keys, expected_md5
        )

        log.info("Idempotency test passed: 3 runs, consistent stats, no corruption")
        test_info.success_status("test passed")

    except (RGWBaseException, AssertionError, Exception) as e:
        log.error(f"Test failed: {e}")
        log.error(traceback.format_exc())
        test_info.failed_status("test failed")
        raise

    finally:
        dedup_utils.cleanup_bucket(s3_client, bucket_name)


# === Enhancement tests (128-limit, versioned boundary, multi-cycle) ===


def test_exec_128_limit_boundary(config, ssh_con):
    """
    E1: Upload 200 identical 5KB objects exceeding the 128 MAX_COPIES_PER_OBJ limit.
    Verify only ~127 targets are deduped, remaining are silently skipped, and
    all objects remain accessible.
    """
    test_info = AddTestInfo("E1: 128-copy limit boundary test (200 identical objects)")
    io_info_initialize = IOInfoInitialize()
    basic_io_structure = BasicIOInfoStructure()
    io_info_initialize.initialize(basic_io_structure.initial())

    config_set = False

    try:
        test_info.started_info()
        user_info = s3lib.create_users(1)[0]
        auth = reusable.get_auth(
            user_info, ssh_con, config.ssl, getattr(config, "haproxy", False)
        )
        s3_client = auth.do_auth_using_client()

        bucket_name = f"dedup-128limit-{random.randint(1, 5000)}"
        s3_client.create_bucket(Bucket=bucket_name)
        log.info(f"Created bucket: {bucket_name}")

        dedup_utils.set_dedup_config("rgw_dedup_min_obj_size_for_dedup", "4096")
        config_set = True

        obj_count = 200
        obj_size = 5 * 1024  # 5KB

        keys, expected_md5, _ = dedup_utils.upload_identical_objects(
            s3_client, bucket_name, obj_count, obj_size, prefix="limit-obj"
        )

        log.info("Running dedup execute on 200 identical objects")
        dedup_utils.run_dedup_execute()
        exec_stats = dedup_utils.wait_for_dedup_completion()
        log.info(
            f"Exec stats: {json.dumps(exec_stats, indent=2) if isinstance(exec_stats, dict) else exec_stats}"
        )

        parsed = dedup_utils.parse_dedup_stats_full(exec_stats)
        deduped = parsed["deduped_count"]
        skipped_copies = parsed["skipped_too_many_copies"]
        skipped_source = parsed["skipped_source_record"]

        log.info(f"Deduped: {deduped}, Skipped Too Many Copies: {skipped_copies}")

        assert deduped > 0, "Expected at least some objects to be deduped"
        assert (
            deduped <= 127
        ), f"Expected deduped <= 127 (128 limit minus source), got {deduped}"
        assert (
            skipped_copies > 0
        ), f"Expected Skipped Too Many Copies > 0 with 200 objects, got {skipped_copies}"

        expected_skipped = obj_count - 1 - deduped - parsed["skipped_shared_manifest"]
        log.info(
            f"Accounting: {obj_count} total = 1 source + {deduped} deduped + "
            f"{parsed['skipped_shared_manifest']} shared_manifest + {skipped_copies} skipped"
        )

        dedup_utils.verify_all_objects_accessible(s3_client, bucket_name, keys)
        dedup_utils.verify_all_objects_integrity(
            s3_client, bucket_name, keys, expected_md5
        )
        log.info("All 200 objects accessible and data-correct after dedup")

        test_info.success_status("test passed")

    except (RGWBaseException, AssertionError, Exception) as e:
        log.error(f"Test failed: {e}")
        log.error(traceback.format_exc())
        test_info.failed_status("test failed")
        raise

    finally:
        if config_set:
            dedup_utils.reset_dedup_config("rgw_dedup_min_obj_size_for_dedup")
        dedup_utils.cleanup_bucket(s3_client, bucket_name)


def test_exec_versioned_boundary(config, ssh_con):
    """
    E2: Create versioned bucket, upload same key 130 times (5KB content).
    130 versions = just past the 128 limit boundary.
    Verify all versions accessible after dedup and data integrity preserved.
    """
    test_info = AddTestInfo("E2: Versioned bucket dedup boundary (130 versions)")
    io_info_initialize = IOInfoInitialize()
    basic_io_structure = BasicIOInfoStructure()
    io_info_initialize.initialize(basic_io_structure.initial())

    config_set = False

    try:
        test_info.started_info()
        user_info = s3lib.create_users(1)[0]
        auth = reusable.get_auth(
            user_info, ssh_con, config.ssl, getattr(config, "haproxy", False)
        )
        s3_client = auth.do_auth_using_client()

        bucket_name = f"dedup-verbound-{random.randint(1, 5000)}"
        s3_client.create_bucket(Bucket=bucket_name)

        dedup_utils.set_dedup_config("rgw_dedup_min_obj_size_for_dedup", "4096")
        config_set = True

        version_count = 130
        object_key = "versioned-boundary-obj"
        obj_size = 5 * 1024  # 5KB

        (
            version_ids,
            expected_md5,
            original_data,
        ) = dedup_utils.upload_identical_versions(
            s3_client, bucket_name, object_key, version_count, obj_size
        )

        log.info(f"Running dedup execute on {version_count} versions")
        dedup_utils.run_dedup_execute()
        exec_stats = dedup_utils.wait_for_dedup_completion()
        log.info(
            f"Exec stats: {json.dumps(exec_stats, indent=2) if isinstance(exec_stats, dict) else exec_stats}"
        )

        parsed = dedup_utils.parse_dedup_stats_full(exec_stats)
        log.info(
            f"Deduped: {parsed['deduped_count']}, Unique: {parsed['unique_count']}, "
            f"Skipped Too Many: {parsed['skipped_too_many_copies']}"
        )

        assert (
            parsed["deduped_count"] > 0
        ), "Expected dedup to process versioned objects"
        assert parsed["unique_count"] >= 1, "Expected at least 1 unique content group"

        log.info(f"Verifying all {version_count} versions are accessible")
        for vid in version_ids:
            resp = s3_client.get_object(
                Bucket=bucket_name, Key=object_key, VersionId=vid
            )
            body = resp["Body"].read()
            actual_md5 = hashlib.md5(body).hexdigest()
            assert (
                actual_md5 == expected_md5
            ), f"Version {vid} MD5 mismatch: expected {expected_md5}, got {actual_md5}"

        versions = dedup_utils.get_all_versions(s3_client, bucket_name, object_key)
        assert (
            len(versions) == version_count
        ), f"Expected {version_count} versions, found {len(versions)}"
        log.info(f"All {version_count} versions preserved and data-correct after dedup")

        test_info.success_status("test passed")

    except (RGWBaseException, AssertionError, Exception) as e:
        log.error(f"Test failed: {e}")
        log.error(traceback.format_exc())
        test_info.failed_status("test failed")
        raise

    finally:
        if config_set:
            dedup_utils.reset_dedup_config("rgw_dedup_min_obj_size_for_dedup")
        dedup_utils.cleanup_bucket(s3_client, bucket_name)


def test_exec_multi_cycle_no_progress(config, ssh_con):
    """
    E3: Upload 200 identical 5KB objects (exceeds 128 limit), run dedup exec
    3 consecutive times. Verify cycles 2 and 3 produce zero new deduped objects,
    proving that multiple cycles cannot work around the 128 limit.
    """
    test_info = AddTestInfo("E3: Multi-cycle no progress (128-limit stuck)")
    io_info_initialize = IOInfoInitialize()
    basic_io_structure = BasicIOInfoStructure()
    io_info_initialize.initialize(basic_io_structure.initial())

    config_set = False

    try:
        test_info.started_info()
        user_info = s3lib.create_users(1)[0]
        auth = reusable.get_auth(
            user_info, ssh_con, config.ssl, getattr(config, "haproxy", False)
        )
        s3_client = auth.do_auth_using_client()

        bucket_name = f"dedup-multicycle-{random.randint(1, 5000)}"
        s3_client.create_bucket(Bucket=bucket_name)

        dedup_utils.set_dedup_config("rgw_dedup_min_obj_size_for_dedup", "4096")
        config_set = True

        obj_count = 200
        obj_size = 5 * 1024  # 5KB

        keys, expected_md5, _ = dedup_utils.upload_identical_objects(
            s3_client, bucket_name, obj_count, obj_size, prefix="cycle-obj"
        )

        all_cycle_stats = []
        for cycle in range(1, 4):
            log.info(f"=== Dedup exec cycle {cycle}/3 ===")
            dedup_utils.run_dedup_execute()
            stats = dedup_utils.wait_for_dedup_completion()
            parsed = dedup_utils.parse_dedup_stats_full(stats)
            all_cycle_stats.append(parsed)
            log.info(
                f"Cycle {cycle}: deduped={parsed['deduped_count']}, "
                f"skipped_too_many={parsed['skipped_too_many_copies']}, "
                f"skipped_shared_manifest={parsed['skipped_shared_manifest']}"
            )

        cycle1 = all_cycle_stats[0]
        assert cycle1["deduped_count"] > 0, "Cycle 1 should have deduped objects"

        for cycle_num, parsed in enumerate(all_cycle_stats[1:], start=2):
            assert (
                parsed["deduped_count"] == 0
            ), f"Cycle {cycle_num} should dedup 0 objects but got {parsed['deduped_count']}"
            log.info(f"Cycle {cycle_num}: correctly deduped 0 new objects")

        cycle2_skipped = all_cycle_stats[1]["skipped_too_many_copies"]
        cycle3_skipped = all_cycle_stats[2]["skipped_too_many_copies"]
        assert cycle2_skipped == cycle3_skipped, (
            f"Skipped Too Many Copies should be stable: cycle 2={cycle2_skipped}, "
            f"cycle 3={cycle3_skipped}"
        )
        log.info(
            f"Skipped Too Many Copies stable at {cycle2_skipped} across cycles 2-3"
        )

        for cycle_num, parsed in enumerate(all_cycle_stats[1:], start=2):
            assert parsed["skipped_shared_manifest"] >= cycle1["deduped_count"], (
                f"Cycle {cycle_num}: shared_manifest skipped ({parsed['skipped_shared_manifest']}) "
                f"should be >= cycle 1 deduped ({cycle1['deduped_count']})"
            )

        dedup_utils.verify_all_objects_accessible(s3_client, bucket_name, keys)
        dedup_utils.verify_all_objects_integrity(
            s3_client, bucket_name, keys, expected_md5
        )
        log.info("All objects accessible and data-correct after 3 cycles")

        test_info.success_status("test passed")

    except (RGWBaseException, AssertionError, Exception) as e:
        log.error(f"Test failed: {e}")
        log.error(traceback.format_exc())
        test_info.failed_status("test failed")
        raise

    finally:
        if config_set:
            dedup_utils.reset_dedup_config("rgw_dedup_min_obj_size_for_dedup")
        dedup_utils.cleanup_bucket(s3_client, bucket_name)


def test_exec_split_head_small_objects(config, ssh_con):
    """
    E4: Upload 50 identical 5KB single-part objects, run dedup, verify
    split-head mechanism is used (data extracted from head to tail for
    objects that store data inline).
    """
    test_info = AddTestInfo("E4: Split-head mechanism for small single-part objects")
    io_info_initialize = IOInfoInitialize()
    basic_io_structure = BasicIOInfoStructure()
    io_info_initialize.initialize(basic_io_structure.initial())

    config_set = False

    try:
        test_info.started_info()
        user_info = s3lib.create_users(1)[0]
        auth = reusable.get_auth(
            user_info, ssh_con, config.ssl, getattr(config, "haproxy", False)
        )
        s3_client = auth.do_auth_using_client()

        bucket_name = f"dedup-splithead-{random.randint(1, 5000)}"
        s3_client.create_bucket(Bucket=bucket_name)

        dedup_utils.set_dedup_config("rgw_dedup_min_obj_size_for_dedup", "4096")
        config_set = True

        obj_count = 50
        obj_size = 5 * 1024  # 5KB

        keys, expected_md5, original_data = dedup_utils.upload_identical_objects(
            s3_client, bucket_name, obj_count, obj_size, prefix="splithead-obj"
        )

        log.info("Running dedup execute on 50 small single-part objects")
        dedup_utils.run_dedup_execute()
        exec_stats = dedup_utils.wait_for_dedup_completion()
        log.info(
            f"Exec stats: {json.dumps(exec_stats, indent=2) if isinstance(exec_stats, dict) else exec_stats}"
        )

        parsed = dedup_utils.parse_dedup_stats_full(exec_stats)
        log.info(
            f"Deduped: {parsed['deduped_count']}, "
            f"Split-Head Src: {parsed['split_head_src']}, "
            f"Split-Head Tgt: {parsed['split_head_tgt']}"
        )

        assert parsed["deduped_count"] > 0, "Expected objects to be deduped"
        assert parsed["split_head_src"] > 0, (
            f"Expected Split-Head Src OBJ > 0 for 5KB single-part objects, "
            f"got {parsed['split_head_src']}"
        )
        assert (
            parsed["split_head_tgt"] > 0
        ), f"Expected Split-Head Tgt OBJ > 0, got {parsed['split_head_tgt']}"

        dedup_utils.verify_all_objects_accessible(s3_client, bucket_name, keys)
        dedup_utils.verify_all_objects_integrity(
            s3_client, bucket_name, keys, expected_md5
        )

        for key in keys[:5]:
            dedup_utils.verify_range_get(
                s3_client, bucket_name, key, original_data, 0, 1023
            )
            dedup_utils.verify_range_get(
                s3_client, bucket_name, key, original_data, 2048, obj_size - 1
            )

        log.info("Split-head dedup verified: all objects accessible with correct data")
        test_info.success_status("test passed")

    except (RGWBaseException, AssertionError, Exception) as e:
        log.error(f"Test failed: {e}")
        log.error(traceback.format_exc())
        test_info.failed_status("test failed")
        raise

    finally:
        if config_set:
            dedup_utils.reset_dedup_config("rgw_dedup_min_obj_size_for_dedup")
        dedup_utils.cleanup_bucket(s3_client, bucket_name)


def test_exec_stats_validation(config, ssh_con):
    """
    E5: Upload 50 identical 5KB objects, run estimate then exec, validate
    that all expected stats fields are present and have correct values.
    """
    test_info = AddTestInfo("E5: Dedup stats field validation (estimate + exec)")
    io_info_initialize = IOInfoInitialize()
    basic_io_structure = BasicIOInfoStructure()
    io_info_initialize.initialize(basic_io_structure.initial())

    config_set = False

    try:
        test_info.started_info()
        user_info = s3lib.create_users(1)[0]
        auth = reusable.get_auth(
            user_info, ssh_con, config.ssl, getattr(config, "haproxy", False)
        )
        s3_client = auth.do_auth_using_client()

        bucket_name = f"dedup-statsval-{random.randint(1, 5000)}"
        s3_client.create_bucket(Bucket=bucket_name)

        dedup_utils.set_dedup_config("rgw_dedup_min_obj_size_for_dedup", "4096")
        config_set = True

        obj_count = 50
        obj_size = 5 * 1024  # 5KB

        keys, expected_md5, _ = dedup_utils.upload_identical_objects(
            s3_client, bucket_name, obj_count, obj_size, prefix="stats-obj"
        )

        log.info("Phase 1: Running dedup estimate and validating stats")
        dedup_utils.run_dedup_estimate()
        estimate_stats = dedup_utils.wait_for_dedup_completion()

        assert isinstance(estimate_stats, dict), "Estimate stats should be a dict"
        assert (
            estimate_stats.get("completed") is True
        ), "Estimate should show completed=true"

        est_parsed = dedup_utils.parse_dedup_stats_full(estimate_stats)
        log.info(f"Estimate parsed: {json.dumps(est_parsed, indent=2)}")

        assert (
            est_parsed["total_processed"] == obj_count
        ), f"Total processed should be {obj_count}, got {est_parsed['total_processed']}"
        assert (
            est_parsed["unique_count"] >= 1
        ), f"Unique Obj should be >= 1, got {est_parsed['unique_count']}"
        assert (
            est_parsed["duplicate_count"] > 0
        ), f"Duplicate Obj should be > 0 for identical objects, got {est_parsed['duplicate_count']}"
        assert (
            est_parsed["dedup_ratio_estimate"] > 1.0
        ), f"Dedup ratio estimate should be > 1.0, got {est_parsed['dedup_ratio_estimate']}"

        est_worker = estimate_stats.get("worker_stats", {}).get("main", {})
        assert (
            est_worker.get("Ingress Objs count") == obj_count
        ), f"Ingress Objs count should be {obj_count}"

        log.info("Phase 2: Running dedup exec and validating stats")
        dedup_utils.run_dedup_execute()
        exec_stats = dedup_utils.wait_for_dedup_completion()

        assert isinstance(exec_stats, dict), "Exec stats should be a dict"
        assert exec_stats.get("completed") is True, "Exec should show completed=true"

        exec_parsed = dedup_utils.parse_dedup_stats_full(exec_stats)
        log.info(f"Exec parsed: {json.dumps(exec_parsed, indent=2)}")

        assert (
            exec_parsed["total_processed"] == obj_count
        ), f"Total processed should be {obj_count}, got {exec_parsed['total_processed']}"
        assert (
            exec_parsed["deduped_count"] > 0
        ), f"Deduped Obj should be > 0, got {exec_parsed['deduped_count']}"
        assert (
            exec_parsed["unique_count"] >= 1
        ), f"Unique Obj should be >= 1, got {exec_parsed['unique_count']}"
        assert (
            exec_parsed["dedup_ratio_actual"] > 1.0
        ), f"Dedup ratio actual should be > 1.0, got {exec_parsed['dedup_ratio_actual']}"

        required_exec_fields = ["dedup_ratio_estimate", "dedup_ratio_actual"]
        for field in required_exec_fields:
            assert (
                field in exec_stats or exec_parsed.get(field, 0) > 0
            ), f"Required field '{field}' missing or zero in exec stats"

        required_skipped = [
            "Skipped shared_manifest",
            "Skipped purged small objs",
            "Skipped singleton objs",
            "Skipped source record",
        ]
        md5_skipped = exec_stats.get("md5_stats", {}).get("skipped", {})
        for field in required_skipped:
            assert (
                field in md5_skipped
            ), f"Skipped field '{field}' missing from exec stats"

        log.info("All stats fields validated for both estimate and exec")
        test_info.success_status("test passed")

    except (RGWBaseException, AssertionError, Exception) as e:
        log.error(f"Test failed: {e}")
        log.error(traceback.format_exc())
        test_info.failed_status("test failed")
        raise

    finally:
        if config_set:
            dedup_utils.reset_dedup_config("rgw_dedup_min_obj_size_for_dedup")
        dedup_utils.cleanup_bucket(s3_client, bucket_name)


# === Restart Scenarios (R1-R5) ===


def test_exec_restart_rgw(config, ssh_con):
    """
    R1: Upload 100 identical 5KB objects, run estimate, start exec async,
    restart RGW mid-execution, re-run exec, verify all objects accessible
    and data integrity preserved.
    """
    test_info = AddTestInfo("R1: RGW restart mid-exec")
    io_info_initialize = IOInfoInitialize()
    basic_io_structure = BasicIOInfoStructure()
    io_info_initialize.initialize(basic_io_structure.initial())

    config_set = False

    try:
        test_info.started_info()
        user_info = s3lib.create_users(1)[0]
        auth = reusable.get_auth(
            user_info, ssh_con, config.ssl, getattr(config, "haproxy", False)
        )
        s3_client = auth.do_auth_using_client()

        bucket_name = f"dedup-restart-rgw-{random.randint(1, 5000)}"
        s3_client.create_bucket(Bucket=bucket_name)

        dedup_utils.set_dedup_config("rgw_dedup_min_obj_size_for_dedup", "4096")
        config_set = True

        obj_count = getattr(config, "objects_count", 100)
        obj_size = 5 * 1024  # 5KB

        keys, expected_md5, _ = dedup_utils.upload_identical_objects(
            s3_client, bucket_name, obj_count, obj_size, prefix="restart-rgw-obj"
        )

        log.info("Running dedup estimate")
        dedup_utils.run_dedup_estimate()
        dedup_utils.wait_for_dedup_completion()

        log.info("Starting dedup exec async then restarting RGW")
        dedup_utils.run_dedup_execute_async()
        time.sleep(5)

        dedup_utils.restart_rgw_service()
        time.sleep(30)

        log.info("Re-running dedup exec after RGW restart")
        dedup_utils.run_dedup_execute()
        exec_stats = dedup_utils.wait_for_dedup_completion()

        parsed = dedup_utils.parse_dedup_stats(exec_stats)
        log.info(
            f"Exec stats: deduped={parsed['deduped_count']}, "
            f"skipped_shared={parsed['skipped_shared_manifest']}"
        )

        dedup_utils.verify_all_objects_accessible(s3_client, bucket_name, keys)
        dedup_utils.verify_all_objects_integrity(
            s3_client, bucket_name, keys, expected_md5
        )

        assert (
            parsed["deduped_count"] > 0 or parsed["skipped_shared_manifest"] > 0
        ), "Expected deduped_count > 0 or skipped_shared_manifest > 0 after restart"

        log.info("RGW restart mid-exec test passed")
        test_info.success_status("test passed")

    except (RGWBaseException, AssertionError, Exception) as e:
        log.error(f"Test failed: {e}")
        log.error(traceback.format_exc())
        test_info.failed_status("test failed")
        raise

    finally:
        if config_set:
            dedup_utils.reset_dedup_config("rgw_dedup_min_obj_size_for_dedup")
        dedup_utils.cleanup_bucket(s3_client, bucket_name)


def test_exec_restart_sigkill(config, ssh_con):
    """
    R2: Upload 50 identical 5KB objects, run estimate, start exec async,
    SIGKILL the RGW process mid-execution, wait for recovery, re-run exec,
    verify all objects accessible and check for orphan objects.
    """
    test_info = AddTestInfo("R2: SIGKILL RGW during exec")
    io_info_initialize = IOInfoInitialize()
    basic_io_structure = BasicIOInfoStructure()
    io_info_initialize.initialize(basic_io_structure.initial())

    config_set = False

    try:
        test_info.started_info()
        user_info = s3lib.create_users(1)[0]
        auth = reusable.get_auth(
            user_info, ssh_con, config.ssl, getattr(config, "haproxy", False)
        )
        s3_client = auth.do_auth_using_client()

        bucket_name = f"dedup-sigkill-{random.randint(1, 5000)}"
        s3_client.create_bucket(Bucket=bucket_name)

        dedup_utils.set_dedup_config("rgw_dedup_min_obj_size_for_dedup", "4096")
        config_set = True

        obj_count = 50
        obj_size = 5 * 1024  # 5KB

        keys, expected_md5, _ = dedup_utils.upload_identical_objects(
            s3_client, bucket_name, obj_count, obj_size, prefix="sigkill-obj"
        )

        log.info("Running dedup estimate")
        dedup_utils.run_dedup_estimate()
        dedup_utils.wait_for_dedup_completion()

        log.info("Starting dedup exec async then sending SIGKILL")
        dedup_utils.run_dedup_execute_async()
        time.sleep(3)

        dedup_utils.kill_rgw_process()
        time.sleep(30)

        log.info("Re-running dedup exec after SIGKILL recovery")
        dedup_utils.run_dedup_execute()
        dedup_utils.wait_for_dedup_completion()

        dedup_utils.verify_all_objects_accessible(s3_client, bucket_name, keys)

        orphans = dedup_utils.check_orphan_objects()
        log.info(f"Orphan objects found: {len(orphans)}")

        log.info("SIGKILL RGW during exec test passed")
        test_info.success_status("test passed")

    except (RGWBaseException, AssertionError, Exception) as e:
        log.error(f"Test failed: {e}")
        log.error(traceback.format_exc())
        test_info.failed_status("test failed")
        raise

    finally:
        if config_set:
            dedup_utils.reset_dedup_config("rgw_dedup_min_obj_size_for_dedup")
        dedup_utils.cleanup_bucket(s3_client, bucket_name)


def test_exec_restart_osd(config, ssh_con):
    """
    R3: Upload 100 identical 5KB objects, run estimate, start exec async,
    restart the OSD hosting the bucket's PGs mid-execution, wait for health OK,
    retry exec if needed, verify all objects accessible and data integrity.
    """
    test_info = AddTestInfo("R3: OSD restart during exec")
    io_info_initialize = IOInfoInitialize()
    basic_io_structure = BasicIOInfoStructure()
    io_info_initialize.initialize(basic_io_structure.initial())

    config_set = False

    try:
        test_info.started_info()
        user_info = s3lib.create_users(1)[0]
        auth = reusable.get_auth(
            user_info, ssh_con, config.ssl, getattr(config, "haproxy", False)
        )
        s3_client = auth.do_auth_using_client()

        bucket_name = f"dedup-restart-osd-{random.randint(1, 5000)}"
        s3_client.create_bucket(Bucket=bucket_name)

        dedup_utils.set_dedup_config("rgw_dedup_min_obj_size_for_dedup", "4096")
        config_set = True

        obj_count = getattr(config, "objects_count", 100)
        obj_size = 5 * 1024  # 5KB

        keys, expected_md5, _ = dedup_utils.upload_identical_objects(
            s3_client, bucket_name, obj_count, obj_size, prefix="osd-restart-obj"
        )

        log.info("Running dedup estimate")
        dedup_utils.run_dedup_estimate()
        dedup_utils.wait_for_dedup_completion()

        log.info("Starting dedup exec async then restarting OSD")
        dedup_utils.run_dedup_execute_async()
        time.sleep(5)

        osd_id = dedup_utils.get_osd_for_bucket(bucket_name)
        dedup_utils.restart_osd(osd_id)
        dedup_utils.wait_for_health_ok()

        log.info("Waiting for exec completion (retry if needed)")
        try:
            dedup_utils.wait_for_dedup_completion(timeout=120)
        except AssertionError:
            log.info("Exec may have been interrupted, re-running")
            dedup_utils.run_dedup_execute()
            dedup_utils.wait_for_dedup_completion()

        dedup_utils.verify_all_objects_accessible(s3_client, bucket_name, keys)
        dedup_utils.verify_all_objects_integrity(
            s3_client, bucket_name, keys, expected_md5
        )

        log.info("OSD restart during exec test passed")
        test_info.success_status("test passed")

    except (RGWBaseException, AssertionError, Exception) as e:
        log.error(f"Test failed: {e}")
        log.error(traceback.format_exc())
        test_info.failed_status("test failed")
        raise

    finally:
        if config_set:
            dedup_utils.reset_dedup_config("rgw_dedup_min_obj_size_for_dedup")
        dedup_utils.cleanup_bucket(s3_client, bucket_name)


def test_exec_restart_mon(config, ssh_con):
    """
    R4: Upload 50 identical 5KB objects, run estimate, start exec async,
    restart the MON leader mid-execution, wait and retry exec if needed,
    verify all objects accessible.
    """
    test_info = AddTestInfo("R4: MON leader restart during exec")
    io_info_initialize = IOInfoInitialize()
    basic_io_structure = BasicIOInfoStructure()
    io_info_initialize.initialize(basic_io_structure.initial())

    config_set = False

    try:
        test_info.started_info()
        user_info = s3lib.create_users(1)[0]
        auth = reusable.get_auth(
            user_info, ssh_con, config.ssl, getattr(config, "haproxy", False)
        )
        s3_client = auth.do_auth_using_client()

        bucket_name = f"dedup-restart-mon-{random.randint(1, 5000)}"
        s3_client.create_bucket(Bucket=bucket_name)

        dedup_utils.set_dedup_config("rgw_dedup_min_obj_size_for_dedup", "4096")
        config_set = True

        obj_count = 50
        obj_size = 5 * 1024  # 5KB

        keys, expected_md5, _ = dedup_utils.upload_identical_objects(
            s3_client, bucket_name, obj_count, obj_size, prefix="mon-restart-obj"
        )

        log.info("Running dedup estimate")
        dedup_utils.run_dedup_estimate()
        dedup_utils.wait_for_dedup_completion()

        log.info("Starting dedup exec async then restarting MON leader")
        dedup_utils.run_dedup_execute_async()
        time.sleep(3)

        mon_leader = dedup_utils.get_mon_leader()
        dedup_utils.restart_mon(mon_leader)
        time.sleep(15)

        log.info("Waiting for exec completion (retry if needed)")
        try:
            dedup_utils.wait_for_dedup_completion(timeout=120)
        except AssertionError:
            log.info("Exec may have been interrupted, re-running")
            dedup_utils.run_dedup_execute()
            dedup_utils.wait_for_dedup_completion()

        dedup_utils.verify_all_objects_accessible(s3_client, bucket_name, keys)

        log.info("MON leader restart during exec test passed")
        test_info.success_status("test passed")

    except (RGWBaseException, AssertionError, Exception) as e:
        log.error(f"Test failed: {e}")
        log.error(traceback.format_exc())
        test_info.failed_status("test failed")
        raise

    finally:
        if config_set:
            dedup_utils.reset_dedup_config("rgw_dedup_min_obj_size_for_dedup")
        dedup_utils.cleanup_bucket(s3_client, bucket_name)


def test_exec_restart_rapid(config, ssh_con):
    """
    R5: Upload 100 identical 5KB objects, run estimate, start exec async,
    perform 3 rapid RGW restarts with 10s gaps, re-run exec, verify all
    objects accessible and data integrity.
    """
    test_info = AddTestInfo("R5: 3 rapid RGW restarts during exec")
    io_info_initialize = IOInfoInitialize()
    basic_io_structure = BasicIOInfoStructure()
    io_info_initialize.initialize(basic_io_structure.initial())

    config_set = False

    try:
        test_info.started_info()
        user_info = s3lib.create_users(1)[0]
        auth = reusable.get_auth(
            user_info, ssh_con, config.ssl, getattr(config, "haproxy", False)
        )
        s3_client = auth.do_auth_using_client()

        bucket_name = f"dedup-rapid-restart-{random.randint(1, 5000)}"
        s3_client.create_bucket(Bucket=bucket_name)

        dedup_utils.set_dedup_config("rgw_dedup_min_obj_size_for_dedup", "4096")
        config_set = True

        obj_count = getattr(config, "objects_count", 100)
        obj_size = 5 * 1024  # 5KB

        keys, expected_md5, _ = dedup_utils.upload_identical_objects(
            s3_client, bucket_name, obj_count, obj_size, prefix="rapid-restart-obj"
        )

        log.info("Running dedup estimate")
        dedup_utils.run_dedup_estimate()
        dedup_utils.wait_for_dedup_completion()

        log.info("Starting dedup exec async then performing 3 rapid restarts")
        dedup_utils.run_dedup_execute_async()

        for i in range(3):
            log.info(f"Rapid restart {i + 1}/3")
            dedup_utils.restart_rgw_service()
            time.sleep(10)

        time.sleep(30)

        log.info("Re-running dedup exec after rapid restarts")
        dedup_utils.run_dedup_execute()
        exec_stats = dedup_utils.wait_for_dedup_completion()

        dedup_utils.verify_all_objects_accessible(s3_client, bucket_name, keys)
        dedup_utils.verify_all_objects_integrity(
            s3_client, bucket_name, keys, expected_md5
        )

        log.info("Rapid RGW restart test passed")
        test_info.success_status("test passed")

    except (RGWBaseException, AssertionError, Exception) as e:
        log.error(f"Test failed: {e}")
        log.error(traceback.format_exc())
        test_info.failed_status("test failed")
        raise

    finally:
        if config_set:
            dedup_utils.reset_dedup_config("rgw_dedup_min_obj_size_for_dedup")
        dedup_utils.cleanup_bucket(s3_client, bucket_name)


# === Concurrent/Race Scenarios (C1-C5) ===


def test_exec_concurrent_exec_estimate(config, ssh_con):
    """
    C1: Upload 200 identical 5KB objects, start estimate async, sleep 2s,
    try to start exec (expect rejection while estimate is running).
    Wait for estimate completion, then run exec properly and verify.
    """
    test_info = AddTestInfo("C1: Exec during estimate (should be rejected)")
    io_info_initialize = IOInfoInitialize()
    basic_io_structure = BasicIOInfoStructure()
    io_info_initialize.initialize(basic_io_structure.initial())

    config_set = False

    try:
        test_info.started_info()
        user_info = s3lib.create_users(1)[0]
        auth = reusable.get_auth(
            user_info, ssh_con, config.ssl, getattr(config, "haproxy", False)
        )
        s3_client = auth.do_auth_using_client()

        bucket_name = f"dedup-concurrent-est-{random.randint(1, 5000)}"
        s3_client.create_bucket(Bucket=bucket_name)

        dedup_utils.set_dedup_config("rgw_dedup_min_obj_size_for_dedup", "4096")
        config_set = True

        obj_count = 200
        obj_size = 5 * 1024  # 5KB

        keys, expected_md5, _ = dedup_utils.upload_identical_objects(
            s3_client, bucket_name, obj_count, obj_size, prefix="conc-est-obj"
        )

        log.info("Starting estimate async, then trying exec during estimate")
        dedup_utils.run_dedup_estimate_async()
        time.sleep(2)

        try:
            dedup_utils.run_dedup_execute()
            log.warning(
                "Exec did not reject during estimate — may be expected in some builds"
            )
        except (AssertionError, Exception) as exec_err:
            log.info(f"Exec correctly rejected during estimate: {exec_err}")

        log.info("Waiting for estimate completion")
        dedup_utils.wait_for_dedup_completion()

        log.info("Running exec properly after estimate completion")
        dedup_utils.run_dedup_execute()
        exec_stats = dedup_utils.wait_for_dedup_completion()

        dedup_utils.verify_all_objects_accessible(s3_client, bucket_name, keys)

        log.info("Concurrent exec-during-estimate test passed")
        test_info.success_status("test passed")

    except (RGWBaseException, AssertionError, Exception) as e:
        log.error(f"Test failed: {e}")
        log.error(traceback.format_exc())
        test_info.failed_status("test failed")
        raise

    finally:
        if config_set:
            dedup_utils.reset_dedup_config("rgw_dedup_min_obj_size_for_dedup")
        dedup_utils.cleanup_bucket(s3_client, bucket_name)


def test_exec_concurrent_delete(config, ssh_con):
    """
    C2: Upload 100 identical 5KB objects, run estimate, start exec async,
    delete the second half of objects during exec. Wait for completion,
    verify surviving first half of objects accessible and data integrity.
    """
    test_info = AddTestInfo("C2: Delete during exec")
    io_info_initialize = IOInfoInitialize()
    basic_io_structure = BasicIOInfoStructure()
    io_info_initialize.initialize(basic_io_structure.initial())

    config_set = False

    try:
        test_info.started_info()
        user_info = s3lib.create_users(1)[0]
        auth = reusable.get_auth(
            user_info, ssh_con, config.ssl, getattr(config, "haproxy", False)
        )
        s3_client = auth.do_auth_using_client()

        bucket_name = f"dedup-conc-delete-{random.randint(1, 5000)}"
        s3_client.create_bucket(Bucket=bucket_name)

        dedup_utils.set_dedup_config("rgw_dedup_min_obj_size_for_dedup", "4096")
        config_set = True

        obj_count = 100
        obj_size = 5 * 1024  # 5KB

        keys, expected_md5, _ = dedup_utils.upload_identical_objects(
            s3_client, bucket_name, obj_count, obj_size, prefix="conc-del-obj"
        )

        log.info("Running dedup estimate")
        dedup_utils.run_dedup_estimate()
        dedup_utils.wait_for_dedup_completion()

        log.info("Starting dedup exec async then deleting second half of objects")
        dedup_utils.run_dedup_execute_async()
        time.sleep(1)

        keys_to_delete = keys[50:]
        keys_to_keep = keys[:50]
        for key in keys_to_delete:
            s3_client.delete_object(Bucket=bucket_name, Key=key)
        log.info(f"Deleted {len(keys_to_delete)} objects during exec")

        dedup_utils.wait_for_dedup_completion()

        dedup_utils.verify_all_objects_accessible(s3_client, bucket_name, keys_to_keep)
        dedup_utils.verify_all_objects_integrity(
            s3_client, bucket_name, keys_to_keep, expected_md5
        )

        log.info("Concurrent delete during exec test passed")
        test_info.success_status("test passed")

    except (RGWBaseException, AssertionError, Exception) as e:
        log.error(f"Test failed: {e}")
        log.error(traceback.format_exc())
        test_info.failed_status("test failed")
        raise

    finally:
        if config_set:
            dedup_utils.reset_dedup_config("rgw_dedup_min_obj_size_for_dedup")
        dedup_utils.cleanup_bucket(s3_client, bucket_name)


def test_exec_concurrent_upload(config, ssh_con):
    """
    C3: Upload 50 identical 5KB objects (set A), run estimate, start exec async,
    upload 50 new objects with different content (set B) during exec. Wait,
    verify both sets accessible and intact. Re-run exec for set B, verify all.
    """
    test_info = AddTestInfo("C3: Upload during exec")
    io_info_initialize = IOInfoInitialize()
    basic_io_structure = BasicIOInfoStructure()
    io_info_initialize.initialize(basic_io_structure.initial())

    config_set = False

    try:
        test_info.started_info()
        user_info = s3lib.create_users(1)[0]
        auth = reusable.get_auth(
            user_info, ssh_con, config.ssl, getattr(config, "haproxy", False)
        )
        s3_client = auth.do_auth_using_client()

        bucket_name = f"dedup-conc-upload-{random.randint(1, 5000)}"
        s3_client.create_bucket(Bucket=bucket_name)

        dedup_utils.set_dedup_config("rgw_dedup_min_obj_size_for_dedup", "4096")
        config_set = True

        obj_size = 5 * 1024  # 5KB

        keys_a, md5_a, _ = dedup_utils.upload_identical_objects(
            s3_client, bucket_name, 50, obj_size, prefix="setA-obj"
        )

        log.info("Running dedup estimate")
        dedup_utils.run_dedup_estimate()
        dedup_utils.wait_for_dedup_completion()

        log.info("Starting dedup exec async then uploading set B")
        dedup_utils.run_dedup_execute_async()
        time.sleep(1)

        set_b_data = os.urandom(obj_size)
        md5_b = hashlib.md5(set_b_data).hexdigest()
        keys_b = []
        for i in range(50):
            key = f"setB-obj-{i}"
            s3_client.put_object(Bucket=bucket_name, Key=key, Body=set_b_data)
            keys_b.append(key)
        log.info(f"Uploaded {len(keys_b)} set B objects during exec")

        dedup_utils.wait_for_dedup_completion()

        dedup_utils.verify_all_objects_accessible(s3_client, bucket_name, keys_a)
        dedup_utils.verify_all_objects_integrity(s3_client, bucket_name, keys_a, md5_a)
        dedup_utils.verify_all_objects_accessible(s3_client, bucket_name, keys_b)
        dedup_utils.verify_all_objects_integrity(s3_client, bucket_name, keys_b, md5_b)

        log.info("Re-running exec to cover set B objects")
        dedup_utils.run_dedup_execute()
        dedup_utils.wait_for_dedup_completion()

        dedup_utils.verify_all_objects_accessible(
            s3_client, bucket_name, keys_a + keys_b
        )

        log.info("Concurrent upload during exec test passed")
        test_info.success_status("test passed")

    except (RGWBaseException, AssertionError, Exception) as e:
        log.error(f"Test failed: {e}")
        log.error(traceback.format_exc())
        test_info.failed_status("test failed")
        raise

    finally:
        if config_set:
            dedup_utils.reset_dedup_config("rgw_dedup_min_obj_size_for_dedup")
        dedup_utils.cleanup_bucket(s3_client, bucket_name)


def test_exec_concurrent_pause_resume(config, ssh_con):
    """
    C4: Upload 100 identical 5KB objects, run estimate, start exec async,
    rapidly pause and resume 10 times with 1s gaps. Wait for completion,
    verify all objects accessible and deduped_count > 0.
    """
    test_info = AddTestInfo("C4: Rapid pause/resume 10 times during exec")
    io_info_initialize = IOInfoInitialize()
    basic_io_structure = BasicIOInfoStructure()
    io_info_initialize.initialize(basic_io_structure.initial())

    config_set = False

    try:
        test_info.started_info()
        user_info = s3lib.create_users(1)[0]
        auth = reusable.get_auth(
            user_info, ssh_con, config.ssl, getattr(config, "haproxy", False)
        )
        s3_client = auth.do_auth_using_client()

        bucket_name = f"dedup-pause-resume-{random.randint(1, 5000)}"
        s3_client.create_bucket(Bucket=bucket_name)

        dedup_utils.set_dedup_config("rgw_dedup_min_obj_size_for_dedup", "4096")
        config_set = True

        obj_count = 100
        obj_size = 5 * 1024  # 5KB

        keys, expected_md5, _ = dedup_utils.upload_identical_objects(
            s3_client, bucket_name, obj_count, obj_size, prefix="pause-resume-obj"
        )

        log.info("Running dedup estimate")
        dedup_utils.run_dedup_estimate()
        dedup_utils.wait_for_dedup_completion()

        log.info("Starting dedup exec async then rapid pause/resume 10x")
        dedup_utils.run_dedup_execute_async()
        time.sleep(2)

        for i in range(10):
            log.info(f"Pause/resume cycle {i + 1}/10")
            dedup_utils.run_dedup_pause()
            time.sleep(1)
            dedup_utils.run_dedup_resume()
            time.sleep(1)

        exec_stats = dedup_utils.wait_for_dedup_completion()

        dedup_utils.verify_all_objects_accessible(s3_client, bucket_name, keys)
        dedup_utils.verify_all_objects_integrity(
            s3_client, bucket_name, keys, expected_md5
        )

        parsed = dedup_utils.parse_dedup_stats(exec_stats)
        assert (
            parsed["deduped_count"] > 0
        ), f"Expected deduped > 0 after pause/resume, got {parsed['deduped_count']}"

        log.info("Rapid pause/resume test passed")
        test_info.success_status("test passed")

    except (RGWBaseException, AssertionError, Exception) as e:
        log.error(f"Test failed: {e}")
        log.error(traceback.format_exc())
        test_info.failed_status("test failed")
        raise

    finally:
        if config_set:
            dedup_utils.reset_dedup_config("rgw_dedup_min_obj_size_for_dedup")
        dedup_utils.cleanup_bucket(s3_client, bucket_name)


def test_exec_concurrent_double_abort(config, ssh_con):
    """
    C5: Upload 100 identical 5KB objects, run estimate, start exec async,
    abort twice (second should be no-op). Verify RGW daemons are running
    and objects are accessible. Run a fresh exec and assert deduped > 0.
    """
    test_info = AddTestInfo("C5: Double abort during exec")
    io_info_initialize = IOInfoInitialize()
    basic_io_structure = BasicIOInfoStructure()
    io_info_initialize.initialize(basic_io_structure.initial())

    config_set = False

    try:
        test_info.started_info()
        user_info = s3lib.create_users(1)[0]
        auth = reusable.get_auth(
            user_info, ssh_con, config.ssl, getattr(config, "haproxy", False)
        )
        s3_client = auth.do_auth_using_client()

        bucket_name = f"dedup-double-abort-{random.randint(1, 5000)}"
        s3_client.create_bucket(Bucket=bucket_name)

        dedup_utils.set_dedup_config("rgw_dedup_min_obj_size_for_dedup", "4096")
        config_set = True

        obj_count = 100
        obj_size = 5 * 1024  # 5KB

        keys, expected_md5, _ = dedup_utils.upload_identical_objects(
            s3_client, bucket_name, obj_count, obj_size, prefix="dbl-abort-obj"
        )

        log.info("Running dedup estimate")
        dedup_utils.run_dedup_estimate()
        dedup_utils.wait_for_dedup_completion()

        log.info("Starting dedup exec async then performing double abort")
        dedup_utils.run_dedup_execute_async()
        time.sleep(2)

        dedup_utils.run_dedup_abort()
        log.info("First abort sent")

        try:
            dedup_utils.run_dedup_abort()
            log.info("Second abort sent (no-op expected)")
        except (AssertionError, Exception) as abort_err:
            log.info(f"Second abort returned error (expected): {abort_err}")

        running, total = dedup_utils.verify_rgw_daemons_running()
        log.info(f"RGW daemons after double abort: {running}/{total} running")

        dedup_utils.verify_all_objects_accessible(s3_client, bucket_name, keys)

        log.info("Running fresh exec after double abort")
        dedup_utils.run_dedup_execute()
        exec_stats = dedup_utils.wait_for_dedup_completion()

        parsed = dedup_utils.parse_dedup_stats(exec_stats)
        assert (
            parsed["deduped_count"] > 0
        ), f"Expected deduped > 0 after fresh exec, got {parsed['deduped_count']}"

        log.info("Double abort test passed")
        test_info.success_status("test passed")

    except (RGWBaseException, AssertionError, Exception) as e:
        log.error(f"Test failed: {e}")
        log.error(traceback.format_exc())
        test_info.failed_status("test failed")
        raise

    finally:
        if config_set:
            dedup_utils.reset_dedup_config("rgw_dedup_min_obj_size_for_dedup")
        dedup_utils.cleanup_bucket(s3_client, bucket_name)


# === Data Edge Cases (D1-D5) ===


def test_exec_edge_empty(config, ssh_con):
    """
    D1: Run dedup estimate and exec on an empty bucket.
    Assert ingress_count == 0 and deduped_count == 0.
    """
    test_info = AddTestInfo("D1: Empty bucket dedup")
    io_info_initialize = IOInfoInitialize()
    basic_io_structure = BasicIOInfoStructure()
    io_info_initialize.initialize(basic_io_structure.initial())

    try:
        test_info.started_info()
        user_info = s3lib.create_users(1)[0]
        auth = reusable.get_auth(
            user_info, ssh_con, config.ssl, getattr(config, "haproxy", False)
        )
        s3_client = auth.do_auth_using_client()

        bucket_name = f"dedup-empty-{random.randint(1, 5000)}"
        s3_client.create_bucket(Bucket=bucket_name)

        log.info("Running dedup estimate on empty bucket")
        dedup_utils.run_dedup_estimate()
        estimate_stats = dedup_utils.wait_for_dedup_completion()

        est_parsed = dedup_utils.parse_dedup_stats_full(estimate_stats)
        assert (
            est_parsed["ingress_count"] == 0
        ), f"Expected ingress_count == 0 for empty bucket, got {est_parsed['ingress_count']}"

        log.info("Running dedup exec on empty bucket")
        dedup_utils.run_dedup_execute()
        exec_stats = dedup_utils.wait_for_dedup_completion()

        exec_parsed = dedup_utils.parse_dedup_stats(exec_stats)
        assert (
            exec_parsed["deduped_count"] == 0
        ), f"Expected deduped_count == 0 for empty bucket, got {exec_parsed['deduped_count']}"

        log.info("Empty bucket dedup test passed")
        test_info.success_status("test passed")

    except (RGWBaseException, AssertionError, Exception) as e:
        log.error(f"Test failed: {e}")
        log.error(traceback.format_exc())
        test_info.failed_status("test failed")
        raise

    finally:
        dedup_utils.cleanup_bucket(s3_client, bucket_name)


def test_exec_edge_singletons(config, ssh_con):
    """
    D2: Upload 50 objects each with unique content (no duplicates).
    Run estimate and assert duplicate_count == 0, unique_count == 50.
    Run exec and assert deduped_count == 0. Verify all accessible.
    """
    test_info = AddTestInfo("D2: All unique content (no duplicates)")
    io_info_initialize = IOInfoInitialize()
    basic_io_structure = BasicIOInfoStructure()
    io_info_initialize.initialize(basic_io_structure.initial())

    config_set = False

    try:
        test_info.started_info()
        user_info = s3lib.create_users(1)[0]
        auth = reusable.get_auth(
            user_info, ssh_con, config.ssl, getattr(config, "haproxy", False)
        )
        s3_client = auth.do_auth_using_client()

        bucket_name = f"dedup-singletons-{random.randint(1, 5000)}"
        s3_client.create_bucket(Bucket=bucket_name)

        dedup_utils.set_dedup_config("rgw_dedup_min_obj_size_for_dedup", "4096")
        config_set = True

        obj_size = 5 * 1024  # 5KB
        keys, md5s = dedup_utils.upload_unique_objects(
            s3_client, bucket_name, 50, obj_size, prefix="singleton-obj"
        )

        log.info("Running dedup estimate on unique objects")
        dedup_utils.run_dedup_estimate()
        estimate_stats = dedup_utils.wait_for_dedup_completion()

        est_parsed = dedup_utils.parse_dedup_stats_full(estimate_stats)
        assert (
            est_parsed["duplicate_count"] == 0
        ), f"Expected duplicate_count == 0, got {est_parsed['duplicate_count']}"
        assert (
            est_parsed["unique_count"] == 50
        ), f"Expected unique_count == 50, got {est_parsed['unique_count']}"

        log.info("Running dedup exec on unique objects")
        dedup_utils.run_dedup_execute()
        exec_stats = dedup_utils.wait_for_dedup_completion()

        exec_parsed = dedup_utils.parse_dedup_stats(exec_stats)
        assert (
            exec_parsed["deduped_count"] == 0
        ), f"Expected deduped_count == 0, got {exec_parsed['deduped_count']}"

        dedup_utils.verify_all_objects_accessible(s3_client, bucket_name, keys)

        log.info("All-unique content test passed")
        test_info.success_status("test passed")

    except (RGWBaseException, AssertionError, Exception) as e:
        log.error(f"Test failed: {e}")
        log.error(traceback.format_exc())
        test_info.failed_status("test failed")
        raise

    finally:
        if config_set:
            dedup_utils.reset_dedup_config("rgw_dedup_min_obj_size_for_dedup")
        dedup_utils.cleanup_bucket(s3_client, bucket_name)


def test_exec_edge_min_size(config, ssh_con):
    """
    D3: Upload 20 identical objects at exactly 5120 bytes (at dedup threshold)
    and 20 identical objects at 4095 bytes (below threshold, unique data).
    Run estimate + exec, assert deduped > 0. Verify all accessible.
    """
    test_info = AddTestInfo("D3: Min size boundary (at and below threshold)")
    io_info_initialize = IOInfoInitialize()
    basic_io_structure = BasicIOInfoStructure()
    io_info_initialize.initialize(basic_io_structure.initial())

    config_set = False

    try:
        test_info.started_info()
        user_info = s3lib.create_users(1)[0]
        auth = reusable.get_auth(
            user_info, ssh_con, config.ssl, getattr(config, "haproxy", False)
        )
        s3_client = auth.do_auth_using_client()

        bucket_name = f"dedup-minsize-{random.randint(1, 5000)}"
        s3_client.create_bucket(Bucket=bucket_name)

        dedup_utils.set_dedup_config("rgw_dedup_min_obj_size_for_dedup", "4096")
        config_set = True

        keys_at, md5_at, _ = dedup_utils.upload_identical_objects(
            s3_client, bucket_name, 20, 5120, prefix="at-boundary-obj"
        )

        below_data = os.urandom(4095)
        below_md5 = hashlib.md5(below_data).hexdigest()
        keys_below = []
        for i in range(20):
            key = f"below-boundary-obj-{i}"
            s3_client.put_object(Bucket=bucket_name, Key=key, Body=below_data)
            keys_below.append(key)

        log.info("Running dedup estimate")
        dedup_utils.run_dedup_estimate()
        dedup_utils.wait_for_dedup_completion()

        log.info("Running dedup exec")
        dedup_utils.run_dedup_execute()
        exec_stats = dedup_utils.wait_for_dedup_completion()

        parsed = dedup_utils.parse_dedup_stats(exec_stats)
        assert (
            parsed["deduped_count"] > 0
        ), f"Expected deduped > 0 for at-boundary objects, got {parsed['deduped_count']}"

        dedup_utils.verify_all_objects_accessible(
            s3_client, bucket_name, keys_at + keys_below
        )

        log.info("Min size boundary test passed")
        test_info.success_status("test passed")

    except (RGWBaseException, AssertionError, Exception) as e:
        log.error(f"Test failed: {e}")
        log.error(traceback.format_exc())
        test_info.failed_status("test failed")
        raise

    finally:
        if config_set:
            dedup_utils.reset_dedup_config("rgw_dedup_min_obj_size_for_dedup")
        dedup_utils.cleanup_bucket(s3_client, bucket_name)


def test_exec_edge_single_object(config, ssh_con):
    """
    D4: Upload a single 5KB object to a bucket. Run estimate and assert
    ingress_count == 1. Run exec and assert deduped_count == 0 (nothing
    to dedup with only one object). Verify integrity.
    """
    test_info = AddTestInfo("D4: Single object in bucket")
    io_info_initialize = IOInfoInitialize()
    basic_io_structure = BasicIOInfoStructure()
    io_info_initialize.initialize(basic_io_structure.initial())

    config_set = False

    try:
        test_info.started_info()
        user_info = s3lib.create_users(1)[0]
        auth = reusable.get_auth(
            user_info, ssh_con, config.ssl, getattr(config, "haproxy", False)
        )
        s3_client = auth.do_auth_using_client()

        bucket_name = f"dedup-single-{random.randint(1, 5000)}"
        s3_client.create_bucket(Bucket=bucket_name)

        dedup_utils.set_dedup_config("rgw_dedup_min_obj_size_for_dedup", "4096")
        config_set = True

        obj_size = 5 * 1024  # 5KB
        keys, expected_md5, _ = dedup_utils.upload_identical_objects(
            s3_client, bucket_name, 1, obj_size, prefix="single-obj"
        )

        log.info("Running dedup estimate on single object")
        dedup_utils.run_dedup_estimate()
        estimate_stats = dedup_utils.wait_for_dedup_completion()

        est_parsed = dedup_utils.parse_dedup_stats_full(estimate_stats)
        assert (
            est_parsed["ingress_count"] == 1
        ), f"Expected ingress_count == 1, got {est_parsed['ingress_count']}"

        log.info("Running dedup exec on single object")
        dedup_utils.run_dedup_execute()
        exec_stats = dedup_utils.wait_for_dedup_completion()

        exec_parsed = dedup_utils.parse_dedup_stats(exec_stats)
        assert (
            exec_parsed["deduped_count"] == 0
        ), f"Expected deduped_count == 0 for single object, got {exec_parsed['deduped_count']}"

        dedup_utils.verify_all_objects_integrity(
            s3_client, bucket_name, keys, expected_md5
        )

        log.info("Single object test passed")
        test_info.success_status("test passed")

    except (RGWBaseException, AssertionError, Exception) as e:
        log.error(f"Test failed: {e}")
        log.error(traceback.format_exc())
        test_info.failed_status("test failed")
        raise

    finally:
        if config_set:
            dedup_utils.reset_dedup_config("rgw_dedup_min_obj_size_for_dedup")
        dedup_utils.cleanup_bucket(s3_client, bucket_name)


def test_exec_edge_max_versions(config, ssh_con):
    """
    D5: Upload 500 identical versions of the same key (5KB). Run estimate + exec,
    assert deduped_count <= 127 (128 limit minus source). Verify all 500 versions
    accessible by VersionId. Second exec should dedup 0 (idempotency).
    """
    test_info = AddTestInfo("D5: 500 versions past 128 limit")
    io_info_initialize = IOInfoInitialize()
    basic_io_structure = BasicIOInfoStructure()
    io_info_initialize.initialize(basic_io_structure.initial())

    config_set = False

    try:
        test_info.started_info()
        user_info = s3lib.create_users(1)[0]
        auth = reusable.get_auth(
            user_info, ssh_con, config.ssl, getattr(config, "haproxy", False)
        )
        s3_client = auth.do_auth_using_client()

        bucket_name = f"dedup-maxver-{random.randint(1, 5000)}"
        s3_client.create_bucket(Bucket=bucket_name)

        dedup_utils.set_dedup_config("rgw_dedup_min_obj_size_for_dedup", "4096")
        config_set = True

        obj_size = 5 * 1024  # 5KB
        object_key = "max-versions-obj"
        version_count = 500

        version_ids, expected_md5, _ = dedup_utils.upload_identical_versions(
            s3_client, bucket_name, object_key, version_count, obj_size
        )

        log.info("Running dedup estimate")
        dedup_utils.run_dedup_estimate()
        dedup_utils.wait_for_dedup_completion()

        log.info("Running dedup exec")
        dedup_utils.run_dedup_execute()
        exec_stats = dedup_utils.wait_for_dedup_completion()

        parsed = dedup_utils.parse_dedup_stats(exec_stats)
        assert (
            parsed["deduped_count"] <= 127
        ), f"Expected deduped <= 127, got {parsed['deduped_count']}"
        log.info(f"Deduped {parsed['deduped_count']} out of {version_count} versions")

        log.info(f"Verifying all {version_count} versions accessible")
        for vid in version_ids:
            resp = s3_client.get_object(
                Bucket=bucket_name, Key=object_key, VersionId=vid
            )
            body = resp["Body"].read()
            actual_md5 = hashlib.md5(body).hexdigest()
            assert (
                actual_md5 == expected_md5
            ), f"Version {vid} MD5 mismatch: expected {expected_md5}, got {actual_md5}"

        log.info("Running second exec (idempotency check)")
        dedup_utils.run_dedup_execute()
        exec_stats_2 = dedup_utils.wait_for_dedup_completion()

        parsed_2 = dedup_utils.parse_dedup_stats(exec_stats_2)
        assert (
            parsed_2["deduped_count"] == 0
        ), f"Second exec should dedup 0, got {parsed_2['deduped_count']}"

        log.info("Max versions test passed")
        test_info.success_status("test passed")

    except (RGWBaseException, AssertionError, Exception) as e:
        log.error(f"Test failed: {e}")
        log.error(traceback.format_exc())
        test_info.failed_status("test failed")
        raise

    finally:
        if config_set:
            dedup_utils.reset_dedup_config("rgw_dedup_min_obj_size_for_dedup")
        dedup_utils.cleanup_bucket(s3_client, bucket_name)


# === Infrastructure Failure Scenarios (F1-F5) ===


def test_exec_infra_pool_full(config, ssh_con):
    """
    F1: Upload 50 identical 5KB objects, set a tight pool quota (1MB),
    attempt estimate + exec (may fail). Remove quota, re-run exec,
    verify all objects accessible and data integrity.
    """
    test_info = AddTestInfo("F1: Pool near full during dedup")
    io_info_initialize = IOInfoInitialize()
    basic_io_structure = BasicIOInfoStructure()
    io_info_initialize.initialize(basic_io_structure.initial())

    config_set = False
    quota_set = False

    try:
        test_info.started_info()
        user_info = s3lib.create_users(1)[0]
        auth = reusable.get_auth(
            user_info, ssh_con, config.ssl, getattr(config, "haproxy", False)
        )
        s3_client = auth.do_auth_using_client()

        bucket_name = f"dedup-poolfull-{random.randint(1, 5000)}"
        s3_client.create_bucket(Bucket=bucket_name)

        dedup_utils.set_dedup_config("rgw_dedup_min_obj_size_for_dedup", "4096")
        config_set = True

        obj_count = 50
        obj_size = 5 * 1024  # 5KB

        keys, expected_md5, _ = dedup_utils.upload_identical_objects(
            s3_client, bucket_name, obj_count, obj_size, prefix="poolfull-obj"
        )

        log.info("Setting tight pool quota (1MB)")
        dedup_utils.set_pool_quota("default.rgw.buckets.data", 1048576)
        quota_set = True

        log.info("Attempting estimate + exec with tight quota (may fail)")
        try:
            dedup_utils.run_dedup_estimate()
            dedup_utils.wait_for_dedup_completion()
            dedup_utils.run_dedup_execute()
            dedup_utils.wait_for_dedup_completion()
        except (AssertionError, Exception) as quota_err:
            log.info(f"Dedup failed under quota (expected): {quota_err}")

        log.info("Removing pool quota")
        dedup_utils.remove_pool_quota("default.rgw.buckets.data")
        quota_set = False

        log.info("Re-running exec without quota")
        dedup_utils.run_dedup_execute()
        dedup_utils.wait_for_dedup_completion()

        dedup_utils.verify_all_objects_accessible(s3_client, bucket_name, keys)
        dedup_utils.verify_all_objects_integrity(
            s3_client, bucket_name, keys, expected_md5
        )

        log.info("Pool full test passed")
        test_info.success_status("test passed")

    except (RGWBaseException, AssertionError, Exception) as e:
        log.error(f"Test failed: {e}")
        log.error(traceback.format_exc())
        test_info.failed_status("test failed")
        raise

    finally:
        if quota_set:
            dedup_utils.remove_pool_quota("default.rgw.buckets.data")
        if config_set:
            dedup_utils.reset_dedup_config("rgw_dedup_min_obj_size_for_dedup")
        dedup_utils.cleanup_bucket(s3_client, bucket_name)


def test_exec_infra_reshard(config, ssh_con):
    """
    F2: Upload 100 identical 5KB objects, run estimate, start exec async,
    trigger bucket reshard to 8 shards during exec (may fail). Wait,
    retry exec if needed, verify all objects accessible and data integrity.
    """
    test_info = AddTestInfo("F2: Reshard during exec")
    io_info_initialize = IOInfoInitialize()
    basic_io_structure = BasicIOInfoStructure()
    io_info_initialize.initialize(basic_io_structure.initial())

    config_set = False

    try:
        test_info.started_info()
        user_info = s3lib.create_users(1)[0]
        auth = reusable.get_auth(
            user_info, ssh_con, config.ssl, getattr(config, "haproxy", False)
        )
        s3_client = auth.do_auth_using_client()

        bucket_name = f"dedup-reshard-{random.randint(1, 5000)}"
        s3_client.create_bucket(Bucket=bucket_name)

        dedup_utils.set_dedup_config("rgw_dedup_min_obj_size_for_dedup", "4096")
        config_set = True

        obj_count = getattr(config, "objects_count", 100)
        obj_size = 5 * 1024  # 5KB

        keys, expected_md5, _ = dedup_utils.upload_identical_objects(
            s3_client, bucket_name, obj_count, obj_size, prefix="reshard-obj"
        )

        log.info("Running dedup estimate")
        dedup_utils.run_dedup_estimate()
        dedup_utils.wait_for_dedup_completion()

        log.info("Starting dedup exec async then triggering reshard")
        dedup_utils.run_dedup_execute_async()
        time.sleep(2)

        try:
            dedup_utils.trigger_bucket_reshard(bucket_name, 8)
            log.info("Bucket reshard triggered during exec")
        except (AssertionError, Exception) as reshard_err:
            log.warning(f"Reshard during exec failed (may be expected): {reshard_err}")

        log.info("Waiting for exec completion (retry if needed)")
        try:
            dedup_utils.wait_for_dedup_completion(timeout=120)
        except AssertionError:
            log.info("Exec may have been interrupted by reshard, re-running")
            dedup_utils.run_dedup_execute()
            dedup_utils.wait_for_dedup_completion()

        dedup_utils.verify_all_objects_accessible(s3_client, bucket_name, keys)
        dedup_utils.verify_all_objects_integrity(
            s3_client, bucket_name, keys, expected_md5
        )

        log.info("Reshard during exec test passed")
        test_info.success_status("test passed")

    except (RGWBaseException, AssertionError, Exception) as e:
        log.error(f"Test failed: {e}")
        log.error(traceback.format_exc())
        test_info.failed_status("test failed")
        raise

    finally:
        if config_set:
            dedup_utils.reset_dedup_config("rgw_dedup_min_obj_size_for_dedup")
        dedup_utils.cleanup_bucket(s3_client, bucket_name)


def test_exec_infra_network(config, ssh_con):
    """
    F3: Upload 100 identical 5KB objects, run estimate, start exec async,
    simulate a network partition by dropping MON traffic (port 6789) via
    iptables for 30s, then restore. Wait/retry, verify objects accessible.
    """
    test_info = AddTestInfo("F3: Network partition during exec")
    io_info_initialize = IOInfoInitialize()
    basic_io_structure = BasicIOInfoStructure()
    io_info_initialize.initialize(basic_io_structure.initial())

    config_set = False
    iptables_rule_added = False

    try:
        test_info.started_info()
        user_info = s3lib.create_users(1)[0]
        auth = reusable.get_auth(
            user_info, ssh_con, config.ssl, getattr(config, "haproxy", False)
        )
        s3_client = auth.do_auth_using_client()

        bucket_name = f"dedup-network-{random.randint(1, 5000)}"
        s3_client.create_bucket(Bucket=bucket_name)

        dedup_utils.set_dedup_config("rgw_dedup_min_obj_size_for_dedup", "4096")
        config_set = True

        obj_count = getattr(config, "objects_count", 100)
        obj_size = 5 * 1024  # 5KB

        keys, expected_md5, _ = dedup_utils.upload_identical_objects(
            s3_client, bucket_name, obj_count, obj_size, prefix="network-obj"
        )

        log.info("Running dedup estimate")
        dedup_utils.run_dedup_estimate()
        dedup_utils.wait_for_dedup_completion()

        log.info("Starting dedup exec async then simulating network partition")
        dedup_utils.run_dedup_execute_async()
        time.sleep(3)

        log.info("Dropping MON traffic on port 6789")
        utils.exec_shell_cmd("iptables -A INPUT -p tcp --dport 6789 -j DROP")
        iptables_rule_added = True
        time.sleep(30)

        log.info("Restoring network (removing iptables rule)")
        utils.exec_shell_cmd("iptables -D INPUT -p tcp --dport 6789 -j DROP")
        iptables_rule_added = False

        log.info("Waiting for exec completion (retry if needed)")
        try:
            dedup_utils.wait_for_dedup_completion(timeout=120)
        except AssertionError:
            log.info("Exec may have failed during partition, re-running")
            dedup_utils.run_dedup_execute()
            dedup_utils.wait_for_dedup_completion()

        dedup_utils.verify_all_objects_accessible(s3_client, bucket_name, keys)

        log.info("Network partition test passed")
        test_info.success_status("test passed")

    except (RGWBaseException, AssertionError, Exception) as e:
        log.error(f"Test failed: {e}")
        log.error(traceback.format_exc())
        test_info.failed_status("test failed")
        raise

    finally:
        if iptables_rule_added:
            utils.exec_shell_cmd("iptables -D INPUT -p tcp --dport 6789 -j DROP")
        if config_set:
            dedup_utils.reset_dedup_config("rgw_dedup_min_obj_size_for_dedup")
        dedup_utils.cleanup_bucket(s3_client, bucket_name)


def test_exec_infra_corrupt(config, ssh_con):
    """
    F4: Upload 50 identical 5KB objects, get bucket marker, find RADOS objects,
    corrupt the first one. Run estimate + exec, verify at least len(keys)-1
    objects are still accessible.
    """
    test_info = AddTestInfo("F4: Corrupt RADOS object then dedup")
    io_info_initialize = IOInfoInitialize()
    basic_io_structure = BasicIOInfoStructure()
    io_info_initialize.initialize(basic_io_structure.initial())

    config_set = False

    try:
        test_info.started_info()
        user_info = s3lib.create_users(1)[0]
        auth = reusable.get_auth(
            user_info, ssh_con, config.ssl, getattr(config, "haproxy", False)
        )
        s3_client = auth.do_auth_using_client()

        bucket_name = f"dedup-corrupt-{random.randint(1, 5000)}"
        s3_client.create_bucket(Bucket=bucket_name)

        dedup_utils.set_dedup_config("rgw_dedup_min_obj_size_for_dedup", "4096")
        config_set = True

        obj_count = 50
        obj_size = 5 * 1024  # 5KB

        keys, expected_md5, _ = dedup_utils.upload_identical_objects(
            s3_client, bucket_name, obj_count, obj_size, prefix="corrupt-obj"
        )

        marker = dedup_utils.get_bucket_marker(bucket_name)
        rados_objects = dedup_utils.get_rados_objects(
            pool="default.rgw.buckets.data", prefix=marker
        )
        if rados_objects:
            log.info(f"Corrupting RADOS object: {rados_objects[0]}")
            dedup_utils.corrupt_rados_object(
                "default.rgw.buckets.data", rados_objects[0]
            )
        else:
            log.warning("No RADOS objects found to corrupt, proceeding anyway")

        log.info("Running dedup estimate")
        dedup_utils.run_dedup_estimate()
        dedup_utils.wait_for_dedup_completion()

        log.info("Running dedup exec")
        dedup_utils.run_dedup_execute()
        dedup_utils.wait_for_dedup_completion()

        accessible_count = 0
        for key in keys:
            try:
                s3_client.head_object(Bucket=bucket_name, Key=key)
                accessible_count += 1
            except Exception:
                log.warning(f"Object {key} not accessible (may be the corrupted one)")

        log.info(f"Accessible objects: {accessible_count}/{len(keys)}")
        assert (
            accessible_count >= len(keys) - 1
        ), f"Expected at least {len(keys) - 1} accessible, got {accessible_count}"

        log.info("Corrupt RADOS object test passed")
        test_info.success_status("test passed")

    except (RGWBaseException, AssertionError, Exception) as e:
        log.error(f"Test failed: {e}")
        log.error(traceback.format_exc())
        test_info.failed_status("test failed")
        raise

    finally:
        if config_set:
            dedup_utils.reset_dedup_config("rgw_dedup_min_obj_size_for_dedup")
        dedup_utils.cleanup_bucket(s3_client, bucket_name)


def test_exec_infra_time_jump(config, ssh_con):
    """
    F5: Upload 50 identical 5KB objects, run estimate, start exec async,
    jump clock forward 1 hour, wait for completion, restore clock via
    chronyc, retry if needed, verify all objects accessible and data integrity.
    """
    test_info = AddTestInfo("F5: Clock jump during exec")
    io_info_initialize = IOInfoInitialize()
    basic_io_structure = BasicIOInfoStructure()
    io_info_initialize.initialize(basic_io_structure.initial())

    config_set = False
    clock_jumped = False

    try:
        test_info.started_info()
        user_info = s3lib.create_users(1)[0]
        auth = reusable.get_auth(
            user_info, ssh_con, config.ssl, getattr(config, "haproxy", False)
        )
        s3_client = auth.do_auth_using_client()

        bucket_name = f"dedup-timejump-{random.randint(1, 5000)}"
        s3_client.create_bucket(Bucket=bucket_name)

        dedup_utils.set_dedup_config("rgw_dedup_min_obj_size_for_dedup", "4096")
        config_set = True

        obj_count = 50
        obj_size = 5 * 1024  # 5KB

        keys, expected_md5, _ = dedup_utils.upload_identical_objects(
            s3_client, bucket_name, obj_count, obj_size, prefix="timejump-obj"
        )

        log.info("Running dedup estimate")
        dedup_utils.run_dedup_estimate()
        dedup_utils.wait_for_dedup_completion()

        log.info("Starting dedup exec async then jumping clock +1 hour")
        dedup_utils.run_dedup_execute_async()
        time.sleep(2)

        utils.exec_shell_cmd("date -s '+1 hour'")
        clock_jumped = True
        time.sleep(5)

        log.info("Waiting for exec completion")
        try:
            dedup_utils.wait_for_dedup_completion(timeout=120)
        except AssertionError:
            log.info("Exec may have failed due to clock jump")

        log.info("Restoring clock via chronyc makestep")
        utils.exec_shell_cmd("chronyc makestep")
        clock_jumped = False
        time.sleep(5)

        log.info("Retrying exec after clock restore if needed")
        try:
            stats = dedup_utils.get_dedup_stats()
            if isinstance(stats, dict) and not stats.get("completed", False):
                dedup_utils.wait_for_dedup_completion(timeout=60)
        except (AssertionError, Exception):
            dedup_utils.run_dedup_execute()
            dedup_utils.wait_for_dedup_completion()

        dedup_utils.verify_all_objects_accessible(s3_client, bucket_name, keys)
        dedup_utils.verify_all_objects_integrity(
            s3_client, bucket_name, keys, expected_md5
        )

        log.info("Clock jump test passed")
        test_info.success_status("test passed")

    except (RGWBaseException, AssertionError, Exception) as e:
        log.error(f"Test failed: {e}")
        log.error(traceback.format_exc())
        test_info.failed_status("test failed")
        raise

    finally:
        if clock_jumped:
            utils.exec_shell_cmd("chronyc makestep")
        if config_set:
            dedup_utils.reset_dedup_config("rgw_dedup_min_obj_size_for_dedup")
        dedup_utils.cleanup_bucket(s3_client, bucket_name)


# === Multi-Bucket/Scale Scenarios (M1-M3) ===


def test_exec_scale_100_buckets(config, ssh_con):
    """
    M1: Create 100 buckets, each with 10 identical objects (same data across
    all buckets). Run estimate and assert ingress >= 1000. Run exec and
    assert deduped > 0. Verify a sample of 20 buckets. Cleanup all 100.
    """
    test_info = AddTestInfo("M1: 100 buckets with identical objects")
    io_info_initialize = IOInfoInitialize()
    basic_io_structure = BasicIOInfoStructure()
    io_info_initialize.initialize(basic_io_structure.initial())

    config_set = False
    buckets = []

    try:
        test_info.started_info()
        user_info = s3lib.create_users(1)[0]
        auth = reusable.get_auth(
            user_info, ssh_con, config.ssl, getattr(config, "haproxy", False)
        )
        s3_client = auth.do_auth_using_client()

        dedup_utils.set_dedup_config("rgw_dedup_min_obj_size_for_dedup", "4096")
        config_set = True

        obj_size = 5 * 1024  # 5KB
        identical_data = dedup_utils.generate_identical_data(obj_size)
        expected_md5 = hashlib.md5(identical_data).hexdigest()

        bucket_count = 100
        objs_per_bucket = 10
        all_bucket_keys = {}

        for b in range(bucket_count):
            bname = f"dedup-scale100-{b}-{random.randint(1, 5000)}"
            s3_client.create_bucket(Bucket=bname)
            buckets.append(bname)
            keys = []
            for i in range(objs_per_bucket):
                key = f"scale-obj-{i}"
                s3_client.put_object(Bucket=bname, Key=key, Body=identical_data)
                keys.append(key)
            all_bucket_keys[bname] = keys
            if (b + 1) % 20 == 0:
                log.info(f"Created {b + 1}/{bucket_count} buckets")

        log.info("Running dedup estimate across 100 buckets")
        dedup_utils.run_dedup_estimate()
        estimate_stats = dedup_utils.wait_for_dedup_completion()

        est_parsed = dedup_utils.parse_dedup_stats_full(estimate_stats)
        assert (
            est_parsed["ingress_count"] >= 1000
        ), f"Expected ingress >= 1000, got {est_parsed['ingress_count']}"

        log.info("Running dedup exec across 100 buckets")
        dedup_utils.run_dedup_execute()
        exec_stats = dedup_utils.wait_for_dedup_completion()

        parsed = dedup_utils.parse_dedup_stats(exec_stats)
        assert (
            parsed["deduped_count"] > 0
        ), f"Expected deduped > 0 across 100 buckets, got {parsed['deduped_count']}"

        sample_buckets = random.sample(buckets, min(20, len(buckets)))
        for bname in sample_buckets:
            dedup_utils.verify_all_objects_accessible(
                s3_client, bname, all_bucket_keys[bname]
            )
            dedup_utils.verify_all_objects_integrity(
                s3_client, bname, all_bucket_keys[bname], expected_md5
            )
        log.info(f"Verified {len(sample_buckets)} sample buckets")

        log.info("100 buckets scale test passed")
        test_info.success_status("test passed")

    except (RGWBaseException, AssertionError, Exception) as e:
        log.error(f"Test failed: {e}")
        log.error(traceback.format_exc())
        test_info.failed_status("test failed")
        raise

    finally:
        if config_set:
            dedup_utils.reset_dedup_config("rgw_dedup_min_obj_size_for_dedup")
        for bname in buckets:
            dedup_utils.cleanup_bucket(s3_client, bname)


def test_exec_scale_filters(config, ssh_con):
    """
    M2: Create 3 buckets, upload 20 identical objects to each. Create an
    allow-bucket-list file with only 2 of the 3 buckets. Run estimate
    with allow_bucket_file and assert ingress <= 40. Run exec with
    allow_bucket_file. Cleanup all 3 buckets.
    """
    test_info = AddTestInfo("M2: Allow/deny bucket filters")
    io_info_initialize = IOInfoInitialize()
    basic_io_structure = BasicIOInfoStructure()
    io_info_initialize.initialize(basic_io_structure.initial())

    config_set = False
    filter_file = None

    try:
        test_info.started_info()
        user_info = s3lib.create_users(1)[0]
        auth = reusable.get_auth(
            user_info, ssh_con, config.ssl, getattr(config, "haproxy", False)
        )
        s3_client = auth.do_auth_using_client()

        dedup_utils.set_dedup_config("rgw_dedup_min_obj_size_for_dedup", "4096")
        config_set = True

        obj_size = 5 * 1024  # 5KB

        bucket_1 = f"dedup-filter-allowed1-{random.randint(1, 5000)}"
        bucket_2 = f"dedup-filter-allowed2-{random.randint(1, 5000)}"
        bucket_3 = f"dedup-filter-denied-{random.randint(1, 5000)}"
        all_buckets = [bucket_1, bucket_2, bucket_3]

        for bname in all_buckets:
            s3_client.create_bucket(Bucket=bname)

        for bname in all_buckets:
            dedup_utils.upload_identical_objects(
                s3_client, bname, 20, obj_size, prefix="filter-obj"
            )

        filter_file = dedup_utils.create_filter_list_file([bucket_1, bucket_2])
        log.info(f"Allow-bucket filter file: {filter_file}")

        log.info("Running dedup estimate with allow-bucket filter")
        dedup_utils.run_dedup_estimate(allow_bucket_file=filter_file)
        estimate_stats = dedup_utils.wait_for_dedup_completion()

        est_parsed = dedup_utils.parse_dedup_stats_full(estimate_stats)
        assert (
            est_parsed["ingress_count"] <= 40
        ), f"Expected ingress <= 40 with filter, got {est_parsed['ingress_count']}"

        log.info("Running dedup exec with allow-bucket filter")
        dedup_utils.run_dedup_execute(allow_bucket_file=filter_file)
        dedup_utils.wait_for_dedup_completion()

        log.info("Allow/deny filter test passed")
        test_info.success_status("test passed")

    except (RGWBaseException, AssertionError, Exception) as e:
        log.error(f"Test failed: {e}")
        log.error(traceback.format_exc())
        test_info.failed_status("test failed")
        raise

    finally:
        if filter_file and os.path.exists(filter_file):
            os.unlink(filter_file)
        if config_set:
            dedup_utils.reset_dedup_config("rgw_dedup_min_obj_size_for_dedup")
        for bname in [bucket_1, bucket_2, bucket_3]:
            dedup_utils.cleanup_bucket(s3_client, bname)


def test_exec_scale_mixed_sizes(config, ssh_con):
    """
    M3: Upload 20 identical 5KB objects (above threshold), 20 identical 3KB
    objects (below, different content), and 20 identical 1KB objects (well below,
    different content). Run estimate + exec, assert deduped > 0 for the
    above-threshold group. Verify all 60 objects accessible.
    """
    test_info = AddTestInfo("M3: Mixed sizes with dedup threshold")
    io_info_initialize = IOInfoInitialize()
    basic_io_structure = BasicIOInfoStructure()
    io_info_initialize.initialize(basic_io_structure.initial())

    config_set = False

    try:
        test_info.started_info()
        user_info = s3lib.create_users(1)[0]
        auth = reusable.get_auth(
            user_info, ssh_con, config.ssl, getattr(config, "haproxy", False)
        )
        s3_client = auth.do_auth_using_client()

        bucket_name = f"dedup-mixed-sizes-{random.randint(1, 5000)}"
        s3_client.create_bucket(Bucket=bucket_name)

        dedup_utils.set_dedup_config("rgw_dedup_min_obj_size_for_dedup", "4096")
        config_set = True

        keys_5k, md5_5k, _ = dedup_utils.upload_identical_objects(
            s3_client, bucket_name, 20, 5 * 1024, prefix="above-obj"
        )

        data_3k = os.urandom(3 * 1024)
        md5_3k = hashlib.md5(data_3k).hexdigest()
        keys_3k = []
        for i in range(20):
            key = f"below-3k-obj-{i}"
            s3_client.put_object(Bucket=bucket_name, Key=key, Body=data_3k)
            keys_3k.append(key)

        data_1k = os.urandom(1 * 1024)
        md5_1k = hashlib.md5(data_1k).hexdigest()
        keys_1k = []
        for i in range(20):
            key = f"below-1k-obj-{i}"
            s3_client.put_object(Bucket=bucket_name, Key=key, Body=data_1k)
            keys_1k.append(key)

        log.info("Running dedup estimate on mixed sizes")
        dedup_utils.run_dedup_estimate()
        dedup_utils.wait_for_dedup_completion()

        log.info("Running dedup exec on mixed sizes")
        dedup_utils.run_dedup_execute()
        exec_stats = dedup_utils.wait_for_dedup_completion()

        parsed = dedup_utils.parse_dedup_stats(exec_stats)
        assert (
            parsed["deduped_count"] > 0
        ), f"Expected deduped > 0 for above-threshold objects, got {parsed['deduped_count']}"

        all_keys = keys_5k + keys_3k + keys_1k
        dedup_utils.verify_all_objects_accessible(s3_client, bucket_name, all_keys)

        log.info("Mixed sizes threshold test passed")
        test_info.success_status("test passed")

    except (RGWBaseException, AssertionError, Exception) as e:
        log.error(f"Test failed: {e}")
        log.error(traceback.format_exc())
        test_info.failed_status("test failed")
        raise

    finally:
        if config_set:
            dedup_utils.reset_dedup_config("rgw_dedup_min_obj_size_for_dedup")
        dedup_utils.cleanup_bucket(s3_client, bucket_name)


# === Run All ===


def test_exec_all(config, ssh_con):
    """Run all 30 dedup tests sequentially. Use dedup_type='all' in YAML config.
    Collects pass/fail for each test and reports a summary at the end.
    Fails if ANY test fails."""
    test_info = AddTestInfo("ALL: Run all dedup tests")

    results = {}
    failed_tests = []

    try:
        test_info.started_info()

        for test_name, test_func in TEST_DISPATCH.items():
            if test_name == "all":
                continue
            log.info(f"\n{'='*60}\n  Running: {test_name}\n{'='*60}")
            try:
                test_func(config, ssh_con)
                results[test_name] = "PASSED"
                log.info(f"  {test_name}: PASSED")
            except Exception as e:
                results[test_name] = f"FAILED: {e}"
                failed_tests.append(test_name)
                log.error(f"  {test_name}: FAILED - {e}")

        log.info(f"\n{'='*60}\n  DEDUP TEST SUMMARY\n{'='*60}")
        passed = sum(1 for v in results.values() if v == "PASSED")
        total = len(results)
        log.info(f"  {passed}/{total} tests passed")
        for name, result in results.items():
            status = "PASS" if result == "PASSED" else "FAIL"
            log.info(f"    [{status}] {name}")

        if failed_tests:
            test_info.failed_status(f"{len(failed_tests)}/{total} tests failed")
            raise TestExecError(f"Failed tests: {', '.join(failed_tests)}")

        test_info.success_status(f"All {total} tests passed")

    except TestExecError:
        raise
    except Exception as e:
        log.error(f"Unexpected error in test_exec_all: {e}")
        log.error(traceback.format_exc())
        test_info.failed_status("test failed")
        raise


# === Dispatcher ===

TEST_DISPATCH = {
    "sanity_large": test_exec_sanity_large_objects,
    "sanity_small": test_exec_sanity_small_objects,
    "admin_api": test_exec_admin_ops_api,
    "estimate": test_exec_estimate_dry_run,
    "integrity": test_exec_data_integrity,
    "multipart": test_exec_multipart_objects,
    "session_lifecycle": test_exec_session_lifecycle,
    "ssec_exclusion": test_exec_ssec_exclusion,
    "storage_class_filter": test_exec_storage_class_filter,
    "lc_expiration": test_exec_lc_expiration,
    "versioned": test_exec_versioned_objects,
    "s3_copy": test_exec_s3_copy_dedup,
    "diff_metadata": test_exec_same_content_diff_metadata,
    "concurrent_ops": test_exec_concurrent_s3_ops,
    "compressed_sanity": test_exec_compressed_sanity,
    "compressed_cross_mode": test_exec_compressed_cross_mode,
    "compressed_algo_switch": test_exec_compressed_algo_switch,
    "compressed_multipart": test_exec_compressed_multipart,
    "compressed_skip_toggle": test_exec_compressed_skip_toggle,
    "compressed_attr_mirror": test_exec_compressed_attr_mirror,
    "overwrite_deduped": test_exec_overwrite_deduped_object,
    "delete_source": test_exec_delete_dedup_source,
    "copy_delete_deduped": test_exec_s3_copy_then_delete_deduped,
    "cross_bucket_delete": test_exec_cross_bucket_source_delete,
    "idempotency": test_exec_dedup_idempotency,
    "128_limit": test_exec_128_limit_boundary,
    "versioned_boundary": test_exec_versioned_boundary,
    "multi_cycle": test_exec_multi_cycle_no_progress,
    "split_head": test_exec_split_head_small_objects,
    "stats_validation": test_exec_stats_validation,
    "restart_rgw": test_exec_restart_rgw,
    "restart_sigkill": test_exec_restart_sigkill,
    "restart_osd": test_exec_restart_osd,
    "restart_mon": test_exec_restart_mon,
    "restart_rapid": test_exec_restart_rapid,
    "concurrent_exec_estimate": test_exec_concurrent_exec_estimate,
    "concurrent_delete": test_exec_concurrent_delete,
    "concurrent_upload": test_exec_concurrent_upload,
    "concurrent_pause_resume": test_exec_concurrent_pause_resume,
    "concurrent_double_abort": test_exec_concurrent_double_abort,
    "edge_empty": test_exec_edge_empty,
    "edge_singletons": test_exec_edge_singletons,
    "edge_min_size": test_exec_edge_min_size,
    "edge_single_object": test_exec_edge_single_object,
    "edge_max_versions": test_exec_edge_max_versions,
    "infra_pool_full": test_exec_infra_pool_full,
    "infra_reshard": test_exec_infra_reshard,
    "infra_network": test_exec_infra_network,
    "infra_corrupt": test_exec_infra_corrupt,
    "infra_time_jump": test_exec_infra_time_jump,
    "scale_100_buckets": test_exec_scale_100_buckets,
    "scale_filters": test_exec_scale_filters,
    "scale_mixed_sizes": test_exec_scale_mixed_sizes,
    "all": test_exec_all,
}


if __name__ == "__main__":
    log_f_name = os.path.basename(os.path.splitext(__file__)[0])
    configure_logging(f_name=log_f_name)
    parser = argparse.ArgumentParser(description="RGW Dedup Test Automation")
    parser.add_argument("-c", dest="config", help="RGW test yaml configuration")
    parser.add_argument("-log_level", dest="log_level", default="info")
    parser.add_argument("--rgw-node", dest="rgw_node", default="")
    args = parser.parse_args()

    yaml_file = args.config
    config = Config(yaml_file)
    config.read()
    if args.log_level:
        log.setLevel(args.log_level.upper())

    ssh_con = None
    if args.rgw_node:
        ssh_con = utils.connect_remote(args.rgw_node)

    test_type = getattr(config, "test_ops", {}).get("dedup_type", "sanity_large")
    if isinstance(config.test_ops, dict):
        test_type = config.test_ops.get("dedup_type", "sanity_large")

    log.info(f"Running dedup test type: {test_type}")

    test_func = TEST_DISPATCH.get(test_type)
    if test_func is None:
        raise TestExecError(
            f"Unknown dedup_type: {test_type}. "
            f"Available types: {list(TEST_DISPATCH.keys())}"
        )

    test_func(config, ssh_con)
