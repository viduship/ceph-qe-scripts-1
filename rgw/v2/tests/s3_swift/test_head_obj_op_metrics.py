"""Validate HEAD Object op metrics (ISCE-3042).

Usage: test_head_obj_op_metrics.py -c <input_yaml>

<input_yaml>
        test_head_obj_op_metrics.yaml

Operation:
        Enable user/bucket counter caches
        Create user, bucket, and object
        Issue successful HEAD Object calls
        Assert head_obj_ops / head_obj_lat in counter dump at
        gateway, user, and bucket scope
        Verify metrics appear via ceph-exporter and Prometheus
"""

import os
import sys

sys.path.append(os.path.abspath(os.path.join(__file__, "../../../..")))
import argparse
import logging
import time
import traceback

import v2.lib.resource_op as s3lib
import v2.utils.utils as utils
from v2.lib.exceptions import RGWBaseException, TestExecError
from v2.lib.resource_op import Config
from v2.lib.rgw_config_opts import CephConfOp, ConfigOpts
from v2.lib.s3.write_io_info import BasicIOInfoStructure, IOInfoInitialize
from v2.tests.s3_swift import reusable
from v2.tests.s3cmd import reusable as s3cmd_reusable
from v2.utils.log import configure_logging
from v2.utils.test_desc import AddTestInfo
from v2.utils.utils import RGWService

log = logging.getLogger()
TEST_DATA_PATH = None


def _enable_counter_caches(config, ceph_conf, rgw_service, ssh_con):
    """Enable per-user / per-bucket op metric caches and restart RGW if needed."""
    restart_needed = False
    if config.test_ops.get("enable_user_counters_cache", True):
        log.info("Enabling rgw_user_counters_cache")
        ceph_conf.set_to_ceph_conf(
            "global",
            ConfigOpts.rgw_user_counters_cache,
            True,
            ssh_con,
            set_to_all=True,
        )
        restart_needed = True
    if config.test_ops.get("enable_bucket_counters_cache", True):
        log.info("Enabling rgw_bucket_counters_cache")
        ceph_conf.set_to_ceph_conf(
            "global",
            ConfigOpts.rgw_bucket_counters_cache,
            True,
            ssh_con,
            set_to_all=True,
        )
        restart_needed = True
    if restart_needed:
        log.info("Restarting RGW after counter cache config changes")
        # Restart via local ceph CLI (RGW nodes may lack ceph.conf)
        if rgw_service.restart(None) is False:
            raise TestExecError("RGW service restart failed")
        time.sleep(30)


def test_exec(config, ssh_con):
    if not config.test_ops.get("test_head_obj_metrics", False):
        raise TestExecError("test_head_obj_metrics is not enabled in config")

    io_info_initialize = IOInfoInitialize()
    basic_io_structure = BasicIOInfoStructure()
    io_info_initialize.initialize(basic_io_structure.initial())

    ceph_conf = CephConfOp(ssh_con)
    rgw_service = RGWService()
    ip_and_port = s3cmd_reusable.get_rgw_ip_and_port(ssh_con, config.ssl)

    _enable_counter_caches(config, ceph_conf, rgw_service, ssh_con)

    all_users_info = s3lib.create_users(config.user_count)
    head_count = int(config.test_ops.get("head_object_count", 5))

    for each_user in all_users_info:
        auth = reusable.get_auth(each_user, ssh_con, config.ssl, config.haproxy)
        rgw_conn = auth.do_auth()
        s3_client = auth.do_auth_using_client()
        user_id = each_user["user_id"]

        for bc in range(config.bucket_count):
            bucket_name = utils.gen_bucket_name_from_userid(user_id, rand_no=bc)
            log.info(f"creating bucket: {bucket_name}")
            bucket = reusable.create_bucket(
                bucket_name, rgw_conn, each_user, ip_and_port
            )

            config.obj_size = utils.get_file_size(
                config.objects_size_range.get("min"),
                config.objects_size_range.get("max"),
            )
            s3_object_name = utils.gen_s3_object_name(bucket_name, 0)
            reusable.upload_object(
                s3_object_name, bucket, TEST_DATA_PATH, config, each_user
            )

            before_dump = reusable.get_counter_dump(ssh_con=ssh_con)
            before_gw = reusable.get_rgw_op_counters(before_dump, section="rgw_op")
            before_user = reusable.get_rgw_op_counters(
                before_dump,
                section="rgw_op_per_user",
                labels={"User": user_id},
            )
            before_bucket = reusable.get_rgw_op_counters(
                before_dump,
                section="rgw_op_per_bucket",
                labels={"Bucket": bucket_name},
            )

            log.info(f"Issuing {head_count} HEAD Object requests")
            for _ in range(head_count):
                s3_client.head_object(Bucket=bucket_name, Key=s3_object_name)

            after_dump = reusable.get_counter_dump(ssh_con=ssh_con)
            after_gw = reusable.get_rgw_op_counters(after_dump, section="rgw_op")
            after_user = reusable.get_rgw_op_counters(
                after_dump,
                section="rgw_op_per_user",
                labels={"User": user_id},
            )
            after_bucket = reusable.get_rgw_op_counters(
                after_dump,
                section="rgw_op_per_bucket",
                labels={"Bucket": bucket_name},
            )

            log.info("Asserting gateway-scope head_obj_* counters")
            reusable.assert_head_obj_counters(before_gw, after_gw, head_count)

            log.info("Asserting user-scope head_obj_* counters")
            if not after_user:
                raise TestExecError(
                    f"rgw_op_per_user counters missing for User={user_id}"
                )
            reusable.assert_head_obj_counters(before_user, after_user, head_count)

            log.info("Asserting bucket-scope head_obj_* counters")
            if not after_bucket:
                raise TestExecError(
                    f"rgw_op_per_bucket counters missing for Bucket={bucket_name}"
                )
            reusable.assert_head_obj_counters(before_bucket, after_bucket, head_count)

            if config.test_ops.get("verify_prometheus", True):
                log.info("Verifying head_obj_* via ceph-exporter and Prometheus")
                # ip_and_port is host:port (optionally with https:// prefix)
                rgw_host = (
                    str(ip_and_port)
                    .replace("https://", "")
                    .replace("http://", "")
                    .split(":")[0]
                )
                reusable.wait_for_head_obj_prometheus_metrics(
                    rgw_host=rgw_host, ssh_con=ssh_con
                )

            if config.test_ops.get("delete_bucket_object", False):
                reusable.delete_objects(bucket)
                reusable.delete_bucket(bucket)

        reusable.remove_user(each_user)


if __name__ == "__main__":
    test_info = AddTestInfo("Testing HEAD Object op metrics (ISCE-3042)")
    test_info.started_info()

    try:
        project_dir = os.path.abspath(os.path.join(__file__, "../../.."))
        test_data_dir = "test_data"
        TEST_DATA_PATH = os.path.join(project_dir, test_data_dir)
        log.info("TEST_DATA_PATH: %s" % TEST_DATA_PATH)
        if not os.path.exists(TEST_DATA_PATH):
            log.info("test data dir not exists, creating.. ")
            os.makedirs(TEST_DATA_PATH)
        parser = argparse.ArgumentParser(description="Testing HEAD Object op metrics")
        parser.add_argument(
            "-c", dest="config", help="YAML config for HEAD Object op metrics test"
        )
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
