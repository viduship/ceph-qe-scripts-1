"""
Usage: test_cors_options_preflight.py -c <input_yaml>

<input_yaml>
    configs/test_aws_cors_options_multi_rule_preflight.yaml

Operation:
    Automates CORS OPTIONS preflight with multiple CORSRules
    (separate GET and PUT rules for the same origin):
    1. Create bucket (aws s3api create-bucket)
    2. put-bucket-cors with multi-rule cors.json
    3. OPTIONS PUT + Access-Control-Request-Headers -> expect 200
    4. OPTIONS GET for allowed origin -> expect 200
    5. OPTIONS PUT for disallowed origin -> expect 403

    Works with http, https, and haproxy endpoints via get_endpoint
    (config.ssl / config.haproxy). HTTPS curl uses -k; AWS CLI uses
    --no-verify-ssl when ssl is enabled.

    Note: Affected RGW builds return 403 for step 3 (PUT + request
    headers) even though AllowedHeaders is "*". This test asserts
    correct S3 CORS behavior (200) so the failure tracks the bug.
"""

import argparse
import json
import logging
import os
import re
import sys
import tempfile
import traceback

sys.path.append(os.path.abspath(os.path.join(__file__, "../../../..")))

from v2.lib import resource_op
from v2.lib.aws import auth as aws_auth
from v2.lib.aws.resource_op import AWS
from v2.lib.exceptions import RGWBaseException, TestExecError
from v2.lib.s3.write_io_info import BasicIOInfoStructure, IOInfoInitialize
from v2.tests.aws import reusable as aws_reusable
from v2.tests.s3_swift import reusable as s3_reusable
from v2.utils import utils
from v2.utils.log import configure_logging
from v2.utils.test_desc import AddTestInfo

log = logging.getLogger(__name__)
TEST_DATA_PATH = None


def test_exec(config, ssh_con):
    """
    Executes test based on configuration passed
    Args:
        config(object): Test configuration
        ssh_con: SSH connection object (optional)
    """
    io_info_initialize = IOInfoInitialize()
    basic_io_structure = BasicIOInfoStructure()
    io_info_initialize.initialize(basic_io_structure.initial())

    endpoint = aws_reusable.get_endpoint(
        ssh_con, ssl=config.ssl, haproxy=config.haproxy
    )

    user_info = resource_op.create_users(no_of_users_to_create=config.user_count)
    policy_document = config.test_ops.get("policy_document")
    if not policy_document:
        raise TestExecError("test_ops.policy_document is required")
    preflight_checks = config.test_ops.get("preflight_checks")
    if not preflight_checks:
        raise TestExecError("test_ops.preflight_checks is required")

    for user in user_info:
        user_name = user["user_id"]
        log.info(user_name)
        cli_aws = AWS(ssl=config.ssl)
        aws_auth.do_auth_aws(user)

        for bc in range(config.bucket_count):
            bucket_name = utils.gen_bucket_name_from_userid(user_name, rand_no=bc)
            aws_reusable.create_bucket(cli_aws, bucket_name, endpoint)
            log.info(f"Bucket {bucket_name} created")

            cors_path = None
            try:
                with tempfile.NamedTemporaryFile(
                    mode="w", suffix=".json", delete=False
                ) as cors_file:
                    cors_file.write(json.dumps(policy_document, indent=2))
                    cors_path = cors_file.name
                aws_reusable.put_bucket_cors(cli_aws, bucket_name, cors_path, endpoint)
                log.info(f"Put CORS configuration on {bucket_name}")
            finally:
                if cors_path and os.path.exists(cors_path):
                    os.remove(cors_path)

            for check in preflight_checks:
                name = check.get("name", "preflight")
                origin = check["origin"]
                method = check["method"]
                request_headers = check.get("request_headers")
                expected_status = int(check["expected_status"])
                log.info(
                    f"Preflight '{name}': origin={origin}, method={method}, "
                    f"request_headers={request_headers}, "
                    f"expected_status={expected_status}"
                )
                # http: plain curl; https/haproxy+ssl: curl -k (QE self-signed)
                insecure = " -k" if endpoint.startswith("https://") else ""
                curl_cmd = (
                    f'curl -si{insecure} -X OPTIONS "{endpoint}/{bucket_name}"'
                    f' -H "Origin: {origin}"'
                    f' -H "Access-Control-Request-Method: {method}"'
                )
                if request_headers:
                    curl_cmd += (
                        f' -H "Access-Control-Request-Headers: {request_headers}"'
                    )
                log.info(f"Executing: {curl_cmd}")
                response = utils.exec_shell_cmd(curl_cmd)
                if response is False:
                    raise TestExecError(
                        f"OPTIONS curl failed for bucket={bucket_name}, method={method}"
                    )
                response = str(response)
                log.info(f"OPTIONS response:\n{response}")
                response_oneline = " ".join(response.split())
                # Status line is HTTP/1.x or HTTP/2 for both http and https endpoints
                status_match = re.search(r"HTTP/\S+\s+(\d+)", response)
                if not status_match:
                    raise TestExecError(
                        f"Could not parse status code from OPTIONS response: endpoint={endpoint}, bucket={bucket_name}, method={method}, response={response_oneline}"
                    )
                status = int(status_match.group(1))
                if status != expected_status:
                    raise AssertionError(
                        f"Preflight '{name}': expected status code {expected_status}, found status code {status}, endpoint={endpoint}, origin={origin}, method={method}, request_headers={request_headers}, response={response_oneline}"
                    )
                if expected_status == 200:
                    if "Access-Control-Allow-Origin" not in response:
                        raise AssertionError(
                            f"Preflight '{name}': missing Access-Control-Allow-Origin, endpoint={endpoint}, origin={origin}, method={method}, response={response_oneline}"
                        )
                    if (
                        origin not in response
                        and "Access-Control-Allow-Origin: *" not in response
                    ):
                        raise AssertionError(
                            f"Preflight '{name}': Allow-Origin mismatch for {origin}, endpoint={endpoint}, method={method}, response={response_oneline}"
                        )
                    if method not in response:
                        raise AssertionError(
                            f"Preflight '{name}': missing method {method} in Allow-Methods, endpoint={endpoint}, origin={origin}, response={response_oneline}"
                        )
                log.info(
                    f"Preflight '{name}' passed: status code {status}, endpoint={endpoint}"
                )

            if config.test_ops.get("delete_bucket", True):
                aws_reusable.delete_bucket(cli_aws, bucket_name, endpoint)
                log.info(f"Bucket {bucket_name} deleted")

    if config.user_remove is True:
        for user in user_info:
            s3_reusable.remove_user(user)

    crash_info = s3_reusable.check_for_crash()
    if crash_info:
        raise TestExecError("ceph daemon crash found!")


if __name__ == "__main__":
    test_info = AddTestInfo("RGW CORS multi-rule OPTIONS preflight")

    try:
        project_dir = os.path.abspath(os.path.join(__file__, "../../.."))
        test_data_dir = "test_data"
        TEST_DATA_PATH = os.path.join(project_dir, test_data_dir)
        log.info(f"TEST_DATA_PATH: {TEST_DATA_PATH}")
        if not os.path.exists(TEST_DATA_PATH):
            log.info("test data dir not exists, creating.. ")
            os.makedirs(TEST_DATA_PATH)
        parser = argparse.ArgumentParser(
            description="RGW CORS multi-rule OPTIONS preflight"
        )
        parser.add_argument("-c", dest="config", help="input yaml config")
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
        config = resource_op.Config(yaml_file)
        config.read(ssh_con)
        if config.mapped_sizes is None:
            if config.objects_size_range is None:
                raise TestExecError(
                    "objects_size_range is required in config yaml (min/max) for make_mapped_sizes"
                )
            config.mapped_sizes = utils.make_mapped_sizes(config)
        test_exec(config, ssh_con)
        test_info.success_status("test passed")
        sys.exit(0)

    except (RGWBaseException, Exception) as e:
        log.error(e)
        log.error(traceback.format_exc())
        test_info.failed_status("test failed")
        sys.exit(1)

    finally:
        utils.cleanup_test_data_path(TEST_DATA_PATH)
