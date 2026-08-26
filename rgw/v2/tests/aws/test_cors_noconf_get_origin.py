"""
Usage: test_cors_noconf_get_origin.py -c <input_yaml>

<input_yaml>
    configs/test_aws_cors_noconf_get_without_origin.yaml
    configs/test_aws_cors_noconf_get_with_origin.yaml
    configs/test_aws_cors_set_get_without_origin.yaml
    configs/test_aws_cors_set_get_with_origin.yaml

Operation:
    1. Configure aws, s3cmd, and curl
    2. Create bucket and upload an object of at least 1 MiB
    3. Optional: put CORS config (set_cors)
    4. Curl GET and AWS GET of the same object, with the same Origin header when set

    All four configs work with http, https, and haproxy via
    get_endpoint(config.ssl, config.haproxy). HTTPS curl uses --insecure;
    AWS GET uses boto3 get_object with the same SigV4 extras as curl
    (same Host, same payload hash, Origin when set, no x-amz-checksum-mode).
"""

import argparse
import hashlib
import json
import logging
import os
import re
import subprocess
import sys
import traceback
import warnings
from urllib.parse import urlparse

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError

sys.path.append(os.path.abspath(os.path.join(__file__, "../../../..")))

from v2.lib import resource_op
from v2.lib.aws import auth as aws_auth
from v2.lib.aws.resource_op import AWS
from v2.lib.exceptions import RGWBaseException, TestExecError
from v2.lib.manage_data import io_generator
from v2.lib.s3.write_io_info import BasicIOInfoStructure, IOInfoInitialize
from v2.lib.s3cmd import auth as s3_auth
from v2.tests.aws import reusable as aws_reusable
from v2.tests.curl import reusable as curl_reusable
from v2.tests.s3_swift import reusable as s3_reusable
from v2.tests.s3cmd import reusable as s3cmd_reusable
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

    curl_reusable.install_curl(version="7.88.1")
    curl_ver = utils.exec_shell_cmd("curl --version") or ""
    log.info(
        f"curl for SigV4 GET: {curl_ver.splitlines()[0] if curl_ver else curl_ver}"
    )
    curl_match = re.search(r"curl (\d+)\.(\d+)\.(\d+)", curl_ver)
    curl_have = tuple(int(x) for x in curl_match.groups()) if curl_match else (0, 0, 0)
    if curl_have < (7, 88, 0):
        raise TestExecError(
            f"curl {curl_have[0]}.{curl_have[1]}.{curl_have[2]} cannot sign "
            "HTTP GET the same as boto3 (403 AccessDenied). Need curl >= 7.88.1. "
            "Sync rgw/v2/tests/curl/reusable.py so install_curl upgrades 7.76."
        )
    endpoint = aws_reusable.get_endpoint(
        ssh_con, ssl=config.ssl, haproxy=config.haproxy
    )
    https = endpoint.startswith("https://")
    insecure = https
    log.info(
        f"RGW endpoint: {endpoint} "
        f"(ssl={config.ssl}, haproxy={config.haproxy}, https={https})"
    )
    tool = config.test_ops.get("tool", "aws")
    object_name = config.test_ops.get("object_name", "testfile")
    object_size = int(config.test_ops.get("object_size", 1048576))
    if config.mapped_sizes:
        object_size = max(config.mapped_sizes.values())
    if object_size < 1048576:
        object_size = 1048576
    origin = config.test_ops.get("origin", "test")
    steps = config.test_ops.get("steps")
    if not steps:
        raise TestExecError("test_ops.steps is required in config yaml")
    cors_configuration = config.test_ops.get("cors_configuration")
    if "set_cors" in steps and not cors_configuration:
        raise TestExecError(
            "test_ops.cors_configuration is required when set_cors is in steps"
        )

    user_info = resource_op.create_users(no_of_users_to_create=config.user_count)
    get_err = None
    created = []
    for user in user_info:
        user_name = user["user_id"]
        log.info(user_name)
        cli_aws = AWS(ssl=https)
        log.info("Configuring aws, s3cmd, and curl")
        aws_auth.do_auth_aws(user)
        s3_auth.do_auth(user, endpoint)

        for bc in range(config.bucket_count):
            bucket_name = utils.gen_bucket_name_from_userid(user_name, rand_no=bc)
            local_file = os.path.join(TEST_DATA_PATH, object_name)
            log.info(
                f"Create bucket and upload object of size {object_size} using {tool}"
            )
            if tool == "s3cmd":
                s3cmd_reusable.create_bucket(bucket_name, endpoint, ssl=https)
                s3cmd_reusable.upload_file(
                    bucket_name,
                    file_name=object_name,
                    file_size=object_size,
                    test_data_path=TEST_DATA_PATH,
                )
            else:
                aws_reusable.create_bucket(cli_aws, bucket_name, endpoint)
                file_info = io_generator(local_file, object_size)
                if file_info is False:
                    raise TestExecError(
                        f"Failed to create {object_size} byte test file"
                    )
                # put_object uses --body <object_name> in cwd
                utils.exec_shell_cmd(f"cp {local_file} {object_name}")
                aws_reusable.put_object(cli_aws, bucket_name, object_name, endpoint)
            if not os.path.exists(local_file):
                raise TestExecError(f"Local upload file not found: {local_file}")
            object_size = os.path.getsize(local_file)
            if object_size < 1048576:
                raise TestExecError(
                    f"Uploaded object size {object_size} is below 1 MiB"
                )
            log.info(f"Bucket {bucket_name} created with no CORS config")
            created.append(
                {
                    "cli": cli_aws,
                    "tool": tool,
                    "bucket": bucket_name,
                    "object": object_name,
                    "user": user,
                }
            )
            listed = aws_reusable.list_objects(cli_aws, bucket_name, endpoint)
            if listed is False:
                raise TestExecError(f"list-objects failed for {bucket_name}")
            log.info(f"list-objects: {listed.strip()}")
            try:
                contents = json.loads(listed).get("Contents") or []
            except (TypeError, ValueError) as e:
                raise TestExecError(
                    f"list-objects did not return JSON for {bucket_name}: {e}"
                )
            listed_obj = next(
                (obj for obj in contents if obj.get("Key") == object_name), None
            )
            if not listed_obj:
                raise TestExecError(
                    f"list-objects did not show {object_name} in {bucket_name}: {listed.strip()}"
                )
            if int(listed_obj.get("Size", 0)) != object_size:
                raise TestExecError(
                    f"list-objects size for {object_name} is {listed_obj.get('Size')}, "
                    f"expected {object_size}: {listed.strip()}"
                )

            # Host for SigV4:
            #   http :80  -> http://ip
            #   https :443 -> https://ip
            #   https :444 / haproxy :5000 -> keep the port
            parsed = urlparse(endpoint)
            if (
                parsed.port is None
                or (parsed.scheme == "https" and parsed.port == 443)
                or (parsed.scheme == "http" and parsed.port == 80)
            ):
                curl_base = f"{parsed.scheme}://{parsed.hostname}"
            else:
                curl_base = f"{parsed.scheme}://{parsed.hostname}:{parsed.port}"
            url = f"{curl_base}/{bucket_name}/{object_name}"
            # HTTPS (ssl and ssl+haproxy): UNSIGNED-PAYLOAD + curl --insecure.
            # HTTP (plain and haproxy :5000): empty-body SHA256, no --insecure.
            payload_sha = (
                "UNSIGNED-PAYLOAD" if https else hashlib.sha256(b"").hexdigest()
            )
            log.info(
                f"GET transport: base={curl_base} https={https} "
                f"haproxy={config.haproxy} "
                f"payload={'UNSIGNED-PAYLOAD' if https else 'empty-sha256'} "
                f"curl_insecure={https}"
            )
            log.info(f"curl GET url: {url}")
            results = []
            cors_set = False
            for idx, step in enumerate(steps):
                if step == "set_cors":
                    log.info(f"Set CORS config on {bucket_name}")
                    cors_path = os.path.join(TEST_DATA_PATH, "cors.json")
                    with open(cors_path, "w") as cors_file:
                        json.dump(cors_configuration, cors_file)
                    aws_reusable.put_bucket_cors(
                        cli_aws, bucket_name, cors_path, endpoint
                    )
                    cors_set = True
                    continue
                if step not in ("curl_get_with_origin", "curl_get_without_origin"):
                    raise TestExecError(f"Unknown test_ops.steps entry: {step}")
                use_origin = step == "curl_get_with_origin"
                origin_val = origin if use_origin else None
                cors_state = "CORS set" if cors_set else "no CORS"
                step_name = (
                    f"GET {'with' if use_origin else 'without'} Origin ({cors_state})"
                )
                out_file = os.path.join(TEST_DATA_PATH, f"got-{idx}-{step}.bin")
                header_file = f"{out_file}.headers"
                log.info(f"curl {step_name}: {url}")
                curl_cmd = [
                    "curl",
                    "-sS",
                    "--max-time",
                    "90",
                    "--aws-sigv4",
                    "aws:amz:us-east-1:s3",
                    "--user",
                    f"{user['access_key']}:{user['secret_key']}",
                    "-H",
                    f"x-amz-content-sha256: {payload_sha}",
                ]
                if insecure:
                    curl_cmd.insert(2, "--insecure")
                if origin_val:
                    curl_cmd.extend(["-H", f"Origin: {origin_val}"])
                curl_cmd.extend(
                    [
                        "-D",
                        header_file,
                        "-o",
                        out_file,
                        "-w",
                        "%{http_code} %{size_download}",
                        url,
                    ]
                )
                log.info(
                    "executing cmd: "
                    + " ".join(curl_cmd).replace(
                        f"{user['access_key']}:{user['secret_key']}",
                        f"{user['access_key']}:***",
                    )
                )
                proc = subprocess.Popen(
                    curl_cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    universal_newlines=False,
                )
                out, err = proc.communicate()
                out = out.decode("utf-8", errors="ignore")
                err = err.decode("utf-8", errors="ignore")
                headers = ""
                if os.path.exists(header_file):
                    with open(header_file, "r", errors="ignore") as fh:
                        headers = fh.read()
                body_size = os.path.getsize(out_file) if os.path.exists(out_file) else 0
                body_text = ""
                if os.path.exists(out_file) and body_size <= 8192:
                    with open(out_file, "r", errors="ignore") as fh:
                        body_text = fh.read()
                http_code = None
                write_out = out.strip().split()
                if write_out and write_out[0].isdigit():
                    http_code = int(write_out[0])
                content_length = None
                match = re.search(r"Content-Length:\s*(\d+)", headers, re.IGNORECASE)
                if match:
                    content_length = int(match.group(1))
                err_code_match = re.search(r"<Code>([^<]+)</Code>", body_text)
                curl_err_code = err_code_match.group(1) if err_code_match else ""
                err_msg_match = re.search(r"<Message>([^<]*)</Message>", body_text)
                curl_err_msg = ""
                if err_msg_match and err_msg_match.group(1):
                    curl_err_msg = err_msg_match.group(1)
                elif curl_err_code:
                    curl_err_msg = curl_err_code
                curl_response = "\n".join(
                    part
                    for part in (headers.strip(), body_text.strip(), err.strip())
                    if part
                )
                curl_ok = http_code == 200 and body_size == object_size
                curl_status = "PASS" if curl_ok else "FAIL"
                curl_found = str(http_code)
                if curl_err_code:
                    curl_found = f"{http_code} {curl_err_code}"
                log.info(
                    f"curl {step_name}: {curl_status}: expected 200, "
                    f"found: {curl_found}, body_bytes={body_size}"
                    + (f", error={curl_err_msg}" if curl_err_msg else "")
                )
                log.info(
                    f"----- curl {step_name}: {curl_status} -----\n"
                    f"body_bytes={body_size}, content_length={content_length}\n"
                    f"response:\n{curl_response}"
                )
                results.append(
                    {
                        "tool": "curl",
                        "ok": curl_ok,
                        "step": step_name,
                        "found": curl_found,
                        "error": curl_err_msg,
                        "body_bytes": body_size,
                        "content_length": content_length,
                        "response": curl_response,
                    }
                )

                # Match curl: SigV4, same Host, same Origin, same payload hash.
                aws_out_file = os.path.join(TEST_DATA_PATH, f"got-{idx}-{step}-aws.bin")
                origin_to_send = origin_val
                boto_endpoint = curl_base
                log.info(
                    f"aws {step_name}: boto3 get_object endpoint={boto_endpoint} "
                    f"origin={origin_to_send!r} x-amz-content-sha256={payload_sha}"
                )
                warnings.filterwarnings("ignore", message="Unverified HTTPS request")
                boto_cfg_kwargs = dict(
                    signature_version="s3v4",
                    s3={"addressing_style": "path"},
                    connect_timeout=10,
                    read_timeout=90,
                    retries={"max_attempts": 1},
                    request_checksum_calculation="when_required",
                    response_checksum_validation="when_required",
                )
                try:
                    boto_cfg = BotoConfig(**boto_cfg_kwargs)
                except TypeError:
                    boto_cfg_kwargs.pop("request_checksum_calculation", None)
                    boto_cfg_kwargs.pop("response_checksum_validation", None)
                    boto_cfg = BotoConfig(**boto_cfg_kwargs)
                s3 = boto3.client(
                    "s3",
                    endpoint_url=boto_endpoint,
                    aws_access_key_id=user["access_key"],
                    aws_secret_access_key=user["secret_key"],
                    region_name="us-east-1",
                    verify=not insecure,
                    config=boto_cfg,
                )

                def _drop_header(headers, name):
                    # HTTPMessage.__delitem__ removes every copy of that name.
                    try:
                        del headers[name]
                    except KeyError:
                        pass
                    for key in list(headers.keys()):
                        if str(key).lower() == name:
                            try:
                                del headers[key]
                            except KeyError:
                                pass

                def _set_header(headers, name, value):
                    # AWSRequest.headers is HTTPMessage: __setitem__ appends.
                    # Two Origin values make SigV4 sign origin:test,test while
                    # RGW sees Origin: test -> SignatureDoesNotMatch.
                    _drop_header(headers, name)
                    headers[name] = value

                def _hdr_val(headers, name):
                    if hasattr(headers, "get_all"):
                        allv = headers.get_all(name) or []
                        if allv:
                            val = allv[-1]
                            if isinstance(val, bytes):
                                return val.decode("utf-8", errors="ignore")
                            return val
                    for key in headers.keys():
                        if str(key).lower() == name:
                            val = headers[key]
                            if isinstance(val, bytes):
                                return val.decode("utf-8", errors="ignore")
                            return val
                    return None

                def _before_sign(
                    request=None,
                    origin=origin_to_send,
                    payload_sha=payload_sha,
                    **kwargs,
                ):
                    if request is None:
                        request = kwargs.get("request")
                    if request is None:
                        return
                    headers = request.headers
                    _drop_header(headers, "content-type")
                    _drop_header(headers, "x-amz-checksum-mode")
                    _set_header(headers, "x-amz-content-sha256", payload_sha)
                    if origin:
                        _set_header(headers, "origin", origin)
                    else:
                        _drop_header(headers, "origin")
                    origin_hdr = _hdr_val(headers, "origin")
                    hdr_names = [str(k).lower() for k in headers.keys()]
                    log.info(
                        f"aws request Origin={origin_hdr!r} "
                        f"header_names={hdr_names}"
                    )
                    if origin and not origin_hdr:
                        log.error("aws Origin was requested but is not on the request")

                signed_info = {"headers": "", "origin": None}
                orig_sign = s3._request_signer.sign

                def _sign(operation_name, request, *args, **kwargs):
                    result = orig_sign(operation_name, request, *args, **kwargs)
                    try:
                        auth = request.headers.get("Authorization")
                        if isinstance(auth, bytes):
                            auth = auth.decode("utf-8", errors="ignore")
                        signed = ""
                        if auth:
                            match = re.search(r"SignedHeaders=([^,\s]+)", str(auth))
                            if match:
                                signed = match.group(1)
                        origin_hdr = _hdr_val(request.headers, "origin")
                        signed_info["headers"] = signed
                        signed_info["origin"] = origin_hdr
                        log.info(
                            f"aws SignedHeaders={signed or 'none'} "
                            f"Origin={origin_hdr!r}"
                        )
                    except Exception:
                        pass
                    return result

                s3._request_signer.sign = _sign
                if hasattr(s3.meta.events, "register_last"):
                    s3.meta.events.register_last(
                        "before-sign.s3.GetObject", _before_sign
                    )
                else:
                    s3.meta.events.register("before-sign.s3.GetObject", _before_sign)
                aws_body_size = 0
                aws_content_length = None
                aws_found = None
                aws_response = ""
                aws_err_msg = ""
                try:
                    resp = s3.get_object(Bucket=bucket_name, Key=object_name)
                    aws_content_length = resp.get("ContentLength")
                    aws_found = str(
                        resp.get("ResponseMetadata", {}).get("HTTPStatusCode", 200)
                    )
                    body = resp["Body"].read()
                    with open(aws_out_file, "wb") as fh:
                        fh.write(body)
                    aws_body_size = len(body)
                    meta = {k: v for k, v in resp.items() if k != "Body"}
                    aws_response = json.dumps(meta, indent=4, default=str)
                except ClientError as e:
                    err_info = e.response.get("Error") or {}
                    aws_err_code = err_info.get("Code") or ""
                    aws_err_msg = err_info.get("Message") or str(e)
                    meta = e.response.get("ResponseMetadata") or {}
                    aws_found = str(meta.get("HTTPStatusCode") or "")
                    if aws_err_code:
                        aws_found = f"{aws_found} {aws_err_code}".strip()
                    hdrs = meta.get("HTTPHeaders") or {}
                    cl = hdrs.get("content-length")
                    if cl:
                        aws_content_length = int(cl)
                    aws_response = (
                        f"error: {aws_err_code}: {aws_err_msg}\n"
                        f"{json.dumps(e.response, indent=4, default=str)}"
                    )
                    if os.path.exists(aws_out_file):
                        aws_body_size = os.path.getsize(aws_out_file)
                except Exception as e:
                    aws_found = type(e).__name__
                    aws_err_msg = str(e)
                    aws_response = f"error: {aws_err_msg}"
                    if os.path.exists(aws_out_file):
                        aws_body_size = os.path.getsize(aws_out_file)
                signed_list = [
                    h for h in (signed_info["headers"] or "").split(";") if h
                ]
                if origin_to_send and "origin" not in signed_list:
                    aws_err_msg = (
                        f"SignedHeaders missing origin: "
                        f"{signed_info['headers'] or 'none'}"
                    )
                    log.error(f"aws {step_name}: {aws_err_msg}")
                    if aws_found == "200":
                        aws_found = "200 (unsigned Origin)"
                aws_ok = (
                    aws_found == "200"
                    and aws_body_size == object_size
                    and (
                        aws_content_length is None
                        or int(aws_content_length) == object_size
                    )
                    and (not origin_to_send or "origin" in signed_list)
                )
                aws_status = "PASS" if aws_ok else "FAIL"
                log.info(
                    f"aws {step_name}: {aws_status}: expected 200, "
                    f"found: {aws_found}, body_bytes={aws_body_size}"
                    + (f", error={aws_err_msg}" if aws_err_msg else "")
                )
                log.info(
                    f"----- aws {step_name}: {aws_status} -----\n"
                    f"body_bytes={aws_body_size}, content_length={aws_content_length}\n"
                    f"response:\n{aws_response}"
                )
                results.append(
                    {
                        "tool": "aws",
                        "ok": aws_ok,
                        "step": step_name,
                        "found": aws_found,
                        "error": aws_err_msg,
                        "body_bytes": aws_body_size,
                        "content_length": aws_content_length,
                        "response": aws_response,
                    }
                )

            curl_ok_all = all(r["ok"] for r in results if r["tool"] == "curl")
            aws_ok_all = all(r["ok"] for r in results if r["tool"] == "aws")
            if not curl_ok_all or not aws_ok_all:
                summary = (
                    f"aws {'PASS' if aws_ok_all else 'FAIL'}, "
                    f"curl {'PASS' if curl_ok_all else 'FAIL'}"
                )
                fail_blocks = []
                status_lines = []
                for r in results:
                    if not r["ok"]:
                        err_line = ""
                        if r.get("error"):
                            err_line = f"error: {r['error']}\n"
                        fail_blocks.append(
                            f"----- {r['tool']} {r['step']}: FAIL -----\n"
                            f"found: {r['found']}\n"
                            f"{err_line}"
                            f"body_bytes={r['body_bytes']}, "
                            f"content_length={r['content_length']}\n"
                            f"response:\n{r['response']}"
                        )
                for client in ("aws", "curl"):
                    for r in results:
                        if r["tool"] != client:
                            continue
                        status = "PASS" if r["ok"] else "FAIL"
                        extra = f", error={r['error']}" if r.get("error") else ""
                        status_lines.append(
                            f"{r['tool']} {r['step']}: {status}. "
                            f"expected 200 , found: {r['found']}{extra}"
                        )
                err = TestExecError(summary)
                err.report = "\n\n".join(fail_blocks + ["\n".join(status_lines)])
                if get_err is None:
                    get_err = err
            else:
                log.info(f"aws PASS, curl PASS for {bucket_name}/{object_name}")

    crash_info = s3_reusable.check_for_crash()
    all_ok = get_err is None and not crash_info
    if not all_ok:
        left = ", ".join(
            f"{c['bucket']}/{c['object']} (uid={c['user']['user_id']})" for c in created
        ) or ", ".join(u["user_id"] for u in user_info)
        log.info(
            "Test did not fully pass; not deleting object, bucket, or user "
            f"for debug: {left}"
        )
        if get_err:
            raise get_err
        raise TestExecError("ceph daemon crash found!")

    if config.test_ops.get("delete_bucket", True):
        for c in created:
            try:
                if c["tool"] == "s3cmd":
                    s3cmd_reusable.delete_file(c["bucket"], c["object"])
                    s3cmd_reusable.delete_bucket(c["bucket"])
                else:
                    aws_reusable.delete_object(
                        c["cli"], c["bucket"], c["object"], endpoint
                    )
                    aws_reusable.delete_bucket(c["cli"], c["bucket"], endpoint)
                log.info(f"Bucket {c['bucket']} deleted")
            except Exception as cleanup_e:
                log.error(f"Bucket cleanup failed: {cleanup_e}")
    if config.user_remove is True:
        for user in user_info:
            s3_reusable.remove_user(user)


if __name__ == "__main__":
    test_info = AddTestInfo("RGW CORS Origin GET with and without CORS config")
    test_passed = False

    try:
        project_dir = os.path.abspath(os.path.join(__file__, "../../.."))
        test_data_dir = "test_data"
        TEST_DATA_PATH = os.path.join(project_dir, test_data_dir)
        log.info(f"TEST_DATA_PATH: {TEST_DATA_PATH}")
        if not os.path.exists(TEST_DATA_PATH):
            log.info("test data dir not exists, creating.. ")
            os.makedirs(TEST_DATA_PATH)
        parser = argparse.ArgumentParser(
            description="RGW CORS Origin GET with and without CORS config"
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
        test_passed = True
        test_info.success_status("test passed")
        sys.exit(0)

    except (RGWBaseException, Exception) as e:
        report = getattr(e, "report", None)
        if report:
            print(report)
            log.error(e)
        else:
            log.error(e)
            log.error(traceback.format_exc())
        test_info.failed_status("test failed")
        sys.exit(1)

    finally:
        if test_passed:
            utils.cleanup_test_data_path(TEST_DATA_PATH)
