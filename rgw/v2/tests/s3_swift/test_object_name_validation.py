"""
test_object_name_validation - Validate S3 object key names against AWS guidelines

Usage: test_object_name_validation.py -c configs/test_object_name_validation.yaml

<input_yaml>
    configs/test_object_name_validation.yaml

Operation:
    Create an RGW user and bucket, then PUT/GET/DELETE objects whose keys cover
    the Amazon S3 object key naming guidelines:

      https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html

    Categories:
      - Safe characters
      - Period-only path segments
      - Characters that might require special handling
      - Characters to avoid
      - Hard limits (empty key, 1024-byte UTF-8 maximum)
"""

import argparse
import logging
import os
import sys
import traceback

sys.path.append(os.path.abspath(os.path.join(__file__, "../../../..")))

import v2.lib.resource_op as s3lib
import v2.utils.utils as utils
from botocore.exceptions import BotoCoreError, ClientError, ParamValidationError
from v2.lib.exceptions import RGWBaseException, TestExecError
from v2.lib.resource_op import Config
from v2.lib.rgw_config_opts import CephConfOp
from v2.lib.s3.auth import Auth
from v2.lib.s3.write_io_info import BasicIOInfoStructure, IOInfoInitialize
from v2.tests.s3_swift import reusable
from v2.utils.log import configure_logging
from v2.utils.test_desc import AddTestInfo

log = logging.getLogger()

MAX_KEY_UTF8_BYTES = 1024
OBJECT_BODY = b"s3-object-name-validation"

# AWS "Safe characters" — alphanumeric plus the listed specials.
# Forward slash is the prefix delimiter and is allowed in otherwise-safe keys.
SAFE_SPECIAL_CHARS = set("!-_.*'()")
SAFE_CHARS = (
    set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
    | SAFE_SPECIAL_CHARS
)

# AWS "Characters that might require special handling"
SPECIAL_HANDLING_CHARS = set("&$@=;/: +,?")

# AWS "Characters to avoid"
AVOID_CHARS = set('\\{^}%`]">[~<#|')

_SAFE_SPECIAL_CHAR_CASES = (
    ("exclamation", "!", "Exclamation point"),
    ("hyphen", "-", "Hyphen"),
    ("underscore", "_", "Underscore"),
    ("period", ".", "Period"),
    ("asterisk", "*", "Asterisk"),
    ("single_quote", "'", "Single quotation mark"),
    ("open_paren", "(", "Opening parenthesis"),
    ("close_paren", ")", "Closing parenthesis"),
)

_SPECIAL_HANDLING_CHAR_CASES = (
    ("ampersand", "&", "Ampersand"),
    ("dollar", "$", "Dollar sign"),
    ("at", "@", "At symbol"),
    ("equal", "=", "Equal sign"),
    ("semicolon", ";", "Semicolon"),
    ("slash", "/", "Forward slash"),
    ("colon", ":", "Colon"),
    ("plus", "+", "Plus sign"),
    ("space", " ", "Space"),
    ("comma", ",", "Comma"),
    ("question", "?", "Question mark"),
)

_AVOID_CHAR_CASES = (
    ("backslash", "\\", "Backslash"),
    ("left_brace", "{", "Left brace"),
    ("right_brace", "}", "Right brace"),
    ("caret", "^", "Caret or circumflex"),
    ("percent", "%", "Percent character"),
    ("backtick", "`", "Grave accent or backtick"),
    ("right_bracket", "]", "Right bracket"),
    ("double_quote", '"', "Quotation mark"),
    ("greater_than", ">", "Greater than sign"),
    ("left_bracket", "[", "Left bracket"),
    ("tilde", "~", "Tilde"),
    ("less_than", "<", "Less than sign"),
    ("pound", "#", "Pound sign"),
    ("pipe", "|", "Vertical bar or pipe"),
)


def _utf8_len(key):
    return len(key.encode("utf-8"))


def relative_path_allowed(key):
    """
    AWS relative-path rule for object keys.

    Keys that contain relative path elements (`../`) are valid if, parsed
    left-to-right, the cumulative count of relative segments never exceeds
    the number of non-relative path elements. Equivalent model: treat `..`
    as moving up one directory; depth must never go negative.

    Standalone `.` is a period-only segment that can confuse clients, but it
    does not change depth, and a `./` prefix is allowed via the S3 API.
    """
    depth = 0
    for segment in key.split("/"):
        if segment == "..":
            depth -= 1
            if depth < 0:
                return False
        elif segment not in ("", "."):
            depth += 1
    return True


def _is_safe_key(key):
    if not key:
        return False
    return all(ch in SAFE_CHARS or ch == "/" for ch in key)


def _format_key_for_log(key):
    return f"{key!r} (chars={len(key)} utf8_bytes={_utf8_len(key)})"


def _char_key(char, suffix="name.txt"):
    return f"file{char}{suffix}"


def _safe_character_cases():
    """AWS Safe characters, including documented example keys."""
    cat = "safe_characters"
    cases = [
        (
            "safe_digits",
            cat,
            "Alphanumeric digits 0-9",
            "0123456789",
            True,
        ),
        (
            "safe_lowercase",
            cat,
            "Alphanumeric lowercase a-z",
            "abcdefghijklmnopqrstuvwxyz",
            True,
        ),
        (
            "safe_uppercase",
            cat,
            "Alphanumeric uppercase A-Z (keys are case sensitive)",
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ",
            True,
        ),
        (
            "safe_mixed_alnum",
            cat,
            "Mixed letters and digits",
            "aB3xY9",
            True,
        ),
        (
            "safe_all_specials",
            cat,
            "All listed safe special characters",
            "a!-_.*'()z",
            True,
        ),
        (
            "safe_aws_example_org",
            cat,
            "AWS example: 4my-organization",
            "4my-organization",
            True,
        ),
        (
            "safe_aws_example_photos",
            cat,
            "AWS example: my.great_photos-2014/jan/myvacation.jpg",
            "my.great_photos-2014/jan/myvacation.jpg",
            True,
        ),
        (
            "safe_aws_example_videos",
            cat,
            "AWS example: videos/2014/birthday/video1.wmv",
            "videos/2014/birthday/video1.wmv",
            True,
        ),
        (
            "safe_prefix_folder",
            cat,
            "Prefix/delimiter included in the key (Development/Projects.xls)",
            "Development/Projects.xls",
            True,
        ),
        (
            "safe_root_object",
            cat,
            "Object at bucket root (s3-dg.pdf)",
            "s3-dg.pdf",
            True,
        ),
        (
            "safe_trailing_period",
            cat,
            "Key ending with period (console strips on download; API keeps it)",
            "filename.",
            True,
        ),
        (
            "safe_case_sensitive_photos",
            cat,
            "Mixed-case key (object keys are case sensitive)",
            "Photos.jpg",
            True,
        ),
        (
            "safe_case_sensitive_photos_lower",
            cat,
            "Lowercase counterpart of Photos.jpg (distinct key)",
            "photos.jpg",
            True,
        ),
    ]
    for cid, char, label in _SAFE_SPECIAL_CHAR_CASES:
        cases.append(
            (
                f"safe_{cid}",
                cat,
                f"{label} (`{char}`) is a safe character",
                _char_key(char),
                True,
            )
        )
    return cases


def _period_only_path_segment_cases():
    """AWS Period-only path segments and relative-path validity examples."""
    cat = "period_only_path_segments"
    return [
        (
            "period_current_dir_mid",
            cat,
            "Contains current-directory segment (folder/./file.txt)",
            "folder/./file.txt",
            True,
        ),
        (
            "period_parent_dir_mid",
            cat,
            "Contains parent-directory segment (folder/../file.txt)",
            "folder/../file.txt",
            True,
        ),
        (
            "period_current_dir_prefix",
            cat,
            "Starts with ./ (console cannot upload; API/CLI/SDK can)",
            "./file.txt",
            True,
        ),
        (
            "period_hidden_file",
            cat,
            "Period is part of the filename, not a standalone segment",
            "folder/.hidden/file.txt",
            True,
        ),
        (
            "period_backup_file",
            cat,
            "Periods are part of the filename, not a standalone segment",
            "folder/..backup/file.txt",
            True,
        ),
        (
            "period_aws_valid_relative",
            cat,
            "AWS valid relative path: videos/2014/../../video1.wmv",
            "videos/2014/../../video1.wmv",
            True,
        ),
        (
            "period_only_dot",
            cat,
            "Key is a single period-only segment '.'",
            ".",
            True,
        ),
    ]


def _special_handling_cases(expand_ascii_ranges):
    """AWS characters that might require special handling (URL/HEX encoding)."""
    cat = "special_handling"
    cases = []
    for cid, char, label in _SPECIAL_HANDLING_CHAR_CASES:
        cases.append(
            (
                f"special_{cid}",
                cat,
                f"{label} (`{char!r}`) might require special handling",
                _char_key(char),
                True,
            )
        )
    cases.extend(
        [
            (
                "special_multiple_spaces",
                cat,
                "Multiple spaces (sequences of spaces might be lost)",
                "file    name.txt",
                True,
            ),
            (
                "special_leading_space",
                cat,
                "Leading space",
                " leading.txt",
                True,
            ),
            (
                "special_trailing_space",
                cat,
                "Trailing space",
                "trailing.txt ",
                True,
            ),
            (
                "special_leading_slash",
                cat,
                "Leading forward slash",
                "/file.txt",
                True,
            ),
            (
                "special_trailing_slash",
                cat,
                "Trailing forward slash (console-style folder object)",
                "folder/",
                True,
            ),
            (
                "special_consecutive_slashes",
                cat,
                "Consecutive forward slashes",
                "file//name.txt",
                True,
            ),
            (
                "special_utf8_non_ascii",
                cat,
                "Non-ASCII UTF-8 is allowed (not in the safe character set)",
                "café/中文.jpg",
                True,
            ),
        ]
    )
    if expand_ascii_ranges:
        # ASCII 00–1F and 7F. NUL (0) cannot be sent on the HTTP wire.
        for code in list(range(0x00, 0x20)) + [0x7F]:
            should_succeed = code != 0x00
            note = "NUL cannot be represented in HTTP" if code == 0x00 else "control"
            cases.append(
                (
                    f"special_ascii_{code:02x}",
                    cat,
                    f"ASCII {code:#04x} ({note}) requires special handling",
                    f"ctrl-{code:02x}-{chr(code)}-end",
                    should_succeed,
                )
            )
    return cases


def _characters_to_avoid_cases(expand_ascii_ranges):
    """AWS characters to avoid; still valid UTF-8 object keys via the S3 API."""
    cat = "characters_to_avoid"
    cases = []
    for cid, char, label in _AVOID_CHAR_CASES:
        cases.append(
            (
                f"avoid_{cid}",
                cat,
                f"{label} (`{char!r}`) is recommended to avoid; API still allows it",
                _char_key(char),
                True,
            )
        )
    cases.append(
        (
            "avoid_mixed_set",
            cat,
            "Several characters-to-avoid in one key",
            'a\\b{c^d}e%f`g]h"i>j[k~l<m#n|o',
            True,
        )
    )
    if expand_ascii_ranges:
        # Non-printable ASCII 128–255 (Latin-1 / C1 controls and related)
        for code in range(128, 256):
            cases.append(
                (
                    f"avoid_ascii_{code:02x}",
                    cat,
                    f"Non-printable ASCII {code} (0x{code:02x}) is recommended to avoid",
                    f"high-{code:02x}-{chr(code)}-end",
                    True,
                )
            )
    return cases


def _hard_limit_cases():
    """Hard S3 object-key limits from the same AWS naming page."""
    cat = "hard_limits"
    return [
        (
            "limit_empty_key",
            cat,
            "Empty object key is not supported",
            "",
            False,
        ),
        (
            "limit_max_utf8_1024",
            cat,
            "Maximum UTF-8 length (1024 bytes)",
            "a" * MAX_KEY_UTF8_BYTES,
            True,
        ),
        (
            "limit_over_utf8_1025",
            cat,
            "Above maximum UTF-8 length (1025 bytes)",
            "a" * (MAX_KEY_UTF8_BYTES + 1),
            False,
        ),
        (
            "limit_utf8_multibyte_within",
            cat,
            "Multibyte UTF-8 within 1024-byte limit (341 x U+4E2D = 1023 bytes)",
            "中" * 341,
            True,
        ),
        (
            "limit_utf8_multibyte_over",
            cat,
            "Multibyte UTF-8 over 1024-byte limit (342 x U+4E2D = 1026 bytes)",
            "中" * 342,
            False,
        ),
        (
            "limit_soap_key",
            cat,
            'Key "soap" is unsupported for virtual-hosted-style; path-style API allows it',
            "soap",
            True,
        ),
    ]


def _object_key_test_cases(categories, expand_ascii_ranges):
    all_cases = (
        _safe_character_cases()
        + _period_only_path_segment_cases()
        + _special_handling_cases(expand_ascii_ranges)
        + _characters_to_avoid_cases(expand_ascii_ranges)
        + _hard_limit_cases()
    )
    if not categories:
        return all_cases
    wanted = set(categories)
    return [case for case in all_cases if case[1] in wanted]


def _precheck_case(case_id, category, object_key, should_succeed):
    """Shape-check generated keys. Returns an error message or None."""
    if case_id == "limit_empty_key":
        if object_key != "":
            return "expected an empty object key"
        return None

    utf8_len = _utf8_len(object_key)

    if case_id == "limit_max_utf8_1024" and utf8_len != MAX_KEY_UTF8_BYTES:
        return f"expected {MAX_KEY_UTF8_BYTES} UTF-8 bytes, got {utf8_len}"
    if case_id == "limit_over_utf8_1025" and utf8_len != MAX_KEY_UTF8_BYTES + 1:
        return f"expected {MAX_KEY_UTF8_BYTES + 1} UTF-8 bytes, got {utf8_len}"
    if case_id == "limit_utf8_multibyte_within" and utf8_len > MAX_KEY_UTF8_BYTES:
        return f"expected within {MAX_KEY_UTF8_BYTES} UTF-8 bytes, got {utf8_len}"
    if case_id == "limit_utf8_multibyte_over" and utf8_len <= MAX_KEY_UTF8_BYTES:
        return f"expected over {MAX_KEY_UTF8_BYTES} UTF-8 bytes, got {utf8_len}"

    if category == "safe_characters" and not _is_safe_key(object_key):
        return "expected only AWS safe characters (and '/' as delimiter)"

    if category == "period_only_path_segments":
        allowed = relative_path_allowed(object_key)
        if should_succeed and not allowed:
            return "expected relative-path rule to allow this key"
        if not should_succeed and allowed:
            return "expected relative-path rule to reject this key"

    if utf8_len > MAX_KEY_UTF8_BYTES and should_succeed:
        return f"key is {utf8_len} UTF-8 bytes; S3 max is {MAX_KEY_UTF8_BYTES}"

    return None


def _attempt_put_object(s3_client, bucket_name, object_key, body):
    """Try to PUT an object. Returns (created: bool, error: Exception|None)."""
    try:
        s3_client.put_object(Bucket=bucket_name, Key=object_key, Body=body)
        return True, None
    except (ClientError, ParamValidationError, BotoCoreError, ValueError) as exc:
        return False, exc


def _attempt_get_object(s3_client, bucket_name, object_key):
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        return True, response["Body"].read(), None
    except (ClientError, ParamValidationError, BotoCoreError, ValueError) as exc:
        return False, None, exc


def _format_error(exc):
    if exc is None:
        return ""
    if isinstance(exc, ClientError):
        err = exc.response.get("Error", {})
        return f"{err.get('Code', type(exc).__name__)}: {err.get('Message', exc)}"
    return f"{type(exc).__name__}: {exc}"


def _delete_object(s3_client, bucket_name, object_key):
    try:
        s3_client.delete_object(Bucket=bucket_name, Key=object_key)
        return True
    except ClientError as exc:
        log.warning(
            "Failed to delete object %s: %s",
            _format_key_for_log(object_key),
            _format_error(exc),
        )
        return False


def test_exec(config, ssh_con):
    io_info_initialize = IOInfoInitialize()
    basic_io_structure = BasicIOInfoStructure()
    io_info_initialize.initialize(basic_io_structure.initial())

    all_users_info = s3lib.create_users(config.user_count)

    for each_user in all_users_info:
        auth = Auth(each_user, ssh_con, ssl=config.ssl, haproxy=config.haproxy)
        s3_client = auth.do_auth_using_client()

        if config.test_ops.get("object_name_validation", False) is not True:
            log.warning(
                "object_name_validation not enabled in test_ops; skipping validation"
            )
            if config.user_remove:
                reusable.remove_user(each_user)
            continue

        categories = config.test_ops.get("categories") or []
        expand_ascii_ranges = config.test_ops.get("expand_ascii_ranges", True)
        verify_roundtrip = config.test_ops.get("verify_roundtrip", True)
        cases = _object_key_test_cases(categories, expand_ascii_ranges)
        log.info(
            "Running %d object key validation cases for user %s (categories=%s)",
            len(cases),
            each_user["user_id"],
            categories or "all",
        )

        bucket_name = utils.gen_bucket_name_from_userid(each_user["user_id"], rand_no=0)
        s3_client.create_bucket(Bucket=bucket_name)
        log.info("Created bucket %s", bucket_name)

        failures = []
        keys_to_cleanup = []

        for case_id, category, description, object_key, should_succeed in cases:
            log.info(
                "Case %s [%s] (%s): key=%s expect_success=%s",
                case_id,
                category,
                description,
                _format_key_for_log(object_key),
                should_succeed,
            )

            precheck_error = _precheck_case(
                case_id, category, object_key, should_succeed
            )
            if precheck_error:
                failures.append(f"{case_id}: {precheck_error}")
                continue

            body = OBJECT_BODY + b":" + case_id.encode("utf-8")
            created, error = _attempt_put_object(
                s3_client, bucket_name, object_key, body
            )

            if should_succeed and not created:
                failures.append(
                    f"{case_id} ({description}): expected success, got failure — "
                    f"{_format_error(error)}"
                )
                continue
            if not should_succeed and created:
                failures.append(
                    f"{case_id} ({description}): expected failure, object was created"
                )
                keys_to_cleanup.append(object_key)
                continue
            if not should_succeed:
                log.info(
                    "PASS: %s — PUT failed as expected (%s)",
                    case_id,
                    _format_error(error),
                )
                continue

            keys_to_cleanup.append(object_key)
            if verify_roundtrip:
                got, payload, get_error = _attempt_get_object(
                    s3_client, bucket_name, object_key
                )
                if not got:
                    failures.append(
                        f"{case_id} ({description}): PUT succeeded but GET failed — "
                        f"{_format_error(get_error)}"
                    )
                    continue
                if payload != body:
                    failures.append(
                        f"{case_id} ({description}): GET body mismatch "
                        f"(got {payload!r})"
                    )
                    continue
            log.info("PASS: %s — object stored as expected", case_id)

        for object_key in keys_to_cleanup:
            _delete_object(s3_client, bucket_name, object_key)

        try:
            s3_client.delete_bucket(Bucket=bucket_name)
            log.info("Cleaned up bucket %s", bucket_name)
        except ClientError as exc:
            log.warning(
                "Failed to delete bucket %s during cleanup: %s",
                bucket_name,
                _format_error(exc),
            )

        if failures:
            raise TestExecError(
                "Object name validation failed:\n" + "\n".join(failures)
            )

        if config.user_remove:
            reusable.remove_user(each_user)


if __name__ == "__main__":
    test_info = AddTestInfo("S3 object key naming guideline validation")
    test_info.started_info()

    try:
        parser = argparse.ArgumentParser(description="RGW object name validation")
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
        CephConfOp(ssh_con)
        config.read(ssh_con)
        if config.mapped_sizes is None:
            config.mapped_sizes = utils.make_mapped_sizes(config)

        test_exec(config, ssh_con)
        test_info.success_status("test passed")
        sys.exit(0)

    except (RGWBaseException, Exception) as e:
        log.error(e)
        log.error(traceback.format_exc())
        test_info.failed_status("test failed")
        sys.exit(1)
