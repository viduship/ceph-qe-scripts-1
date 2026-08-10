"""
Reusable helpers for RGW-backed SMB share operations.
"""

import json
import logging
import os
import tempfile
import time

import yaml
from v2.lib.exceptions import TestExecError
from v2.utils import utils

log = logging.getLogger()


def is_smb_mgr_module_enabled():
    """
    Check whether the smb mgr module is enabled.
    Uses: ceph mgr module ls | grep smb
    Module is enabled when the matched line contains 'on'.
    """
    log.info("Checking if smb mgr module is enabled")
    out = utils.exec_shell_cmd("ceph mgr module ls | grep smb")
    if out is False or not str(out).strip():
        log.info("smb mgr module is not enabled")
        return False

    # Example: "smb                   on"
    enabled = "on" in str(out).lower()
    log.info(f"smb mgr module ls|grep smb output: {out.strip()}")
    log.info(f"smb mgr module enabled: {enabled}")
    return enabled


def enable_smb_mgr_module():
    """
    Enable the smb mgr module.
    """
    log.info("Enabling smb mgr module")
    out = utils.exec_shell_cmd("ceph mgr module enable smb")
    if out is False:
        raise TestExecError("Failed to enable smb mgr module")
    log.info(f"smb mgr module enable output: {out}")
    return out


def ensure_smb_mgr_module_enabled():
    """
    Ensure smb mgr module is enabled; enable it if needed.
    """
    if is_smb_mgr_module_enabled():
        log.info("smb mgr module already enabled")
        return True

    enable_smb_mgr_module()
    # Re-check after enable
    if not is_smb_mgr_module_enabled():
        raise TestExecError("smb mgr module is still not enabled after enable")
    log.info("smb mgr module enabled successfully")
    return True


def create_smb_cluster(
    cluster_id, auth_mode="user", define_user_pass=None, placement=None
):
    """
    Create an SMB cluster.
    Args:
        cluster_id(str): SMB cluster id
        auth_mode(str): Authentication mode (user|active-directory)
        define_user_pass(str): Optional user%password for user auth mode
        placement(str): Optional placement string
    """
    log.info(f"Creating SMB cluster: {cluster_id}")
    cmd = f"ceph smb cluster create {cluster_id} {auth_mode}"
    if define_user_pass:
        cmd += f" --define-user-pass={define_user_pass}"
    if placement:
        cmd += f" --placement={placement}"
    out = utils.exec_shell_cmd(cmd)
    if out is False:
        raise TestExecError(f"Failed to create SMB cluster {cluster_id}")
    log.info(f"SMB cluster create output: {out}")
    return out


def remove_smb_cluster(cluster_id):
    """
    Remove an SMB cluster.
    Args:
        cluster_id(str): SMB cluster id
    """
    log.info(f"Removing SMB cluster: {cluster_id}")
    cmd = f"ceph smb cluster rm {cluster_id}"
    out = utils.exec_shell_cmd(cmd)
    if out is False:
        raise TestExecError(f"Failed to remove SMB cluster {cluster_id}")
    log.info(f"SMB cluster remove output: {out}")
    return out


def list_smb_clusters(fmt="json"):
    """
    List SMB clusters.
    Args:
        fmt(str): Output format (json|yaml)
    Returns:
        Parsed JSON (list/dict) or raw string for non-json formats
    """
    log.info("Listing SMB clusters")
    cmd = f"ceph smb cluster ls --format={fmt}"
    out = utils.exec_shell_cmd(cmd)
    if out is False:
        raise TestExecError("Failed to list SMB clusters")
    log.info(f"SMB cluster list output: {out}")
    if fmt == "json":
        try:
            return json.loads(out)
        except (TypeError, json.JSONDecodeError):
            return out
    return out


def verify_cluster_in_list(cluster_list, cluster_id, expect_present=True):
    """
    Verify whether cluster_id is present in cluster list output.
    """
    cluster_ids = []
    if isinstance(cluster_list, list):
        for item in cluster_list:
            if isinstance(item, str):
                cluster_ids.append(item)
            elif isinstance(item, dict):
                cluster_ids.append(
                    item.get("cluster_id") or item.get("id") or str(item)
                )
    elif isinstance(cluster_list, dict):
        cluster_ids = list(cluster_list.keys())
    elif isinstance(cluster_list, str):
        cluster_ids = [
            line.strip() for line in cluster_list.splitlines() if line.strip()
        ]

    present = cluster_id in cluster_ids or any(
        cluster_id in str(c) for c in cluster_ids
    )
    log.info(
        f"Cluster ids found: {cluster_ids}; looking for {cluster_id}; "
        f"present={present}"
    )
    if expect_present and not present:
        raise TestExecError(
            f"Expected cluster {cluster_id} in list, but it was not found"
        )
    if not expect_present and present:
        raise TestExecError(
            f"Cluster {cluster_id} still present in list after deletion"
        )
    return present


def list_smb_orch_services(fmt="json"):
    """
    List SMB services from ceph orch.
    """
    log.info("Listing SMB services via ceph orch ls")
    cmd = f"ceph orch ls --service-type smb --format {fmt}"
    out = utils.exec_shell_cmd(cmd)
    if out is False:
        raise TestExecError("Failed to list SMB services via ceph orch ls")
    log.info(f"SMB orch ls output: {out}")
    if fmt == "json":
        if not out or not str(out).strip():
            return []
        try:
            parsed = json.loads(out)
            return parsed if parsed else []
        except (TypeError, json.JSONDecodeError):
            return out
    return out


def list_smb_orch_daemons(fmt="json"):
    """
    List SMB daemons from ceph orch ps.
    """
    log.info("Listing SMB daemons via ceph orch ps")
    cmd = f"ceph orch ps --daemon-type smb --format {fmt}"
    out = utils.exec_shell_cmd(cmd)
    if out is False:
        # Empty/no matching daemons can still be a valid post-delete state.
        log.warning("ceph orch ps for smb returned failure/empty output")
        return []
    log.info(f"SMB orch ps output: {out}")
    if fmt == "json":
        if not out or not str(out).strip():
            return []
        try:
            parsed = json.loads(out)
            return parsed if parsed else []
        except (TypeError, json.JSONDecodeError):
            return out
    return out


def _smb_service_matches_cluster(entry, cluster_id):
    """
    Return True if orch service/daemon entry belongs to cluster_id.
    Typical service_name: smb.<cluster_id>
    """
    if isinstance(entry, str):
        return cluster_id in entry
    if not isinstance(entry, dict):
        return cluster_id in str(entry)

    service_name = entry.get("service_name") or entry.get("service_id") or ""
    daemon_name = entry.get("daemon_name") or entry.get("daemon_id") or ""
    service_id = entry.get("service_id") or ""
    candidates = [service_name, daemon_name, service_id, str(entry)]
    expected = f"smb.{cluster_id}"
    return any(
        expected == c
        or c.endswith(f".{cluster_id}")
        or cluster_id == c
        or cluster_id in c
        for c in candidates
        if c
    )


def get_smb_service_daemon_status(cluster_id):
    """
    Return (service_available, daemon_available) for cluster_id.
    """
    services = list_smb_orch_services()
    daemons = list_smb_orch_daemons()
    service_available = False
    daemon_available = False

    if isinstance(services, list):
        service_available = any(
            _smb_service_matches_cluster(svc, cluster_id) for svc in services
        )
    elif isinstance(services, str):
        service_available = cluster_id in services

    if isinstance(daemons, list):
        daemon_available = any(
            _smb_service_matches_cluster(dmn, cluster_id) for dmn in daemons
        )
    elif isinstance(daemons, str):
        daemon_available = cluster_id in daemons

    log.info(
        f"SMB orch status for cluster {cluster_id}: "
        f"service_available={service_available}, "
        f"daemon_available={daemon_available}"
    )
    return service_available, daemon_available


def is_smb_service_available(cluster_id):
    """
    Available only when both service and daemon are present.
    """
    service_available, daemon_available = get_smb_service_daemon_status(cluster_id)
    available = service_available and daemon_available
    log.info(
        f"SMB availability for cluster {cluster_id}: available={available} "
        f"(service AND daemon)"
    )
    return available


def is_smb_service_unavailable(cluster_id):
    """
    Not available only when both service and daemon are absent.
    """
    service_available, daemon_available = get_smb_service_daemon_status(cluster_id)
    unavailable = (not service_available) and (not daemon_available)
    log.info(
        f"SMB unavailability for cluster {cluster_id}: unavailable={unavailable} "
        f"(service AND daemon absent)"
    )
    return unavailable


def verify_smb_service_available(
    cluster_id, expect_present=True, retry_count=12, retry_interval=10
):
    """
    Verify SMB service availability for a cluster via ceph orch.

    available = service available AND daemon available
    not available = service not available AND daemon not available

    Args:
        cluster_id(str): SMB cluster id
        expect_present(bool): True if service should exist, False if removed
        retry_count(int): Number of retries while waiting for orch state
        retry_interval(int): Seconds between retries
    """
    for attempt in range(1, retry_count + 1):
        service_available, daemon_available = get_smb_service_daemon_status(cluster_id)
        available = service_available and daemon_available
        unavailable = (not service_available) and (not daemon_available)
        log.info(
            f"Attempt {attempt}/{retry_count}: cluster={cluster_id}, "
            f"service_available={service_available}, "
            f"daemon_available={daemon_available}, "
            f"available={available}, unavailable={unavailable}, "
            f"expect_present={expect_present}"
        )
        if expect_present and available:
            log.info(f"SMB service and daemon for cluster {cluster_id} are available")
            return True
        if not expect_present and unavailable:
            log.info(
                f"SMB service and daemon for cluster {cluster_id} are not available"
            )
            return True
        if attempt < retry_count:
            time.sleep(retry_interval)

    service_available, daemon_available = get_smb_service_daemon_status(cluster_id)
    if expect_present:
        raise TestExecError(
            f"SMB service/daemon for cluster {cluster_id} not fully available "
            f"(service_available={service_available}, "
            f"daemon_available={daemon_available})"
        )
    raise TestExecError(
        f"SMB service/daemon for cluster {cluster_id} still present after "
        f"deletion (service_available={service_available}, "
        f"daemon_available={daemon_available})"
    )


def build_rgw_share_resources(
    cluster_id,
    share_id,
    share_name,
    bucket_name,
    credential_id,
    user_id,
    access_key,
    secret_key,
):
    """
    Build declarative SMB resource list for RGW-backed share + credential.
    """
    return [
        {
            "resource_type": "ceph.smb.rgw.credential",
            "rgw_credential_id": credential_id,
            "user_id": user_id,
            "access_key_id": access_key,
            "secret_access_key": secret_key,
        },
        {
            "resource_type": "ceph.smb.share",
            "cluster_id": cluster_id,
            "share_id": share_id,
            "name": share_name,
            "rgw": {
                "bucket": bucket_name,
                "credential_ref": credential_id,
            },
        },
    ]


def build_remove_rgw_share_resources(
    cluster_id, share_id, credential_id, user_id, access_key, secret_key
):
    """
    Build declarative SMB resource list to remove share and credential.

    Credential specification must include the user_id, access_key and
    secret_key associated with the RGW credential.
    """
    return [
        {
            "resource_type": "ceph.smb.share",
            "cluster_id": cluster_id,
            "share_id": share_id,
            "intent": "removed",
        },
        {
            "resource_type": "ceph.smb.rgw.credential",
            "rgw_credential_id": credential_id,
            "intent": "removed",
            "user_id": user_id,
            "access_key_id": access_key,
            "secret_access_key": secret_key,
        },
    ]


def write_smb_resources_file(resources, file_path=None):
    """
    Write SMB resource list to a YAML file.
    Args:
        resources(list): List of SMB resource dicts
        file_path(str): Optional path; tempfile used if not provided
    Returns:
        str: Path to written YAML file
    """
    if file_path is None:
        fd, file_path = tempfile.mkstemp(prefix="smb_resources_", suffix=".yaml")
        os.close(fd)
    with open(file_path, "w") as fout:
        yaml.safe_dump(resources, fout, default_flow_style=False, sort_keys=False)
    log.info(f"SMB resources written to: {file_path}")
    log.info(f"SMB resources content:\n{yaml.safe_dump(resources, sort_keys=False)}")
    return file_path


def apply_smb_resources(resources, file_path=None, cleanup_file=True):
    """
    Apply SMB resources using declarative ceph smb apply.
    Args:
        resources(list): List of SMB resource dicts
        file_path(str): Optional YAML path to write/use
        cleanup_file(bool): Delete temp YAML after apply
    """
    resource_file = write_smb_resources_file(resources, file_path=file_path)
    try:
        log.info(f"Applying SMB resources from {resource_file}")
        cmd = f"ceph smb apply -i {resource_file}"
        out = utils.exec_shell_cmd(cmd)
        if out is False:
            raise TestExecError("ceph smb apply failed")
        log.info(f"ceph smb apply output: {out}")
        return out
    finally:
        if cleanup_file and os.path.exists(resource_file):
            os.remove(resource_file)


def list_smb_shares(cluster_id, fmt="json"):
    """
    List SMB shares for a cluster.
    Args:
        cluster_id(str): SMB cluster id
        fmt(str): Output format (json|yaml)
    Returns:
        Parsed JSON (list/dict) or raw string for non-json formats
    """
    log.info(f"Listing SMB shares for cluster: {cluster_id}")
    cmd = f"ceph smb share ls {cluster_id} --format={fmt}"
    out = utils.exec_shell_cmd(cmd)
    if out is False:
        raise TestExecError(f"Failed to list SMB shares for cluster {cluster_id}")
    log.info(f"SMB share list output: {out}")
    if fmt == "json":
        try:
            return json.loads(out)
        except (TypeError, json.JSONDecodeError):
            return out
    return out


def show_smb_share(cluster_id, share_id, fmt="json"):
    """
    Show a specific SMB share resource.
    """
    resource_name = f"ceph.smb.share.{cluster_id}.{share_id}"
    log.info(f"Showing SMB share resource: {resource_name}")
    cmd = f"ceph smb show {resource_name} --format={fmt}"
    out = utils.exec_shell_cmd(cmd)
    if out is False:
        raise TestExecError(f"Failed to show SMB share {resource_name}")
    log.info(f"SMB share show output: {out}")
    if fmt == "json":
        try:
            return json.loads(out)
        except (TypeError, json.JSONDecodeError):
            return out
    return out


def remove_smb_share(cluster_id, share_id):
    """
    Remove an SMB share using imperative command.
    """
    log.info(f"Removing SMB share {share_id} from cluster {cluster_id}")
    cmd = f"ceph smb share rm {cluster_id} {share_id}"
    out = utils.exec_shell_cmd(cmd)
    if out is False:
        raise TestExecError(
            f"Failed to remove SMB share {share_id} from cluster {cluster_id}"
        )
    log.info(f"SMB share remove output: {out}")
    return out


def verify_share_in_list(share_list, share_id, expect_present=True):
    """
    Verify whether share_id is present in share list output.
    """
    share_ids = []
    if isinstance(share_list, list):
        for item in share_list:
            if isinstance(item, str):
                share_ids.append(item)
            elif isinstance(item, dict):
                share_ids.append(item.get("share_id") or item.get("id") or str(item))
    elif isinstance(share_list, dict):
        share_ids = list(share_list.keys())
    elif isinstance(share_list, str):
        share_ids = [line.strip() for line in share_list.splitlines() if line.strip()]

    present = share_id in share_ids or any(share_id in str(s) for s in share_ids)
    log.info(f"Share ids found: {share_ids}; looking for {share_id}; present={present}")
    if expect_present and not present:
        raise TestExecError(f"Expected share {share_id} in list, but it was not found")
    if not expect_present and present:
        raise TestExecError(f"Share {share_id} still present in list after deletion")
    return present
