# aws_networking_verify_handler.py (snippets)
import logging

import boto3
from botocore.exceptions import ClientError
from workers.helpers.aws_auth import build_boto3_session, debug_identity

logger = logging.getLogger(__name__)

def _elb_client(sess):      return sess.client("elbv2")          # NLB/ALB
def _route53_client(sess):  return sess.client("route53")

def check_nlbs_exist(sess, lb_names: list[str]) -> tuple[dict, list[str]]:
    elb = _elb_client(sess)
    found_dns = {}
    missing = []
    # API allows <=20 names per call
    for i in range(0, len(lb_names), 20):
        chunk = lb_names[i:i+20]
        try:
            resp = elb.describe_load_balancers(Names=chunk)
            for lb in resp.get("LoadBalancers", []):
                found_dns[lb["LoadBalancerName"]] = lb.get("DNSName")
            # anything not returned is missing
            for name in chunk:
                if name not in found_dns:
                    missing.append(name)
        except ClientError as ce:
            if ce.response["Error"]["Code"] in ("LoadBalancerNotFound", "ValidationError"):
                # collect all missing in the chunk
                missing.extend(chunk)
            else:
                raise
    return found_dns, missing

def check_dns_records(sess, hosted_zone_id: str, names: list[str]) -> tuple[set[str], list[str]]:
    r53 = _route53_client(sess)
    existing = set()
    missing = []
    # list once; for large zones you might need pagination
    paginator = r53.get_paginator("list_resource_record_sets")
    for page in paginator.paginate(HostedZoneId=hosted_zone_id):
        for rr in page.get("ResourceRecordSets", []):
            existing.add(rr["Name"].rstrip("."))  # normalize
    for n in names:
        if n not in existing and (n + ".") not in existing:
            missing.append(n)
    return existing, missing

def verify_networking(project_id: str, hosted_zone_id: str, lb_names: list, record_set: list, region: str | None = None) -> dict:
    sess = build_boto3_session(purpose="network-verify")
    debug_identity(sess, logger, purpose="network-verify")

    # Optionally override region on the clients if you pass region here
    if region and sess.region_name != region:
        # Rebuild a region-specific session without changing creds
        sess = boto3.Session(
            aws_access_key_id=sess.get_credentials().access_key,
            aws_secret_access_key=sess.get_credentials().secret_key,
            aws_session_token=sess.get_credentials().token,
            region_name=region,
        )

    lb_dns_by_name, lb_missing = check_nlbs_exist(sess, lb_names)
    _, dns_missing = check_dns_records(sess, hosted_zone_id, record_set)

    return {
        "project_id": project_id,
        "region": sess.region_name,
        "nlb": {
            "expected": lb_names,
            "found": lb_dns_by_name,
            "missing": lb_missing,
            "ready": len(lb_missing) == 0,
        },
        "dns": {
            "expected": record_set,
            "missing": dns_missing,
            "ready": len(dns_missing) == 0,
        },
        "ready": len(lb_missing) == 0 and len(dns_missing) == 0,
    }