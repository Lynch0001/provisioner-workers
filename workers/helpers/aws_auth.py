# aws_auth.py
import os
import boto3
from botocore.exceptions import BotoCoreError, ClientError

DEFAULT_REGION = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "us-east-1"

def _sanitize_env_creds():
    # Strip accidental quotes/whitespace that break signing
    for k in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"):
        v = os.getenv(k)
        if v and v.strip() != v:
            os.environ[k] = v.strip()

def build_boto3_session(purpose="generic") -> boto3.Session:
    """
    Build a boto3 Session with precedence:
      1) IRSA / EC2 role / ECS task role (metadata) if present
      2) Named profile via AWS_PROFILE (SSO or static)
      3) Raw env creds (AWS_ACCESS_KEY_ID/SECRET[/TOKEN])
    If AWS_ROLE_ARN is set:
      - Use AssumeRoleWithWebIdentity when AWS_WEB_IDENTITY_TOKEN_FILE exists
      - Else AssumeRole using whatever base creds were resolved in steps above
    """
    _sanitize_env_creds()

    profile = os.getenv("AWS_PROFILE")
    region  = DEFAULT_REGION

    # Step 1/2/3: base session (this will auto-pick up metadata creds if on EC2/EKS/ECS)
    base = boto3.Session(profile_name=profile, region_name=region) if profile else boto3.Session(region_name=region)

    role_arn = os.getenv("AWS_ROLE_ARN")
    web_identity = os.getenv("AWS_WEB_IDENTITY_TOKEN_FILE")

    if not role_arn:
        return base

    sts = base.client("sts", region_name=region)

    # IRSA path
    if web_identity and os.path.isfile(web_identity):
        with open(web_identity, "r", encoding="utf-8") as f:
            token = f.read().strip()
        kwargs = {
            "RoleArn": role_arn,
            "RoleSessionName": os.getenv("AWS_ROLE_SESSION_NAME", f"{purpose}-irsa"),
            "WebIdentityToken": token,
        }
        external_id = os.getenv("AWS_EXTERNAL_ID")
        if external_id:
            kwargs["ExternalId"] = external_id

        resp = sts.assume_role_with_web_identity(**kwargs)
    else:
        # AssumeRole with base creds (env/profile/instance role)
        kwargs = {
            "RoleArn": role_arn,
            "RoleSessionName": os.getenv("AWS_ROLE_SESSION_NAME", f"{purpose}-assume"),
        }
        external_id = os.getenv("AWS_EXTERNAL_ID")
        if external_id:
            kwargs["ExternalId"] = external_id
        resp = sts.assume_role(**kwargs)

    c = resp["Credentials"]
    return boto3.Session(
        aws_access_key_id=c["AccessKeyId"],
        aws_secret_access_key=c["SecretAccessKey"],
        aws_session_token=c["SessionToken"],
        region_name=region,
    )


def debug_identity(sess: boto3.Session, logger, purpose="verify"):
    try:
        sts = sess.client("sts")
        ident = sts.get_caller_identity()
        creds = sess.get_credentials()
        method = getattr(creds, "method", "unknown") if creds else "none"
        logger.info(
            "[aws-auth] purpose=%s account=%s arn=%s method=%s region=%s",
            purpose, ident.get("Account"), ident.get("Arn"), method, sess.region_name
        )
        return ident
    except Exception as e:
        logger.error("[aws-auth] failed to get caller identity: %s", e)
        raise