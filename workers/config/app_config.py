import os

APP_LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# --- AWS ---
AWS_ROLE_ARN = os.getenv("AWS_ROLE_ARN", "")
AWS_ROUTE53_ZONE_ID = os.getenv("AWS_ROUTE53_ZONE_ID", "")
AWS_BASE_DOMAIN = os.getenv("AWS_BASE_DOMAIN", "")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
AWS_PROFILE = os.getenv("AWS_PROFILE", "default")

# --- Git/GitLab ---
GITLAB_BASE_URL = os.getenv("GITLAB_BASE_URL", "https://gitlab.com")
GITLAB_TOKEN = os.getenv("GITLAB_TOKEN", "glpat-VKkhQ0OUk6R1jvAwKIi9ZG86MQp1OjJzZDdnCw.01.120bwrd0c")
GITLAB_INFR_PROJECT_ID = os.getenv("GITLAB_INFR_PROJECT_ID", "71208092") # number
GITLAB_HELM_PROJECT_ID = os.getenv("GITLAB_HELM_PROJECT_ID", "74187819") # number

GIT_INFR_REPO_URL = os.getenv("GIT_INFR_REPO_URL", f"https://oauth2:{GITLAB_TOKEN}@gitlab.com/group-lynch/infra-live.git")
GIT_INFR_HELM_URL = os.getenv("GIT_HELM_REPO_URL", f"https://oauth2:{GITLAB_TOKEN}@gitlab.com/group-lynch/demo-helm-charts.git")

GIT_INFR_BASE_BRANCH = os.getenv("GIT_INFR_BASE_BRANCH", "main")
GIT_HELM_BASE_BRANCH = os.getenv("GIT_HELM_BASE_BRANCH", "main")

# --- Merge Request / Git ---
GIT_REBASE_WAIT_SECONDS = int(os.getenv("GIT_REBASE_WAIT_SECONDS", "15"))

# --- Celery retry policy ---
CELERY_RETRY_BACKOFF = int(os.getenv("CELERY_RETRY_BACKOFF", "3"))
CELERY_RETRY_BACKOFF_MAX = int(os.getenv("CELERY_RETRY_BACKOFF_MAX", "60"))
CELERY_RETRY_JITTER = os.getenv("CELERY_RETRY_JITTER", "true").lower() in ("1","true","yes")
CELERY_MAX_RETRIES = int(os.getenv("CELERY_MAX_RETRIES", "6"))

# --- Atlantis ---
ATLANTIS_POLL_SECS = int(os.getenv("ATLANTIS_POLL_SECS", "25"))
ATLANTIS_MAX_MINUTES = int(os.getenv("ATLANTIS_MAX_MINUTES", "40"))
ATLANTIS_USERNAMES = {"atlantis-prod"}

# KUBECONFIG PATH
KUBECONFIG_FILE_PATH = "/Users/timothylynch/.kube/config"

# PATHS
INFR_BASE_PATH_DEMO = os.getenv("INFR_BASE_PATH_DEMO","prod/us-east-1/xib/xib-demo")
INFR_BASE_PATH_ARGO = os.getenv("INFR_BASE_PATH_ARGO","prod/us-east-1/ir/downstream/argocd/xib/xib-demo")

# Active Microservices
ACTIVE_MICROSERVICE_LIST = ["archiver", "client", "console", "datastore", "deliver", "inbound", "ingester", "parser", "preparser", "resubmit", "router", "twingate", "twinning"]

# TODO Add RestInbound