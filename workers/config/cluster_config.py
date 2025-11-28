# cluster_config.py
from kubernetes import config as k8s_config

from workers.config.app_config import KUBECONFIG_FILE_PATH

# 1. When running *outside* the cluster (dev laptop, CI/CD runner):
#    Uses ~/.kube/config by default.
k8s_config.load_kube_config()

# Or explicitly, if using different name:
k8s_config.load_kube_config(config_file=KUBECONFIG_FILE_PATH)

# 2. When running *inside* a pod in the cluster:
#    Uses service account token mounted in /var/run/secrets/kubernetes.io/serviceaccount
#k8s_config.load_incluster_config()