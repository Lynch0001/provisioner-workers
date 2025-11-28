# workers/worker_celery.py
from celery import Celery
from kombu import Queue, Exchange
import os


celery = Celery(
    "tasks-workers",
    broker="amqp://user:pass@localhost:5672//",
    backend="db+postgresql://postgres:password@localhost:5432/provisioner",
    include=[
        # files containing create tasks
        "workers.tasks.create.create_core_project_tasks",
        "workers.tasks.create.create_core_ns_tasks",
        "workers.tasks.create.create_core_keda_tasks",
        "workers.tasks.create.create_core_secrets_tasks",
        "workers.tasks.create.create_core_networking_tasks",
        "workers.tasks.create.create_core_keycloak_tasks",
        "workers.tasks.create.create_core_registries_tasks",
        "workers.tasks.create.create_zookeeper_tasks",
        "workers.tasks.create.create_artemis_tasks",
        "workers.tasks.create.create_kafka_tasks",
        "workers.tasks.create.create_helm_tasks",
        "workers.tasks.create.create_argo_tasks",
        "workers.tasks.create.create_finalize_tasks",
        # files containing destroy tasks
        "workers.tasks.destroy.destroy_core_project_tasks",
        "workers.tasks.destroy.destroy_core_ns_tasks",
        "workers.tasks.destroy.destroy_core_keda_tasks",
        "workers.tasks.destroy.destroy_core_secrets_tasks",
        "workers.tasks.destroy.destroy_core_networking_tasks",
        "workers.tasks.destroy.destroy_core_keycloak_tasks",
        "workers.tasks.destroy.destroy_core_registries_tasks",
        "workers.tasks.destroy.destroy_zookeeper_tasks",
        "workers.tasks.destroy.destroy_artemis_tasks",
        "workers.tasks.destroy.destroy_kafka_tasks",
        "workers.tasks.destroy.destroy_helm_tasks",
        "workers.tasks.destroy.destroy_argo_tasks",
        "workers.tasks.destroy.destroy_finalize_tasks"
    ],
)

celery.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    task_acks_late=True,
    worker_prefetch_multiplier=1,  # prefetch=1 ensures strict serial with concurrency=1
    enable_utc=True,
    timezone=os.getenv("CELERY_TIMEZONE", "UTC"),
    task_create_missing_queues=True,
    task_default_queue = "provisioner"
)

# One queue per component (match the names used in routes)
celery.conf.task_queues = (
    Queue("core", Exchange("core"), routing_key="core"),
    Queue("artemis", Exchange("artemis"), routing_key="artemis"),
    Queue("kafka", Exchange("kafka"), routing_key="kafka"),
    Queue("zookeeper", Exchange("zookeeper"), routing_key="zookeeper"),
    Queue("helm", Exchange("helm"), routing_key="helm"),
    Queue("argo", Exchange("argo"), routing_key="argo"),
)

celery.conf.task_routes = {
    # Collection of all tasks worker will execute from task files with matching component queues
    # Core-Project
    "workers.tasks.create.create_core_project": {"queue": "core"},
    "workers.tasks.create.create_core_project_mr": {"queue": "core"},
    "workers.tasks.create.core_project_wait_atlantis_plan": {"queue": "core"},
    "workers.tasks.create.deploy_core_project": {"queue": "core"},
    "workers.tasks.create.core_project_wait_atlantis_apply": {"queue": "core"},
    "workers.tasks.create.verify_core_project": {"queue": "core"},
    "workers.tasks.create.merge_core_project_mr": {"queue": "core"},
    # Core-NS
    "workers.tasks.create.create_core_ns": {"queue": "core"},
    "workers.tasks.create.create_core_ns_mr": {"queue": "core"},
    "workers.tasks.create.deploy_core_ns": {"queue": "core"},
    "workers.tasks.create.verify_core_ns": {"queue": "core"},
    "workers.tasks.create.merge_core_ns_mr": {"queue": "core"},
    # Core-Secrets
    "workers.tasks.create.create_core_secrets": {"queue": "core"},
    "workers.tasks.create.create_core_secrets_mr": {"queue": "core"},
    "workers.tasks.create.deploy_core_secrets": {"queue": "core"},
    "workers.tasks.create.verify_core_secrets": {"queue": "core"},
    "workers.tasks.create.merge_core_secrets_mr": {"queue": "core"},
    # Core-Registry
    "workers.tasks.create.create_core_registries": {"queue": "core"},
    "workers.tasks.create.create_core_registries_mr": {"queue": "core"},
    "workers.tasks.create.deploy_core_registries": {"queue": "core"},
    "workers.tasks.create.verify_core_registries": {"queue": "core"},
    "workers.tasks.create.merge_core_registries_mr": {"queue": "core"},
    # Core-Keda
    "workers.tasks.create.create_core_keda": {"queue": "core"},
    "workers.tasks.create.create_core_keda_mr": {"queue": "core"},
    "workers.tasks.create.deploy_core_keda": {"queue": "core"},
    "workers.tasks.create.verify_core_keda": {"queue": "core"},
    "workers.tasks.create.merge_core_keda_mr": {"queue": "core"},
    # Core-Networking
    "workers.tasks.create.create_core_networking": {"queue": "core"},
    "workers.tasks.create.create_core_networking_mr": {"queue": "core"},
    "workers.tasks.create.deploy_core_networking": {"queue": "core"},
    "workers.tasks.create.verify_core_networking": {"queue": "core"},
    "workers.tasks.create.merge_core_networking_mr": {"queue": "core"},
    # Core-Keycloak
    "workers.tasks.create.create_core_keycloak": {"queue": "core"},
    "workers.tasks.create.create_core_keycloak_mr": {"queue": "core"},
    "workers.tasks.create.deploy_core_keycloak": {"queue": "core"},
    "workers.tasks.create.verify_core_keycloak": {"queue": "core"},
    "workers.tasks.create.merge_core_keycloak_mr": {"queue": "core"},
    # ZooKeeper
    "workers.tasks.create.create_zookeeper_cluster": {"queue": "zookeeper"},
    "workers.tasks.create.create_zookeeper_mr": {"queue": "zookeeper"},
    "workers.tasks.create.deploy_zookeeper_cluster": {"queue": "zookeeper"},
    "workers.tasks.create.verify_zookeeper_cluster": {"queue": "zookeeper"},
    "workers.tasks.create.merge_zookeeper_mr": {"queue": "zookeeper"},
    # Artemis
    "workers.tasks.create.create_artemis_cluster": {"queue": "artemis"},
    "workers.tasks.create.create_artemis_mr": {"queue": "artemis"},
    "workers.tasks.create.deploy_artemis_cluster": {"queue": "artemis"},
    "workers.tasks.create.verify_artemis_cluster": {"queue": "artemis"},
    "workers.tasks.create.merge_artemis_mr": {"queue": "artemis"},
    # Kafka
    "workers.tasks.create.create_kafka_cluster": {"queue": "kafka"},
    "workers.tasks.create.create_kafka_mr": {"queue": "kafka"},
    "workers.tasks.create.deploy_kafka_cluster": {"queue": "kafka"},
    "workers.tasks.create.verify_kafka_cluster": {"queue": "kafka"},
    "workers.tasks.create.merge_kafka_mr": {"queue": "kafka"},
    # Helm
    "workers.tasks.create.create_helm_values": {"queue": "helm"},
    "workers.tasks.create.create_helm_values_mr": {"queue": "helm"},
    "workers.tasks.create.merge_helm_values_mr": {"queue": "helm"},
    # Argo
    "workers.tasks.create.create_argo_apps": {"queue": "argo"},
    "workers.tasks.create.create_argo_apps_mr": {"queue": "argo"},
    "workers.tasks.create.deploy_argo_apps": {"queue": "argo"},
    "workers.tasks.create.verify_argo_apps": {"queue": "argo"},
    "workers.tasks.create.merge_argo_apps_mr": {"queue": "argo"},

    # TODO Add atlantis plan and apply wait tasks after tested
    # TODO Add Verify Applications Deployed and Ready tasks after verify tested

    # Finalizer
    "workers.tasks.create.finalize_workflow": {"queue": "provisioner"},

    # TODO Add Destroy tasks - reorder in orchestrator workflow -> destroy, delete, verify
}