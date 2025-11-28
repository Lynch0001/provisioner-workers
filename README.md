# Provisioner

The **Provisioner** automates the creation of XIBus project environments to a Kubernetes cluster using a FastAPI producer service and Celery workers.
Workflows are orchestrated via RabbitMQ and persisted in PostgreSQL for tracking state, retries, and finalization.
---

# Worker Setup

Execute the following steps, in order, to prepare the provisioner worker for local testing. 

## Create python virtual environment

Note: created with Python 3.11

```bash
python3 -m venv .venv
```
## Install required python packages
```bash
pip3 install -r requirements.txt
```
## Run the worker

Make sure the required RabbitMQ broker and Postgres database have been created and are available.

```bash
celery -A workers.worker_celery:celery worker \
  -Q provisioner,core,zookeeper,kafka,artemis,helm,argo -l INFO --concurrency=10
```