FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Install OS dependencies for psycopg2 and git ops
RUN apt-get update && apt-get install -y \
    git curl bash openssh-client libpq-dev gcc \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Copy workers code
WORKDIR /app
COPY . /app

# Default queue and concurrency (can override via env vars)
ENV QUEUES=provisioner,core,zookeeper,kafka,artemis,helm,argo \
    CONCURRENCY=4

CMD ["sh", "-c", "celery -A tasks worker --loglevel=INFO -Q ${QUEUES} --concurrency=${CONCURRENCY}"]