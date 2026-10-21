# Minimal image to run firehose_producer.py without depending on an open
# terminal (alternative: a systemd unit on the host, see scripts/).
FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY firehose_producer.py .

# The image's default user isn't explicitly root, but we still create an
# unprivileged user so the process never runs as root on the host.
RUN useradd --create-home --shell /usr/sbin/nologin appuser
USER appuser

# Required variables (MSK_BOOTSTRAP_SERVERS, AWS_REGION) are passed at
# runtime, never baked into the image:
#   docker run --rm \
#     -e MSK_BOOTSTRAP_SERVERS="b-1...:9098,b-2...:9098" \
#     -e AWS_REGION="us-east-2" \
#     -e AWS_ACCESS_KEY_ID=... -e AWS_SECRET_ACCESS_KEY=... -e AWS_SESSION_TOKEN=... \
#     firehose-producer
ENTRYPOINT ["python3", "firehose_producer.py"]
