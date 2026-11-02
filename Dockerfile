# ingest-hub as one image: the runtime and every built-in adapter.
FROM python:3.14-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    # No .pyc files, so the image runs with a read-only root filesystem.
    PYTHONDONTWRITEBYTECODE=1 \
    DATA_DIR=/data

WORKDIR /app
COPY backend/pyproject.toml ./
COPY backend/src ./src
RUN pip install .

# The commit the image was built from. Declared late, so a new commit does not invalidate the
# layers above.
ARG REVISION=""
ENV REVISION=$REVISION

# Unprivileged, so everything written into the mounted data directory belongs to a real account
# on the host rather than to root.
RUN mkdir -p /data && chown 1000:1000 /data
USER 1000:1000

HEALTHCHECK --interval=60s --timeout=10s --start-period=60s --retries=3 \
    CMD ["python", "-m", "ingest_hub.healthcheck"]
ENTRYPOINT ["ingest-hub"]
CMD ["run"]
