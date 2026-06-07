FROM python:3.12-slim-bookworm

ARG DOLT_VERSION=2.1.2
ARG DOLT_REMOTE=deeleeramone/sec-company-facts
ARG BAKE_DATA=false

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential curl ca-certificates tini \
    && rm -rf /var/lib/apt/lists/*

RUN ARCH="$(dpkg --print-architecture)" \
    && case "$ARCH" in \
         amd64)  DOLT_ARCH=amd64 ;; \
         arm64)  DOLT_ARCH=arm64 ;; \
         *) echo "unsupported arch $ARCH"; exit 1 ;; \
       esac \
    && curl -fsSL "https://github.com/dolthub/dolt/releases/download/v${DOLT_VERSION}/dolt-linux-${DOLT_ARCH}.tar.gz" \
         -o /tmp/dolt.tgz \
    && tar -xzf /tmp/dolt.tgz -C /usr/local --strip-components=1 \
    && rm /tmp/dolt.tgz \
    && dolt version

WORKDIR /app

COPY docker/entrypoint.sh docker/dolt-sql-server.yaml ./

# Provider comes from PyPI; the serving deps are installed explicitly. The local
# serving code (sec_app) is copied below — it imports openbb_sec from the
# installed package, never from local source.
RUN pip install \
        "openbb-sec>=1.6.1" \
        "uvicorn[standard]>=0.30" \
        "fastapi>=0.110" \
        "pymysql>=1.1" \
        "pyarrow>=17" \
        "pandas>=2.0" \
        "psutil>=5.9" \
    && chmod +x /app/entrypoint.sh

COPY sec_app ./sec_app

RUN if [ "$BAKE_DATA" = "true" ]; then \
        mkdir -p /data \
        && dolt clone "$DOLT_REMOTE" /data/sec_company_facts ; \
    fi

RUN useradd -u 1000 -m -s /bin/bash app \
    && mkdir -p /data \
    && chown -R app:app /app /data
USER app

ENV PYTHONPATH=/app \
    SEC_DATA_DIR=/data \
    DOLT_SQL_HOST=127.0.0.1 \
    DOLT_SQL_PORT=3306 \
    DOLT_SQL_DB=sec_company_facts \
    DOLT_SQL_USER=root \
    DOLT_SQL_PASSWORD="" \
    DOLT_REMOTE=${DOLT_REMOTE} \
    GOMEMLIMIT=1536MiB \
    GOGC=30 \
    WIDGETS_HOST=0.0.0.0 \
    WIDGETS_PORT=8000 \
    WIDGETS_WORKERS=1

EXPOSE 8000
VOLUME ["/data"]

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["/app/entrypoint.sh"]
