FROM python:3.11-slim-bookworm

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
  && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir --default-timeout=180 --retries=12 -r /app/requirements.txt

COPY . /app

WORKDIR /app/whmcs-goodday-sync

HEALTHCHECK --interval=60s --timeout=10s --start-period=30s --retries=3 \
  CMD python -c "import os,sys; p=os.environ.get('STATE_FILE','/data/state.json'); d=os.path.dirname(p) or '.'; sys.exit(0 if os.path.isdir(d) and os.access(d, os.W_OK) else 1)"

CMD ["python", "-u", "app.py"]
