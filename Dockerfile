FROM python:3.12-slim

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY apps ./apps
COPY packages ./packages
COPY scripts ./scripts

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Non-root: the gateway holds many long-lived sockets but needs no
# privileged ports (Nginx terminates :80).
RUN useradd --create-home --uid 10001 relay && chown -R relay:relay /app
USER relay

EXPOSE 8000
CMD ["uvicorn", "apps.gateway.main:app", "--host", "0.0.0.0", "--port", "8000"]
