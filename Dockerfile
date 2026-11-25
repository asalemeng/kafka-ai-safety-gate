# One image for every component. The command picks the program:
#   docker run safety-gate safety_gate.gate          (default)
#   docker run safety-gate safety_gate.audit_sink
#   docker run safety-gate safety_gate.switchctl disable --reason "incident" --by oncall
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /app

COPY requirements.txt ./
RUN pip install -r requirements.txt

COPY pyproject.toml README.md ./
COPY src ./src
COPY examples ./examples
RUN pip install --no-deps . \
 && useradd --create-home --uid 10001 gate \
 && mkdir -p /var/lib/safety-gate/keys /var/lib/safety-gate/checkpoints \
 && chown -R gate:gate /var/lib/safety-gate

USER gate
ENTRYPOINT ["python", "-m"]
CMD ["safety_gate.gate"]
