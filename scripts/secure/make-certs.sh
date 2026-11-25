#!/usr/bin/env bash
# Creates everything the secure stack needs, in ./secure (ignored by git):
#   ca.crt            the certificate clients use to check the broker
#   broker.p12        the broker's key and certificate
#   .env              one random password per user
# Local demo only: in production the keys come from your PKI and the passwords
# from a secrets store.
set -euo pipefail

OUT="${1:-secure}"
mkdir -p "$OUT"
cd "$OUT"

if [ -f .env ]; then
  echo "$OUT/.env exists already; delete the folder to start again" >&2
  exit 1
fi

password() { openssl rand -hex 16; }
KEYSTORE_PASSWORD="$(password)"

# 1. A private certificate authority
openssl req -x509 -newkey rsa:2048 -nodes -days 365 \
  -keyout ca.key -out ca.crt -subj "/CN=safety-gate-demo-ca" 2>/dev/null

# 2. A broker certificate, valid for the names clients use to reach it
openssl req -newkey rsa:2048 -nodes -keyout broker.key -out broker.csr -subj "/CN=kafka" 2>/dev/null
printf "subjectAltName=DNS:kafka,DNS:localhost,IP:127.0.0.1\n" > san.cnf
openssl x509 -req -in broker.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
  -days 365 -extfile san.cnf -out broker.crt 2>/dev/null
openssl pkcs12 -export -name broker -in broker.crt -inkey broker.key -certfile ca.crt \
  -out broker.p12 -passout "pass:${KEYSTORE_PASSWORD}"
chmod 644 broker.p12 ca.crt            # the broker runs as another user inside its container
rm -f broker.csr san.cnf ca.srl

# 3. One password per user
cat > .env <<ENV
KEYSTORE_PASSWORD=${KEYSTORE_PASSWORD}
ADMIN_PASSWORD=$(password)
CLIENT_PASSWORD=$(password)
GATE_PASSWORD=$(password)
AUDIT_PASSWORD=$(password)
OPERATOR_PASSWORD=$(password)
MODEL_PASSWORD=$(password)
ENV
chmod 600 .env ca.key broker.key

echo "created $OUT/ca.crt, $OUT/broker.p12 and $OUT/.env"
