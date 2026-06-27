#!/usr/bin/env bash
set -euo pipefail

out_dir="${1:-examples/certs}"
mkdir -p "$out_dir"

openssl req -x509 -newkey rsa:2048 -nodes -days 365 \
  -keyout "$out_dir/ca.key" \
  -out "$out_dir/ca.crt" \
  -subj "/CN=remote-dev-ca"

openssl req -newkey rsa:2048 -nodes \
  -keyout "$out_dir/server.key" \
  -out "$out_dir/server.csr" \
  -subj "/CN=localhost"

openssl x509 -req -in "$out_dir/server.csr" \
  -CA "$out_dir/ca.crt" \
  -CAkey "$out_dir/ca.key" \
  -CAcreateserial \
  -out "$out_dir/server.crt" \
  -days 365

rm -f "$out_dir/server.csr" "$out_dir/ca.srl"

echo "Wrote:"
echo "  $out_dir/ca.crt"
echo "  $out_dir/server.crt"
echo "  $out_dir/server.key"
