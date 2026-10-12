#!/usr/bin/env bash
#
# Regenerates the gRPC stubs in lib/wiggle/proto/ from the vendored proto (proto/wiggle.proto).
# The proto is the wire contract with the Wiggle server; keep proto/wiggle.proto in sync with the
# canonical copy in the server repo (wiggle: proto/src/main/proto/wiggle.proto) and re-run this.
#
#   gem install grpc-tools     # (or: bundle install)
#   ./codegen.sh
set -euo pipefail
cd "$(dirname "$0")"

PROTO_DIR="proto"
OUT="lib/wiggle/proto"

grpc_tools_ruby_protoc \
    -I "$PROTO_DIR" \
    --ruby_out="$OUT" \
    --grpc_out="$OUT" \
    "$PROTO_DIR/wiggle.proto"

# the generated service file requires its sibling 'wiggle_pb' by bare name; make it relative so it
# loads from inside the gem without putting lib/wiggle/proto on the load path.
perl -pi -e "s/^require 'wiggle_pb'/require_relative 'wiggle_pb'/" "$OUT"/wiggle_services_pb.rb
echo "regenerated stubs in $OUT"
