#!/bin/sh
# Creates/updates local multi-arch Docker manifest lists after both
# platform-specific images have been built by the Jib executions.
VERSION=$1
docker manifest rm dazzleduck/dazzleduck:${VERSION} 2>/dev/null || true
docker manifest rm dazzleduck/dazzleduck:latest    2>/dev/null || true
docker manifest create dazzleduck/dazzleduck:${VERSION} \
    dazzleduck/dazzleduck:${VERSION}-arm64 \
    dazzleduck/dazzleduck:${VERSION}-amd64
docker manifest create dazzleduck/dazzleduck:latest \
    dazzleduck/dazzleduck:latest-arm64 \
    dazzleduck/dazzleduck:latest-amd64
echo "Manifest lists created: dazzleduck/dazzleduck:${VERSION} and dazzleduck/dazzleduck:latest"
