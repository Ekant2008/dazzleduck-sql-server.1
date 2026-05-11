#!/bin/sh
# Post-push utility: creates or updates the multi-arch manifest lists on the registry
# after both platform images have been pushed.
#
# Usage (run after `docker push` for both platform images):
#   ./docker/manifest.sh <version>
#   ./docker/manifest.sh 0.2.1
set -e

IMAGE="dazzleduck/dazzleduck"
VERSION=$1

if [ -z "$VERSION" ]; then
    echo "Usage: $0 <version>" >&2
    exit 1
fi

create_manifest() {
    TAG=$1
    IMAGES="${IMAGE}:${TAG}-arm64"

    # Include amd64 only if the tag exists in the registry
    if docker manifest inspect "${IMAGE}:${TAG}-amd64" >/dev/null 2>&1; then
        IMAGES="${IMAGES} ${IMAGE}:${TAG}-amd64"
    fi

    # --amend updates an existing manifest or creates it if absent
    docker manifest create --amend "${IMAGE}:${TAG}" ${IMAGES}
    echo "Manifest created: ${IMAGE}:${TAG} [ ${IMAGES} ]"
}

create_manifest "${VERSION}"
create_manifest "latest"
