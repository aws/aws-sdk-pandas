#!/usr/bin/env bash
# verify-layer-size.sh
#
# Build a Lambda layer and assert its UNZIPPED size is below a threshold.
# AWS Lambda enforces a 250 MB limit on the combined unzipped size of all
# layers attached to a function (262144000 bytes). The 3.16.1 release
# breached this in practice because Arrow 22 pulled in ~37 MB of bundled
# ICU libraries; this script catches that kind of regression.
#
# Usage:
#   ./verify-layer-size.sh                 # builds py3.13, threshold 180 MB
#   ./verify-layer-size.sh 3.12            # builds py3.12, threshold 180 MB
#   ./verify-layer-size.sh 3.13 200        # custom threshold in MB
#
# Exit codes:
#   0  layer is at or below the threshold
#   1  layer exceeds the threshold (regression)
#   2  the build itself failed

set -eo pipefail

PYTHON_VERSION="${1:-3.13}"
THRESHOLD_MB="${2:-180}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "${SCRIPT_DIR}")"

pushd "${SCRIPT_DIR}" > /dev/null

echo "==> Building Lambda layer for Python ${PYTHON_VERSION}"
if ! ./build-lambda-layers.sh "${PYTHON_VERSION}"; then
  echo "ERROR: layer build failed" >&2
  exit 2
fi

VERSION=$(cd "${REPO_ROOT}" && uv version --short)

ARCH=$(arch)
ARCH_SUFFIX=""
[ "${ARCH}" = "aarch64" ] && ARCH_SUFFIX="-arm64"

ZIP_PATH="${SCRIPT_DIR}/lambda/dist/awswrangler-layer-${VERSION}-py${PYTHON_VERSION}${ARCH_SUFFIX}.zip"

if [ ! -f "${ZIP_PATH}" ]; then
  echo "ERROR: expected layer artifact not found at ${ZIP_PATH}" >&2
  exit 2
fi

ZIP_BYTES=$(stat -c '%s' "${ZIP_PATH}" 2>/dev/null || stat -f '%z' "${ZIP_PATH}")
ZIP_MB=$(( ZIP_BYTES / 1024 / 1024 ))

EXTRACT_DIR=$(mktemp -d)
trap 'rm -rf "${EXTRACT_DIR}"' EXIT
unzip -q "${ZIP_PATH}" -d "${EXTRACT_DIR}"

UNZIPPED_BYTES=$(du -sb "${EXTRACT_DIR}" | awk '{print $1}')
UNZIPPED_MB=$(( UNZIPPED_BYTES / 1024 / 1024 ))

echo
echo "==> Layer size report for py${PYTHON_VERSION}${ARCH_SUFFIX}"
echo "    zipped:    ${ZIP_MB} MB"
echo "    unzipped:  ${UNZIPPED_MB} MB"
echo "    threshold: ${THRESHOLD_MB} MB"
echo

if [ "${UNZIPPED_MB}" -le "${THRESHOLD_MB}" ]; then
  echo "PASS: unzipped layer size (${UNZIPPED_MB} MB) is within threshold (${THRESHOLD_MB} MB)"
  popd > /dev/null
  exit 0
fi

echo "FAIL: unzipped layer size (${UNZIPPED_MB} MB) exceeds threshold (${THRESHOLD_MB} MB)" >&2
echo
echo "Largest entries in the layer:" >&2
du -h "${EXTRACT_DIR}"/* "${EXTRACT_DIR}"/*/* 2>/dev/null | sort -h | tail -20 >&2
popd > /dev/null
exit 1
