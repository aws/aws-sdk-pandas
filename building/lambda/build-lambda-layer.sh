#!/usr/bin/env bash
set -ex

FILENAME="awswrangler-layer-${1}.zip"
NINJA=${2}

pushd /aws-sdk-pandas
rm -rf python dist/pyarrow_files "dist/${FILENAME}" "${FILENAME}"
popd

rm -rf dist arrow

export ARROW_HOME=$(pwd)/dist
export ARROW_VERSION=24.0.0
export LD_LIBRARY_PATH=$(pwd)/dist/lib:$LD_LIBRARY_PATH
export CMAKE_PREFIX_PATH=$ARROW_HOME:$CMAKE_PREFIX_PATH
export SETUPTOOLS_SCM_PRETEND_VERSION=$ARROW_VERSION

git clone \
  --depth 1 \
  --branch "apache-arrow-${ARROW_VERSION}" \
  --single-branch \
  https://github.com/apache/arrow.git

mkdir $ARROW_HOME
mkdir arrow/cpp/build
pushd arrow/cpp/build

# -Wl,--as-needed drops DT_NEEDED entries for libraries whose symbols are never
# referenced. On AL2023, boost-devel pulls libicu into the linker's library
# path; without --as-needed, libparquet.so picks up libicudata/libicui18n/
# libicuuc as DT_NEEDED even though Arrow uses zero ICU symbols. Lambda's
# AL2023 runtime doesn't ship libicu, so the resulting layer fails to import.
cmake \
    -DCMAKE_INSTALL_PREFIX=$ARROW_HOME \
    -DCMAKE_INSTALL_LIBDIR=lib \
    -DCMAKE_SHARED_LINKER_FLAGS="-Wl,--as-needed" \
    -DCMAKE_EXE_LINKER_FLAGS="-Wl,--as-needed" \
    -DARROW_PYTHON=ON \
    -DARROW_PARQUET=ON \
    -DARROW_DATASET=ON \
    -DARROW_WITH_SNAPPY=ON \
    -DARROW_WITH_ZLIB=ON \
    -DARROW_FLIGHT=OFF \
    -DARROW_GANDIVA=OFF \
    -DARROW_ORC=OFF \
    -DARROW_CSV=ON \
    -DARROW_JSON=ON \
    -DARROW_COMPUTE=ON \
    -DARROW_FILESYSTEM=ON \
    -DARROW_PLASMA=OFF \
    -DARROW_WITH_BZ2=OFF \
    -DARROW_WITH_ZSTD=OFF \
    -DARROW_WITH_LZ4=OFF \
    -DARROW_WITH_BROTLI=OFF \
    -DARROW_BUILD_TESTS=OFF \
    -DARROW_WITH_ICU=OFF \
    -GNinja \
    ..

eval $NINJA
eval "${NINJA} install"

popd

pushd arrow/python

export CMAKE_PREFIX_PATH=${ARROW_HOME}${CMAKE_PREFIX_PATH:+:${CMAKE_PREFIX_PATH}}
export PYARROW_BUNDLE_ARROW_CPP=ON
export PYARROW_WITH_HDFS=0
export PYARROW_WITH_FLIGHT=0
export PYARROW_WITH_GANDIVA=0
export PYARROW_WITH_ORC=0
export PYARROW_WITH_CUDA=0
export PYARROW_WITH_PARQUET=1
export PYARROW_WITH_DATASET=1

# pyarrow 24+ dropped setup.py in favour of scikit-build-core via pyproject.toml.
# --no-isolation reuses build deps already installed in the image (scikit-build-core,
# cython>=3.1, libcst, setuptools_scm, numpy) instead of pip resolving them again.
python3 -m build --wheel . --no-isolation \
  -C cmake.build-type=Release

pip3 install dist/pyarrow-*.whl -t /aws-sdk-pandas/dist/pyarrow_files

PYARROW_WHEEL_DIR=$(pwd)/dist

popd

pushd /aws-sdk-pandas

# Building numpy/pandas from source on AL2023 without pinning the build-time
# numpy to the runtime numpy produces an ABI-mismatched pandas binary that
# SIGSEGVs on datetime64 construction (issue #3393). AL2023's glibc 2.34
# satisfies the manylinux_2_28 tag that PyPI ships numpy/pandas wheels under,
# so use those wheels. AL2 (glibc 2.26) is too old to load manylinux_2_28,
# so keep forcing source builds there.
NO_BINARY_FLAG=""
if [ -r /etc/os-release ] && grep -q '^VERSION_ID="2"' /etc/os-release; then
  NO_BINARY_FLAG="--no-binary numpy,pandas"
fi

pip3 install . ${NO_BINARY_FLAG} --find-links="${PYARROW_WHEEL_DIR}" -t ./python ".[redshift,mysql,postgres,gremlin,opensearch,openpyxl]" "pyarrow==${ARROW_VERSION}"

# CVE-2026-41066: upgrade lxml past redshift-connector's <=6.0.2 cap.
# pyproject.toml's [tool.uv] override-dependencies only applies to uv, not pip,
# so the Lambda layer needs this force-upgrade. Remove the capped install
# first — pip -t doesn't clean old dist-info, so scanners would see both versions.
rm -rf python/lxml python/lxml-*.dist-info
pip3 install --no-deps -t ./python "lxml>=6.1.0"

rm -rf python/pyarrow*
rm -rf python/boto*
rm -rf python/urllib3*
rm -rf python/s3transfer*

cp -r /aws-sdk-pandas/dist/pyarrow_files/pyarrow* python/

# Removing nonessential files
find python -name '*.so' -type f -exec strip "{}" \;
find python -wholename "*/tests/*" -type f -delete
find python -regex '^.*\(__pycache__\|\.py[co]\)$' -delete

# Bundle system shared libraries needed at runtime (e.g. libxslt for lxml,
# libatomic for pyarrow 22+). Lambda extracts layers to /opt/ and /opt/lib
# is on LD_LIBRARY_PATH. Search ldconfig cache first, then fall back to a
# filesystem search (libatomic under gcc10 lives in /usr/lib/gcc/*).
# ICU libraries are intentionally excluded: Arrow is built with -DARROW_WITH_ICU=OFF
# so pyarrow has no ICU dependency, keeping the layer size minimal.
mkdir -p lib
for libfile in libxslt.so.1 libexslt.so.0 libatomic.so.1; do
  src=$(ldconfig -p 2>/dev/null | awk -v lib="${libfile}" '$1 == lib { print $NF; exit }')
  if [ -z "${src}" ] || [ ! -e "${src}" ]; then
    src=$(find /usr/lib /usr/lib64 -name "${libfile}" -print -quit 2>/dev/null)
  fi
  if [ -n "${src}" ] && [ -e "${src}" ]; then
    cp -L "${src}" "lib/${libfile}"
    echo "bundled ${libfile} from ${src}"
  else
    echo "WARNING: ${libfile} not found on this image"
  fi
done

# Strip symbol tables and debug info to reduce binary size
find lib -name '*.so*' -type f -exec strip "{}" \;

zip -r9 "${FILENAME}" ./python ./lib
mv "${FILENAME}" dist/

rm -rf python lib dist/pyarrow_files "${FILENAME}"

popd

rm -rf dist arrow
