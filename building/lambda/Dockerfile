ARG base_image
ARG python_version=base

FROM ${base_image} AS base

RUN yum install -y \
    boost-devel \
    jemalloc-devel \
    libxml2-devel \
    libxslt-devel \
    bison \
    make \
    gcc10 \
    gcc10-c++ \
    flex \
    autoconf \
    zip \
    git \
    ninja-build

WORKDIR /root

ENV CC=/usr/bin/gcc10-cc
ENV CXX=/usr/bin/gcc10-c++
ENV LD=/usr/bin/gcc10-gcc

RUN ln -s /usr/bin/gcc10-gcc /usr/bin/gcc
RUN ln -s /usr/bin/gcc10-g++ /usr/bin/g++
RUN ln -s /usr/bin/gcc10-nm /usr/bin/nm
RUN ln -s /usr/bin/gcc10-ar /usr/bin/ar
RUN ln -s /usr/bin/gcc10-mpn /usr/bin/mpn
RUN ln -s /usr/bin/gcc10-ld /usr/bin/ld

FROM ${python_version}
COPY pyproject.toml uv.lock ./

# Setuptools is a build dependency of arrow and runtime dependency of some of our dependencies (mainly redshift-connector).
# Remove when arrow version shipped with lambda layers and dependencies are updated.
RUN pip3 install --upgrade pip wheel setuptools>=78.1.1 setuptools_scm>=8
RUN pip3 install --upgrade urllib3==1.26.16  # temporary to avoid https://github.com/urllib3/urllib3/issues/2168 (TODO remove when the AL2 image updates to support OpenSSL 1.1.1+)
# In new CMake 4, compatibility with CMake < 3.5 has been removed. 
# Unpin CMake when arrow version shipped with lambda layers is updated.
RUN pip3 install --upgrade cmake==3.31.6
RUN pip3 install --upgrade six cython hypothesis uv

ENV PIP_NO_BINARY="numpy,pandas"
ENV UV_PROJECT_ENVIRONMENT="/var/lang"
RUN uv sync --frozen --inexact --no-dev --no-install-project \
    --no-binary-package numpy \
    --no-binary-package pandas

RUN rm -f pyproject.toml uv.lock

ENTRYPOINT ["/bin/sh"]
