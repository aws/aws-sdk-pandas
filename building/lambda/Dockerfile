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
    gcc \
    gcc-c++ \
    flex \
    autoconf \
    zip \
    git \
    ninja-build

# Upgrade OpenSSL to avoid https://github.com/urllib3/urllib3/issues/2168
RUN yum update -y openssl

WORKDIR /root

FROM ${python_version}
COPY pyproject.toml poetry.lock ./

RUN pip3 install --upgrade pip wheel && pip3 install --upgrade six cython cmake hypothesis poetry
RUN poetry config virtualenvs.create false --local && poetry install --no-root --no-dev

RUN rm -f pyproject.toml poetry.lock

ENTRYPOINT ["/bin/sh"]
