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
COPY pyproject.toml poetry.lock ./

RUN pip3 install --upgrade pip wheel setuptools>=78.1.1 setuptools_scm>=8
RUN pip3 install --upgrade urllib3==1.26.16  # temporary to avoid https://github.com/urllib3/urllib3/issues/2168 (TODO remove when the AL2 image updates to support OpenSSL 1.1.1+)
RUN pip3 install --upgrade six cython cmake hypothesis poetry
ENV PIP_NO_BINARY="numpy,pandas"
RUN poetry config virtualenvs.create false --local && poetry install --no-root --only main

RUN rm -f pyproject.toml poetry.lock

ENTRYPOINT ["/bin/sh"]
