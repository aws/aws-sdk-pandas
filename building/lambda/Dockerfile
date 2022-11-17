ARG base_image
ARG python_version=base

FROM ${base_image} AS base

RUN yum install -y \
    boost-devel \
    jemalloc-devel \
    bison \
    make \
    gcc \
    gcc-c++ \
    flex \
    autoconf \
    zip \
    git \
    ninja-build

RUN pip3 install --upgrade pip wheel && pip3 install --upgrade six cython cmake hypothesis poetry

WORKDIR /root

FROM base AS python37
# Python3.7 lambda image is based on AL1
# Install and configure gcc72 that supports c++17 standard, and ld.gold linker
RUN yum install -y  \
    gcc72 \
    gcc72-c++
# Set compiler version to use
ENV CC=gcc72
ENV CXX=g++72
# Replace ld with ld.gold
RUN rm /etc/alternatives/ld && ln -s /usr/bin/ld.gold /etc/alternatives/ld

FROM ${python_version}
COPY pyproject.toml poetry.lock ./
RUN poetry config virtualenvs.create false --local && poetry install --no-root

RUN rm -f pyproject.toml poetry.lock

ENTRYPOINT ["/bin/sh"]
