ARG base_image
ARG python_version=base

FROM ${base_image} AS base

RUN yum install -y \
    boost-devel \
    jemalloc-devel \
    bison \
    make \
    gcc72 \
    gcc72-c++ \
    flex \
    autoconf \
    zip \
    git \
    ninja-build

ENV CC=gcc72
ENV CXX=g++72

# Replace ld with ld.gold
RUN rm /etc/alternatives/ld && ln -s /usr/bin/ld.gold /etc/alternatives/ld

RUN pip3 install --upgrade pip wheel && pip3 install --upgrade six cython cmake hypothesis poetry

WORKDIR /root

FROM ${python_version}
COPY pyproject.toml poetry.lock ./
RUN poetry config virtualenvs.create false --local && poetry install --no-root

RUN rm -f pyproject.toml poetry.lock

ENTRYPOINT ["/bin/sh"]
