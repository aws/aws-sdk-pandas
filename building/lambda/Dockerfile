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

FROM base AS python36
# only for python3.6
RUN yum install -y python36-devel && \
    mkdir -p /var/lang/include/ && \
    ln -s /usr/include/python3.6m /var/lang/include/python3.6m

FROM ${python_version}
COPY pyproject.toml poetry.lock ./
RUN poetry config virtualenvs.create false --local && poetry install --no-root

RUN rm -f pyproject.toml poetry.lock

ENTRYPOINT ["/bin/sh"]
