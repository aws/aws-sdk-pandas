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

WORKDIR /root

FROM ${python_version}
COPY pyproject.toml poetry.lock ./

RUN pip3 install --upgrade pip wheel && pip3 install --upgrade six cython cmake hypothesis poetry
RUN poetry config virtualenvs.create false --local && poetry install --extras "redshift mysql postgres gremlin opensearch openpyxl" --no-root

RUN rm -f pyproject.toml poetry.lock

ENTRYPOINT ["/bin/sh"]
