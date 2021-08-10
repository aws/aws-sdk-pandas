ARG base_image
ARG py_dev

FROM ${base_image}

RUN yum install -y \
    boost-devel \
    jemalloc-devel \
    bison \
    flex \
    autoconf \
    ninja-build \
    ${py_dev}

RUN pip3 install --upgrade pip six cython cmake hypothesis poetry

WORKDIR /root

COPY pyproject.toml poetry.lock ./
RUN poetry config virtualenvs.create false --local && poetry install --no-root

RUN rm -f pyproject.toml poetry.lock

ENTRYPOINT ["/bin/sh"]