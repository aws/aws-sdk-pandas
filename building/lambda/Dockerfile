ARG base_image

FROM ${base_image}

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

COPY pyproject.toml poetry.lock ./
RUN poetry config virtualenvs.create false --local && poetry install --no-root

RUN rm -f pyproject.toml poetry.lock

ENTRYPOINT ["/bin/sh"]
