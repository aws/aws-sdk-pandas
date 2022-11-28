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

FROM base AS python37
# Install C devtools & Python37
ARG PYTHON_VERSION="3.7.15"
RUN yum -y group install "C Development Tools and Libraries" "Development Tools" \
    && yum -y install openssl openssl-devel libffi-devel bzip2-devel \
    && curl -fsSL https://www.python.org/ftp/python/${PYTHON_VERSION}/Python-${PYTHON_VERSION}.tgz | tar xzf - \
    && cd Python-${PYTHON_VERSION}/ \
    && ./configure --enable-optimizations --prefix=/var/lang \
    && make altinstall \
    && ln -fs /var/lang/bin/python3.7 /var/lang/bin/python \
    && ln -fs /var/lang/bin/python3.7 /var/lang/bin/python3 \
    && ln -fs /var/lang/bin/pip3.7 /var/lang/bin/pip \
    && ln -fs /var/lang/bin/pip3.7 /var/lang/bin/pip3 \
    && rm -Rf ./Python-${PYTHON_VERSION}

FROM ${python_version}
COPY pyproject.toml poetry.lock ./

RUN pip3 install --upgrade pip wheel && pip3 install --upgrade six cython cmake hypothesis poetry
RUN poetry config virtualenvs.create false --local && poetry install --no-root

RUN rm -f pyproject.toml poetry.lock

ENTRYPOINT ["/bin/sh"]
